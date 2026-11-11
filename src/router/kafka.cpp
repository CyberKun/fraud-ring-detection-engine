#include "router/kafka.hpp"
#include "service/commands.hpp"
#include <iostream>
namespace fraud::routing {
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;
namespace {
template <class R> Input input(const R &r) {
    if (!r.has_source())
        throw std::invalid_argument("Kafka mutation requires producer_id and sequence");
    return {r.tenant_id(), {r.source().producer_id(), r.source().sequence()}, service::command(r)};
}
using List =
    std::unique_ptr<rd_kafka_topic_partition_list_t, decltype(&rd_kafka_topic_partition_list_destroy)>;
List list() { return {rd_kafka_topic_partition_list_new(4), rd_kafka_topic_partition_list_destroy}; }
void check(rd_kafka_resp_err_t err) {
    if (err)
        throw std::runtime_error(rd_kafka_err2str(err));
}
void check_list(const List &parts) {
    for (int i = 0; i < parts->cnt; ++i)
        check(parts->elems[i].err);
}
} // namespace
Input unpack(const rpc::KafkaMutation &e) {
    switch (e.command_case()) {
    case rpc::KafkaMutation::kUpsert:
        return input(e.upsert());
    case rpc::KafkaMutation::kRemove:
        return input(e.remove());
    case rpc::KafkaMutation::kUpdate:
        return input(e.update());
    default:
        throw std::invalid_argument("Kafka mutation missing command");
    }
}
std::unique_ptr<rd_kafka_t, decltype(&rd_kafka_destroy)>
kafka_client(rd_kafka_type_t type, const std::map<std::string, std::string> &opts, rd_kafka_conf_t *config) {
    std::unique_ptr<rd_kafka_conf_t, decltype(&rd_kafka_conf_destroy)> conf(
        config ? config : rd_kafka_conf_new(), rd_kafka_conf_destroy);
    char error[512]{};
    for (auto &[key, value] : opts)
        if (rd_kafka_conf_set(conf.get(), key.c_str(), value.c_str(), error, sizeof(error)) !=
            RD_KAFKA_CONF_OK)
            throw std::runtime_error(error);
    auto *client = rd_kafka_new(type, conf.get(), error, sizeof(error));
    if (!client)
        throw std::runtime_error(error);
    conf.release();
    return {client, rd_kafka_destroy};
}
Ingestion::Ingestion(replica::Cluster &cluster, Id shard, std::string etcd, std::string brokers,
                     std::string topic, std::string group, rpc::AckMode mode, int interval)
    : cluster_(cluster), shard_(shard), directory_(etcd), brokers_(brokers), topic_(topic), group_(group),
      mode_(mode), interval_(interval), worker_([this](std::stop_token stop) {
          while (!stop.stop_requested()) {
              try {
                  auto epoch = cluster_.leader_epoch();
                  if (epoch) {
                      auto parts = partitions(*directory_.get());
                      if (!parts.empty() && cluster_.owns_epoch(epoch))
                          session(stop, epoch, parts);
                  }
              } catch (const std::exception &e) {
                  ++errors_;
                  std::cerr << "Kafka ingestion: " << e.what() << '\n';
              }
              assigned_ = 0;
              std::this_thread::sleep_for(100ms);
          }
      }) {}
std::vector<int> Ingestion::partitions(const Topology &map) const {
    std::vector<int> parts;
    for (std::size_t i = 0; i < map.partitions.size(); ++i)
        if (map.partitions[i] == shard_)
            parts.push_back(i);
    return parts;
}
void Ingestion::session(std::stop_token stop, Id epoch, const std::vector<int> &parts) {
    auto client = kafka_client(RD_KAFKA_CONSUMER, {{"bootstrap.servers", brokers_},
                                                   {"group.id", group_},
                                                   {"enable.auto.commit", "false"},
                                                   {"enable.auto.offset.store", "false"},
                                                   {"auto.offset.reset", "earliest"},
                                                   {"queued.max.messages.kbytes", "1024"},
                                                   {"fetch.wait.max.ms", "50"},
                                                   {"allow.auto.create.topics", "false"}});
    auto *c = client.get();
    check(rd_kafka_poll_set_consumer(c));
    auto assignments = list();
    for (int part : parts)
        rd_kafka_topic_partition_list_add(assignments.get(), topic_.c_str(), part)->offset =
            RD_KAFKA_OFFSET_STORED;
    check(rd_kafka_assign(c, assignments.get()));
    assigned_ = parts.size();
    std::map<int, int64_t> durable;
    auto next = Clock::now() + std::chrono::milliseconds(interval_);
    std::unique_ptr<rd_kafka_message_t, decltype(&rd_kafka_message_destroy)> msg(nullptr,
                                                                                 rd_kafka_message_destroy);
    bool paused = false;
    while (!stop.stop_requested() && cluster_.leader_epoch() == epoch) {
        auto map = directory_.get();
        if (partitions(*map) != parts)
            break;
        if (!durable.empty() && Clock::now() >= next) {
            if (!cluster_.owns_epoch(epoch))
                break;
            auto offsets = list();
            for (auto [part, offset] : durable)
                rd_kafka_topic_partition_list_add(offsets.get(), topic_.c_str(), part)->offset = offset;
            check(rd_kafka_commit(c, offsets.get(), 0));
            check_list(offsets);
            ++commits_;
            durable.clear();
            next = Clock::now() + std::chrono::milliseconds(interval_);
        }
        if (!msg)
            msg.reset(rd_kafka_consumer_poll(c, 50));
        if (!msg)
            continue;
        check(msg->err);
        rpc::KafkaMutation event;
        if (msg->len > storage::max_payload || !event.ParseFromArray(msg->payload, msg->len))
            throw std::invalid_argument("invalid Kafka payload at offset " + std::to_string(msg->offset));
        auto data = unpack(event);
        if (map->partition(data.tenant) != msg->partition)
            throw std::invalid_argument("tenant partition mismatch");
        try {
            auto result = cluster_.write(data.tenant, data.command, data.source, mode_, msg->len);
            if (result.status != Status::ok && result.status != Status::duplicate)
                throw std::invalid_argument("invalid Kafka mutation");
            if (result.position.epoch != epoch)
                break;
            if (result.status == Status::duplicate)
                ++duplicates_;
            durable[msg->partition] = msg->offset + 1;
            msg.reset();
            if (paused) {
                check(rd_kafka_resume_partitions(c, assignments.get()));
                check_list(assignments);
                paused = false;
                ++resumes_;
            }
        } catch (const std::overflow_error &) {
            if (!paused) {
                check(rd_kafka_pause_partitions(c, assignments.get()));
                check_list(assignments);
                paused = true;
                ++pauses_;
            }
            std::this_thread::sleep_for(50ms);
        }
    }
    rd_kafka_assign(c, nullptr);
}
std::string Ingestion::metrics() const {
    return "kafka_assigned_partitions " + std::to_string(assigned_) + "\nkafka_duplicate_records_total " +
           std::to_string(duplicates_) + "\nkafka_pause_total " + std::to_string(pauses_) +
           "\nkafka_resume_total " + std::to_string(resumes_) + "\nkafka_offset_commits_total " +
           std::to_string(commits_) + "\nkafka_ingestion_errors_total " + std::to_string(errors_) + "\n";
}
} // namespace fraud::routing
