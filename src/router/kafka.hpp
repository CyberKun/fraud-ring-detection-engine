#pragma once
#include "replica/cluster.hpp"
#include "router/directory.hpp"
#include <librdkafka/rdkafka.h>
namespace fraud::routing {
struct Input {
    Id tenant;
    ProducerSequence source;
    Command command;
};
Input unpack(const rpc::KafkaMutation &);
std::unique_ptr<rd_kafka_t, decltype(&rd_kafka_destroy)>
kafka_client(rd_kafka_type_t, const std::map<std::string, std::string> &, rd_kafka_conf_t * = nullptr);
class Ingestion {
  public:
    Ingestion(replica::Cluster &, Id, std::string etcd, std::string brokers, std::string topic,
              std::string group, rpc::AckMode, int commit_ms);
    std::string metrics() const;

  private:
    replica::Cluster &cluster_;
    Id shard_;
    Directory directory_;
    std::string brokers_, topic_, group_;
    rpc::AckMode mode_;
    int interval_;
    std::atomic<Id> duplicates_{}, pauses_{}, resumes_{}, commits_{}, assigned_{}, errors_{};
    void session(std::stop_token, Id, const std::vector<int> &);
    std::vector<int> partitions(const Topology &) const;
    std::jthread worker_;
};
} // namespace fraud::routing
