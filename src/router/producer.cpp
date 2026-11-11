#include "router/kafka.hpp"
#include <google/protobuf/util/json_util.h>
#include <iostream>
int main(int argc, char **argv) {
    if (argc != 4) {
        std::cerr << "usage: fraud_produce etcd-url brokers topic < mutations.jsonl\n";
        return 2;
    }
    try {
        fraud::routing::Directory directory(argv[1]);
        bool failed = false;
        auto *conf = rd_kafka_conf_new();
        rd_kafka_conf_set_opaque(conf, &failed);
        rd_kafka_conf_set_dr_msg_cb(conf, [](rd_kafka_t *, const rd_kafka_message_t *m, void *context) {
            if (m->err) {
                *static_cast<bool *>(context) = true;
                std::cerr << rd_kafka_err2str(m->err) << '\n';
            }
        });
        auto p = fraud::routing::kafka_client(
            RD_KAFKA_PRODUCER,
            {{"bootstrap.servers", argv[2]}, {"enable.idempotence", "true"}, {"message.timeout.ms", "10000"}},
            conf);
        std::string line;
        while (std::getline(std::cin, line)) {
            fraud::rpc::KafkaMutation msg;
            if (!google::protobuf::util::JsonStringToMessage(line, &msg).ok())
                throw std::invalid_argument("invalid mutation JSON");
            auto data = fraud::routing::unpack(msg);
            auto part = directory.get()->partition(data.tenant);
            auto bytes = msg.SerializeAsString();
            auto key = std::to_string(data.tenant);
            auto err = rd_kafka_producev(p.get(), RD_KAFKA_V_TOPIC(argv[3]), RD_KAFKA_V_PARTITION(part),
                                         RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                                         RD_KAFKA_V_VALUE(bytes.data(), bytes.size()),
                                         RD_KAFKA_V_KEY(key.data(), key.size()), RD_KAFKA_V_END);
            if (err)
                throw std::runtime_error(rd_kafka_err2str(err));
            rd_kafka_poll(p.get(), 0);
        }
        if (rd_kafka_flush(p.get(), 15000) || failed)
            return 1;
    } catch (const std::exception &e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}
