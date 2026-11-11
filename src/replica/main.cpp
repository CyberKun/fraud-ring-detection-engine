#include "replica/cluster.hpp"
#include "router/kafka.hpp"
#include <iostream>
namespace {
std::string option(const char *name, std::string fallback) {
    auto value = std::getenv(name);
    return value ? value : fallback;
}
std::size_t limit(const char *name, std::size_t fallback) {
    auto s = option(name, std::to_string(fallback));
    std::size_t end{};
    auto n = std::stoull(s, &end);
    if (!n || end != s.size() || s[0] == '-')
        throw std::invalid_argument(name);
    return n;
}
} // namespace
int main(int argc, char **argv) {
    // Fixed membership: all replicas receive the same three ordered addresses.
    if (argc != 10 && argc != 11) {
        std::cerr << "usage: fraud_replica data shard member-index etcd-url ttl-sec tick-ms addr0 addr1 "
                     "addr2 [metrics-port]\n";
        return 2;
    }
    try {
        int index = std::stoi(argv[3]);
        std::map<std::string, std::string> members;
        for (int i = 0; i < 3; ++i)
            members.emplace(std::to_string(i), argv[7 + i]);
        auto id = std::to_string(index);
        fraud::replica::Cluster cluster(std::stoull(argv[2]), id, members, argv[4], argv[1], {},
                                        std::stoi(argv[5]), std::stoi(argv[6]),
                                        limit("FRAUD_CHECKPOINT_FRAMES", 4096));
        cluster.limits({limit("FRAUD_PENDING_REQUESTS", 256), limit("FRAUD_PENDING_BYTES", 1048576),
                        limit("FRAUD_REPLICATION_LAG", 16384)});
        std::unique_ptr<fraud::routing::Ingestion> ingestion;
        if (auto brokers = std::getenv("FRAUD_KAFKA_BROKERS")) {
            auto mode = option("FRAUD_KAFKA_ACK", "QUORUM");
            if (mode != "ASYNC" && mode != "QUORUM")
                throw std::invalid_argument("invalid Kafka ack mode");
            ingestion = std::make_unique<fraud::routing::Ingestion>(
                cluster, std::stoull(argv[2]), argv[4], brokers,
                option("FRAUD_KAFKA_TOPIC", "fraud-mutations"), option("FRAUD_KAFKA_GROUP", "fraud-ingest"),
                mode == "ASYNC" ? fraud::rpc::ASYNC : fraud::rpc::QUORUM,
                limit("FRAUD_OFFSET_COMMIT_MS", 250));
        }
        fraud::service::Api api(cluster.view, &cluster);
        int port = argc == 11 ? std::stoi(argv[10]) : 0;
        if (port < 0 || port > 65535)
            throw std::invalid_argument("invalid metrics port");
        fraud::service::MetricsHttp metrics(cluster.view, port, "0.0.0.0", [&] {
            return cluster.metrics() + (ingestion ? ingestion->metrics() : "");
        });
        std::cout << "replica=" << id << " metrics_port=" << metrics.port() << std::endl;
        grpc::ServerBuilder b;
        b.SetMaxReceiveMessageSize(fraud::storage::max_payload + 4096);
        b.AddListeningPort(members.at(id), grpc::InsecureServerCredentials());
        b.RegisterService(&api);
        b.RegisterService(&cluster);
        auto server = b.BuildAndStart();
        if (!server)
            throw std::runtime_error("listen failed");
        cluster.start();
        server->Wait();
    } catch (const std::exception &e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}
