#pragma once
#include "replica/election.hpp"
#include "replica/journal.hpp"
#include "service/server.hpp"

namespace fraud::replica {
class Cluster final : public service::Replication, public rpc::ReplicaPeer::Service {
  public:
    Cluster(Id shard, std::string id, std::map<std::string, std::string> members, std::string etcd,
            std::filesystem::path data, service::Options, int ttl, int tick_ms);
    void start();
    std::string metrics();
    ~Cluster();
    service::Node view;
    service::Mutation write(Id, Command, std::optional<ProducerSequence>, rpc::AckMode) override;
    std::unique_lock<std::mutex> read_guard(Id, std::string &) override;
    void health(rpc::ShardHealth &, bool) override;
    grpc::Status Exchange(grpc::ServerContext *, const rpc::PeerRequest *, rpc::PeerReply *) override;

  private:
    using Request = rpc::PeerRequest;
    using Reply = rpc::PeerReply;
    Id shard_;
    std::string id_, session_;
    int ttl_, tick_ms_;
    Journal journal_;
    Election election_;
    service::Node writer_;
    std::map<std::string, std::unique_ptr<rpc::ReplicaPeer::Stub>> peers_;
    std::mutex drive_, mu_;
    Lease lease_;
    bool ready_{};
    Id visible_{}, fetched_{}, observed_tail_{};
    service::Clock::time_point acquired_{};
    service::Histogram failover_;
    std::jthread heartbeat_, worker_;
    void follow();
    void publish(Id index);
    void handle(const Request &, Reply &);
    Reply exchange(const std::string &peer, Request);
    Request request(const Lease &, rpc::PeerOp);
    Boundary common(const std::string &, const Lease &, const Boundary &);
    void align(const std::string &, const Lease &, const Boundary &);
    void reconcile(const Lease &);
    bool replicate(const Lease &, bool all);
    void verify(const Lease &);
};
} // namespace fraud::replica
