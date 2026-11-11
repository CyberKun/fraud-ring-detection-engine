#pragma once
#include "service/node.hpp"
#include "fraud_engine.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <thread>

namespace fraud::service {
Time wall_ms();
class Replication {
public:
    virtual Mutation write(Id,Command,std::optional<ProducerSequence>,rpc::AckMode)=0;
    virtual std::unique_lock<std::mutex> read_guard(Id min_lsn,std::string& role)=0;
    virtual void health(rpc::ShardHealth&,bool checksum)=0;
    virtual ~Replication()=default;
};
class Api final : public rpc::FraudEngine::Service {
public:
    explicit Api(Node& node,Replication* group=nullptr):node_(node),group_(group){}
    grpc::Status UpsertEdge(grpc::ServerContext*,const rpc::UpsertEdgeRequest*,rpc::MutationResponse*) override;
    grpc::Status DeleteEdge(grpc::ServerContext*,const rpc::DeleteEdgeRequest*,rpc::MutationResponse*) override;
    grpc::Status UpdateVertex(grpc::ServerContext*,const rpc::UpdateVertexRequest*,rpc::MutationResponse*) override;
    grpc::Status Connected(grpc::ServerContext*,const rpc::ConnectedRequest*,rpc::ConnectedResponse*) override;
    grpc::Status ComponentStats(grpc::ServerContext*,const rpc::ComponentStatsRequest*,rpc::ComponentStatsResponse*) override;
    grpc::Status GetComponentSubgraph(grpc::ServerContext*,const rpc::SubgraphRequest*,rpc::SubgraphResponse*) override;
    grpc::Status SubscribeAlerts(grpc::ServerContext*,const rpc::SubscribeAlertsRequest*,grpc::ServerWriter<rpc::AlertEvent>*) override;
    grpc::Status AuditAt(grpc::ServerContext*,const rpc::AuditAtRequest*,rpc::AuditAtResponse*) override;
    grpc::Status GetClusterHealth(grpc::ServerContext*,const rpc::HealthRequest*,rpc::ClusterHealthResponse*) override;
private:
    Node& node_;
    Replication* group_;
};
class MetricsHttp {
public:
    MetricsHttp(Node&,std::uint16_t port,std::string address="127.0.0.1",std::function<std::string()> extra={});
    ~MetricsHttp();
    std::uint16_t port() const {return port_;}
private:
    int socket_;
    std::uint16_t port_;
    std::jthread thread_;
};
}
