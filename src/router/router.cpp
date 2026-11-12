#include "fraud_engine.grpc.pb.h"
#include "router/directory.hpp"
#include <atomic>
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <thread>

namespace fraud::routing {
using namespace std::chrono_literals;
using Code = grpc::StatusCode;
using Clock = std::chrono::system_clock;
class Router final : public rpc::FraudEngine::Service {
    using Stub = rpc::FraudEngine::Stub;
    Directory directory_;
    std::mutex mutex_;
    std::map<std::string, std::shared_ptr<Stub>> clients_;
    std::atomic<unsigned> cursor_{};

    std::shared_ptr<Stub> client(const std::string &address) {
        std::lock_guard lock(mutex_);
        auto &stub = clients_[address];
        if (!stub)
            stub =
                rpc::FraudEngine::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
        return stub;
    }
    std::optional<rpc::ShardHealth> health(const std::string &address, Clock::time_point deadline,
                                           bool checksum = false) {
        grpc::ClientContext ctx;
        ctx.set_deadline(std::min(deadline, Clock::now() + 500ms));
        rpc::ClusterHealthResponse response;
        rpc::HealthRequest req;
        req.set_checksum(checksum);
        if (!client(address)->GetClusterHealth(&ctx, req, &response).ok() || response.shards_size() != 1)
            return {};
        return response.shards(0);
    }
    template <class R, class O, class Method>
    grpc::Status write(grpc::ServerContext *ctx, const R *req, O *out, Method method) {
        try {
            auto deadline = std::min(ctx->deadline(), Clock::now() + 6s);
            auto map = directory_.get();
            grpc::Status status(Code::UNAVAILABLE, "no reconciled leader");
            for (int attempt = 0; attempt < 2; ++attempt) {
                auto shard = map->shard(req->tenant_id());
                auto leader = map->leaders.find(shard);
                if (leader != map->leaders.end()) {
                    grpc::ClientContext call;
                    call.set_deadline(deadline);
                    status = ((*client(leader->second.address)).*method)(&call, *req, out);
                    if (status.ok() || status.error_code() != Code::UNAVAILABLE)
                        return status;
                    const auto &error = status.error_message();
                    // An ambiguous reply can be retried only with a WAL deduplication key.
                    if (!req->has_source() && error.find("NOT_LEADER") == std::string::npos &&
                        error.find("failed to connect") == std::string::npos)
                        return status;
                }
                if (attempt)
                    break;
                do {
                    map = directory_.get(true);
                    auto id = map->shard(req->tenant_id());
                    if (auto it = map->leaders.find(id); it != map->leaders.end()) {
                        auto h = health(it->second.address, deadline);
                        if (h && h->shard_id() == id && h->replica_role() == "LEADER" &&
                            h->epoch() == it->second.epoch)
                            break;
                    }
                    std::this_thread::sleep_for(75ms);
                } while (!ctx->IsCancelled() && Clock::now() < deadline);
            }
            return status;
        } catch (const std::exception &e) {
            return {Code::UNAVAILABLE, e.what()};
        }
    }
    template <class R, class O, class Method>
    grpc::Status read(grpc::ServerContext *ctx, const R *req, O *out, Method method) {
        try {
            auto map = directory_.get();
            auto shard = map->shard(req->tenant_id());
            auto members = map->members.at(shard);
            std::rotate(members.begin(), members.begin() + (cursor_++ % members.size()), members.end());
            if (auto l = map->leaders.find(shard); l != map->leaders.end()) {
                std::erase(members, l->second.address);
                members.push_back(l->second.address);
            }
            for (auto &address : members) {
                auto h = health(address, ctx->deadline());
                if (!h || h->shard_id() != shard || h->applied_lsn() < req->min_lsn())
                    continue;
                grpc::ClientContext call;
                call.set_deadline(std::min(ctx->deadline(), Clock::now() + 5s));
                auto status = ((*client(address)).*method)(&call, *req, out);
                if (status.error_code() == Code::UNAVAILABLE)
                    continue;
                if (status.ok()) {
                    ctx->AddTrailingMetadata("applied-lsn", std::to_string(out->applied_lsn()));
                    ctx->AddTrailingMetadata("replica-role", out->replica_role());
                    ctx->AddTrailingMetadata("replica-address", address);
                }
                return status;
            }
            return {Code::UNAVAILABLE, "no eligible replica for read watermark"};
        } catch (const std::exception &e) {
            return {Code::UNAVAILABLE, e.what()};
        }
    }

  public:
    explicit Router(std::string url) : directory_(std::move(url)) {}
    grpc::Status UpsertEdge(grpc::ServerContext *ctx, const rpc::UpsertEdgeRequest *req,
                            rpc::MutationResponse *out) override {
        return write(ctx, req, out, &Stub::UpsertEdge);
    }
    grpc::Status DeleteEdge(grpc::ServerContext *ctx, const rpc::DeleteEdgeRequest *req,
                            rpc::MutationResponse *out) override {
        return write(ctx, req, out, &Stub::DeleteEdge);
    }
    grpc::Status UpdateVertex(grpc::ServerContext *ctx, const rpc::UpdateVertexRequest *req,
                              rpc::MutationResponse *out) override {
        return write(ctx, req, out, &Stub::UpdateVertex);
    }
    grpc::Status Connected(grpc::ServerContext *ctx, const rpc::ConnectedRequest *req,
                           rpc::ConnectedResponse *out) override {
        return read(ctx, req, out, &Stub::Connected);
    }
    grpc::Status ComponentStats(grpc::ServerContext *ctx, const rpc::ComponentStatsRequest *req,
                                rpc::ComponentStatsResponse *out) override {
        return read(ctx, req, out, &Stub::ComponentStats);
    }
    grpc::Status AuditAt(grpc::ServerContext *ctx, const rpc::AuditAtRequest *req,
                         rpc::AuditAtResponse *out) override {
        return read(ctx, req, out, &Stub::AuditAt);
    }
    grpc::Status GetComponentSubgraph(grpc::ServerContext *ctx, const rpc::SubgraphRequest *req,
                                      rpc::SubgraphResponse *out) override {
        return read(ctx, req, out, &Stub::GetComponentSubgraph);
    }
    grpc::Status DevStopLeader(grpc::ServerContext* ctx,const rpc::DevStopRequest* req,rpc::DevStopResponse* out) override {
        try {
            auto map=directory_.get(true);
            const auto& leader=map->leaders.at(req->shard_id());
            if(req->epoch()!=leader.epoch)return {Code::FAILED_PRECONDITION,"refresh shard health"};
            auto stub=rpc::ReplicaPeer::NewStub(grpc::CreateChannel(leader.address,grpc::InsecureChannelCredentials()));
            grpc::ClientContext call;call.set_deadline(ctx->deadline());
            return stub->DevStopLeader(&call,*req,out);
        }catch(const std::exception& e){return {Code::UNAVAILABLE,e.what()};}
    }
    grpc::Status GetClusterHealth(grpc::ServerContext *ctx, const rpc::HealthRequest *req,
                                  rpc::ClusterHealthResponse *out) override {
        try {
            auto map = directory_.get();
            for (auto &[shard, members] : map->members) {
                auto *state = out->add_shards();
                state->set_shard_id(shard);
                state->set_replica_role("UNAVAILABLE");
                auto elected=map->leaders.find(shard);
                std::vector<rpc::MemberHealth> observations;
                for(const auto& address:members){
                    auto& m=observations.emplace_back();
                    m.set_address(address);m.set_replica_role("UNAVAILABLE");
                    auto h=health(address,ctx->deadline(),req->checksum());
                    if(!h || h->shard_id()!=shard)continue;
                    m.set_replica_role(h->replica_role());m.set_epoch(h->epoch());m.set_applied_lsn(h->applied_lsn());
                    if(elected!=map->leaders.end() && elected->second.address==address &&
                       h->epoch()==elected->second.epoch && h->replica_role()=="LEADER") *state=*h;
                }
                for(auto& m:observations){
                    m.set_lag(state->applied_lsn()>m.applied_lsn()?state->applied_lsn()-m.applied_lsn():0);
                    *state->add_members()=m;
                }
            }
            return grpc::Status::OK;
        } catch (const std::exception &e) {
            return {Code::UNAVAILABLE, e.what()};
        }
    }
    grpc::Status SubscribeAlerts(grpc::ServerContext *ctx, const rpc::SubscribeAlertsRequest *req,
                                 grpc::ServerWriter<rpc::AlertEvent> *out) override {
        try {
            auto map = directory_.get();
            auto stub = client(map->leaders.at(map->shard(req->tenant_id())).address);
            grpc::ClientContext call;
            call.set_deadline(ctx->deadline());
            std::jthread watcher([&](std::stop_token stop) {
                while (!stop.stop_requested() && !ctx->IsCancelled())
                    std::this_thread::sleep_for(50ms);
                if (ctx->IsCancelled())
                    call.TryCancel();
            });
            auto stream = stub->SubscribeAlerts(&call, *req);
            stream->WaitForInitialMetadata();
            out->SendInitialMetadata();
            rpc::AlertEvent e;
            while (stream->Read(&e)) {
                if (!out->Write(e)) {
                    call.TryCancel();
                    break;
                }
            }
            return stream->Finish();
        } catch (const std::exception &e) {
            return {Code::UNAVAILABLE, e.what()};
        }
    }
};
} // namespace fraud::routing
int main(int argc, char **argv) {
    if (argc != 3) {
        std::cerr << "usage: fraud_router etcd-url listen-address\n";
        return 2;
    }
    fraud::routing::Router router(argv[1]);
    grpc::ServerBuilder b;
    b.AddListeningPort(argv[2], grpc::InsecureServerCredentials());
    b.RegisterService(&router);
    auto server = b.BuildAndStart();
    if (!server)
        return 1;
    server->Wait();
}
