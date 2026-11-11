#include "service/server.hpp"
#include "service/commands.hpp"
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <poll.h>
#include <array>

namespace fraud::service {
Time wall_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}
namespace {
using Code=grpc::StatusCode;
template<class F> grpc::Status call(F f) {
    try { return f(); }
    catch(const std::overflow_error& e){return {Code::RESOURCE_EXHAUSTED,e.what()};}
    catch(const std::exception& e){return {Code::UNAVAILABLE,e.what()};}
}
template<class Request>
grpc::Status mutate(Node& node,Replication* group,const Request& req,Command cmd,rpc::MutationResponse& out) {
    return call([&] {
        std::optional<ProducerSequence> source;
        if(req.has_source()) source=ProducerSequence{req.source().producer_id(),req.source().sequence()};
        auto result=group?group->write(req.tenant_id(),cmd,source,req.mode(),req.ByteSizeLong()):node.mutate(req.tenant_id(),wall_ms(),cmd,source);
        if(result.status!=Status::ok && result.status!=Status::duplicate)
            return grpc::Status(Code::INVALID_ARGUMENT,"mutation rejected");
        out.set_epoch(result.position.epoch); out.set_applied_lsn(result.position.lsn);
        out.set_alert_triggered(result.alert); out.set_duplicate(result.status==Status::duplicate);
        return grpc::Status::OK;
    });
}
void stats(const Aggregate& a,Id lsn,rpc::ComponentStatsResponse& out) {
    out.set_vertex_count(a.vertex_count); out.set_account_count(a.account_count);
    out.set_volume(a.volume); out.set_flagged_accounts(a.flagged_accounts);
    out.set_flagged_ratio(a.flagged_ratio()); out.set_applied_lsn(lsn);
}
void graph(const Subgraph& g,Id lsn,rpc::SubgraphResponse& out) {
    out.set_applied_lsn(lsn); out.set_truncated(g.truncated); out.set_total_vertices(g.total_vertices);
    for(auto& v:g.vertices) {
        auto x=out.add_vertices(); x->set_vertex_id(v.id);
        x->set_entity_type(static_cast<rpc::EntityType>(entity(v.id)));
        x->set_volume(v.weight.volume); x->set_is_flagged(v.weight.flagged);
    }
    for(auto& e:g.edges) {
        auto x=out.add_edges(); x->set_u(e.key.u); x->set_v(e.key.v); x->set_edge_type(e.key.type);
        x->set_handle(e.handle); x->set_version(e.version); x->set_expiry_ms(e.expiry);
    }
}
grpc::Status missing(){return {Code::NOT_FOUND,"vertex absent at requested position"};}
template<class Response,class Query,class Populate>
grpc::Status read_at(grpc::ServerContext& ctx,Replication* group,Id minimum,Response& out,
                     Query query,Populate populate) {
    return call([&] {
        std::string role="leader";
        std::unique_lock<std::mutex> guard;
        if(group) guard=group->read_guard(minimum,role);
        auto answer=query();
        if(answer.lsn<minimum) return grpc::Status(Code::UNAVAILABLE,"below min_lsn");
        ctx.AddTrailingMetadata("applied-lsn",std::to_string(answer.lsn));
        ctx.AddTrailingMetadata("replica-role",role);
        out.set_replica_role(role);
        out.set_applied_lsn(answer.lsn);
        return populate(answer);
    });
}
}
grpc::Status Api::UpsertEdge(grpc::ServerContext*,const rpc::UpsertEdgeRequest* r,rpc::MutationResponse* out) {
    return mutate(node_,group_,*r,command(*r),*out);
}
grpc::Status Api::DeleteEdge(grpc::ServerContext*,const rpc::DeleteEdgeRequest* r,rpc::MutationResponse* out) {
    return mutate(node_,group_,*r,command(*r),*out);
}
grpc::Status Api::UpdateVertex(grpc::ServerContext*,const rpc::UpdateVertexRequest* r,rpc::MutationResponse* out) {
    return mutate(node_,group_,*r,command(*r),*out);
}
grpc::Status Api::Connected(grpc::ServerContext* ctx,const rpc::ConnectedRequest* r,rpc::ConnectedResponse* out) {
    return read_at(*ctx,group_,r->min_lsn(),*out,
        [&]{return node_.connected(r->tenant_id(),r->u(),r->v());},
        [&](const auto& a){out->set_connected(a.value);return grpc::Status::OK;});
}
grpc::Status Api::ComponentStats(grpc::ServerContext* ctx,const rpc::ComponentStatsRequest* r,rpc::ComponentStatsResponse* out) {
    return read_at(*ctx,group_,r->min_lsn(),*out,
        [&]{return node_.stats(r->tenant_id(),r->vertex_id());},
        [&](const auto& a){
            if(!a.value) return missing();
            stats(*a.value,a.lsn,*out);
            return grpc::Status::OK;
        });
}
grpc::Status Api::GetComponentSubgraph(grpc::ServerContext* ctx,const rpc::SubgraphRequest* r,rpc::SubgraphResponse* out) {
    return read_at(*ctx,group_,r->min_lsn(),*out,
        [&]{return node_.subgraph(r->tenant_id(),r->vertex_id(),r->max_nodes());},
        [&](const auto& a){
            if(!a.value) return missing();
            graph(*a.value,a.lsn,*out);
            return grpc::Status::OK;
        });
}
grpc::Status Api::AuditAt(grpc::ServerContext* ctx,const rpc::AuditAtRequest* r,rpc::AuditAtResponse* out) {
    return read_at(*ctx,group_,r->min_lsn(),*out,
        [&]{return node_.audit(r->tenant_id(),r->timestamp_ms(),r->vertex_id(),r->max_nodes());},
        [&](const auto& a){
            if(!a.value) return missing();
            stats(a.value->stats,a.lsn,*out->mutable_stats());
            graph(a.value->subgraph,a.lsn,*out->mutable_subgraph());
            if(a.value->first_alert_timestamp_ms) out->set_first_alert_timestamp_ms(*a.value->first_alert_timestamp_ms);
            return grpc::Status::OK;
        });
}
grpc::Status Api::SubscribeAlerts(grpc::ServerContext* ctx,const rpc::SubscribeAlertsRequest* r,
                                  grpc::ServerWriter<rpc::AlertEvent>* writer) {
    return call([&]{
        Id cursor=node_.subscribe();
        writer->SendInitialMetadata();
        while(!ctx->IsCancelled()) {
            for(auto& a:node_.poll(r->tenant_id(),cursor)) {
                rpc::AlertEvent out;
                out.set_tenant_id(a.tenant); out.set_vertex_id(a.vertex); out.set_epoch(a.epoch);
                out.set_lsn(a.lsn); out.set_event_time_ms(a.time); stats(a.stats,a.lsn,*out.mutable_stats());
                if(!writer->Write(out)) return grpc::Status::OK;
            }
        }
        return grpc::Status::OK;
    });
}
grpc::Status Api::GetClusterHealth(grpc::ServerContext*,const rpc::HealthRequest* req,rpc::ClusterHealthResponse* out) {
    return call([&]{auto p=node_.position(); auto x=out->add_shards();
        if(group_){group_->health(*x,req->checksum());return grpc::Status::OK;}
        x->set_shard_id(0); x->set_replica_role("leader"); x->set_epoch(p.epoch);
        x->set_applied_lsn(p.lsn); x->set_lag(0); x->set_ingestion_rate(node_.ingestion_rate());
        return grpc::Status::OK;});
}
MetricsHttp::MetricsHttp(Node& node,std::uint16_t port,std::string host,std::function<std::string()> extra):socket_(::socket(AF_INET,SOCK_STREAM|SOCK_CLOEXEC,0)) {
    if(socket_<0) throw storage::Error("metrics socket");
    sockaddr_in address{}; address.sin_family=AF_INET; address.sin_port=htons(port);
    if(inet_pton(AF_INET,host.c_str(),&address.sin_addr)!=1) {::close(socket_);throw storage::Error("invalid metrics IPv4 address");}
    int reuse=1; setsockopt(socket_,SOL_SOCKET,SO_REUSEADDR,&reuse,sizeof(reuse));
    socklen_t size=sizeof(address);
    if(bind(socket_,reinterpret_cast<sockaddr*>(&address),size) || listen(socket_,16) ||
       getsockname(socket_,reinterpret_cast<sockaddr*>(&address),&size)) {
        ::close(socket_); throw storage::Error("metrics listen failed");
    }
    port_=ntohs(address.sin_port);
    thread_=std::jthread([this,&node,extra=std::move(extra)](std::stop_token stop){
        while(!stop.stop_requested()) {
            pollfd p{socket_,POLLIN,0};
            if(::poll(&p,1,100)<=0) continue;
            int client=accept4(socket_,nullptr,nullptr,SOCK_CLOEXEC);
            if(client<0) continue;
            timeval timeout{1,0};
            setsockopt(client,SOL_SOCKET,SO_RCVTIMEO,&timeout,sizeof(timeout));
            setsockopt(client,SOL_SOCKET,SO_SNDTIMEO,&timeout,sizeof(timeout));
            std::string line;
            char buf[1024];
            while(line.size()<4096 && line.find("\r\n")==std::string::npos && !stop.stop_requested()) {
                auto n=recv(client,buf,sizeof(buf),0);
                if(n<=0) break;
                line.append(buf,n);
            }
            auto first=line.substr(0,line.find("\r\n"));
            bool ok=first=="GET /metrics HTTP/1.1" || first=="GET /metrics HTTP/1.0";
            std::string body=ok?node.metrics()+(extra?extra():""):"not found\n";
            std::string response=std::string("HTTP/1.1 ")+(ok?"200 OK":"404 Not Found")+
                "\r\nContent-Type: text/plain; version=0.0.4\r\nConnection: close\r\nContent-Length: "+
                std::to_string(body.size())+"\r\n\r\n"+body;
            std::string_view remaining=response;
            while(!remaining.empty()) {
                auto n=send(client,remaining.data(),remaining.size(),MSG_NOSIGNAL);
                if(n<=0) break;
                remaining.remove_prefix(n);
            }
            ::close(client);
        }
    });
}
MetricsHttp::~MetricsHttp(){thread_.request_stop(); thread_.join(); ::close(socket_);}
}
