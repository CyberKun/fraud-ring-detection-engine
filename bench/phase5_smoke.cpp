#include "service/server.hpp"
#include "../tests/temp_dir.hpp"
#include <curl/curl.h>
#include <sys/wait.h>
#include <unistd.h>
#include <csignal>
#include <cstdio>
#include <iostream>

namespace {
using namespace fraud;
using namespace fraud::service;
void require(bool ok,const std::string& msg){if(!ok) throw std::runtime_error(msg);}
struct Process {
    pid_t pid{-1}; int grpc_port{},metrics_port{};
    Process(const fs::path& binary,const fs::path& dir,const std::string& engine) {
        int output[2];
        if(pipe(output)) throw std::runtime_error("pipe");
        pid=::fork();
        if(pid==0) {
            ::close(output[0]); dup2(output[1],STDOUT_FILENO); ::close(output[1]);
            execl(binary.c_str(),binary.c_str(),"--data",dir.c_str(),"--listen","127.0.0.1:0",
                  "--metrics-port","0","--tick-ms","40","--snapshot-ms","500","--engine",engine.c_str(),nullptr);
            ::_exit(127);
        }
        ::close(output[1]);
        FILE* f=fdopen(output[0],"r"); char line[256]{};
        bool ready=pid>0 && fgets(line,sizeof(line),f) &&
                   std::sscanf(line,"grpc_port=%d metrics_port=%d",&grpc_port,&metrics_port)==2;
        fclose(f);
        if(!ready){stop(SIGKILL);throw std::runtime_error("server startup failed");}
    }
    void stop(int signal){if(pid>0){kill(pid,signal);int status; waitpid(pid,&status,0);pid=-1;}}
    ~Process(){stop(SIGTERM);}
    auto client() const {
        return rpc::FraudEngine::NewStub(grpc::CreateChannel("127.0.0.1:"+std::to_string(grpc_port),
                                                              grpc::InsecureChannelCredentials()));
    }
};
template<class Response,class Request,class Method>
Response unary(rpc::FraudEngine::Stub& stub,Method method,const Request& req) {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now()+std::chrono::seconds(10));
    Response out;
    auto status=(stub.*method)(&ctx,req,&out);
    require(status.ok(),"RPC: "+status.error_message()); return out;
}
using Stub=rpc::FraudEngine::Stub;
rpc::MutationResponse upsert(Stub& s,Id tenant,Id a,Id b,Time expiry) {
    rpc::UpsertEdgeRequest r; r.set_u(a);r.set_v(b);r.set_tenant_id(tenant);r.set_expiry_ms(expiry);
    return unary<rpc::MutationResponse>(s,&Stub::UpsertEdge,r);
}
void remove(Stub& s,Id tenant,Id a,Id b) {
    rpc::DeleteEdgeRequest r; r.set_u(a);r.set_v(b);r.set_tenant_id(tenant);
    unary<rpc::MutationResponse>(s,&Stub::DeleteEdge,r);
}
bool connected(Stub& s,Id tenant,Id a,Id b) {
    rpc::ConnectedRequest r; r.set_u(a);r.set_v(b);r.set_tenant_id(tenant);
    return unary<rpc::ConnectedResponse>(s,&Stub::Connected,r).connected();
}
std::string scrape(int port) {
    std::string text;
    auto c=curl_easy_init();
    require(c,"curl init");
    auto url="http://127.0.0.1:"+std::to_string(port)+"/metrics";
    curl_easy_setopt(c,CURLOPT_URL,url.c_str());curl_easy_setopt(c,CURLOPT_TIMEOUT,5L);
    curl_easy_setopt(c,CURLOPT_WRITEFUNCTION,+[](char* ptr,size_t n,size_t k,void* user)->size_t {
        static_cast<std::string*>(user)->append(ptr,n*k);return n*k;
    });
    curl_easy_setopt(c,CURLOPT_WRITEDATA,&text);
    auto result=curl_easy_perform(c);long code=0;curl_easy_getinfo(c,CURLINFO_RESPONSE_CODE,&code);
    curl_easy_cleanup(c);require(result==CURLE_OK && code==200,"HTTP scrape failed");return text;
}
double metric(const std::string& text,const std::string& name) {
    auto at=text.find("\n"+name+" ");
    require(at!=std::string::npos,"missing metric "+name);
    return std::stod(text.substr(at+2+name.size()));
}
void run(const fs::path& binary,const std::string& engine) {
    test::TempDir tmp;
    Process server(binary,tmp.path,engine);
    auto client=server.client();
    grpc::ClientContext stream_context;
    stream_context.set_deadline(std::chrono::system_clock::now()+std::chrono::seconds(20));
    rpc::SubscribeAlertsRequest subscribe;subscribe.set_tenant_id(7);
    auto stream=client->SubscribeAlerts(&stream_context,subscribe);
    stream->WaitForInitialMetadata();
    rpc::UpdateVertexRequest weight; weight.set_tenant_id(7);weight.set_is_flagged(true);weight.set_volume(10);
    for(Id v:{1,2}) {weight.set_vertex_id(v);unary<rpc::MutationResponse>(*client,&Stub::UpdateVertex,weight);}
    Time expiry=wall_ms()+120000;
    for(Id v:{2,3,4}) upsert(*client,7,1,v,expiry);
    auto ack=upsert(*client,7,1,5,expiry);
    require(ack.alert_triggered(),"missing alert response");
    rpc::AlertEvent event;
    require(stream->Read(&event) && event.stats().account_count()==5 && event.stats().flagged_accounts()==2 &&
            event.lsn()==ack.applied_lsn(),"missing streamed alert");
    stream_context.TryCancel();stream->Finish();
    rpc::ComponentStatsRequest stats;stats.set_tenant_id(7);stats.set_vertex_id(1);
    auto initial=unary<rpc::ComponentStatsResponse>(*client,&Stub::ComponentStats,stats);
    require(initial.flagged_ratio()==0.4 && initial.volume()==20,"bad stats");
    rpc::SubgraphRequest graph;graph.set_tenant_id(7);graph.set_vertex_id(1);graph.set_max_nodes(2);
    auto sub=unary<rpc::SubgraphResponse>(*client,&Stub::GetComponentSubgraph,graph);
    require(sub.truncated() && sub.total_vertices()==5 && sub.vertices_size()==2 && sub.edges_size()==1,"bad subgraph");
    while(wall_ms()==event.event_time_ms()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    upsert(*client,7,1,2,expiry+1000); remove(*client,7,1,5);
    rpc::AuditAtRequest audit;audit.set_tenant_id(7);audit.set_vertex_id(1);
    audit.set_timestamp_ms(event.event_time_ms());audit.set_max_nodes(5);
    auto historical=unary<rpc::AuditAtResponse>(*client,&Stub::AuditAt,audit);
    require(historical.subgraph().edges_size()==4 && historical.first_alert_timestamp_ms()==event.event_time_ms(),
            "historical threshold or graph");
    Id device=*encode(Entity::device,2);
    for(Id i=0;i<50;++i) upsert(*client,8,device,i,expiry);
    require(!connected(*client,8,device,0),"hub suppression");
    for(Id i=35;i<50;++i) remove(*client,8,device,i);
    require(connected(*client,8,device,0),"hub restoration");
    upsert(*client,9,1,2,wall_ms()+250);
    require(connected(*client,9,1,2),"short TTL");
    weight.set_vertex_id(1);weight.mutable_source()->set_producer_id(19);
    std::vector<double> latencies;
    auto start=Clock::now();
    constexpr int requests=500;
    for(int i=0;i<requests;++i) {
        weight.set_volume(i);weight.mutable_source()->set_sequence(i);
        auto before=Clock::now();
        ack=unary<rpc::MutationResponse>(*client,&Stub::UpdateVertex,weight);
        latencies.push_back(std::chrono::duration<double,std::milli>(Clock::now()-before).count());
    }
    double seconds=std::chrono::duration<double>(Clock::now()-start).count();
    std::sort(latencies.begin(),latencies.end());
    auto deadline=Clock::now()+std::chrono::seconds(10);
    std::string metrics;
    do {
        metrics=scrape(server.metrics_port);
        if(metric(metrics,"snapshot_duration_seconds_count")>0 && !connected(*client,9,1,2)) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(40));
    } while(Clock::now()<deadline);
    require(!connected(*client,9,1,2),"idle TTL expiry");
    for(auto op:{"insert","extend_ttl","expire","manual_delete","connected","component_stats","subgraph","audit"})
        require(metric(metrics,"fraud_operation_latency_seconds_count{op=\""+std::string(op)+"\"}")>0,op);
    for(auto name:{"wal_fsync_latency_seconds_count","wal_bytes_written_total","snapshot_duration_seconds_count",
                    "engine_memory_bytes","hub_transition_events_total"})
        require(metric(metrics,name)>0,name);
    require(metric(metrics,engine=="hdt"?"hdt_replacement_search_levels_traversed":"lct_repair_triggered_total")>0,"engine counters");
    rpc::HealthRequest hr;
    auto health=unary<rpc::ClusterHealthResponse>(*client,&Stub::GetClusterHealth,hr);
    require(health.shards_size()==1 && health.shards(0).applied_lsn()>=ack.applied_lsn() &&
            health.shards(0).replica_role()=="leader","cluster health");
    server.stop(SIGKILL);
    Process restarted(binary,tmp.path,engine);
    auto again=restarted.client();
    require(unary<rpc::MutationResponse>(*again,&Stub::UpdateVertex,weight).duplicate(),"dedup after crash");
    auto after=unary<rpc::ComponentStatsResponse>(*again,&Stub::ComponentStats,stats);
    require(after.volume()==509 && after.applied_lsn()>=ack.applied_lsn(),"lost acknowledged update");
    auto past=unary<rpc::AuditAtResponse>(*again,&Stub::AuditAt,audit);
    require(past.first_alert_timestamp_ms()==event.event_time_ms() && past.stats().vertex_count()==5,"retained audit");
    std::cout<<"PASS engine="<<engine<<" public_rpcs=9 metrics_http=200 crash_restart=pass sync_writes="<<requests
             <<" elapsed_s="<<seconds<<" writes_per_s="<<requests/seconds
             <<" p50_ms="<<latencies[249]<<" p99_ms="<<latencies[494]<<" wal_bytes="<<metric(metrics,"wal_bytes_written_total")<<'\n';
}
}
int main(int argc,char** argv) {
    try {alarm(90);run(std::filesystem::absolute(argv[0]).parent_path()/"fraud_server",argc==2?argv[1]:"hdt");return 0;}
    catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}
}
