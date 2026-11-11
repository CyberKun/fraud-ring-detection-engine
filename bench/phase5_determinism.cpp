#include "service/node.hpp"
#include "../tests/temp_dir.hpp"
#include <array>
#include <csignal>
#include <iostream>
#include <fstream>
#include <sys/wait.h>
#include <unistd.h>

namespace {
using namespace fraud;
using namespace fraud::service;
using namespace fraud::storage;
void require(bool ok,const char* message){if(!ok) throw Error(message);}
struct Fixture {
    Options options{{6,2,2,.25,30},256,false};
    std::array<Tenant<HdtEngine>,2> tenants{{{1,options.policy,HdtEngine(256)},{2,options.policy,HdtEngine(256)}}};
    std::array<Id,7> tags{};
    Id total{},duplicates{},sequence{},time{},steps{};
    static constexpr Id hub=*encode(Entity::device,1),card=*encode(Entity::card,2);
    void half(const fs::path& file,Id target) {
        Wal wal(file);
        Frame chunk;
        auto add=[&](Event e) {
            require(e.lsn==total+1,"fixture LSN");
            ++total; ++tags[e.payload.index()]; chunk.push_back(e);
        };
        while(total<target) {
            unsigned tenant=steps%2;
            auto i=steps/2,p=i%64;
            Id id=tenant+1;
            Command cmd=Tick{};
            std::optional<ProducerSequence> source;
            if(target-total>128) {
                ++steps; ++time;
                Id v=(i/64*3+p)%32;
                if(i<10) cmd=EdgeUpsert{{card,i,1},10000000};
                else if(p<12) cmd=EdgeUpsert{{hub,v,0},time+64};
                else if(p<18) cmd=EdgeUpsert{{hub,(i/64*3+p-12)%32,0},time+120};
                else if(p<26) cmd=EdgeDelete{{hub,(i/64*3+p-18)%32,0}};
                else if(p%4==0) cmd=EdgeUpsert{{v,(v+1)%32,2},time+200};
                else if(p%4==1 || p%4==2) cmd=VertexUpdate{v,{steps*13,p%3==0}};
                source=ProducerSequence{3,++sequence};
            }
            auto b=tenants[tenant].submit(1,total+1,time,cmd,source);
            require(b.status==Status::ok && !b.records.empty(),"fixture command");
            for(auto& e:b.records) add(e);
            if(source && steps%31==0 && total<target) {
                Event duplicate{id,1,total+1,time,source,VertexUpdate{0,{UINT64_MAX,true}}};
                std::vector<Alert> unused;
                require(tenants[tenant].apply_record(duplicate,unused)==Status::duplicate,"fixture duplicate");
                add(duplicate); ++duplicates;
            }
            if(chunk.size()>=1024 || total==target) {
                wal.append(std::span(&chunk,1));
                chunk.clear();
            }
        }
    }
};
void append_file(const fs::path& destination,const fs::path& source) {
    auto data=read_file(source,128*1024*1024);
    std::ofstream out(destination,std::ios::binary|std::ios::app);
    out.write(reinterpret_cast<const char*>(data.data()),data.size());
    out.close();
    require(bool(out),"fixture copy failed");
}
bool same_graph(const Subgraph& a,const Subgraph& b) {
    if(a.total_vertices!=b.total_vertices || a.truncated!=b.truncated ||
       a.vertices.size()!=b.vertices.size() || a.edges.size()!=b.edges.size()) return false;
    for(std::size_t i=0;i<a.vertices.size();++i) {
        auto x=a.vertices[i],y=b.vertices[i];
        if(x.id!=y.id || x.weight.flagged!=y.weight.flagged || x.weight.volume!=y.weight.volume) return false;
    }
    for(std::size_t i=0;i<a.edges.size();++i) {
        auto x=a.edges[i],y=b.edges[i];
        if(x.key!=y.key || x.handle!=y.handle || x.expiry!=y.expiry || x.version!=y.version || x.shadow!=y.shadow) return false;
    }
    return true;
}
void gate() {
    auto start=Clock::now();
    test::TempDir tmp;
    Fixture f;
    auto first=tmp.path/"first",second=tmp.path/"second";
    f.half(first,250000); f.half(second,500000);
    for(auto count:f.tags) require(count>0,"missing event type");
    auto a=tmp.path/"a",b=tmp.path/"b",c=tmp.path/"c";
    for(auto dir:{a,b,c}) {
        fs::create_directory(dir);
        fs::copy_file(first,dir/"active.wal");
        if(dir!=c) append_file(dir/"active.wal",second);
    }
    // The child dies without stack unwinding or graceful shutdown after the
    // midpoint snapshot and archive have reached stable storage.
    auto pid=::fork();
    if(pid<0) throw Error("fork");
    if(pid==0) {
        try {
            Node midpoint(c,f.options);
            require(midpoint.position().lsn==250000,"midpoint boundary");
            midpoint.snapshot();
            ::kill(::getpid(),SIGKILL);
        } catch (...) {}
        ::_exit(2);
    }
    int status=0;
    require(::waitpid(pid,&status,0)==pid && WIFSIGNALED(status) && WTERMSIG(status)==SIGKILL,"midpoint crash");
    append_file(c/"active.wal",second);
    Node one(a,f.options),two(b,f.options),restored(c,f.options);
    require(one.replayed()==500000 && two.replayed()==500000 && restored.replayed()==250000,"replay counts");
    auto hash=logical_checksum(one.image());
    for(Node* n:{&one,&two,&restored}) {
        require(n->position().lsn==500000 && logical_checksum(n->image())==hash,"logical checksum");
        n->snapshot();
    }
    auto bytes=read_file(a/"snapshot.bin",snapshot_cap);
    require(bytes==read_file(b/"snapshot.bin",snapshot_cap) && bytes==read_file(c/"snapshot.bin",snapshot_cap),"snapshot bytes");
    Id queries=0;
    for(Id tenant:{1,2}) {
        std::vector<Id> ids{Fixture::hub,Fixture::card};
        for(Id v=0;v<32;++v) ids.push_back(v);
        for(Id v:ids) {
            auto s=one.stats(tenant,v).value;
            require(s==two.stats(tenant,v).value && s==restored.stats(tenant,v).value &&
                    s==f.tenants[tenant-1].engine().stats(v),"aggregate"); ++queries;
            for(Id u:ids) {
                auto x=one.connected(tenant,v,u).value;
                require(x==two.connected(tenant,v,u).value && x==restored.connected(tenant,v,u).value,"connected");
                ++queries;
            }
            for(auto k:{0u,2u,100u}) {
                auto x=one.subgraph(tenant,v,k).value,y=two.subgraph(tenant,v,k).value,z=restored.subgraph(tenant,v,k).value;
                require(x.has_value()==y.has_value() && x.has_value()==z.has_value() &&
                        (!x || (same_graph(*x,*y) && same_graph(*x,*z))),"subgraph"); ++queries;
            }
        }
        auto x=one.audit(tenant,f.time/2,Fixture::card,8).value;
        for(Node* n:{&two,&restored}) {
            auto y=n->audit(tenant,f.time/2,Fixture::card,8).value;
            require(x && y && x->stats==y->stats && x->first_alert_timestamp_ms==y->first_alert_timestamp_ms &&
                    same_graph(x->subgraph,y->subgraph),"historical query");
        }
        ++queries;
    }
    for(Node* n:{&one,&two,&restored}) {
        require(n->mutate(1,0,Tick{},ProducerSequence{3,1}).status==Status::duplicate,"restored dedup");
        n->tick(10000001); // Drain both reconstructed and uninterrupted timing wheels.
    }
    require(serialize(one.image())==serialize(restored.image()) && serialize(one.image())==serialize(two.image()),"continued state");
    std::cout<<"PASS records=500000 fresh_replays=2 midpoint_lsn=250000 restart_tail=250000 duplicates="<<f.duplicates
             <<" query_comparisons="<<queries<<" snapshot_bytes="<<bytes.size()<<" sha256="<<hash
             <<" elapsed_s="<<std::chrono::duration<double>(Clock::now()-start).count()<<"\n"
             <<"upsert="<<f.tags[0]<<" delete="<<f.tags[1]<<" update="<<f.tags[2]<<" tick="<<f.tags[3]
             <<" expire="<<f.tags[4]<<" suppress="<<f.tags[5]<<" restore="<<f.tags[6]<<'\n';
}
}
int main(){try{gate();return 0;}catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}}
