#include "engines/brute_force.hpp"
#include "engines/hdt.hpp"
#include "engines/lct.hpp"
#include <chrono>
#include <cmath>
#include <iostream>
#include <fstream>
#include <numeric>
#include <random>
#include <stdexcept>
using namespace fraud;
using Clock = std::chrono::steady_clock;

struct Link { Id u,v; };
struct BatchDsu {
    std::vector<Id> parent,size;
    std::unordered_map<Handle,Link> live;
    bool dirty{};
    explicit BatchDsu(unsigned n):parent(n),size(n,1) { std::iota(parent.begin(),parent.end(),0); }
    Status set_vertex(Id,Weight) { return Status::ok; }
    Id root(Id u) { while(u!=parent[u]) u=parent[u]=parent[parent[u]]; return u; }
    void join(Id u,Id v) {
        u=root(u); v=root(v);
        if(u==v)return;
        if(size[u]<size[v])std::swap(u,v);
        parent[v]=u; size[u]+=size[v];
    }
    Status insert_edge(Handle h,Id u,Id v,Time) {
        live.emplace(h,Link{u,v}); if(!dirty)join(u,v); return Status::ok;
    }
    Status extend_ttl(Handle,Time) { return Status::ok; }
    Status delete_edge(Handle h,Removal) { live.erase(h); dirty=true; return Status::ok; }
    std::optional<Aggregate> stats(Id u) {
        if(dirty) {
            std::iota(parent.begin(),parent.end(),0);
            std::fill(size.begin(),size.end(),1);
            for(auto [_,e]:live)join(e.u,e.v);
            dirty=false;
        }
        return Aggregate{size[root(u)]};
    }
};
struct Op { char kind; unsigned index; };
struct Trace {
    std::vector<Link> edges;
    std::vector<Op> ops;
    std::vector<Id> vertices;
    Trace(unsigned n,std::string topology,unsigned cuts,unsigned rounds) {
        std::mt19937 rng(9173);
        vertices.resize(n);
        std::iota(vertices.begin(),vertices.end(),0);
        std::vector<unsigned> bridges,leaves;
        unsigned width=topology=="sweep"?250:n;
        for(unsigned base=0;base<n;base+=width) {
            unsigned limit=std::min(n,base+width),mid=(base+limit)/2;
            // Two sparse halves, one central bridge and a pendant expiry edge.
            for(auto [first,end]:{std::pair{base,mid},std::pair{mid,limit-1}}) {
                std::vector<unsigned> urn{first};
                for(unsigned v=first+1;v<end;++v) {
                    unsigned u=topology=="power-law"?urn[rng()%urn.size()]:first+rng()%(v-first);
                    edges.push_back({u,v}); urn.push_back(u); urn.push_back(v);
                }
                if(topology=="power-law")
                    vertices[first]=*encode(first==base?Entity::device:Entity::card,first);
                for(unsigned j=0;j<(end-first)/4;++j) {
                    unsigned u=first+rng()%(end-first),v=first+rng()%(end-first);
                    if(u!=v) edges.push_back({u,v});
                }
            }
            bridges.push_back(edges.size()); edges.push_back({base,mid});
            leaves.push_back(edges.size()); edges.push_back({mid,limit-1});
        }
        for(unsigned window=0;window<rounds;++window) {
            // Each 1000 mutations contains exactly cuts manual tree deletions.
            unsigned expiries=topology=="repeat90"?50:20;
            std::vector<char> actions(cuts,'d');
            actions.insert(actions.end(),expiries,'x');
            actions.resize(1000-cuts-expiries,'t');
            std::shuffle(actions.begin(),actions.end(),rng);
            for(char k:actions) {
                unsigned e=k=='d'?bridges[rng()%bridges.size()]:k=='x'?leaves[rng()%leaves.size()]:rng()%edges.size();
                ops.push_back({k,e});
                if(k!='t') ops.push_back({'i',e});
            }
        }
    }
};

template<class E> Id measure(const Trace& t,E& e,const std::string& label,const std::string& topology,unsigned cuts) {
    for(auto id:t.vertices)e.set_vertex(id,{});
    auto add=[&](unsigned h,Time time){
        auto [u,v]=t.edges[h];
        return e.insert_edge(h+1,t.vertices[u],t.vertices[v],time);
    };
    for(unsigned h=0;h<t.edges.size();++h)add(h,1);
    std::vector<double> samples; samples.reserve(t.ops.size()*2);
    Id checksum=0,step=0,manual=0;
    auto start=Clock::now();
    auto record=[&](Clock::time_point begin) {
        samples.push_back(std::chrono::duration<double,std::nano>(Clock::now()-begin).count());
    };
    for(auto [k,h]:t.ops) {
        auto begin=Clock::now();
        Status status;
        switch(k) {
        case 'i': status=add(h,step+2); break;
        case 't': status=e.extend_ttl(h+1,step+2); break;
        default: status=e.delete_edge(h+1,k=='d'?Removal::manual:Removal::expiry); manual+=k=='d';
        }
        record(begin);
        if(status!=Status::ok)throw std::runtime_error("mutation rejected");
        if(step++%16==0) {
            auto v=t.vertices[t.edges[h].v];
            // BatchDsu uses dense indices internally.
            if constexpr(std::is_same_v<E,BatchDsu>)v=t.edges[h].v;
            begin=Clock::now(); checksum+=e.stats(v).value().vertex_count; record(begin);
        }
    }
    double seconds=std::chrono::duration<double>(Clock::now()-start).count();
    if(const auto* raw=std::getenv("FRAUD_TIMINGS")) {
        std::ofstream out(raw,std::ios::app);
        for(std::size_t i=0;i<samples.size();++i)
            out<<topology<<','<<label<<','<<cuts<<','<<i<<','<<static_cast<long long>(samples[i])<<'\n';
        if(!out)throw std::runtime_error("write timing samples");
    }
    std::sort(samples.begin(),samples.end());
    auto q=[&](double p){return samples[std::ceil(p*samples.size())-1]/1000.;};
    Id repairs=0,scanned=0,candidates=0,promotions=0,levels=0,storage=0,slab=0,replacements=0;
    if constexpr(std::is_same_v<E,LctEngine>) {
        auto c=e.counters(); repairs=c.repairs; scanned=c.repair_vertices; candidates=c.candidates;
        replacements=c.promotions; storage=e.storage_bytes(); slab=e.slab_bytes();
    }
    if constexpr(std::is_same_v<E,HdtEngine>) {
        auto c=e.counters(); candidates=c.candidates; levels=c.levels_traversed;
        promotions=c.tree_promotions+c.nontree_promotions; replacements=c.replacements;
        storage=e.storage_bytes(); slab=e.slab_bytes();
    }
    std::cout<<topology<<','<<label<<','<<t.vertices.size()<<','<<t.edges.size()<<','<<cuts/10.<<','<<manual
        <<','<<t.ops.size()<<','<<samples.size()<<','<<samples.size()/seconds<<','<<q(.5)<<','<<q(.99)<<','<<q(.999)
        <<','<<repairs<<','<<scanned<<','<<candidates<<','<<levels<<','<<promotions<<','<<replacements<<','<<storage<<','<<slab<<','<<checksum<<'\n';
    return checksum;
}

int main(int argc,char** argv) {
    if(argc>1 && std::string(argv[1])=="sizes") {
        std::cout<<"lct_node,lct_edge,ett_node,ett_weights,hdt_edge\n"
            <<LctEngine::node_bytes()<<','<<LctEngine::edge_bytes()<<','<<EulerTourForest::node_bytes()
            <<','<<EulerTourForest::weight_bytes()<<','<<HdtEngine::edge_bytes()<<'\n';
        return 0;
    }
    std::cout<<"topology,engine,vertices,edges,manual_pct,manual_cuts,mutations,operations,ops_s,p50_us,p99_us,p999_us,repairs,repair_vertices,candidates,levels,promotions,replacements,index_bytes,slab_bytes,checksum\n";
    auto run=[&](std::string topology,unsigned n,unsigned cuts,unsigned rounds,bool bounded) {
        Trace t(n,topology,cuts,rounds);
        std::vector<Id> sums;
        if(bounded) {
            BruteForce bfs; sums.push_back(measure(t,bfs,"BFS",topology,cuts));
            auto dense=t; std::iota(dense.vertices.begin(),dense.vertices.end(),0);
            BatchDsu dsu(n); sums.push_back(measure(dense,dsu,"DSU",topology,cuts));
        }
        if(argc==1 || std::string(argv[1])=="LCT") {LctEngine b(n); sums.push_back(measure(t,b,"LCT",topology,cuts));}
        if(argc==1 || std::string(argv[1])=="HDT") {HdtEngine c(n); sums.push_back(measure(t,c,"HDT",topology,cuts));}
        if(!std::all_of(sums.begin(),sums.end(),[&](Id x){return x==sums.front();}))throw std::runtime_error("component checksum mismatch");
    };
    if(argc>1){run("sweep",50000,50,500,false);return 0;}
    for(auto name:{"sparse","power-law","bridge","repeat90"})
        run(name,1000,std::string(name)=="bridge"?100:0,20,true);
    for(unsigned pct:{0,1,5,10,20,50,100})run("sweep",50000,pct,40,false);
}

