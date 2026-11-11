#include "compare_subgraphs.hpp"
#include "shrink.hpp"
#include "engines/brute_force.hpp"
#include "engines/lct.hpp"
#include "engines/hdt.hpp"
#include "engines/offline.hpp"
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <sstream>
#include <string>

namespace {
using namespace fraud;
constexpr unsigned phase_number=FRAUD_FUZZ_PHASE;
using Step = std::uint32_t;
using Trace = std::vector<Step>;
struct Mode { unsigned tier, vertices; Step mutations; Id seed; };
struct Failure { std::string kind; Step step; };
using Outcome = std::optional<Failure>;

Id mix(Id x) {
    x = (x ^ (x>>30))*0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x>>27))*0x94d049bb133111ebULL;
    return x ^ (x>>31);
}
Id vertex(Id i) {
    Entity type = i%16==15 ? Entity::device : i%16==14 ? Entity::card : Entity::account;
    return *encode(type,i);
}
struct Input { Command command; Id u,v; };
Input adversarial(Mode m,Step step) {
    unsigned p=step%64;
    Id base=16*(mix(step/64+m.seed)%(m.vertices/16));
    auto edge=[&](unsigned a,unsigned b,bool remove,unsigned type,Time expiry) {
        Id u=vertex(base+a),v=vertex(base+b);
        Key k{u,v,type};
        Command c=remove ? Command{EdgeDelete{k}} : Command{EdgeUpsert{k,expiry}};
        return Input{c,u,v};
    };
    Time end=step-p+63;
    if ((step/64)%2==0) {
        // Build a path and repeatedly bisect the promoted smaller half.
        if (p<15) return edge(p,p+1,false,7,end);
        if (p==15) return edge(0,2,false,8,end);
        if (p<20) {
            unsigned cut=(16u>>(p-15))-1;
            return edge(cut,cut+1,true,7,end);
        }
        unsigned u=p%14;
        return edge(u,u+1,p%3==0,7,step+16);
    }
    // Two hubs share a shadow edge. Restore them at different times while
    // renewing some of their incident keys and expiring others.
    if (!p) return edge(14,15,false,9,end);
    unsigned hub=p%2 ? 14 : 15;
    unsigned leaf=((p-1)/2)%12;
    return edge(hub,leaf,p>=25 && p<49,9,step+32);
}
Input generate(Mode m,Step step) {
    Id t=step, bits=mix(t+m.seed);
    if (t<m.vertices) {
        Id v=vertex(t);
        return {VertexUpdate{v,{bits%10000,t%5==0}},v,v};
    }
    if (phase_number==4 && ((m.tier==1 && t>=100000) || (m.tier==2 && (t/256)%8==7)))
        return adversarial(m,step);
    unsigned phase=t%64;
    Id base=16*(mix(t/64+m.seed)%(m.vertices/16));
    Id u=vertex(base+bits%12), v=vertex(base+(bits>>8)%12);
    Time expiry=t-t%64+192;
    if (phase<12) {
        u=vertex(base+phase);
        v=vertex(base+(phase+1)%12);
    } else if (phase<24) {
        u=vertex(base+phase%12);
        v=vertex(base+(phase+5)%12);
    } else if (phase<30) {
        u=vertex(base+phase%12);
        v=vertex(base+(phase+1)%12);
        if (phase%2) return {EdgeDelete{{u,v,0}},u,v};
        expiry+=64;  // Extend an existing tree or non-tree edge.
    } else if (phase<54) {
        v=vertex(base+15);
        u=vertex(base+(phase-30)%12);
        if (phase>=42) return {EdgeDelete{{v,u,0}},u,v};
    } else {
        u=vertex(bits%m.vertices);
        v=vertex((bits>>21)%m.vertices);
        expiry=t+1+(bits>>32)%m.vertices;
        if (phase%3==0) return {VertexUpdate{u,{bits%99999,(bits&1)!=0}},u,v};
        if (phase==62) return {EdgeDelete{{u,v,0}},u,v};
    }
    return {EdgeUpsert{{u,v,0},expiry},u,v};
}

template<class E>
std::optional<AuditResult> observe(const Tenant<E>& tenant,Id u,Id peer) {
    auto stats=tenant.engine().stats(u);
    if (!stats) return {};
    return AuditResult{*stats,*tenant.subgraph(u,stats->vertex_count),{},
                       tenant.engine().connected(u,peer)};
}
bool same(std::optional<AuditResult>& a,std::optional<AuditResult>& b) {
    if (!a || !b) return !a && !b;
    return a->stats==b->stats && a->connected_to_peer==b->connected_to_peer &&
           test::equal(a->subgraph,b->subgraph);
}
bool same_alerts(const std::vector<Alert>& a,const std::vector<Alert>& b) {
    if (a.size()!=b.size()) return false;
    for (std::size_t i=0;i<a.size();++i)
        if (a[i].vertex!=b[i].vertex || a[i].stats!=b[i].stats ||
            a[i].lsn!=b[i].lsn || a[i].time!=b[i].time) return false;
    return true;
}

// Independently reconstruct the unique maximum spanning forest by Kruskal.
bool check_msf(const Tenant<LctEngine>& tenant,const std::unordered_set<Key,KeyHash>& keys,unsigned n) {
    std::vector<EdgeRecord> edges;
    for (auto k : keys) {
        auto e=tenant.edge(k);
        if (e && !e->shadow) edges.push_back(*e);
    }
    std::sort(edges.begin(),edges.end(),[](auto& a,auto& b) {
        return std::pair(a.expiry,a.handle)>std::pair(b.expiry,b.handle);
    });
    std::vector<unsigned> parents(n);
    std::iota(parents.begin(),parents.end(),0u);
    auto root=[&](Id id) {
        unsigned i=id&id_mask;
        while (parents[i]!=i) i=parents[i]=parents[parents[i]];
        return i;
    };
    std::size_t count=0;
    for (const auto& e : edges) {
        auto a=root(e.key.u), b=root(e.key.v);
        if ((a!=b)!=tenant.engine().tree_edge(e.handle)) return false;
        if (a!=b) { parents[a]=b; ++count; }
    }
    return count==tenant.engine().dummy_count();
}

struct Window {
    OfflineReplay replay;
    std::vector<Event> records;
    std::vector<AuditQuery> queries;
    std::vector<std::optional<AuditResult>> expected;
    Trace steps;
    explicit Window(Policy p) : replay(1,p) {}
    void probe(Time time,Step step,Id u,Id v,unsigned n,std::optional<AuditResult> result) {
        queries.push_back({time,u,n,v});
        steps.push_back(step);
        expected.push_back(std::move(result));
    }
    Outcome compare(Step step) {
        if (records.empty()) return {};
        auto index=replay.next_window(records,records.back().time+1);
        if (!index) return Failure{"offline-window",step};
        auto answers=index->audit_many(queries);
        for (std::size_t i=0;i<answers.size();++i)
            if (!same(answers[i],expected[i])) return Failure{"offline-answer",steps[i]};
        records.clear();
        queries.clear();
        expected.clear();
        steps.clear();
        return {};
    }
};

Outcome run(Mode m,const Trace& trace,bool report) {
    bool exact=m.tier==1;
    Policy policy{8,3,3,0.4,100};
    Tenant<LctEngine> leader(1,policy,LctEngine(m.vertices));
    std::optional<Tenant<LctEngine>> follower;
    std::optional<Tenant<BruteForce>> oracle;
    std::optional<Tenant<HdtEngine>> hdt;
    if (phase_number==4) hdt.emplace(1,policy,HdtEngine(m.vertices));
    if (exact) {
        follower.emplace(1,policy,LctEngine(m.vertices));
        oracle.emplace(1,policy);
    }
    Window window(policy);
    std::unordered_set<Key,KeyHash> known;
    std::unordered_map<Handle,Key> handles;
    Id lsn=1, audits=0, suppress=0, restore=0, expire=0, forest_checks=0;
    auto start=std::chrono::steady_clock::now();
    for (Step step : trace) {
        auto input=generate(m,step);
        auto batch=leader.submit(1,lsn,step,input.command);
        if (batch.status!=Status::ok) return Failure{"submit",step};
        lsn+=batch.records.size();
        std::vector<Alert> emitted,hdt_emitted;
        for (auto e : batch.records) {
            Id u=input.u,v=input.v;
            suppress+=std::holds_alternative<HubSuppress>(e.payload);
            restore+=std::holds_alternative<HubRestore>(e.payload);
            expire+=std::holds_alternative<Expire>(e.payload);
            if (hdt && hdt->apply_record(e,hdt_emitted)!=Status::ok) return Failure{"hdt-replay",step};
            if (exact) {
                std::vector<Alert> a,b;
                if (follower->apply_record(e,a)!=Status::ok || oracle->apply_record(e,b)!=Status::ok)
                    return Failure{"replay",step};
                if (!same_alerts(a,b)) return Failure{"alerts",step};
                emitted.insert(emitted.end(),a.begin(),a.end());
                if (auto x=std::get_if<EdgeUpsert>(&e.payload)) {
                    known.insert(x->key.normalized());
                    auto edge=oracle->edge(x->key);
                    handles[edge->handle]=edge->key;
                } else if (auto x=std::get_if<Expire>(&e.payload)) {
                    auto key=handles.at(x->handle);
                    u=key.u; v=key.v;
                } else if (auto x=std::get_if<HubSuppress>(&e.payload)) u=x->vertex;
                else if (auto x=std::get_if<HubRestore>(&e.payload)) u=x->vertex;
                // Use the ordinal as the offline coordinate to inspect even
                // intermediate records at the same physical event time.
                e.time=e.lsn;
                auto live=observe(*follower,u,v), bfs=observe(*oracle,u,v);
                if (!same(live,bfs)) return Failure{"bfs-answer",step};
                if (hdt) {
                    auto result=observe(*hdt,u,v);
                    if (!same(result,bfs)) return Failure{"hdt-bfs-answer",step};
                    if (!hdt->engine().validate()) return Failure{"hdt-invariants",step};
                }
                window.probe(e.time,step,u,v,m.vertices,std::move(bfs));
                ++audits;
            }
            window.records.push_back(e);
        }
        if (exact && !same_alerts(emitted,batch.alerts)) return Failure{"leader-alerts",step};
        if (hdt && !same_alerts(hdt_emitted,batch.alerts)) return Failure{"hdt-alerts",step};
        if (hdt && !exact) {
            auto b=observe(leader,input.u,input.v), c=observe(*hdt,input.u,input.v);
            if (!same(b,c)) return Failure{"hdt-lct-answer",step};
            if ((step%250000==0 || step==trace.back()) && !hdt->engine().validate())
                return Failure{"hdt-invariants",step};
        }
        if (exact && (step%512==0 || step==trace.back())) {
            if (!check_msf(leader,known,m.vertices)) return Failure{"kruskal-msf",step};
            ++forest_checks;
        }
        if (!exact && (step%2048==0 || step==trace.back())) {
            window.probe(step,step,input.u,input.v,m.vertices,observe(leader,input.u,input.v));
            ++audits;
        }
        if (window.records.size()>=(exact ? 2048u : 250000u))
            if (auto failure=window.compare(step)) return failure;
    }
    if (!trace.empty())
        if (auto failure=window.compare(trace.back())) return failure;
    if (report) {
        const auto& c=leader.engine().counters();
        double elapsed=std::chrono::duration<double>(std::chrono::steady_clock::now()-start).count();
        std::cout<<"PASS tier="<<m.tier<<" seed="<<m.seed<<" vertices="<<m.vertices
                 <<" mutations="<<trace.size()<<" records="<<lsn-1<<" audits="<<audits
                 <<" mismatches=0 elapsed_s="<<std::fixed<<std::setprecision(3)<<elapsed<<'\n'
                 <<"expire="<<expire<<" suppress="<<suppress<<" restore="<<restore
                 <<" cycle_swaps="<<c.swaps<<" repairs="<<c.repairs<<" repair_vertices="<<c.repair_vertices
                 <<" repair_promotions="<<c.promotions<<" peak_dummies="<<c.peak_dummies
                 <<" kruskal_checks="<<forest_checks<<std::endl;
        if (hdt) {
            const auto& h=hdt->engine().counters();
            std::cout<<"hdt_online_checks="<<(exact ? lsn-1 : trace.size())
                     <<" searches="<<h.searches<<" levels_traversed="<<h.levels_traversed
                     <<" candidates_scanned="<<h.candidates<<" tree_promotions="<<h.tree_promotions
                     <<" nontree_promotions="<<h.nontree_promotions<<" replacements="<<h.replacements
                     <<" lower_level_replacements="<<h.lower_level_replacements
                     <<" highest_level="<<h.highest_level<<std::endl;
        }
    }
    return {};
}
void dump(std::ostream& out,Mode m,const Trace& trace) {
    out<<m.tier<<' '<<m.seed<<'\n';
    for (Step step : trace) {
        auto input=generate(m,step);
        out<<step<<' ';
        std::visit([&](auto x) {
            using T=decltype(x);
            if constexpr (std::same_as<T,VertexUpdate>)
                out<<"weight "<<x.vertex<<' '<<x.weight.volume<<' '<<x.weight.flagged;
            else if constexpr (std::same_as<T,EdgeUpsert>)
                out<<"upsert "<<x.key.u<<' '<<x.key.v<<' '<<x.key.type<<' '<<x.expiry;
            else if constexpr (std::same_as<T,EdgeDelete>)
                out<<"delete "<<x.key.u<<' '<<x.key.v<<' '<<x.key.type;
        },input.command);
        out<<'\n';
    }
}
} // namespace

int main(int argc,char** argv) {
    unsigned tier=0;
    Id seed=20261111;
    Trace trace;
    bool replay=argc==3 && std::string_view(argv[1])=="replay";
    if (replay) {
        std::ifstream in(argv[2]);
        if (!(in>>tier>>seed)) return 2;
        Step step;
        std::string rest;
        while (in>>step) {
            trace.push_back(step);
            std::getline(in,rest);
        }
    } else if (argc>=2 && argc<=3) {
        std::string_view arg(argv[1]);
        if (arg=="tier1") tier=1;
        if (arg=="tier2") tier=2;
        if (argc==3) {
            std::istringstream number(argv[2]);
            if (!(number>>seed) || !number.eof()) return 2;
        }
    }
    if (tier!=1 && tier!=2) {
        std::cerr<<"usage: phase"<<phase_number<<"_fuzz tier1|tier2 [seed] | replay TRACE\n";
        return 2;
    }
    Mode mode{tier,tier==1 ? 200u : 50000u,tier==1 ? 100000u : 10000000u,seed};
    if (phase_number==4 && tier==1) mode.mutations+=8192;
    if (!replay) {
        trace.resize(mode.mutations);
        std::iota(trace.begin(),trace.end(),0u);
    }
    auto failure=run(mode,trace,true);
    if (!failure) return 0;
    std::cerr<<"FAIL tier="<<tier<<" seed="<<seed<<" step="<<failure->step
             <<" check="<<failure->kind<<std::endl;
    auto fails=[&](const Trace& candidate) {
        auto result=run(mode,candidate,false);
        return result && result->kind==failure->kind;
    };
    auto prefix=trace;
    prefix.erase(std::upper_bound(prefix.begin(),prefix.end(),failure->step),prefix.end());
    if (fails(prefix)) trace=std::move(prefix);
    auto minimal=test::shrink(std::move(trace),fails);
    auto path="phase"+std::to_string(phase_number)+"-tier"+std::to_string(tier)+"-"+std::to_string(seed)+".trace";
    std::ofstream file(path);
    dump(file,mode,minimal);
    file.close();
    std::cerr<<"1-minimal mutations="<<minimal.size()<<" replay: phase"<<phase_number<<"_fuzz replay "<<path<<'\n';
    if (!file) dump(std::cerr,mode,minimal);
    return 1;
}
