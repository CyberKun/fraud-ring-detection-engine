#include "engines/brute_force.hpp"
#include "engines/offline.hpp"
#include <array>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <random>
#include <string_view>
#include <tuple>

namespace {
using namespace fraud;
using Clock = std::chrono::steady_clock;
constexpr Id seed = 20261111;

Id vertex(std::size_t i) {
    Entity kind = i % 10 == 9 ? Entity::device : (i % 10 == 8 ? Entity::card : Entity::account);
    return *encode(kind,i);
}
struct Workload {
    std::mt19937_64 random{seed};
    std::vector<Key> recent;
    std::size_t n;
    Key last{};
    explicit Workload(std::size_t n) : n(n) {}
    Command command(Time t) {
        if (t < n) return VertexUpdate{vertex(t),{random()%1000,t%4 == 0}};
        auto roll = random()%100;
        Key k{vertex(random()%n),vertex(random()%n),static_cast<std::uint32_t>(random()%2)};
        // Concentrate a share of edges on one device to exercise policy records.
        if (t % 160 < 24) k.v = vertex(n-1);
        if (roll < 38 || recent.empty()) {
            if (recent.size() < 64) recent.push_back(k);
            else recent[t%64] = k;
            last = k;
            return EdgeUpsert{k,t+1+random()%std::max<std::size_t>(80,n/4)};
        }
        k = recent[random()%recent.size()];
        last = k;
        if (roll < 52) return EdgeUpsert{{k.v,k.u,k.type},t+80+random()%160};
        if (roll < 67) return EdgeDelete{k};
        if (roll < 95) return VertexUpdate{vertex(random()%n),{random()%10000,random()%3 == 0}};
        return Tick{};
    }
};

void canonicalize(Subgraph& g) {
    std::sort(g.vertices.begin(),g.vertices.end(),[](auto a, auto b) { return a.id < b.id; });
    std::sort(g.edges.begin(),g.edges.end(),[](auto a, auto b) { return a.handle < b.handle; });
}
bool equal(Subgraph& a, Subgraph& b) {
    canonicalize(a);
    canonicalize(b);
    auto same_vertex = [](VertexView x, VertexView y) {
        return x.id == y.id && x.weight.volume == y.weight.volume && x.weight.flagged == y.weight.flagged;
    };
    auto same_edge = [](EdgeRecord x, EdgeRecord y) {
        return x.key == y.key && x.handle == y.handle && x.version == y.version &&
               x.expiry == y.expiry && x.shadow == y.shadow;
    };
    return a.truncated == b.truncated && a.total_vertices == b.total_vertices &&
           std::equal(a.vertices.begin(),a.vertices.end(),b.vertices.begin(),b.vertices.end(),same_vertex) &&
           std::equal(a.edges.begin(),a.edges.end(),b.edges.begin(),b.edges.end(),same_edge);
}
double elapsed(Clock::time_point a, Clock::time_point b) {
    return std::chrono::duration<double>(b-a).count();
}

int run(bool differential) {
    const std::size_t n = differential ? 200 : 20000;
    const std::size_t target = differential ? 100000 : 1000000;
    Policy policy{8,3,2,0.5,100};
    Tenant<BruteForce> oracle(1,policy);
    Workload workload(n);
    std::mt19937_64 query_rng(seed+1);
    std::vector<Event> records;
    records.reserve(differential ? 2*target : target);
    std::vector<AuditQuery> queries;
    std::vector<std::optional<AuditResult>> expected;
    std::vector<std::optional<Time>> first(n);
    std::array<Id,7> counts{};
    auto start = Clock::now();
    Time t = 0;
    while (differential ? t < target : records.size() < target) {
        Command input = workload.command(t);
        auto batch = oracle.submit(1,records.size()+1,t,input);
        if (batch.status != Status::ok) return 2;
        if (!differential && batch.records.size() > target-records.size()) {
            // Finish at exactly one million records without splitting a batch.
            // Repeated ticks at the previous accepted time have no due work.
            while (records.size() < target) {
                records.push_back({1,1,records.size()+1,t-1,{},Tick{}});
                ++counts[3];
            }
            break;
        }
        for (const auto& e : batch.records) ++counts[e.payload.index()];
        records.insert(records.end(),batch.records.begin(),batch.records.end());
        if (differential) {
            for (std::size_t i = 0; i < n; ++i) {
                if (first[i]) continue;
                auto a = oracle.engine().stats(vertex(i));
                if (a && a->account_count >= policy.min_accounts && a->flagged_ratio() >= policy.min_ratio)
                    first[i] = t;
            }
        }
        if (differential || t%512 == 0) {
            Id u = vertex(query_rng()%n), peer = vertex(query_rng()%n);
            if (t >= n && t%2 == 0) {
                u = workload.last.u;
                peer = workload.last.v;
            }
            queries.push_back({t,u,n,peer});
            auto a = oracle.engine().stats(u);
            if (a) expected.emplace_back(AuditResult{*a,*oracle.subgraph(u,n),first[u & id_mask],
                                                     oracle.engine().connected(u,peer)});
            else expected.emplace_back(std::nullopt);
        }
        ++t;
    }
    auto generated = Clock::now();
    auto engine = OfflineEngine::build(1,policy,records,t);
    if (!engine) return 3;
    auto built = Clock::now();
    auto actual = engine->audit_many(queries);
    auto answered = Clock::now();
    for (std::size_t i = 0; i < actual.size(); ++i) {
        auto& a = actual[i];
        auto& e = expected[i];
        if (a.has_value() == e.has_value() &&
            (!e || (a->stats == e->stats && a->connected_to_peer == e->connected_to_peer &&
                    (!differential || a->first_alert_timestamp_ms == e->first_alert_timestamp_ms) &&
                    equal(a->subgraph,e->subgraph)))) continue;
        std::cerr << "mismatch seed=" << seed << " time=" << queries[i].time
                  << " vertex=" << queries[i].vertex << '\n';
        return 1;
    }
    std::cout << std::fixed << std::setprecision(3)
              << "PASS mode=" << (differential ? "differential" : "scale") << " seed=" << seed
              << " vertices=" << n << " timestamps=" << t << " records=" << records.size()
              << " audits=" << actual.size() << " mismatches=0\n"
              << "expire=" << counts[4] << " suppress=" << counts[5] << " restore=" << counts[6] << '\n'
              << "generate_oracle_s=" << elapsed(start,generated) << " build_s=" << elapsed(generated,built)
              << " audit_batch_s=" << elapsed(built,answered)
              << " total_s=" << elapsed(start,Clock::now()) << '\n';
    return 0;
}
} // namespace

int main(int argc, char** argv) {
    if (argc == 2 && std::string_view(argv[1]) == "differential") return run(true);
    if (argc == 2 && std::string_view(argv[1]) == "scale") return run(false);
    std::cerr << "usage: phase2_check differential|scale\n";
    return 2;
}
