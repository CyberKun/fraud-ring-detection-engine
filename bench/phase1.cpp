#include "core/tenant.hpp"
#include "engines/brute_force.hpp"
#include <array>
#include <chrono>
#include <iomanip>
#include <iostream>

int main() {
    using namespace fraud;
    // 64 independent five-account stars. Staggered cycles revisit each group
    // every 64 commands. Two leaves expire before the next insertion cycle.
    Tenant<BruteForce> graph(1);
    constexpr Id count = 200000, groups = 64;
    Id next = 1, alerts = 0;
    std::array<Id,7> events{};
    auto start = std::chrono::steady_clock::now();
    for (Id i = 0; i < count; ++i) {
        Id group = i % groups, step = (i / groups) % 10;
        Id root = group * 5;
        Key k{root,root + 1 + step % 4,0};
        Command c = Tick{};
        if (step < 4) c = EdgeUpsert{k,i + 8 * groups};
        else if (step == 4) c = VertexUpdate{root,{i,true}};
        else if (step == 5) c = EdgeUpsert{k,i + 6 * groups};
        else if (step == 6) c = EdgeDelete{k};
        else if (step == 7) c = VertexUpdate{root+3,{i,true}};
        else if (step == 9) c = VertexUpdate{root,{i,false}};
        auto b = graph.submit(1,next,i,c);
        if (b.status != Status::ok) return 1;
        next += b.records.size();
        alerts += b.alerts.size();
        for (const Event& e : b.records) ++events[e.payload.index()];
    }
    double seconds = std::chrono::duration<double>(std::chrono::steady_clock::now()-start).count();
    std::cout << std::fixed << std::setprecision(3)
              << "commands=" << count << " records=" << next-1 << " expiries=" << events[4]
              << " alerts=" << alerts << " elapsed_s=" << seconds
              << " commands_per_s=" << static_cast<Id>(count/seconds) << '\n';
}
