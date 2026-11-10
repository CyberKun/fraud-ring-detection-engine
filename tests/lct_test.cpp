#include "engines/lct.hpp"
#include "engines/brute_force.hpp"
#include "engines/offline.hpp"
#include "../bench/shrink.hpp"
#include <gtest/gtest.h>

namespace fraud {
namespace {
TEST(DeltaDebug, ReducesAStateDependentMismatchToSingleDeletionMinimality) {
    using Trace = std::vector<int>;
    // A missed cut gives a different answer only if the edge was active.
    auto mismatch = [](const Trace& commands) {
        LctEngine forest(4);
        BruteForce reference;
        for (int cmd : commands) {
            if (cmd==1) {
                forest.insert_edge(5,0,1,99);
                reference.insert_edge(5,0,1,99);
            } else if (cmd==2) reference.delete_edge(5,Removal::manual);
            else {
                forest.set_vertex(3,{10,false});
                reference.set_vertex(3,{10,false});
            }
        }
        return forest.connected(0,1)!=reference.connected(0,1);
    };
    auto result=test::shrink(Trace{0,2,0,1,0,0,2,0,0},mismatch);
    EXPECT_EQ(result,(Trace{1,2}));
    EXPECT_TRUE(mismatch(result));
    EXPECT_FALSE(mismatch({1}));
    EXPECT_FALSE(mismatch({2}));
}
TEST(Lct, ReversalVirtualChildrenWeightsAndCut) {
    LctEngine lct(32);
    BruteForce bfs;
    for (Id v=0; v<31; ++v) {
        Weight weight{3*v,v%3==0};
        lct.set_vertex(v,weight);
        bfs.set_vertex(v,weight);
        if (v) {
            lct.insert_edge(v,v,(v-1)/2,v);
            bfs.insert_edge(v,v,(v-1)/2,v);
        }
    }
    for (Id i=0; i<30; ++i) {
        Id v=(i*13)%31;
        Weight w{1000-i,true};
        lct.set_vertex(v,w);
        bfs.set_vertex(v,w);
        for (Id u=0; u<31; ++u) {
            EXPECT_EQ(lct.connected(v,u),bfs.connected(v,u));
            ASSERT_EQ(lct.stats(u),bfs.stats(u));
        }
        lct.delete_edge(30-i,Removal::manual);
        bfs.delete_edge(30-i,Removal::manual);
    }
    EXPECT_EQ(lct.dummy_count(),0u);
}

TEST(Lct, MaxKeyRepairScansOnlySmallerSideForManualAndSuppression) {
    for (Removal why : {Removal::manual, Removal::suppression}) {
        LctEngine lct(6);
        for (Handle h=1; h<=5; ++h) lct.insert_edge(h,h-1,h,100);
        lct.insert_edge(6,0,5,70);
        lct.insert_edge(7,1,3,75);
        lct.insert_edge(8,1,4,75);
        lct.delete_edge(2,why);
        EXPECT_TRUE(lct.tree_edge(8));
        EXPECT_FALSE(lct.tree_edge(7));
        EXPECT_EQ(lct.counters().repair_vertices,2u);
        EXPECT_EQ(lct.counters().candidates,3u);
        EXPECT_EQ(lct.stats(0)->vertex_count,6u);
    }
}

TEST(Lct, CycleTiesAndExtensionsRefreshPreferredMinimum) {
    LctEngine lct(3);
    lct.insert_edge(1,0,1,5);
    lct.insert_edge(2,1,2,5);
    lct.insert_edge(3,2,0,5);
    EXPECT_FALSE(lct.tree_edge(1));
    lct.extend_ttl(2,20);
    lct.extend_ttl(1,10);
    EXPECT_TRUE(lct.tree_edge(1));
    EXPECT_TRUE(lct.tree_edge(2));
    EXPECT_FALSE(lct.tree_edge(3));
    lct.delete_edge(3,Removal::expiry);
    lct.delete_edge(1,Removal::expiry);
    EXPECT_FALSE(lct.connected(0,1));
    EXPECT_EQ(lct.counters().repairs,0u);
    lct.delete_edge(2,Removal::expiry);
    EXPECT_EQ(lct.dummy_count(),0u);
}

TEST(Lct, FixedSlabRecyclesThroughDenseChurnAndOrderedExpiry) {
    constexpr Id n=20;
    LctEngine lct(n);
    Id h=0;
    for (int round=0; round<20; ++round) {
        std::vector<Handle> active;
        for (Id u=0; u<n; ++u) for (Id v=u; v<n; ++v) {
            lct.insert_edge(++h,u,v,20);
            active.push_back(h);
            ASSERT_LE(lct.dummy_count(),n-1);
        }
        EXPECT_EQ(lct.dummy_count(),n-1);
        EXPECT_EQ(lct.stats(0)->vertex_count,n);
        for (std::size_t i=0; i<active.size(); i+=4)
            lct.delete_edge(active[i],Removal::manual);
        for (std::size_t i=0; i<active.size(); ++i)
            if (i%4) lct.delete_edge(active[i],Removal::expiry);
        ASSERT_EQ(lct.dummy_count(),0u);
        ASSERT_EQ(lct.node_slots(),2*n-1);
    }
    EXPECT_EQ(lct.counters().peak_dummies,n-1);
}

TEST(Lct, CapacityRejectionIsAtomicOnSubmissionAndReplay) {
    Tenant<LctEngine> tenant(1,{},LctEngine(2));
    tenant.submit(1,1,1,VertexUpdate{0,{10,true}});
    EXPECT_EQ(tenant.submit(1,2,2,EdgeUpsert{{1,2,0},8}).status,Status::invalid_input);
    EXPECT_FALSE(tenant.engine().stats(1));
    EXPECT_EQ(tenant.submit(1,2,2,EdgeUpsert{{0,1,0},8}).status,Status::ok);
    std::vector<Alert> alerts;
    EXPECT_EQ(tenant.apply_record({1,1,3,3,{},VertexUpdate{2,{5,true}}},alerts),Status::invalid_input);
    EXPECT_EQ(tenant.engine().stats(1),(Aggregate{2,2,10,1}));
}
TEST(Offline, ConsecutiveWindowsCarryShadowHandlesWeightsAndCrossings) {
    Policy policy{2,1,1,1.};
    Tenant<BruteForce> live(1,policy);
    OfflineReplay replay(1,policy);
    Id card=*encode(Entity::card,1), next=1;
    for (auto [t,cmd] : std::vector<std::pair<Time,Command>>{
            {1,VertexUpdate{0,{50,true}}},{2,EdgeUpsert{{0,card,0},20}},
            {3,EdgeUpsert{{1,card,0},6}},{6,Tick{}}}) {
        auto b=live.submit(1,next,t,cmd);
        next+=b.records.size();
        auto window=replay.next_window(b.records,t+1);
        ASSERT_TRUE(window);
        auto a=window->audit_at(t,0,5);
        ASSERT_TRUE(a);
        EXPECT_EQ(a->first_alert_timestamp_ms,1u);
        EXPECT_EQ(a->stats,live.engine().stats(0));
        EXPECT_EQ(a->subgraph.edges.size(),live.subgraph(0,5)->edges.size());
        if (t==6) EXPECT_EQ(a->subgraph.edges.at(0).handle,1u);
    }
}
} // namespace
} // namespace fraud
