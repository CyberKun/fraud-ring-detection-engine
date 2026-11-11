#include "engines/hdt.hpp"
#include "core/tenant.hpp"
#include <gtest/gtest.h>

namespace fraud {
namespace {
TEST(EulerTour, RerootCutBothOrdersAndRecycleOnlyDarts) {
    EulerTourForest f(true);
    for (unsigned i=0;i<8;++i) {
        f.add_vertex();
        f.set_weight(i,{1,i!=7,i+1,i==0});
    }
    std::vector<EulerTourForest::Darts> darts;
    for (unsigned i=1;i<8;++i) darts.push_back(f.link(0,i,i,true));
    for (unsigned i=0;i<8;++i) {
        f.reroot(i);
        EXPECT_TRUE(f.contiguous_tour(i));
        EXPECT_EQ(f.stats(i),(Aggregate{8,7,36,1}));
        auto members=f.component_vertices(i,8);
        ASSERT_EQ(members.front(),i);
        std::sort(members.begin(),members.end());
        EXPECT_EQ(members,(std::vector<unsigned>{0,1,2,3,4,5,6,7}));
        ASSERT_TRUE(f.validate(8));
    }
    for (unsigned i=7;i>0;--i) {
        f.reroot(i%2 ? i : 0);
        f.cut(darts[i-1]);
        EXPECT_TRUE(f.contiguous_tour(0));
        EXPECT_TRUE(f.contiguous_tour(i));
        ASSERT_NE(f.root(0),f.root(i));
        EXPECT_EQ(f.count(0),i);
        EXPECT_EQ(f.stats(i),(Aggregate{1,i!=7,i+1,0}));
        EXPECT_EQ(f.dart_count(),2*(i-1));
        ASSERT_TRUE(f.validate(8));
    }
    auto d=f.link(7,2,0,true);
    EXPECT_TRUE(f.matches(d,7,2,0,true));
    EXPECT_EQ(f.stats(2),(Aggregate{2,1,11,0}));
    EXPECT_EQ(f.component_vertices(7,1),(std::vector<unsigned>{7}));
    EXPECT_TRUE(f.component_vertices(7,0).empty());
}
TEST(EulerTour, ExactDartFlagsAndLoopIncidencePropagateIndependently) {
    EulerTourForest f(false);
    for (int i=0;i<4;++i) f.add_vertex();
    auto a=f.link(0,1,11,true);
    auto b=f.link(1,2,12,false);
    f.set_incident(2,true);
    f.reroot(1);
    EXPECT_EQ(f.exact_edge(2),11u);
    EXPECT_EQ(f.incident_vertex(0),2u);
    f.set_exact(a,false);
    EXPECT_FALSE(f.exact_edge(2));
    f.set_exact(b,true);
    EXPECT_EQ(f.exact_edge(0),12u);
    f.cut(a);
    EXPECT_FALSE(f.incident_vertex(0));
    EXPECT_FALSE(f.exact_edge(0));
    EXPECT_EQ(f.exact_edge(1),12u);
    f.set_incident(2,false);
    EXPECT_FALSE(f.incident_vertex(1));
    EXPECT_EQ(f.stats(2),(Aggregate{2,0,0,0}));
    EXPECT_TRUE(f.validate(2));
    EXPECT_FALSE(f.validate(1));
}
void path(HdtEngine& g,unsigned n) {
    for (unsigned i=1;i<n;++i) ASSERT_EQ(g.insert_edge(i,i-1,i,100),Status::ok);
}
TEST(Hdt, PromotesExactTreeThenInternalNontreeAndFindsLevelOneReplacement) {
    HdtEngine g(8);
    path(g,8);
    g.insert_edge(10,0,2,100);
    g.delete_edge(4,Removal::expiry);
    ASSERT_TRUE(g.validate());
    EXPECT_EQ(g.counters().tree_promotions,3u);
    EXPECT_EQ(g.counters().nontree_promotions,1u);
    EXPECT_EQ(g.edge_info(10)->level,1u);
    EXPECT_FALSE(g.edge_info(10)->tree);
    EXPECT_TRUE(g.level_connected(1,0,3));
    EXPECT_FALSE(g.level_connected(1,4,7));
    g.delete_edge(2,Removal::manual);
    ASSERT_TRUE(g.validate());
    EXPECT_TRUE(g.edge_info(10)->tree);
    EXPECT_EQ(g.edge_info(10)->level,1u);
    EXPECT_TRUE(g.level_connected(0,0,3));
    EXPECT_TRUE(g.level_connected(1,0,3));
    EXPECT_FALSE(g.level_connected(2,0,3));
    EXPECT_EQ(g.counters().candidates,2u);
}
TEST(Hdt, ReplacesAtLowerLevelWithoutJoiningHigherForests) {
    HdtEngine g(8);
    path(g,8);
    g.insert_edge(8,0,7,90);
    g.delete_edge(4,Removal::suppression);
    ASSERT_TRUE(g.edge_info(8)->tree);
    EXPECT_TRUE(g.level_connected(1,0,3));
    g.insert_edge(9,3,6,90);
    auto scanned=g.counters().levels_traversed;
    g.delete_edge(2,Removal::expiry);
    EXPECT_EQ(g.counters().levels_traversed-scanned,2u);
    EXPECT_EQ(g.counters().lower_level_replacements,1u);
    EXPECT_EQ(g.edge_info(9)->level,0u);
    EXPECT_TRUE(g.edge_info(9)->tree);
    EXPECT_TRUE(g.connected(0,3));
    EXPECT_FALSE(g.level_connected(1,0,3));
    EXPECT_FALSE(g.level_connected(1,3,6));
    EXPECT_EQ(g.stats(3)->vertex_count,8u);
    EXPECT_TRUE(g.validate());
}
TEST(Hdt, NonPowerOfTwoBoundsParallelKeysAndSelfEdges) {
    HdtEngine g(9);
    path(g,9);
    g.insert_edge(9,0,1,4);
    g.insert_edge(10,0,0,9);
    for (unsigned h : {4,2,1}) {
        g.delete_edge(h,Removal::manual);
        ASSERT_TRUE(g.validate());
    }
    EXPECT_TRUE(g.connected(0,1));
    EXPECT_TRUE(g.edge_info(9)->tree);
    EXPECT_FALSE(g.edge_info(10)->tree);
    EXPECT_EQ(g.extend_ttl(9,200),Status::ok);
    EXPECT_EQ(g.insert_edge(11,0,9,40),Status::invalid_input);
    EXPECT_FALSE(g.stats(9));
    g.delete_edge(9,Removal::expiry);
    EXPECT_FALSE(g.connected(0,1));
    EXPECT_TRUE(g.validate());
    HdtEngine singleton(1);
    singleton.insert_edge(0,0,0,3);
    singleton.delete_edge(0,Removal::expiry);
    EXPECT_EQ(singleton.stats(0),(Aggregate{1,1,0,0}));
    EXPECT_TRUE(singleton.validate());
}
TEST(Hdt, DeepCutsReachPairsAndStopAtFirstOfTwoCrossingCandidates) {
    HdtEngine g(16);
    path(g,16);
    g.insert_edge(16,0,1,10);
    g.insert_edge(17,0,1,10);
    unsigned level=0;
    for (unsigned h : {8,4,2}) {
        g.delete_edge(h,Removal::manual);
        ++level;
        ASSERT_EQ(g.edge_info(1)->level,level);
        ASSERT_EQ(g.edge_info(16)->level,level);
        ASSERT_EQ(g.edge_info(17)->level,level);
        ASSERT_TRUE(g.validate());
    }
    auto before=g.counters();
    g.delete_edge(1,Removal::manual);
    EXPECT_EQ(g.counters().candidates-before.candidates,1u);
    EXPECT_EQ(g.counters().levels_traversed-before.levels_traversed,1u);
    EXPECT_TRUE(g.edge_info(16)->tree);
    EXPECT_FALSE(g.edge_info(17)->tree);
    EXPECT_TRUE(g.level_connected(3,0,1));
    EXPECT_FALSE(g.level_connected(4,0,1));
    EXPECT_TRUE(g.validate());
}
TEST(Hdt, HubSuppressionRestoreAndVertexOverwritesUseLevelZeroWeights) {
    Tenant<HdtEngine> g(1,{3,1},HdtEngine(4));
    Id device=*encode(Entity::device,0),lsn=1;
    auto submit=[&](Command c) {
        auto b=g.submit(1,lsn,0,c);
        EXPECT_EQ(b.status,Status::ok);
        lsn+=b.records.size();
        EXPECT_TRUE(g.engine().validate());
    };
    submit(VertexUpdate{device,{7,true}});
    submit(VertexUpdate{0,{6,true}});
    for (unsigned i=0;i<3;++i) submit(EdgeUpsert{{device,i,0},9});
    EXPECT_TRUE(g.suppressed(device));
    EXPECT_EQ(g.engine().stats(device),(Aggregate{1,0,7,0}));
    submit(EdgeDelete{{device,1,0}});
    submit(EdgeDelete{{device,2,0}});
    EXPECT_FALSE(g.suppressed(device));
    submit(VertexUpdate{0,{80,false}});
    EXPECT_EQ(g.engine().stats(device),(Aggregate{2,1,87,0}));
}
} // namespace
} // namespace fraud
