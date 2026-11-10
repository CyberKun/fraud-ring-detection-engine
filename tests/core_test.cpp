#include "core/tenant.hpp"
#include "engines/brute_force.hpp"
#include <gtest/gtest.h>

namespace fraud {
namespace {
constexpr Id card = *encode(Entity::card, 1);
constexpr Id device = *encode(Entity::device, 2);
using Graph = Tenant<BruteForce>;
struct Driver {
    Graph graph;
    Id next{1};
    explicit Driver(Policy p = {}) : graph(7,p) {}
    Batch send(Time t, Command c) {
        Batch b = graph.submit(3,next,t,c);
        EXPECT_EQ(b.status,Status::ok);
        next += b.records.size();
        return b;
    }
    Batch add(Id u, Id v, Time end = 50, Time t = 0) {
        return send(t, EdgeUpsert{{u,v,0},end});
    }
    Batch del(Id u, Id v) { return send(0,EdgeDelete{{u,v,0}}); }
};

TEST(Core, EncodesBoundariesAndRejectsReservedPrefix) {
    EXPECT_EQ(*encode(Entity::account,id_mask), id_mask);
    EXPECT_EQ(entity(card), Entity::card);
    EXPECT_EQ(entity(device), Entity::device);
    EXPECT_FALSE(encode(Entity::device,id_mask+1));
    EXPECT_FALSE(valid(UINT64_MAX));
    EXPECT_FALSE(encode(static_cast<Entity>(3),0));
}

TEST(Oracle, OverwritesWeightsAndReturnsInducedMultigraph) {
    Driver d;
    EXPECT_FALSE(d.graph.engine().connected(0,0));
    EXPECT_FALSE(d.graph.subgraph(0,10));
    d.send(0,VertexUpdate{0,{80,true}});
    d.send(0,VertexUpdate{0,{5,false}});
    d.send(0,VertexUpdate{card,{7,true}});
    d.add(0,card);
    d.send(0,EdgeUpsert{{card,0,1},50});
    d.add(card,device);
    d.add(0,0);
    EXPECT_EQ(d.graph.active_degree(0),3u);
    EXPECT_EQ(d.graph.engine().stats(0), (Aggregate{3,1,12,0}));
    ASSERT_TRUE(d.graph.engine().connected(0,device));
    auto sub = d.graph.subgraph(0,2).value();
    EXPECT_EQ(sub.total_vertices,3u);
    EXPECT_TRUE(sub.truncated);
    ASSERT_EQ(sub.vertices.size(),2u);
    EXPECT_EQ(sub.vertices[0].id,0u);
    EXPECT_EQ(sub.vertices[1].id,card);
    EXPECT_FALSE(sub.vertices[1].weight.flagged);
    EXPECT_EQ(sub.edges.size(),3u);
    EXPECT_TRUE(d.graph.subgraph(0,0)->vertices.empty());
    EXPECT_FALSE(d.graph.subgraph(0,3)->truncated);
    d.del(card,device);
    EXPECT_FALSE(d.graph.engine().connected(0,device));
    EXPECT_DOUBLE_EQ(d.graph.engine().stats(device)->flagged_ratio(),0.);
}

TEST(Wheel, OrdersAcrossSameSlotBoundariesAndIdleGaps) {
    TimingWheel wheel;
    for (Deadline d : {Deadline{100,4,1},{99,3,2},{100,2,4},{UINT64_MAX,1,1}})
        wheel.schedule(d);
    EXPECT_FALSE(wheel.pop_due(98));
    EXPECT_EQ(wheel.pop_due(100), (Deadline{99,3,2}));
    EXPECT_EQ(wheel.pop_due(100), (Deadline{100,2,4}));
    EXPECT_EQ(wheel.pop_due(100), (Deadline{100,4,1}));
    EXPECT_FALSE(wheel.pop_due(100));
    EXPECT_EQ(wheel.pop_due(UINT64_MAX), (Deadline{UINT64_MAX,1,1}));
}

TEST(Core, ExtensionLazyInvalidationAndHandleRetirement) {
    Driver d;
    d.add(0,1,3);
    d.add(1,0,9);
    d.add(1,0,7);
    EXPECT_EQ(d.graph.edge({0,1,0})->version,2u);
    d.add(1,2,9);
    d.add(2,3,9);
    d.del(2,1);
    d.add(1,2,9);
    EXPECT_EQ(d.graph.active_degree(1),2u);
    EXPECT_EQ(d.send(3,Tick{}).records.size(),1u);
    auto b = d.send(9,VertexUpdate{0,{3,true}});
    ASSERT_EQ(b.records.size(),4u);
    EXPECT_EQ(std::get<Expire>(b.records[0].payload).handle,1u);
    EXPECT_EQ(std::get<Expire>(b.records[1].payload).handle,3u);
    EXPECT_EQ(std::get<Expire>(b.records[2].payload).handle,4u);
    EXPECT_EQ(d.graph.active_degree(1),0u);
    EXPECT_EQ(d.graph.engine().stats(0),(Aggregate{1,1,3,1}));
}

TEST(Hub, HysteresisCountsShadowAndRestoresOnlyUnfilteredEdges) {
    Driver d({3,1});
    d.add(card,device);
    d.add(card,1);
    auto b = d.add(card,2);
    ASSERT_EQ(b.records.size(),2u);
    EXPECT_EQ(std::get<HubSuppress>(b.records[1].payload).vertex,card);
    d.add(device,1);
    d.add(device,3);
    EXPECT_TRUE(d.graph.suppressed(device));
    EXPECT_TRUE(d.graph.suppressed(card));
    EXPECT_EQ(d.graph.active_degree(device),3u);
    d.del(card,1);
    EXPECT_TRUE(d.graph.suppressed(card));
    b = d.del(card,2);
    EXPECT_TRUE(std::holds_alternative<HubRestore>(b.records.back().payload));
    EXPECT_TRUE(d.graph.edge({device,card,0})->shadow);
    EXPECT_TRUE(d.graph.subgraph(card,10)->edges.empty());
    d.del(device,3);
    d.del(device,1);
    EXPECT_TRUE(d.graph.engine().connected(card,device));
    EXPECT_FALSE(d.graph.edge({device,card,0})->shadow);
    EXPECT_EQ(d.graph.active_degree(card),1u);
}

TEST(Hub, ShadowExtensionAndExpiryTriggerRestore) {
    Driver d({2,1});
    d.add(card,1,2);
    d.add(card,2,4);
    d.add(1,card,9);
    EXPECT_TRUE(d.graph.suppressed(card));
    d.send(2,Tick{});
    EXPECT_EQ(d.graph.active_degree(card),2u);
    auto b = d.send(4,Tick{});
    ASSERT_EQ(b.records.size(),3u);
    EXPECT_TRUE(std::holds_alternative<Expire>(b.records[0].payload));
    EXPECT_TRUE(std::holds_alternative<HubRestore>(b.records[1].payload));
    EXPECT_TRUE(d.graph.engine().connected(card,1));
    d.add(1,2,10,4);
    EXPECT_FALSE(d.graph.suppressed(1));
}

TEST(Alerts, UsesAccountDenominatorAndPerVertexCooldown) {
    Driver d({50,35,2,0.5,10});
    d.send(0,VertexUpdate{1,{10,true}});
    d.add(1,card);
    auto b = d.add(card,2);
    ASSERT_EQ(b.alerts.size(),1u);
    EXPECT_DOUBLE_EQ(b.alerts[0].stats.flagged_ratio(),0.5);
    EXPECT_EQ(b.alerts[0].stats.volume,10u);
    EXPECT_TRUE(d.send(1,VertexUpdate{2,{0,true}}).alerts.empty());
    d.send(2,VertexUpdate{2,{0,false}});
    EXPECT_EQ(d.send(10,VertexUpdate{2,{0,true}}).alerts.size(),1u);
    EXPECT_TRUE(d.send(20,VertexUpdate{2,{0,true}}).alerts.empty());
}

TEST(Alerts, SplittingOffUnflaggedSideCanCrossThreshold) {
    Driver d({50,35,1,1.,0});
    d.add(1,2);
    d.send(0,VertexUpdate{1,{0,true}});
    auto b = d.del(1,2);
    ASSERT_EQ(b.alerts.size(),1u);
    EXPECT_EQ(b.alerts[0].vertex,1u);
}

TEST(Core, InvalidRequestsDoNotConsumeTimeOrMutate) {
    Graph g(7);
    auto write = [&](Time t, Command c) { return g.submit(1,2,t,c).status; };
    EXPECT_EQ(g.submit(1,1,1,VertexUpdate{1,{UINT64_MAX,true}}).status,Status::ok);
    EXPECT_EQ(write(9,VertexUpdate{2,{1,false}}),Status::invalid_input);
    EXPECT_EQ(write(9,EdgeUpsert{{1,2,0},9}),Status::invalid_input);
    EXPECT_EQ(write(9,EdgeDelete{{1,UINT64_MAX,0}}),Status::invalid_input);
    EXPECT_EQ(write(0,Tick{}),Status::stale_position);
    EXPECT_FALSE(g.engine().stats(2));
    EXPECT_EQ(write(2,VertexUpdate{1,{8,false}}),Status::ok);
    EXPECT_EQ(g.engine().stats(1)->volume,8u);
}

TEST(Core, FollowerUsesExplicitEventsAndTenantScopedProducerSequences) {
    Driver leader({2,1,1,0.,0});
    Graph follower(7,{2,1,1,0.,0});
    std::vector<Alert> alerts;
    for (auto batch : {leader.add(card,1,2),leader.add(card,2,8),leader.send(2,Tick{})}) {
        alerts.clear();
        for (auto& e : batch.records) EXPECT_EQ(follower.apply_record(e,alerts),Status::ok);
        EXPECT_EQ(alerts.size(),batch.alerts.size());
    }
    EXPECT_EQ(follower.engine().stats(2),leader.graph.engine().stats(2));
    EXPECT_EQ(follower.edge({card,2,0})->shadow,false);
    auto b = leader.graph.submit(3,20,5,VertexUpdate{2,{10,true}},ProducerSequence{0,0});
    ASSERT_EQ(b.status,Status::ok);
    EXPECT_EQ(follower.apply_record(b.records.front(),alerts),Status::ok);
    EXPECT_EQ(follower.apply_record(b.records.front(),alerts),Status::stale_position);
    EXPECT_EQ(follower.submit(4,21,6,VertexUpdate{2,{11,true}},ProducerSequence{0,0}).status,Status::duplicate);
    Graph other(8);
    EXPECT_EQ(other.apply_record(b.records.front(),alerts),Status::stale_position);
    EXPECT_EQ(other.submit(4,21,6,Tick{},ProducerSequence{0,0}).status,Status::ok);
}

TEST(Core, ReplayedDuplicateConsumesPositionAndPromotionRebuildsWheel) {
    Graph replica(7);
    std::vector<Alert> alerts;
    Event e{7,1,8,0,ProducerSequence{3,7},EdgeUpsert{{1,2,0},10}};
    ASSERT_EQ(replica.apply_record(e,alerts),Status::ok);
    e.lsn++;
    e.payload = EdgeDelete{{1,2,0}};
    EXPECT_EQ(replica.apply_record(e,alerts),Status::duplicate);
    EXPECT_EQ(replica.apply_record(e,alerts),Status::stale_position);
    EXPECT_TRUE(replica.engine().connected(1,2));
    auto b = replica.submit(2,10,10,Tick{});
    ASSERT_EQ(b.records.size(),2u);
    EXPECT_TRUE(std::holds_alternative<Expire>(b.records.front().payload));
    EXPECT_FALSE(replica.engine().connected(1,2));
}
} // namespace
} // namespace fraud
