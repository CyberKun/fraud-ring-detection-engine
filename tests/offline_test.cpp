#include "engines/offline.hpp"
#include "engines/brute_force.hpp"
#include "engines/rollback_dsu.hpp"
#include <gtest/gtest.h>

namespace fraud {
namespace {
struct History {
    Policy policy;
    Tenant<BruteForce> live;
    std::vector<Event> records;
    explicit History(Policy p = {}) : policy(p), live(7,p) {}
    void add(Time t, Command c) {
        auto b = live.submit(1,records.size()+1,t,c);
        ASSERT_EQ(b.status,Status::ok);
        records.insert(records.end(),b.records.begin(),b.records.end());
    }
    OfflineEngine index(Time end) { return OfflineEngine::build(7,policy,records,end).value(); }
};
TEST(Rollback, NestedMergesRestoreWeightsMembersAndDistributeFirstMarks) {
    RollbackDsu d(4);
    auto zero = d.snapshot();
    d.add_weight(0,Entity::account,{5,true});
    d.add_weight(1,Entity::card,{10,false});
    d.mark_component(0,2);
    auto before = d.snapshot();
    d.join(1,0);
    d.mark_component(1,5);
    d.join(0,1);
    auto both = d.snapshot();
    d.join(1,2);
    d.add_weight(2,Entity::device,{7,false});
    EXPECT_EQ(d.stats(0),(Aggregate{3,1,22,1}));
    EXPECT_EQ(d.component_vertices(0,4).size(),3u);
    d.mark_component(2,8);
    d.rollback(both);
    EXPECT_EQ(d.stats(1),(Aggregate{2,1,15,1}));
    EXPECT_NE(d.root(0),d.root(2));
    d.rollback(before);
    EXPECT_EQ(d.component_vertices(0,3),(std::vector<RollbackDsu::Index>{0}));
    d.rollback(zero);
    EXPECT_EQ(d.stats(0),Aggregate{});
    EXPECT_EQ(d.singleton_first(0),2u);
    EXPECT_EQ(d.singleton_first(1),5u);
    EXPECT_EQ(d.singleton_first(2),8u);
    EXPECT_FALSE(d.singleton_first(3));
}
TEST(Offline, HalfOpenReinsertionsSameHandleExtensionAndOpenEndedWeights) {
    History h;
    Key k{2,1,4};
    h.add(2,EdgeUpsert{k,6});
    h.add(3,VertexUpdate{1,{5,true}});
    h.add(4,EdgeDelete{k});
    h.add(5,EdgeUpsert{k,7});
    h.add(6,Tick{});
    h.add(7,Tick{});
    h.add(8,EdgeUpsert{k,12});
    h.add(9,EdgeUpsert{{1,2,4},40});
    h.add(10,VertexUpdate{1,{17,false}});
    auto a = h.index(20);
    EXPECT_FALSE(a.audit_at(1,1,4));
    for (Time t : {2,3,5,6,8,9,10,19}) EXPECT_TRUE(a.connected_at(t,1,2));
    for (Time t : {4,7,20}) EXPECT_FALSE(a.connected_at(t,1,2));
    EXPECT_EQ(a.audit_at(3,1,2)->stats,(Aggregate{2,2,5,1}));
    EXPECT_EQ(a.audit_at(4,1,2)->stats,(Aggregate{1,1,5,1}));
    EXPECT_EQ(a.audit_at(19,2,2)->stats,(Aggregate{2,2,17,0}));
    EXPECT_FALSE(a.audit_at(20,1,2));
    EXPECT_EQ(a.audit_at(2,1,2)->subgraph.edges.at(0).handle,1u);
    EXPECT_EQ(a.audit_at(5,1,2)->subgraph.edges.at(0).handle,2u);
    EXPECT_EQ(a.audit_at(8,1,2)->subgraph.edges.at(0).expiry,12u);
    auto e = a.audit_at(19,2,2)->subgraph.edges.at(0);
    EXPECT_EQ(e.handle,3u);
    EXPECT_EQ(e.version,2u);
    EXPECT_EQ(e.expiry,40u);
}
TEST(Offline, HubFilteredIntervalsSpanSuppressRestoreAndShadowExpiry) {
    History h({2,1});
    Id card = *encode(Entity::card,3), device = *encode(Entity::device,4);
    h.add(1,EdgeUpsert{{card,device,0},50});
    h.add(2,EdgeUpsert{{1,card,0},5});
    h.add(3,EdgeUpsert{{2,device,0},8});
    h.add(5,Tick{});
    h.add(8,Tick{});
    auto a = h.index(12);
    EXPECT_TRUE(a.connected_at(1,card,device));
    for (Time t : {2,3,5,7}) EXPECT_FALSE(a.connected_at(t,card,device));
    auto r = a.audit_at(8,card,2).value();
    EXPECT_EQ(r.stats,(Aggregate{2,0,0,0}));
    EXPECT_EQ(r.subgraph.edges.at(0).handle,1u);
    EXPECT_FALSE(r.subgraph.edges.at(0).shadow);
}
TEST(Offline, FirstCrossingPersistsThroughSplitsAndOnlyAppliesToThenMembers) {
    History h({50,35,1,1.,999});
    h.add(1,EdgeUpsert{{1,2,0},50});
    h.add(2,VertexUpdate{1,{50,true}});
    h.add(3,EdgeDelete{{1,2,0}});
    h.add(4,EdgeUpsert{{1,2,0},50});
    h.add(5,VertexUpdate{2,{10,true}});
    h.add(6,VertexUpdate{1,{80,false}});
    auto a = h.index(10);
    EXPECT_FALSE(a.audit_at(2,1,0)->first_alert_timestamp_ms);
    EXPECT_EQ(a.audit_at(3,1,0)->first_alert_timestamp_ms,3u);
    EXPECT_FALSE(a.audit_at(4,2,0)->first_alert_timestamp_ms);
    EXPECT_EQ(a.audit_at(6,1,0)->first_alert_timestamp_ms,3u);
    EXPECT_EQ(a.audit_at(6,2,0)->first_alert_timestamp_ms,5u);
    EXPECT_EQ(a.audit_at(6,1,0)->stats,(Aggregate{2,2,90,1}));
}
TEST(Offline, EqualTimestampUsesFinalStateAndTruncatesInducedParallelEdges) {
    History h({50,35,1,1.});
    h.add(0,VertexUpdate{1,{7,true}});
    h.add(0,VertexUpdate{1,{7,false}});
    h.add(1,EdgeUpsert{{1,1,1},10});
    h.add(1,EdgeUpsert{{2,1,1},10});
    h.add(1,EdgeUpsert{{1,2,2},10});
    h.add(1,VertexUpdate{1,{9,true}});
    auto a = h.index(2);
    EXPECT_FALSE(a.audit_at(1,1,0)->first_alert_timestamp_ms);
    auto r = a.audit_at(1,1,1).value();
    EXPECT_TRUE(r.subgraph.truncated);
    EXPECT_EQ(r.subgraph.vertices.at(0).id,1u);
    EXPECT_EQ(r.subgraph.edges.size(),1u);
    EXPECT_EQ(a.audit_at(1,1,2)->subgraph.edges.size(),3u);
    EXPECT_TRUE(a.audit_at(1,1,0)->subgraph.edges.empty());
}
TEST(Offline, EmptyWindowRejectedHistoryAndDuplicateRecords) {
    History h;
    EXPECT_FALSE(h.index(0).audit_at(0,1,0));
    h.add(0,VertexUpdate{1,{5,true}});
    h.records.front().source = ProducerSequence{2,1};
    auto duplicate = h.records.front();
    duplicate.lsn++;
    duplicate.time++;
    duplicate.payload = VertexUpdate{1,{99,false}};
    h.records.push_back(duplicate);
    EXPECT_EQ(h.index(2).audit_at(1,1,0)->stats.volume,5u);
    EXPECT_FALSE(OfflineEngine::build(7,h.policy,h.records,1));
    EXPECT_FALSE(OfflineEngine::build(8,h.policy,h.records,2));
    h.records.back().lsn = 1;
    EXPECT_FALSE(OfflineEngine::build(7,h.policy,h.records,2));
}
} // namespace
} // namespace fraud
