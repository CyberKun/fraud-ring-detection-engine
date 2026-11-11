#include "replica/admission.hpp"
#include "router/directory.hpp"
#include <gtest/gtest.h>
TEST(Routing, PendingCountAndByteReservations) {
    fraud::replica::Admission queue;
    auto first = queue.reserve(40, 2, 100);
    EXPECT_TRUE(first);
    EXPECT_FALSE(queue.reserve(61, 2, 100));
    auto second = queue.reserve(60, 2, 100);
    EXPECT_TRUE(second);
    EXPECT_FALSE(queue.reserve(0, 2, 100));
    second.reset();
    EXPECT_EQ(queue.usage().bytes, 40u);
    first.reset();
    EXPECT_EQ(queue.usage().requests, 0u);
    EXPECT_TRUE(queue.reserve(100, 2, 100));
}
TEST(Routing, FixedHashByteOrderAndPin) {
    fraud::routing::Topology map;
    map.partitions = {2, 0, 3, 1, 2, 0, 3, 1};
    EXPECT_EQ(map.partition(101), 0);
    EXPECT_EQ(map.partition(100), 1);
    map.pins[101] = 6;
    EXPECT_EQ(map.partition(101), 6);
    EXPECT_EQ(map.shard(101), 3u);
}
