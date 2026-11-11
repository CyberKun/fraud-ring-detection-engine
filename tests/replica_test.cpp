#include "replica/journal.hpp"
#include "temp_dir.hpp"
#include <gtest/gtest.h>
namespace {
using namespace fraud;
using namespace replica;
TEST(ReplicaJournal, PromiseSurvivesRestartWithoutRewritingLastEntry) {
    test::TempDir dir;
    {
        Journal j(dir.path, "group", "a");
        j.promise(8, "old");
        j.append({}, {{1, 8, 1, 100, {}, Tick{}}});
        j.commit(j.state().tail);
        j.promise(15, "candidate");
    }
    Journal j(dir.path, "group", "a");
    EXPECT_EQ(j.state().promised, 15u);
    EXPECT_EQ(j.state().tail.epoch, 8u);
    EXPECT_EQ(j.state().commit.lsn, 1u);
    EXPECT_THROW(j.fence(8, "old"), Rejected);
    EXPECT_THROW(j.promise(15, "other"), Rejected);
    EXPECT_NO_THROW(j.fence(15, "candidate"));
}
TEST(ReplicaJournal, TruncationBelowCommitIsFatalAcrossRestarts) {
    test::TempDir dir;
    {
        Journal j(dir.path, "group", "a");
        j.promise(2, "term");
        j.append({}, {{1, 2, 1, 1, {}, Tick{}}});
        j.commit(j.state().tail);
        j.append(j.state().tail, {{1, 2, 2, 2, {}, Tick{}}});
        j.truncate(j.state().tail, j.at(1));
        ASSERT_EQ(j.state().tail.lsn, 1u);
        EXPECT_THROW(j.truncate(j.state().tail, j.at(0)), Invariant);
    }
    EXPECT_THROW((Journal{dir.path, "group", "a"}), Invariant);
}
TEST(ReplicaJournal, WireFrameRejectsChecksumAndLengthMismatch) {
    storage::Frame f{{1, 2, 1, 100, {}, Tick{}}};
    auto b = storage::encode(f);
    EXPECT_EQ(storage::decode_frame(b).size(), 1u);
    b.back() ^= 1;
    EXPECT_THROW(storage::decode_frame(b), storage::Error);
    b.push_back(0);
    EXPECT_THROW(storage::decode_frame(b), storage::Error);
}
} // namespace
