#include "service/node.hpp"
#include "temp_dir.hpp"
#include <gtest/gtest.h>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <barrier>
#include <thread>

namespace fraud {
namespace {
using namespace storage;
namespace fs=std::filesystem;
Frame frame(Id lsn) { return {{7,9,lsn,11+lsn,{},VertexUpdate{22,{lsn,true}}}}; }
Bytes join(Bytes a,const Bytes& b){a.insert(a.end(),b.begin(),b.end());return a;}
TEST(Wal, GoldenFre1WithProducerSequence) {
    auto bytes=encode({Event{7,9,1,11,ProducerSequence{13,17},VertexUpdate{22,{77,true}}}});
    std::ostringstream hex;
    for(auto b:bytes) hex<<std::hex<<std::setw(2)<<std::setfill('0')<<unsigned(b);
    EXPECT_EQ(hex.str(),"46524531010000003700000001000000090000000000000001000000000000003300000007000000000000000b0000000000000003010d00000000000000110000000000000016000000000000004d00000000000000017c3bd6d8");
}
TEST(Wal, EveryTruncatedByteAndBadFinalCrcRecoverPreviousFrame) {
    test::TempDir tmp;
    auto file=tmp.path/"active.wal";
    auto first=encode(frame(1)),last=encode(frame(2));
    for(std::size_t n=1;n<=last.size();++n) {
        Bytes partial(last.begin(),last.begin()+n);
        if(n==last.size()) partial.back()^=0x80;
        atomic_file(file,join(first,partial));
        service::Node node(tmp.path);
        EXPECT_EQ(node.position().lsn,1u);
        EXPECT_EQ(node.truncated(),n);
        EXPECT_EQ(node.stats(7,22).value,(Aggregate{1,1,1,1}));
        EXPECT_EQ(read_file(file,4096),first);
    }
    std::cout<<"recovery_cases="<<last.size()<<" recovered_lsn=1 state_and_wal=identical\n";
}
TEST(Wal, InteriorCrcLengthMagicAndCapsFailClosed) {
    test::TempDir tmp;
    auto file=tmp.path/"active.wal";
    for(unsigned offset:{0,8,12,32,40,70}) {
        auto a=encode(frame(1)),b=encode(frame(2)),c=encode(frame(3));
        ASSERT_LT(offset,b.size());
        if(offset==8 || offset==12) std::fill_n(b.begin()+offset,4,0xff);
        else b[offset]^=0x7f;
        auto bytes=join(join(a,b),c);
        atomic_file(file,bytes);
        EXPECT_THROW(service::Node node(tmp.path),Error);
        EXPECT_EQ(read_file(file,4096),bytes);
    }
    auto bytes=join(encode(frame(1)),encode(frame(3)));
    atomic_file(file,bytes);
    EXPECT_THROW(service::Node node(tmp.path),Error);
    EXPECT_EQ(read_file(file,4096),bytes);
}
TEST(Wal, RejectsMixedEpochAndNoncontiguousFrame) {
    auto f=frame(1); f.push_back(frame(3).front());
    EXPECT_THROW(encode(f),Error);
    f.back().lsn=2; f.back().epoch++;
    EXPECT_THROW(encode(f),Error);
}
TEST(Wal, UnsupportedVersionAndOversizedHeadersArePreserved) {
    test::TempDir tmp;
    auto file=tmp.path/"active.wal";
    for(unsigned offset:{4u,6u,8u,12u}) {
        auto bytes=encode(frame(1));
        bytes[offset]=0xff;
        if(offset>=8) std::fill_n(bytes.begin()+offset,4,0xff);
        atomic_file(file,bytes);
        EXPECT_THROW(service::Node node(tmp.path),Error);
        EXPECT_EQ(read_file(file,4096),bytes);
    }
}
TEST(Wal, PhysicallyTruncatedInteriorFollowedByValidFrameIsNeverRepaired) {
    test::TempDir tmp;
    for(auto keep:{3u,19u,32u,39u,71u}) {
        auto middle=encode(frame(2)); middle.resize(keep);
        auto bytes=join(join(encode(frame(1)),middle),encode(frame(3)));
        auto file=tmp.path/"active.wal";
        atomic_file(file,bytes);
        EXPECT_THROW(service::Node n(tmp.path),Error);
        EXPECT_EQ(read_file(file,4096),bytes);
    }
}
TEST(Storage, FailedWriteCannotPublishSpeculativeState) {
    test::TempDir tmp;
    auto file=tmp.path/"active.wal";
    fs::create_symlink("/dev/full",file);
    {
        service::Node node(tmp.path);
        EXPECT_THROW(node.mutate(1,1,VertexUpdate{1,{80,true}}),Error);
        EXPECT_THROW(node.stats(1,1),Error);
        EXPECT_THROW(node.snapshot(),Error);
    }
    fs::remove(file);
    service::Node recovered(tmp.path);
    EXPECT_EQ(recovered.position().lsn,0u);
    EXPECT_FALSE(recovered.stats(1,1).value);
}
TEST(Storage, AuditCanIncludeLargestTimestamp) {
    test::TempDir tmp;
    service::Node node(tmp.path,{{50,35,1,1.,0},2,false});
    node.mutate(1,UINT64_MAX,VertexUpdate{0,{18,true}});
    auto a=node.audit(1,UINT64_MAX,0,1);
    ASSERT_TRUE(a.value);
    EXPECT_EQ(a.value->first_alert_timestamp_ms,UINT64_MAX);
    EXPECT_EQ(a.value->stats.volume,18u);
}
TEST(Storage, ConcurrentAdmissionsShareDurabilityAndDeduplicate) {
    test::TempDir tmp;
    constexpr int writers=16;
    {
        service::Node node(tmp.path);
        std::barrier start(writers);
        std::array<std::thread,writers> threads;
        std::atomic<int> accepted{};
        for(int i=0;i<writers;++i) threads[i]=std::thread([&,i]{
            start.arrive_and_wait();
            auto duplicate=node.mutate(1,20,VertexUpdate{0,{1,true}},ProducerSequence{1,7});
            if(duplicate.status==Status::ok) ++accepted;
            EXPECT_EQ(node.mutate(1,10,VertexUpdate{Id(i+1),{Id(i),false}}).status,Status::ok);
        });
        for(auto& t:threads)t.join();
        EXPECT_EQ(accepted.load(),1);
        EXPECT_EQ(node.position().lsn,writers+1u);
    }
    service::Node recovered(tmp.path);
    EXPECT_EQ(recovered.position().lsn,writers+1u);
    for(int i=0;i<writers;++i) EXPECT_EQ(recovered.stats(1,i+1).value->volume,Id(i));
}
TEST(Storage, SnapshotRotationCrashWindowsAndHistoricalQueries) {
    test::TempDir tmp;
    auto dir=tmp.path;
    {
        service::Node node(dir);
        EXPECT_THROW(service::Node second(dir),Error);
        node.mutate(7,4,VertexUpdate{1,{10,true}});
        node.mutate(7,5,EdgeUpsert{{1,2,0},15});
        node.snapshot();
    }
    auto arc=dir/"archive-2.wal";
    ASSERT_TRUE(fs::exists(arc));
    // Snapshot rename durable, process dies immediately before WAL rotation.
    fs::rename(arc,dir/"active.wal");
    {
        service::Node n(dir);
        EXPECT_EQ(n.replayed(),0u);
        EXPECT_TRUE(n.connected(7,1,2).value);
        n.snapshot();
    }
    // Archive rename durable, process dies before creating next active file.
    fs::remove(dir/"active.wal");
    {
        service::Node n(dir);
        n.tick(15); n.snapshot();
        EXPECT_FALSE(n.connected(7,1,2).value);
        EXPECT_EQ(n.audit(7,6,2,2).value->stats,(Aggregate{2,2,10,1}));
    }
    service::Node recovered(dir);
    EXPECT_EQ(recovered.replayed(),0u);
    EXPECT_EQ(recovered.audit(7,6,2,2).value->stats,(Aggregate{2,2,10,1}));
}
TEST(Storage, CorruptSnapshotOrSealedArchiveFailsClosed) {
    test::TempDir tmp;
    { service::Node n(tmp.path); n.mutate(2,1,VertexUpdate{1,{5,false}}); n.snapshot(); }
    auto snapshot=tmp.path/"snapshot.bin";
    auto bytes=read_file(snapshot,snapshot_cap);
    auto bad=bytes; bad.back()^=1; atomic_file(snapshot,bad);
    EXPECT_THROW(service::Node n(tmp.path),Error);
    atomic_file(snapshot,bytes);
    auto archive=tmp.path/"archive-1.wal";
    bad=read_file(archive,4096); bad.pop_back(); atomic_file(archive,bad);
    EXPECT_THROW(service::Node n(tmp.path),Error);
    EXPECT_EQ(read_file(archive,4096),bad);
}
} // namespace
} // namespace fraud
