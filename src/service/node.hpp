#pragma once
#include "storage/snapshot.hpp"
#include "engines/lct.hpp"
#include "engines/hdt.hpp"
#include "engines/offline.hpp"
#include <array>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <map>
#include <mutex>

namespace fraud::service {
namespace fs=std::filesystem;
using Clock=std::chrono::steady_clock;
struct Options {
    Policy policy;
    std::uint32_t capacity{50000};
    bool lct{};
};
struct Position { Id epoch,lsn; };
template<class T> struct Read { Id lsn; T value; };
struct Mutation { Status status; Position position; bool alert; };
struct Histogram {
    static constexpr std::array<double,11> bounds{.000001,.000005,.00001,.00005,.0001,.0005,.001,.005,.01,.1,1};
    std::array<Id,bounds.size()> buckets{};
    double sum{};
    Id count{};
    void observe(double);
    std::string render(std::string name,std::string labels={}) const;
};
class Node {
public:
    Node(fs::path dir,Options={});
    // Cluster projections are rebuilt from the independently fsynced replica journal.
    explicit Node(Options opts):options_(opts){}
    Batch propose(Id epoch,Id tenant,Time wall,Command,std::optional<ProducerSequence>);
    void replay(const storage::Frame&);
    void account_wal_write(storage::WriteStats);
    void reset();
    void load_checkpoint(const storage::Image&,std::span<const storage::Frame> prefix);
    std::vector<Id> tenant_ids() const;
    ~Node();
    Node(const Node&)=delete;
    Mutation mutate(Id tenant,Time wall_ms,Command,std::optional<ProducerSequence> = {});
    void tick(Time wall_ms);
    void snapshot();
    storage::Image image() const;
    Read<bool> connected(Id tenant,Id u,Id v);
    Read<std::optional<Aggregate>> stats(Id tenant,Id v);
    Read<std::optional<Subgraph>> subgraph(Id tenant,Id v,std::size_t k);
    Read<std::optional<AuditResult>> audit(Id tenant,Time at,Id v,std::size_t k);
    Position position() const;
    double ingestion_rate() const;
    Id subscribe() const;
    std::vector<Alert> poll(Id tenant,Id& cursor);
    std::string metrics() const;
    Id replayed() const { return replayed_; }
    Id truncated() const { return truncated_; }
private:
    using Model=std::variant<Tenant<HdtEngine>,Tenant<LctEngine>>;
    fs::path dir_;
    Options options_;
    int lock_fd_{-1};
    std::unique_ptr<storage::Wal> wal_;
    std::map<Id,Model> models_;
    std::map<Id,std::vector<Event>> history_;
    Id epoch_{1},lsn_{},time_{},segment_start_{},replayed_{},truncated_{};
    std::uint32_t boundary_crc_{};
    bool poisoned_{};
    struct Pending {
        Id tenant;
        Time wall;
        Command command;
        std::optional<ProducerSequence> source;
        Mutation result{};
        std::exception_ptr error;
        bool done{};
    };
    std::mutex admission_;
    std::condition_variable writers_;
    std::deque<Pending*> pending_;
    bool writing_{};
    mutable std::mutex mutex_;
    std::condition_variable alert_cv_;
    std::deque<Alert> alerts_;
    Id alert_end_{};
    enum Op { insert,extend,expire,manual,connection,component,sub,update,audit_op,ops };
    std::array<Histogram,ops> latency_;
    Histogram fsync_,snap_;
    Id bytes_{},hub_transitions_{},ingested_{};
    Clock::time_point started_{Clock::now()};
    Model& model(Id);
    template<class T,class F> void apply_measured(T&,const Payload&,F);
    void recover();
    void check() const;
    storage::Image image_locked() const;
    Batch prepare(Id,Time,Command,std::optional<ProducerSequence>,Id first);
    void persist(const std::vector<storage::Frame>&,const std::vector<Alert>&);
    void record_frame(const storage::Frame&);
    void commit_group(std::span<Pending*>);
};
} // namespace fraud::service
