#include "service/node.hpp"
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>
#include <sstream>
#include <charconv>
#include <iomanip>

namespace fraud::service {
namespace {
double elapsed(Clock::time_point start){return std::chrono::duration<double>(Clock::now()-start).count();}
std::uint32_t boundary(const storage::Frame& f) {
    auto bytes=storage::encode(f);
    return storage::checksum(std::span(bytes).first(bytes.size()-4));
}
}
void Histogram::observe(double seconds) {
    ++count; sum+=seconds;
    for(std::size_t i=0;i<bounds.size();++i) if(seconds<=bounds[i]) ++buckets[i];
}
std::string Histogram::render(std::string name,std::string labels) const {
    std::ostringstream s;
    auto label=labels.empty()?std::string{}:labels+",";
    for(std::size_t i=0;i<bounds.size();++i)
        s<<name<<"_bucket{"<<label<<"le=\""<<bounds[i]<<"\"} "<<buckets[i]<<'\n';
    s<<name<<"_bucket{"<<label<<"le=\"+Inf\"} "<<count<<'\n';
    auto suffix=labels.empty()?std::string{}:"{"+labels+"}";
    s<<name<<"_sum"<<suffix<<" "<<std::setprecision(15)<<sum<<'\n'<<name<<"_count"<<suffix<<" "<<count<<'\n';
    return s.str();
}
Node::Node(fs::path dir,Options opts):dir_(fs::absolute(dir)),options_(opts) {
    if (!opts.capacity || !opts.policy.valid()) throw storage::Error("invalid node configuration");
    if (fs::create_directories(dir_)) storage::sync_directory(dir_.parent_path());
    lock_fd_=::open(dir_.c_str(),O_DIRECTORY|O_RDONLY|O_CLOEXEC);
    if (lock_fd_<0) throw storage::Error("cannot open data directory");
    try {
        if (::flock(lock_fd_,LOCK_EX|LOCK_NB)) throw storage::Error("data directory already in use");
        recover();
    } catch (...) { ::close(lock_fd_); throw; }
}
Batch Node::propose(Id epoch,Id tenant,Time wall,Command command,std::optional<ProducerSequence> source) {
    std::lock_guard lock(mutex_);
    epoch_=epoch;
    auto batch=prepare(tenant,std::max(time_,wall),command,source,lsn_+1);
    if(batch.status==Status::ok) record_frame(batch.records);
    return batch;
}
void Node::account_wal_write(storage::WriteStats stats) {
    std::lock_guard lock(mutex_);
    bytes_+=stats.bytes; fsync_.observe(stats.sync_seconds);
}
void Node::replay(const storage::Frame& frame) {
    std::lock_guard lock(mutex_);
    if(frame.front().lsn!=lsn_+1) throw storage::Error("projection prefix mismatch");
    std::vector<Alert> alerts;
    for(auto& event:frame) {
        Status status{};
        std::visit([&](auto& tenant){
            apply_measured(tenant,event.payload,[&]{status=tenant.apply_record(event,alerts);});
        },model(event.tenant));
        if(status!=Status::ok && status!=Status::duplicate) throw storage::Error("replicated transition invalid");
    }
    record_frame(frame); ++ingested_;
    for(auto& alert:alerts) {
        alerts_.push_back(alert); ++alert_end_;
        if(alerts_.size()>4096) alerts_.pop_front();
    }
    alert_cv_.notify_all();
}
void Node::load_checkpoint(const storage::Image& image,std::span<const storage::Frame> prefix) {
    std::lock_guard lock(mutex_);
    if(lsn_ || !models_.empty() || prefix.empty()) throw storage::Error("invalid checkpoint destination");
    const auto& last=prefix.back().back();
    if(image.lsn!=last.lsn || image.epoch!=last.epoch || image.time!=last.time ||
       image.boundary_crc!=boundary(prefix.back()) || image.capacity!=options_.capacity)
        throw storage::Error("checkpoint disagrees with committed WAL");
    options_.policy=image.policy;
    for(const auto& t:image.tenants)
        if(!std::visit([&](auto& m){return m.restore(t);},model(t.tenant))) throw storage::Error("invalid replica checkpoint");
    for(const auto& frame:prefix)
        for(const auto& e:frame) history_[e.tenant].push_back(e);
    epoch_=image.epoch;lsn_=image.lsn;time_=image.time;boundary_crc_=image.boundary_crc;
}
void Node::reset() {
    std::lock_guard lock(mutex_);
    models_.clear(); history_.clear(); alerts_.clear(); lsn_=time_=boundary_crc_=0; epoch_=1;
}
std::vector<Id> Node::tenant_ids() const {
    std::lock_guard lock(mutex_);
    std::vector<Id> ids;
    for(auto& [id,m]:models_) ids.push_back(id);
    return ids;
}
Node::~Node(){ if(lock_fd_>=0) ::close(lock_fd_); }
void Node::check() const {
    if (poisoned_) throw storage::Error("shard unavailable after storage error; restart required");
}
Node::Model& Node::model(Id id) {
    auto it=models_.find(id);
    if(it!=models_.end()) return it->second;
    if (options_.lct) return models_.try_emplace(id,std::in_place_type<Tenant<LctEngine>>,id,options_.policy,
                                                 LctEngine(options_.capacity)).first->second;
    return models_.try_emplace(id,std::in_place_type<Tenant<HdtEngine>>,id,options_.policy,
                               HdtEngine(options_.capacity)).first->second;
}
storage::Image Node::image_locked() const {
    storage::Image s{epoch_,lsn_,time_,options_.policy,options_.capacity};
    for(const auto& [id,m]:models_) s.tenants.push_back(std::visit([](auto& t){return t.image();},m));
    s.boundary_crc=boundary_crc_;
    return s;
}
storage::Image Node::image() const { std::lock_guard guard(mutex_); check(); return image_locked(); }
void Node::recover() {
    auto snapshot=dir_/"snapshot.bin";
    if(fs::exists(snapshot)) {
        auto s=storage::deserialize(storage::read_file(snapshot,storage::snapshot_cap));
        options_.capacity=s.capacity; options_.policy=s.policy;
        for(auto& t:s.tenants)
            if(!std::visit([&](auto& m){return m.restore(t);},model(t.tenant))) throw storage::Error("invalid tenant snapshot");
        epoch_=s.epoch; lsn_=s.lsn; time_=s.time; boundary_crc_=s.boundary_crc;
    }
    Id saved=lsn_;
    bool found=!saved;
    auto consume=[&](const storage::Frame& f) {
        if (f.back().lsn<=saved) {
            for(auto& e:f) history_[e.tenant].push_back(e);
            if(f.back().lsn==saved) {
                if(boundary(f)!=boundary_crc_ || f.back().epoch!=epoch_ || f.back().time!=time_)
                    throw storage::Error("snapshot boundary disagrees with WAL");
                found=true;
            }
            return;
        }
        if(!found) throw storage::Error("snapshot misses a frame boundary");
        for(auto& e:f) {
            std::vector<Alert> ignored;
            auto status=std::visit([&](auto& t){return t.apply_record(e,ignored);},model(e.tenant));
            if(status!=Status::ok && status!=Status::duplicate) throw storage::Error("invalid WAL transition");
        }
        record_frame(f);
        replayed_+=f.size();
    };
    std::map<Id,fs::path> archives;
    for(const auto& file:fs::directory_iterator(dir_)) {
        auto name=file.path().filename().string();
        if(!name.starts_with("archive-") || !name.ends_with(".wal")) continue;
        Id end;
        auto last=name.data()+name.size()-4;
        auto [p,ec]=std::from_chars(name.data()+8,last,end);
        if(ec!=std::errc{} || p!=last || !archives.emplace(end,file.path()).second)
            throw storage::Error("invalid WAL archive name");
    }
    storage::Recovery r;
    for(auto& [end,path]:archives) {
        r=storage::recover(path,r,false,consume);
        if(r.lsn!=end || !end) throw storage::Error("archive boundary mismatch");
    }
    segment_start_=r.lsn;
    auto active=dir_/"active.wal";
    if(fs::exists(active)) r=storage::recover(active,r,true,consume);
    if(!found) throw storage::Error("retained WAL does not cover snapshot");
    truncated_=r.truncated;
    wal_=std::make_unique<storage::Wal>(active);
    if(!fs::exists(snapshot) && !lsn_) storage::atomic_file(snapshot,storage::serialize(image_locked()));
}
void Node::record_frame(const storage::Frame& f) {
    for(auto& e:f) history_[e.tenant].push_back(e);
    auto& last=f.back(); epoch_=last.epoch; lsn_=last.lsn; time_=last.time;
    boundary_crc_=boundary(f);
}
void Node::persist(const std::vector<storage::Frame>& frames,const std::vector<Alert>& alerts) {
    if(frames.empty()) return;
    auto w=wal_->append(frames);
    fsync_.observe(w.sync_seconds);
    bytes_+=w.bytes;
    for(auto& f:frames) record_frame(f);
    for(auto a:alerts) {
        alerts_.push_back(a);
        ++alert_end_;
        if(alerts_.size()>4096) alerts_.pop_front();
    }
    if(!alerts.empty()) alert_cv_.notify_all();
}
template<class T,class F>
void Node::apply_measured(T& t,const Payload& p,F apply) {
    std::optional<Op> op;
    if(auto x=std::get_if<EdgeUpsert>(&p)) op=t.edge(x->key)?extend:insert;
    else if(std::holds_alternative<Expire>(p)) op=expire;
    else if(std::holds_alternative<EdgeDelete>(p)) op=manual;
    else if(std::holds_alternative<VertexUpdate>(p)) op=update;
    auto start=Clock::now(); apply();
    if(op) latency_[*op].observe(elapsed(start));
    if(std::holds_alternative<HubRestore>(p) || std::holds_alternative<HubSuppress>(p)) ++hub_transitions_;
}
Batch Node::prepare(Id id,Time now,Command c,std::optional<ProducerSequence> source,Id first) {
    bool fresh=!models_.contains(id);
    auto b=std::visit([&](auto& t){
        auto observer=[&](const Payload& p,auto apply) {
            apply_measured(t,p,apply);
        };
        return t.submit(epoch_,first,now,c,source,observer);
    },model(id));
    if(fresh && b.status!=Status::ok) models_.erase(id);
    return b;
}
void Node::commit_group(std::span<Pending*> requests) {
    std::lock_guard guard(mutex_); check();
    try {
        std::vector<storage::Frame> frames;
        std::vector<Alert> alerts;
        Id next=lsn_+1;
        Time last=time_;
        for(auto* p:requests) {
            Time now=std::max(last,p->wall);
            auto b=prepare(p->tenant,now,p->command,p->source,next);
            if(b.status==Status::ok) {
                last=now; next+=b.records.size();
                frames.push_back(std::move(b.records));
                alerts.insert(alerts.end(),b.alerts.begin(),b.alerts.end());
            }
            p->result={b.status,{epoch_,next-1},!b.alerts.empty()};
        }
        persist(frames,alerts);
        ingested_+=frames.size();
    } catch (...) { poisoned_=true; throw; }
}
Mutation Node::mutate(Id id,Time wall,Command cmd,std::optional<ProducerSequence> source) {
    Pending request{id,wall,cmd,source};
    std::unique_lock lock(admission_);
    pending_.push_back(&request);
    writers_.wait(lock,[&]{return request.done || (!writing_ && pending_.front()==&request);});
    if(!request.done) {
        writing_=true;
        std::array<Pending*,64> group;
        std::size_t n=0;
        while(n<group.size() && !pending_.empty()) {
            group[n++]=pending_.front(); pending_.pop_front();
        }
        lock.unlock();
        std::exception_ptr error;
        try {commit_group(std::span(group).first(n));} catch (...) {error=std::current_exception();}
        lock.lock();
        for(std::size_t i=0;i<n;++i){group[i]->error=error;group[i]->done=true;}
        writing_=false;
        writers_.notify_all();
    }
    if(request.error) std::rethrow_exception(request.error);
    return request.result;
}
void Node::tick(Time wall) {
    std::lock_guard guard(mutex_); check();
    try {
        std::vector<storage::Frame> frames;
        std::vector<Alert> alerts;
        Id next=lsn_+1;
        for(auto& [tenant,m]:models_) {
            auto b=prepare(tenant,std::max(wall,time_),Tick{},{},next);
            if(b.status!=Status::ok) throw storage::Error("tick rejected");
            next+=b.records.size();
            frames.push_back(std::move(b.records));
            alerts.insert(alerts.end(),b.alerts.begin(),b.alerts.end());
        }
        persist(frames,alerts);
    } catch (...) { poisoned_=true; throw; }
}
void Node::snapshot() {
    std::lock_guard guard(mutex_); check();
    auto start=Clock::now();
    try {
        storage::atomic_file(dir_/"snapshot.bin",storage::serialize(image_locked()));
        if(lsn_>segment_start_) {
            auto archive=dir_/("archive-"+std::to_string(lsn_)+".wal");
            if(fs::exists(archive)) throw storage::Error("archive already exists");
            fs::rename(dir_/"active.wal",archive);
            storage::sync_directory(dir_);
            wal_=std::make_unique<storage::Wal>(dir_/"active.wal");
            segment_start_=lsn_;
        }
        snap_.observe(elapsed(start));
    } catch (...) { poisoned_=true; throw; }
}
Position Node::position() const { std::lock_guard guard(mutex_); check(); return {epoch_,lsn_}; }
double Node::ingestion_rate() const {
    std::lock_guard guard(mutex_); return ingested_/std::max(1e-9,elapsed(started_));
}
Read<bool> Node::connected(Id id,Id u,Id v) {
    std::lock_guard guard(mutex_); check(); auto start=Clock::now();
    bool result=models_.contains(id) && std::visit([&](auto& t){return t.engine().connected(u,v);},models_.at(id));
    latency_[connection].observe(elapsed(start)); return {lsn_,result};
}
Read<std::optional<Aggregate>> Node::stats(Id id,Id v) {
    std::lock_guard guard(mutex_); check(); auto start=Clock::now();
    std::optional<Aggregate> result;
    if(models_.contains(id)) result=std::visit([&](auto& t){return t.engine().stats(v);},models_.at(id));
    latency_[component].observe(elapsed(start)); return {lsn_,result};
}
Read<std::optional<Subgraph>> Node::subgraph(Id id,Id v,std::size_t k) {
    std::lock_guard guard(mutex_); check(); auto start=Clock::now();
    std::optional<Subgraph> result;
    if(models_.contains(id)) result=std::visit([&](auto& t){return t.subgraph(v,k);},models_.at(id));
    latency_[sub].observe(elapsed(start)); return {lsn_,std::move(result)};
}
Read<std::optional<AuditResult>> Node::audit(Id id,Time at,Id v,std::size_t k) {
    std::vector<Event> events; Id watermark; Policy policy;
    {
        std::lock_guard guard(mutex_); check(); watermark=lsn_; policy=options_.policy;
        if(auto it=history_.find(id);it!=history_.end()) {
            auto end=std::upper_bound(it->second.begin(),it->second.end(),at,[](Time t,const Event& e){return t<e.time;});
            events.assign(it->second.begin(),end);
        }
    }
    if(events.empty()) return {watermark,{}};
    auto start=Clock::now();
    std::vector<Time> times;
    // Engine A takes an exclusive end. Use dense time coordinates if the
    // terminal timestamp has no representable successor.
    if(events.back().time==UINT64_MAX) for(auto& e:events) {
        if(times.empty() || times.back()!=e.time) times.push_back(e.time);
        e.time=times.size()-1;
    }
    auto engine=OfflineEngine::build(id,policy,events,events.back().time+1);
    if(!engine) throw storage::Error("invalid retained history");
    auto result=engine->audit_at(events.back().time,v,k);
    if(result && result->first_alert_timestamp_ms && !times.empty())
        result->first_alert_timestamp_ms=times.at(*result->first_alert_timestamp_ms);
    { std::lock_guard guard(mutex_); latency_[audit_op].observe(elapsed(start)); }
    return {watermark,std::move(result)};
}
Id Node::subscribe() const { std::lock_guard guard(mutex_); check(); return alert_end_; }
std::vector<Alert> Node::poll(Id tenant,Id& cursor) {
    std::unique_lock lock(mutex_);
    alert_cv_.wait_for(lock,std::chrono::milliseconds(100),[&]{return cursor!=alert_end_ || poisoned_;});
    check();
    Id base=alert_end_-alerts_.size();
    if(cursor<base) throw std::overflow_error("alert subscriber fell behind");
    std::vector<Alert> out;
    for(;cursor<alert_end_;++cursor) {
        const auto& a=alerts_[cursor-base];
        if(a.tenant==tenant) out.push_back(a);
    }
    return out;
}
std::string Node::metrics() const {
    std::unique_lock guard(mutex_);
    const auto latency=latency_;
    const auto fsync=fsync_,snapshot=snap_;
    const Id bytes=bytes_,transitions=hub_transitions_;
    Id memory=0,hubs=0,levels=0,scan=0,promote=0,repair=0,vertices=0;
    for(const auto& [id,m]:models_) std::visit([&](const auto& t){
        const auto& e=t.engine(); const auto& c=e.counters();
        hubs+=t.suppressed_count(); memory+=e.storage_bytes();
        if constexpr(requires{c.levels_traversed;}) {
            levels+=c.levels_traversed; scan+=c.candidates; promote+=c.tree_promotions+c.nontree_promotions;
        } else { repair+=c.repairs; vertices+=c.repair_vertices; }
    },m);
    guard.unlock();
    std::ostringstream out;
    const char* names[]={"insert","extend_ttl","expire","manual_delete","connected","component_stats","subgraph","vertex_update","audit"};
    out<<"# TYPE fraud_operation_latency_seconds histogram\n";
    for(unsigned i=0;i<ops;++i) out<<latency[i].render("fraud_operation_latency_seconds","op=\""+std::string(names[i])+"\"");
    out<<"# TYPE wal_fsync_latency_seconds histogram\n"<<fsync.render("wal_fsync_latency_seconds")
       <<"# TYPE snapshot_duration_seconds histogram\n"<<snapshot.render("snapshot_duration_seconds");
    auto metric=[&](const char* n,const char* type,Id val){out<<"# TYPE "<<n<<" "<<type<<'\n'<<n<<" "<<val<<'\n';};
    metric("wal_bytes_written_total","counter",bytes);
    metric("hdt_replacement_search_levels_traversed","counter",levels);
    metric("hdt_nontree_edges_scanned_total","counter",scan);
    metric("hdt_edge_promotions_total","counter",promote);
    metric("lct_repair_triggered_total","counter",repair);
    metric("lct_repair_subtree_vertices_scanned","counter",vertices);
    metric("hub_suppressed_active_count","gauge",hubs);
    metric("hub_transition_events_total","counter",transitions);
    out<<"# HELP engine_memory_bytes Live graph index storage in bytes, excluding allocator overhead.\n";
    metric("engine_memory_bytes","gauge",memory);
    return out.str();
}
} // namespace fraud::service
