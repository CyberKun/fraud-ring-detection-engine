#include "storage/snapshot.hpp"
#include "snapshot.pb.h"
#include <openssl/sha.h>

namespace fraud::storage {
namespace {
void save_policy(disk::Policy* out,const Policy& p) {
    out->set_high(p.high); out->set_low(p.low); out->set_min_accounts(p.min_accounts);
    out->set_min_ratio(p.min_ratio); out->set_cooldown(p.cooldown);
}
Policy load_policy(const disk::Policy& p) {
    return {p.high(),p.low(),p.min_accounts(),p.min_ratio(),p.cooldown()};
}
}
Bytes serialize(const Image& state) {
    disk::Snapshot msg;
    msg.set_boundary_crc(state.boundary_crc);
    msg.set_format(1); msg.set_epoch(state.epoch); msg.set_applied_lsn(state.lsn);
    msg.set_committed_lsn(state.lsn); msg.set_time(state.time); msg.set_vertex_capacity(state.capacity);
    save_policy(msg.mutable_default_policy(),state.policy);
    for (const auto& s:state.tenants) {
        auto t=msg.add_tenants();
        t->set_id(s.tenant); t->set_epoch(s.epoch); t->set_lsn(s.lsn); t->set_time(s.time);
        t->set_next_handle(s.next_handle); t->set_total_volume(s.total_volume);
        save_policy(t->mutable_policy(),s.policy);
        for (const auto& v:s.vertices) {
            auto a=t->add_vertices(); a->set_id(v.id); a->set_volume(v.weight.volume);
            a->set_flagged(v.weight.flagged); a->set_suppressed(v.suppressed);
            if (v.cooldown) a->set_cooldown(*v.cooldown);
        }
        for (const auto& e:s.edges) {
            auto a=t->add_edges(); a->set_u(e.key.u); a->set_v(e.key.v); a->set_type(e.key.type);
            a->set_handle(e.handle); a->set_expiry(e.expiry); a->set_version(e.version); a->set_shadow(e.shadow);
        }
        for (auto d:s.deadlines) {
            auto a=t->add_deadlines(); a->set_expiry(d.expiry); a->set_handle(d.handle); a->set_version(d.version);
        }
        for (auto [p,q]:s.sequences) { auto a=t->add_sequences(); a->set_producer(p); a->set_sequence(q); }
    }
    auto size=msg.ByteSizeLong();
    if (size>snapshot_cap-8) throw Error("snapshot size cap");
    Bytes out(size+8);
    std::copy_n("FRS1",4,out.begin());
    if (!msg.SerializeToArray(out.data()+4,size)) throw Error("snapshot serialization");
    auto crc=checksum(std::span(out).first(size+4));
    for (unsigned i=0;i<4;++i) out[4+size+i]=crc>>(8*i);
    return out;
}
Image deserialize(std::span<const std::uint8_t> data) {
    if (data.size()<8 || data.size()>snapshot_cap || !std::equal(data.begin(),data.begin()+4,"FRS1"))
        throw Error("snapshot header");
    auto crc=checksum(data.first(data.size()-4));
    for (unsigned i=0;i<4;++i)
        if (data[data.size()-4+i]!=static_cast<std::uint8_t>(crc>>(8*i))) throw Error("snapshot CRC32");
    disk::Snapshot msg;
    if (!msg.ParseFromArray(data.data()+4,data.size()-8) || msg.format()!=1 || !msg.epoch() ||
        msg.committed_lsn()!=msg.applied_lsn()) throw Error("snapshot metadata");
    Image out{msg.epoch(),msg.applied_lsn(),msg.time(),load_policy(msg.default_policy()),msg.vertex_capacity()};
    out.boundary_crc=msg.boundary_crc();
    if (!out.capacity || !out.policy.valid()) throw Error("snapshot configuration");
    for (const auto& t:msg.tenants()) {
        TenantImage s{t.id(),t.epoch(),t.lsn(),t.next_handle(),t.total_volume(),t.time(),load_policy(t.policy())};
        if ((!out.tenants.empty() && out.tenants.back().tenant>=s.tenant) ||
            s.lsn>out.lsn || s.time>out.time || s.epoch>out.epoch) throw Error("snapshot tenant position");
        for (const auto& a:t.vertices()) s.vertices.push_back({a.id(),{a.volume(),a.flagged()},a.suppressed(),
            a.has_cooldown()?std::optional<Time>(a.cooldown()):std::nullopt});
        for (const auto& a:t.edges()) s.edges.push_back({{a.u(),a.v(),a.type()},a.handle(),a.version(),a.expiry(),a.shadow()});
        for (const auto& a:t.deadlines()) s.deadlines.push_back({a.expiry(),a.handle(),a.version()});
        for (const auto& a:t.sequences()) s.sequences.emplace_back(a.producer(),a.sequence());
        out.tenants.push_back(std::move(s));
    }
    return out;
}
std::string logical_checksum(const Image& image) {
    auto bytes=serialize(image);
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(bytes.data()+4,bytes.size()-8,digest);
    const char* digits="0123456789abcdef";
    std::string hex;
    for (auto b:digest) { hex+=digits[b>>4]; hex+=digits[b&15]; }
    return hex;
}
}
