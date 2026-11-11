#include "replica/journal.hpp"
#include <fcntl.h>
#include <nlohmann/json.hpp>
#include <openssl/sha.h>
#include <sys/file.h>
#include <unistd.h>

namespace fraud::replica {
namespace fs = std::filesystem;
using json = nlohmann::json;
using storage::Error;
Journal::Journal(fs::path dir, std::string group, std::string replica)
    : dir_(fs::absolute(dir)), group_(std::move(group)), replica_(std::move(replica)) {
    if (fs::create_directories(dir_))
        storage::sync_directory(dir_.parent_path());
    lock_ = ::open(dir_.c_str(), O_DIRECTORY | O_RDONLY | O_CLOEXEC);
    if (lock_ < 0)
        throw Error("replica directory open");
    try {
        if (::flock(lock_, LOCK_EX | LOCK_NB))
            throw Error("replica directory busy");
        if (fs::exists(dir_ / "active.wal") || fs::exists(dir_ / "snapshot.bin"))
            throw Error("standalone data directory");
        auto meta = dir_ / "replica.meta", path = dir_ / "replica.wal";
        if (fs::exists(dir_ / "invariant.failed"))
            throw Invariant("replica requires invariant investigation");
        if (!fs::exists(meta)) {
            if (fs::exists(path))
                throw Error("replica metadata missing");
            save();
        }
        auto bytes = storage::read_file(meta, 65536);
        auto envelope = json::parse(bytes);
        auto data = envelope.at("data").get<std::string>();
        if (storage::checksum({reinterpret_cast<const uint8_t *>(data.data()), data.size()}) !=
            envelope.at("crc32"))
            throw Error("corrupt replica metadata");
        auto m = json::parse(data);
        if (m.at("version") != 1 || m.at("group") != group_ || m.at("replica") != replica_)
            throw Error("replica identity mismatch");
        promised_ = m.at("promise");
        owner_ = m.at("owner");
        committed_ = m.at("commit_index");
        if (fs::exists(path))
            storage::recover(path, {}, true, [&](const auto &f) { remember(f, storage::encode(f)); });
        if (at(committed_).lsn != m.at("commit_lsn") || at(committed_).digest != m.at("commit_hash") ||
            marks_.back().epoch > promised_)
            throw Invariant("durable replica metadata disagrees with WAL");
        wal_ = std::make_unique<storage::Wal>(path);
    } catch (...) {
        ::close(lock_);
        throw;
    }
}
Journal::~Journal() { ::close(lock_); }
void Journal::save() {
    auto b = at(committed_);
    auto data = json{{"version", 1},         {"group", group_},        {"replica", replica_},
                     {"promise", promised_}, {"owner", owner_},        {"commit_index", committed_},
                     {"commit_lsn", b.lsn},  {"commit_hash", b.digest}}
                    .dump();
    auto crc = storage::checksum({reinterpret_cast<const uint8_t *>(data.data()), data.size()});
    auto envelope = json{{"crc32", crc}, {"data", data}}.dump();
    storage::atomic_file(dir_ / "replica.meta",
                         {reinterpret_cast<const uint8_t *>(envelope.data()), envelope.size()});
}
State Journal::state() const { return {promised_, marks_.back(), at(committed_)}; }
const Boundary &Journal::at(Id i) const {
    if (i >= marks_.size())
        throw Rejected("unknown frame boundary");
    return marks_[i];
}
void Journal::fence(Id epoch, const std::string &owner) const {
    if (!epoch || epoch != promised_ || owner != owner_)
        throw Rejected("fenced epoch or owner");
}
void Journal::promise(Id epoch, const std::string &owner) {
    if (epoch <= promised_) {
        fence(epoch, owner);
        return;
    }
    promised_ = epoch;
    owner_ = owner;
    save();
}
void Journal::remember(const storage::Frame &f, storage::Bytes bytes) {
    auto size = bytes.size();
    const auto &prev = marks_.back().digest;
    bytes.insert(bytes.end(), prev.begin(), prev.end());
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(bytes.data(), bytes.size(), digest);
    std::string hex;
    constexpr char digits[] = "0123456789abcdef";
    for (auto b : digest) {
        hex += digits[b >> 4];
        hex += digits[b & 15];
    }
    marks_.push_back({marks_.size(), f.back().epoch, f.back().lsn, std::move(hex)});
    offsets_.push_back(offsets_.back() + size);
    frames_.push_back(f);
}
storage::WriteStats Journal::append(const Boundary &prev, const storage::Frame &f) {
    if (prev != marks_.back() || f.empty() || f.front().lsn != prev.lsn + 1 || f.front().epoch < prev.epoch ||
        f.front().epoch > promised_ || (!frames_.empty() && f.front().time < frames_.back().back().time))
        throw Rejected("append prefix mismatch");
    auto bytes = storage::encode(f);
    auto stats = wal_->append(std::span(&f, 1));
    remember(f, std::move(bytes));
    return stats;
}
void Journal::truncate(const Boundary &expected, const Boundary &keep) {
    if (expected != marks_.back() || at(keep.index) != keep)
        throw Rejected("truncate prefix changed");
    if (keep.index < committed_) {
        storage::atomic_file(dir_ / "invariant.failed", {});
        throw Invariant("attempt to truncate below durable commit_lsn");
    }
    if (keep == expected)
        return;
    wal_->truncate(offsets_[keep.index]);
    frames_.resize(keep.index);
    marks_.resize(keep.index + 1);
    offsets_.resize(keep.index + 1);
    ++truncations_;
}
void Journal::commit(const Boundary &b) {
    if (at(b.index) != b)
        throw Rejected("commit prefix mismatch");
    if (b.index <= committed_)
        return;
    committed_ = b.index;
    save();
}
} // namespace fraud::replica
