#pragma once
#include "storage/wal.hpp"
#include <memory>

namespace fraud::replica {
struct Boundary {
    Id index{}, epoch{}, lsn{};
    std::string digest;
    bool operator==(const Boundary &) const = default;
};
struct Invariant : storage::Error {
    using Error::Error;
};
struct Rejected : std::runtime_error {
    using runtime_error::runtime_error;
};
struct State {
    Id promised{};
    Boundary tail, commit;
};
class Journal {
  public:
    Journal(std::filesystem::path, std::string group, std::string replica);
    ~Journal();
    void promise(Id epoch, const std::string &owner);
    void fence(Id epoch, const std::string &owner) const;
    State state() const;
    const Boundary &at(Id index) const;
    const std::vector<storage::Frame> &frames() const { return frames_; }
    storage::WriteStats append(const Boundary &previous, const storage::Frame &);
    void truncate(const Boundary &expected_tail, const Boundary &keep);
    void commit(const Boundary &);
    Id truncations() const { return truncations_; }

  private:
    std::filesystem::path dir_;
    std::string group_, replica_, owner_;
    Id promised_{}, committed_{}, truncations_{};
    int lock_{-1};
    std::unique_ptr<storage::Wal> wal_;
    std::vector<storage::Frame> frames_;
    std::vector<Boundary> marks_{{}};
    std::vector<Id> offsets_{0};
    void save();
    void remember(const storage::Frame &, storage::Bytes);
};
} // namespace fraud::replica
