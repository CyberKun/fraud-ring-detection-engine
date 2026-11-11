#pragma once
#include <cstddef>
#include <mutex>
#include <optional>
namespace fraud::replica {
// Reservations include the running writer and every request waiting for it.
class Admission {
    std::mutex mutex_;
    std::size_t requests_{}, bytes_{};

  public:
    struct Usage {
        std::size_t requests, bytes;
    };
    class Reservation {
        friend class Admission;
        Admission *owner_;
        std::size_t bytes_;
        Reservation(Admission &owner, std::size_t bytes) : owner_(&owner), bytes_(bytes) {}

      public:
        Reservation(const Reservation &) = delete;
        Reservation(Reservation &&other) noexcept : owner_(other.owner_), bytes_(other.bytes_) {
            other.owner_ = nullptr;
        }
        ~Reservation() {
            if (!owner_)
                return;
            std::lock_guard lock(owner_->mutex_);
            --owner_->requests_;
            owner_->bytes_ -= bytes_;
        }
    };
    std::optional<Reservation> reserve(std::size_t bytes, std::size_t max_requests, std::size_t max_bytes) {
        std::lock_guard lock(mutex_);
        if (requests_ >= max_requests || bytes_ > max_bytes || bytes > max_bytes - bytes_)
            return {};
        ++requests_;
        bytes_ += bytes;
        return Reservation(*this, bytes);
    }
    Usage usage() {
        std::lock_guard lock(mutex_);
        return {requests_, bytes_};
    }
};
} // namespace fraud::replica
