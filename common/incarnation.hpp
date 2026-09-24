#ifndef TINYLAMB_INCARNATION_HPP
#define TINYLAMB_INCARNATION_HPP

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "common/constants.hpp"

namespace tinylamb {

class TableIncarnation {
 public:
  enum class State : uint8_t { kLive, kDropPending, kRetired };

  bool Acquire(txn_id_t owner) {
    std::scoped_lock lock(mutex_);
    if (state_ != State::kLive) {
      return false;
    }
    ++active_leases_;
    ++owner_leases_[owner];
    return true;
  }

  void Release(txn_id_t owner) {
    std::scoped_lock lock(mutex_);
    if (active_leases_ == 0) {
      return;
    }
    if (active_leases_ == 1) {
      active_leases_ = 0;
      owner_leases_.clear();
      quiescent_.notify_all();
      return;
    }
    --active_leases_;
    auto found = owner_leases_.find(owner);
    if (found != owner_leases_.end()) {
      if (--found->second == 0) {
        owner_leases_.erase(found);
      }
    }
    if (active_leases_ == 0) {
      quiescent_.notify_all();
    }
  }

  bool BeginDrop() {
    std::scoped_lock lock(mutex_);
    if (state_ != State::kLive) {
      return false;
    }
    state_ = State::kDropPending;
    return true;
  }

  bool WaitForQuiescence(txn_id_t owner) {
    std::unique_lock lock(mutex_);
    if (owner_leases_.contains(owner)) {
      return false;
    }
    quiescent_.wait(lock, [&] { return active_leases_ == 0; });
    return true;
  }

  void CommitDrop() {
    std::scoped_lock lock(mutex_);
    if (state_ == State::kDropPending) {
      state_ = State::kRetired;
    }
    quiescent_.notify_all();
  }

  void AbortDrop() {
    std::scoped_lock lock(mutex_);
    if (state_ == State::kDropPending) {
      state_ = State::kLive;
    }
    quiescent_.notify_all();
  }

 private:
  mutable std::mutex mutex_;
  std::condition_variable quiescent_;
  State state_{State::kLive};
  size_t active_leases_{0};
  std::unordered_map<txn_id_t, size_t> owner_leases_;
};

class TableLease {
 public:
  TableLease() = default;
  TableLease(const TableLease&) = delete;
  TableLease& operator=(const TableLease&) = delete;
  TableLease(TableLease&& other) noexcept
      : incarnation_(std::move(other.incarnation_)), owner_(other.owner_) {
    other.owner_ = static_cast<txn_id_t>(-1);
  }
  TableLease& operator=(TableLease&& other) noexcept {
    if (this != &other) {
      Release();
      incarnation_ = std::move(other.incarnation_);
      owner_ = other.owner_;
      other.owner_ = static_cast<txn_id_t>(-1);
    }
    return *this;
  }
  ~TableLease() { Release(); }

  void Release() {
    if (incarnation_ != nullptr) {
      incarnation_->Release(owner_);
      incarnation_.reset();
      owner_ = static_cast<txn_id_t>(-1);
    }
  }

  [[nodiscard]] bool Valid() const { return incarnation_ != nullptr; }

 private:
  friend class Table;
  TableLease(std::shared_ptr<TableIncarnation> incarnation, txn_id_t owner)
      : incarnation_(std::move(incarnation)), owner_(owner) {}

  std::shared_ptr<TableIncarnation> incarnation_;
  txn_id_t owner_{static_cast<txn_id_t>(-1)};
};

}  // namespace tinylamb

#endif  // TINYLAMB_INCARNATION_HPP
