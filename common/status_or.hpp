//
// Created by kumagi on 22/05/08.
//

#ifndef TINYLAMB_STATUS_OR_HPP
#define TINYLAMB_STATUS_OR_HPP

#include <cassert>
#include <optional>

#include "common/constants.hpp"

namespace tinylamb {

// A struct which could have value only when the status is success.
template <typename T>
class StatusOr {
 public:
  // Constructors are intentionally implicit!
  StatusOr(Status s) : status_(s), value_(std::nullopt) {}
  StatusOr(T v) : status_(Status::kSuccess), value_(std::move(v)) {}

  [[nodiscard]] bool HasValue() const { return status_ == Status::kSuccess; }
  T& Value() {
    assert(status_ == Status::kSuccess);
    return *value_;
  }
  const T& Value() const {
    assert(status_ == Status::kSuccess);
    return *value_;
  }
  [[nodiscard]] Status GetStatus() const { return status_; }

 private:
  Status status_;
  std::optional<T> value_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_STATUS_OR_HPP
