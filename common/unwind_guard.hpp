/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
//
// Runs a cleanup when the scope exits without Disarm().  Replaces
// `try { ... } catch (...) { cleanup; throw; }` so cleanup-on-failure paths
// need no catch/rethrow: an allocation failure unwinding through the scope
// still triggers the cleanup, and the normal return paths disarm it.
#ifndef TINYLAMB_COMMON_UNWIND_GUARD_HPP
#define TINYLAMB_COMMON_UNWIND_GUARD_HPP

#include <utility>

namespace tinylamb {

template <typename Cleanup>
class UnwindGuard {
 public:
  explicit UnwindGuard(Cleanup cleanup) : cleanup_(std::move(cleanup)) {}
  UnwindGuard(const UnwindGuard&) = delete;
  UnwindGuard& operator=(const UnwindGuard&) = delete;
  UnwindGuard(UnwindGuard&&) = delete;
  UnwindGuard& operator=(UnwindGuard&&) = delete;
  ~UnwindGuard() {
    if (armed_) {
      cleanup_();
    }
  }
  void Disarm() { armed_ = false; }

 private:
  Cleanup cleanup_;
  bool armed_{true};
};

}  // namespace tinylamb

#endif  // TINYLAMB_COMMON_UNWIND_GUARD_HPP
