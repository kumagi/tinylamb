/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
//
// Deprecated exception shims: the one sanctioned place where library code
// may still throw.  Layers converted to Status/StatusOr keep their old
// throwing entry points as thin wrappers around this helper so engine layers
// that have not been converted yet preserve their current behaviour
// (throw -> boundary catch).  Phase 7 removes every call site of
// ExcShimUnwrap and deletes this file; grep for "ExcShimUnwrap" to audit.
//
#ifndef TINYLAMB_EXC_SHIM_HPP
#define TINYLAMB_EXC_SHIM_HPP

#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "common/constants.hpp"
#include "common/status_or.hpp"

namespace tinylamb {
namespace detail {

[[noreturn]] inline void ExcShimThrow(std::string_view /*what*/,
                                      const Status& status) {
  // The message must round-trip verbatim: differential tests and server
  // error strings compare these texts, and the old throw sites carried
  // exactly this payload without any wrapper prefix.
  throw std::runtime_error(status.GetMessage().empty()
                               ? std::string(ToString(status.GetCode()))
                               : status.GetMessage());
}

}  // namespace detail

// Status-only variant: rethrow a failed Status through the shim channel.
inline void ExcShimCheck(std::string_view what, const Status& status) {
  if (status != Status::kSuccess) {
    detail::ExcShimThrow(what, status);
  }
}

template <typename T>
[[nodiscard]] inline T ExcShimUnwrap(StatusOr<T>&& result,
                                     std::string_view what) {
  if (!result.HasValue()) {
    detail::ExcShimThrow(what, result.GetStatus());
  }
  return result.MoveValue();
}

}  // namespace tinylamb

#endif  // TINYLAMB_EXC_SHIM_HPP
