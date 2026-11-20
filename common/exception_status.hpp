/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
//
// The single place that turns an in-flight exception into a Status.  Worker
// thread bodies and other no-throw boundaries call this from a catch block;
// nothing is rethrown to callers and no std::exception_ptr crosses threads.
#ifndef TINYLAMB_COMMON_EXCEPTION_STATUS_HPP
#define TINYLAMB_COMMON_EXCEPTION_STATUS_HPP

#include <exception>
#include <new>
#include <string>
#include <string_view>

#include "common/constants.hpp"
#include "common/status_or.hpp"

namespace tinylamb {

// Must be called from inside a catch handler.  std::bad_alloc (an out-of-memory
// condition) maps to kMemoryLimitExceeded, other std::exceptions keep their
// what() text as kRuntimeError, and anything else reports `fallback_message`.
// kNoSpace is reserved for genuine disk/page-space exhaustion (see ADR 0012).
[[nodiscard]] inline Status StatusFromCurrentException(
    std::string_view fallback_message) {
  try {
    throw;
  } catch (const std::bad_alloc&) {
    return Status::kMemoryLimitExceeded;
  } catch (const std::invalid_argument& error) {
    return StatusError(StatusCode::kInvalidArgument, error.what());
  } catch (const std::out_of_range& error) {
    return StatusError(StatusCode::kInvalidArgument, error.what());
  } catch (const std::length_error& error) {
    return StatusError(StatusCode::kTooBigData, error.what());
  } catch (const std::exception& error) {
    return StatusError(StatusCode::kRuntimeError, error.what());
  } catch (...) {
    return StatusError(StatusCode::kRuntimeError, std::string(fallback_message));
  }
}

}  // namespace tinylamb

#endif  // TINYLAMB_COMMON_EXCEPTION_STATUS_HPP
