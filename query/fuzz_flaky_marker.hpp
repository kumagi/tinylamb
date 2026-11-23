/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#ifndef TINYLAMB_QUERY_FUZZ_FLAKY_MARKER_HPP
#define TINYLAMB_QUERY_FUZZ_FLAKY_MARKER_HPP

#include <string_view>

namespace tinylamb {

// Sentinel embedded in a fuzz finding's `verify` text when the serialized
// trace replays clean: the finding is flaky/nondeterministic and must not be
// persisted as a permanently green regression.  This lives in its own
// dependency-free header so light headers (e.g. sql_ooc_fuzzer.hpp) can share
// the exact literal without pulling in the heavy fuzz_scoped_db.hpp.
inline constexpr std::string_view kFlakyVerifyMarker =
    "[tinylamb-fuzz-replay-held-flaky]";

}  // namespace tinylamb

#endif  // TINYLAMB_QUERY_FUZZ_FLAKY_MARKER_HPP
