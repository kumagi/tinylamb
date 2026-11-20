/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#ifndef TINYLAMB_POSTGRES_SESSION_FUZZER_HPP
#define TINYLAMB_POSTGRES_SESSION_FUZZER_HPP

#include <cstddef>
#include <cstdint>

namespace tinylamb {

// Drive the Simple-Query server as a real loopback client with a fuzzed
// message script, asserting the wire-protocol contract after every step.
// Returns false on an observed illegal state (T4876): the fuzz entry turns
// false into abort(), and the replay tests assert the value so the harness
// can never degrade to report-only-and-green.
bool TryPostgresSession(const uint8_t* data, size_t size, bool verbose);

}  // namespace tinylamb

#endif  // TINYLAMB_POSTGRES_SESSION_FUZZER_HPP
