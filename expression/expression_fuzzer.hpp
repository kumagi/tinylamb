/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXPRESSION_EXPRESSION_FUZZER_HPP
#define TINYLAMB_EXPRESSION_EXPRESSION_FUZZER_HPP

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "common/status_or.hpp"

namespace tinylamb {

// Issue #33: the expression_fuzzer differential (AST ground truth vs.
// bytecode VM vs. relational_detail::Evaluate) previously only aborted on
// violations, leaving "wrong answer" artifacts in ad-hoc findings dirs with
// no replay or committed regression path (invariant 3).  The harness body
// is exposed here so a libFuzzer violation can be serialized as a
// self-contained `.test` file and replayed by expression_fuzzer_test.
struct ExpressionFuzzResult {
  bool ok{true};
  std::string violation;  // populated when ok == false
};

// Runs one raw libFuzzer input through the full differential pipeline.
// Inputs that the harness ignores (too small / too large / unparseable
// expression) return ok == true, so replaying them is a no-op guard.
ExpressionFuzzResult ExpressionFuzzRun(const uint8_t* data, size_t size);

// Self-contained `.test` format: '#' comment header lines followed by the
// raw input as lowercase hex (whitespace-insensitive body).
std::string ExpressionFuzzReproText(const uint8_t* data, size_t size);
StatusOr<std::vector<uint8_t>> DecodeExpressionFuzzRepro(std::string_view text);

// Writes `expression_fuzzer_fuzz-repro-<hash>.test` for a violating input
// into the current working directory (libFuzzer artifact convention; see
// AGENTS.md "Bug hunting with SQL oracle fuzzers").
void WriteExpressionFuzzRepro(const uint8_t* data, size_t size);

}  // namespace tinylamb

#endif  // TINYLAMB_EXPRESSION_EXPRESSION_FUZZER_HPP
