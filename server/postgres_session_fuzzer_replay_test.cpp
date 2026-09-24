/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
//
// Pins the two wave-5 ASan findings: a QueryResult whose executor outlived
// the TransactionContext that FailStatement() freed (use-after-free in
// ~RelationalExecutor) was reported by `postgres_session_fuzzer` with these
// exact script bytes.  Replaying them must stay clean.

#include <cstdint>
#include <string>

#include "gtest/gtest.h"
#include "server/postgres_session_fuzzer.hpp"

namespace tinylamb {
namespace {

std::string Unhex(const std::string& hex) {
  std::string out;
  for (size_t i = 0; i + 1 < hex.size(); i += 2) {
    out.push_back(static_cast<char>(std::stoi(hex.substr(i, 2), nullptr, 16)));
  }
  return out;
}

TEST(PostgresSessionFuzzerReplay, FailStatementOrderingCrash) {
  const std::string input = Unhex("0a00000a32");
  TryPostgresSession(reinterpret_cast<const uint8_t*>(input.data()),
                     input.size(), false);
}

TEST(PostgresSessionFuzzerReplay, FailStatementOrderingCrash2) {
  const std::string input = Unhex("640013640013");
  TryPostgresSession(reinterpret_cast<const uint8_t*>(input.data()),
                     input.size(), false);
}

}  // namespace
}  // namespace tinylamb
