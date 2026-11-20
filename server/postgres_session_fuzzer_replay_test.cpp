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
  if (hex.size() % 2 != 0) {
    throw std::invalid_argument("odd-length hex string");
  }
  std::string out;
  for (size_t i = 0; i + 1 < hex.size(); i += 2) {
    auto byte = hex.substr(i, 2);
    size_t consumed = 0;
    int value = 0;
    try {
      value = std::stoi(byte, &consumed, 16);
    } catch (const std::exception&) {
      throw std::invalid_argument("bad hex byte: " + byte);
    }
    if (consumed != byte.size() || value < 0 || value > 255) {
      throw std::invalid_argument("bad hex byte: " + byte);
    }
    out.push_back(static_cast<char>(value));
  }
  return out;
}

TEST(PostgresSessionFuzzerReplay, FailStatementOrderingCrash) {
  const std::string input = Unhex("0a00000a32");
  // T4876: no-crash is not the only contract -- the harness reports clean,
  // so a silent degradation of its checks turns red here instead.
  EXPECT_TRUE(TryPostgresSession(reinterpret_cast<const uint8_t*>(input.data()),
                                 input.size(), false))
      << "replay observed an illegal state without crashing";
}

TEST(PostgresSessionFuzzerReplay, FailStatementOrderingCrash2) {
  const std::string input = Unhex("640013640013");
  EXPECT_TRUE(TryPostgresSession(reinterpret_cast<const uint8_t*>(input.data()),
                                 input.size(), false))
      << "replay observed an illegal state without crashing";
}

}  // namespace
}  // namespace tinylamb
