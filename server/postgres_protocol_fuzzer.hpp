/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#ifndef TINYLAMB_POSTGRES_PROTOCOL_FUZZER_HPP
#define TINYLAMB_POSTGRES_PROTOCOL_FUZZER_HPP

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/byte_stream.hpp"
#include "common/log_message.hpp"
#include "server/postgres_protocol.hpp"

namespace tinylamb {

// Byte-driven pgwire protocol fuzzer.  The input drives three checks:
//  (1) robustness - arbitrary bytes are fed to ParseStartupPacket, which must
//      reject malformed packets via std::nullopt instead of throwing or
//      reading out of bounds;
//  (2) round-trip oracle - a well-formed startup packet assembled from fuzz
//      bytes must parse back to exactly the parameters it was built from;
//  (3) SplitSqlStatements must stay within one statement per top-level ';'
//      plus the unterminated tail.
inline void Try(const uint8_t* data, size_t size, bool verbose) {
  ByteStream stream(data, size);
  const std::string_view raw(reinterpret_cast<const char*>(data), size);

  // (1) Robustness on untrusted bytes.  No try/catch here on purpose: an
  // escaping exception is a finding because ProcessStartup() on the server
  // path calls ParseStartupPacket without one.
  std::string error;
  const std::optional<pgwire::StartupPacket> arbitrary =
      pgwire::ParseStartupPacket(raw, &error);
  if (!arbitrary.has_value() && error.empty()) {
    // Every rejection path must explain itself.
    __builtin_trap();
  }

  // (2) Round-trip oracle.
  constexpr size_t kMaxPairs = 8;
  constexpr size_t kMaxFieldLength = 16;
  std::vector<std::pair<std::string, std::string>> parameters;
  const size_t pair_count = stream.Pick(kMaxPairs);
  for (size_t i = 0; i < pair_count && stream.Remaining(); ++i) {
    std::string key(stream.Bytes(stream.Pick(kMaxFieldLength)));
    if (key.empty() || key.find('\0') != std::string::npos) {
      break;  // An empty name would act as the packet terminator.
    }
    std::string value(stream.Bytes(stream.Pick(kMaxFieldLength)));
    if (value.find('\0') != std::string::npos) {
      break;
    }
    parameters.emplace_back(std::move(key), std::move(value));
  }

  std::string payload;
  pgwire::AppendUint32(&payload, pgwire::kProtocolVersion30);
  for (const auto& [key, value] : parameters) {
    payload += key;
    payload.push_back('\0');
    payload += value;
    payload.push_back('\0');
  }
  payload.push_back('\0');
  std::string packet;
  pgwire::AppendUint32(&packet, static_cast<uint32_t>(payload.size() + 4));
  packet += payload;

  const std::optional<pgwire::StartupPacket> parsed =
      pgwire::ParseStartupPacket(packet, &error);
  if (!parsed.has_value()) {
    if (verbose) {
      LOG(TRACE) << "round-trip rejected: " << error;
    }
    __builtin_trap();
  }
  if (parsed->protocol_version != pgwire::kProtocolVersion30) {
    __builtin_trap();
  }
  // ParseStartupPacket uses insert_or_assign, so the last occurrence of a
  // duplicated parameter name wins; reproduce that here instead of relying on
  // map range construction, which keeps the first.
  std::unordered_map<std::string, std::string> expected;
  for (const auto& [key, value] : parameters) {
    expected.insert_or_assign(key, value);
  }
  if (parsed->parameters != expected) {
    if (verbose) {
      LOG(TRACE) << "parameters diverged: " << parsed->parameters.size()
                 << " vs " << expected.size();
    }
    __builtin_trap();
  }

  // (3) Statement splitting never emits more than one statement per ';' plus
  // the tail after the last one.
  const std::vector<std::string> statements = pgwire::SplitSqlStatements(raw);
  size_t semicolons = 0;
  for (const char c : raw) {
    if (c == ';') {
      ++semicolons;
    }
  }
  if (statements.size() > semicolons + 1) {
    __builtin_trap();
  }
}

}  // namespace tinylamb

#endif  // TINYLAMB_POSTGRES_PROTOCOL_FUZZER_HPP
