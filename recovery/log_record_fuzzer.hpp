/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#ifndef TINYLAMB_LOG_RECORD_FUZZER_HPP
#define TINYLAMB_LOG_RECORD_FUZZER_HPP

#include <cstddef>
#include <cstdint>
#include <sstream>
#include <string>

#include "common/decoder.hpp"
#include "common/log_message.hpp"
#include "recovery/log_record.hpp"

namespace tinylamb {

// Byte-driven WAL record fuzzer.  The input is interpreted as a serialized
// LogRecord of any type.  Malformed inputs are rejected by the decoder with
// an exception or a failed stream - that is ordinary error handling, so the
// first decode runs inside a catch.  Once a record decodes, its
// Serialize/Deserialize round-trip must be byte-stable, and decoding must
// never be memory-unsafe; a large encoded container size (e.g. the checkpoint
// tables) should fail cleanly instead of allocating an unbounded vector.
inline void Try(const uint8_t* data, size_t size, bool verbose) {
  if (size < 4) {
    return;
  }
  const std::string input(reinterpret_cast<const char*>(data), size);
  LogRecord record;
  try {
    std::istringstream stream(input);
    Decoder decoder(stream);
    decoder >> record;
  } catch (const std::exception&) {
    return;
  }
  const std::string serialized = record.Serialize();
  LogRecord roundtrip;
  {  // No catch: bytes we just serialized must parse back.
    std::istringstream stream(serialized);
    Decoder decoder(stream);
    decoder >> roundtrip;
  }
  const std::string reserialized = roundtrip.Serialize();
  if (verbose) {
    LOG(TRACE) << "decoded type: " << record.type;
  }
  if (serialized != reserialized) {
    __builtin_trap();
  }
}

}  // namespace tinylamb

#endif  // TINYLAMB_LOG_RECORD_FUZZER_HPP
