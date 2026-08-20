/**
 * Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0.
 */

#include <cstddef>
#include <cstdint>
#include <sstream>
#include <string>

#include "common/decoder.hpp"
#include "recovery/log_record.hpp"

// Byte-driven WAL record fuzzer.  The input is interpreted as a serialized
// LogRecord of any type.  The fuzzer checks two things: decoding is
// memory-safe on arbitrary bytes, and Serialize/Deserialize round-trips are
// byte-stable.  A large encoded container size (e.g. the checkpoint tables)
// should fail cleanly instead of allocating an unbounded vector.
extern "C" [[maybe_unused]] int LLVMFuzzerTestOneInput(const uint8_t* data,
                                                       size_t size) {
  if (size < 4) {
    return 0;
  }
  const std::string input(reinterpret_cast<const char*>(data), size);
  tinylamb::LogRecord record;
  {
    std::istringstream stream(input);
    tinylamb::Decoder decoder(stream);
    decoder >> record;
  }
  const std::string serialized = record.Serialize();
  tinylamb::LogRecord roundtrip;
  {
    std::istringstream stream(serialized);
    tinylamb::Decoder decoder(stream);
    decoder >> roundtrip;
  }
  const std::string reserialized = roundtrip.Serialize();
  if (serialized != reserialized) {
    __builtin_trap();
  }
  return 0;
}
