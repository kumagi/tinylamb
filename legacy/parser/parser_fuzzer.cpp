/**
 * Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0.
 */

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "parser/parser.hpp"
#include "parser/tokenizer.hpp"

// Byte-driven fuzzer for the legacy hand-written SQL frontend.  The input is
// SQL text; the fuzzer looks for asserts, out-of-bounds accesses, and hangs
// in Tokenizer::Tokenize / Parser::Parse on malformed input.  A rejected
// statement (nullptr or exception) is ordinary error handling and is not a
// finding.
extern "C" [[maybe_unused]] int LLVMFuzzerTestOneInput(const uint8_t* data,
                                                       size_t size) {
  const std::string sql(reinterpret_cast<const char*>(data), size);
  try {
    tinylamb::Tokenizer tokenizer(sql);
    const std::vector<tinylamb::Token> tokens = tokenizer.Tokenize();
    tinylamb::Parser parser(tokens);
    (void)parser.Parse();
  } catch (...) {
    return 0;
  }
  return 0;
}
