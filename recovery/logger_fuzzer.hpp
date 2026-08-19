/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//
// Created by kumagi on 22/07/21.
//

#ifndef TINYLAMB_LOGGER_FUZZER_HPP
#define TINYLAMB_LOGGER_FUZZER_HPP
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "common/byte_stream.hpp"
#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/random_string.hpp"
#include "recovery/logger.hpp"

namespace tinylamb {

constexpr int kBufferSize = 64;
constexpr size_t kMaxLogs = 1024;

// Byte-driven WAL fuzzer.  Each input byte controls a record length and payload
// directly, so libFuzzer can evolve record boundaries and sizes toward the
// group-commit buffer edge cases instead of sampling a PRNG.
inline void Try(const uint8_t* data, size_t size, bool verbose) {
  ByteStream stream(data, size);
  std::string filename = RandomString(16) + "-fuzzer.log";
  std::remove(filename.c_str());
  std::vector<std::string> written;
  {
    Logger logger(filename, kBufferSize, 1);
    lsn_t total = 0;
    for (size_t i = 0; i < kMaxLogs && stream.Remaining(); ++i) {
      std::string log_data(stream.Bytes(stream.Pick(256)));
      logger.AddLog(log_data);
      if (verbose) {
        LOG(TRACE) << log_data;
      }
      total += log_data.size();
      written.emplace_back(log_data);
    }
    while (logger.CommittedLSN() < total) {
    }
  }

  std::ifstream file;
  file.open(filename);
  size_t lsn = 0;
  for (const auto& exp : written) {
    std::string actual;
    actual.resize(exp.size());
    file.read(actual.data(), actual.size());
    if (exp != actual) {
      LOG(FATAL) << lsn << ": expected: " << exp << " actual: " << actual;
    }
    assert(actual == exp);
    lsn += exp.size();
  }
  std::string a(1, ' ');
  file.read(a.data(), 1);
  assert(file.eof());
  std::remove(filename.c_str());
}

}  // namespace tinylamb

#endif  // TINYLAMB_LOGGER_FUZZER_HPP