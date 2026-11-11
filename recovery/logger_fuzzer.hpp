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
#include <memory>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "common/byte_stream.hpp"
#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "recovery/logger.hpp"

namespace tinylamb {

constexpr int kBufferSize = 64;
constexpr size_t kMaxLogs = 1024;

// Append and wait through the same Status boundary as database clients. A
// stopped or failed logger must terminate the input rather than spin forever
// waiting for a byte count that can no longer be committed.
inline Status AppendLoggerFuzzInput(Logger& logger, ByteStream& stream,
                                    std::vector<std::string>* written,
                                    bool verbose) {
  lsn_t durable_target = logger.DurableLSN();
  for (size_t i = 0; i < kMaxLogs && stream.Remaining(); ++i) {
    std::string log_data(stream.Bytes(stream.Pick(256)));
    ASSIGN_OR_RETURN(lsn_t, start, logger.AddLog(log_data));
    durable_target = start + log_data.size();
    if (verbose) {
      LOG(TRACE) << log_data;
    }
    written->emplace_back(std::move(log_data));
  }
  return logger.WaitForDurable(durable_target);
}

// Byte-driven WAL fuzzer. Each input owns a directory so failures also clean
// up segment metadata, instead of leaking it across long libFuzzer runs.
inline void Try(const uint8_t* data, size_t size, bool verbose) {
  ByteStream stream(data, size);
  const std::filesystem::path directory = RandomString(16) + "-logger-fuzz";
  std::error_code error;
  if (!std::filesystem::create_directory(directory, error)) {
    return;
  }
  struct Cleanup {
    explicit Cleanup(std::filesystem::path directory)
        : path(std::move(directory)) {}
    Cleanup(const Cleanup&) = delete;
    Cleanup& operator=(const Cleanup&) = delete;
    Cleanup(Cleanup&&) = delete;
    Cleanup& operator=(Cleanup&&) = delete;
    std::filesystem::path path;
    ~Cleanup() {
      std::error_code ignored;
      std::filesystem::remove_all(path, ignored);
    }
  } cleanup{directory};
  const std::string filename = (directory / "input.log").string();
  auto created = Logger::Create(filename, kBufferSize, 1);
  if (!created.HasValue()) {
    return;
  }
  std::unique_ptr<Logger> logger = created.MoveValue();
  std::vector<std::string> written;
  if (AppendLoggerFuzzInput(*logger, stream, &written, verbose) !=
      Status::kSuccess) {
    return;
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
}

}  // namespace tinylamb

#endif  // TINYLAMB_LOGGER_FUZZER_HPP