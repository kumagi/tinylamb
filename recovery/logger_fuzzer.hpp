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
#include <cstddef>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <vector>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "recovery/logger.hpp"

namespace tinylamb {

static const char alphanum[] =
    "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";
constexpr int kBufferSize = 64;
constexpr size_t kLoop = 1024;

inline void Try(const uint64_t seed, bool verbose) {
  std::mt19937 rand(seed);
  auto RandomString = [&rand](size_t len = 16) {
    std::string ret;
    ret.reserve(len);
    for (size_t i = 0; i < len; ++i) {
      ret.push_back(alphanum[rand() % (sizeof(alphanum) - 1)]);
    }
    return ret;
  };

  std::string filename = RandomString(16) + "-fuzzer.log";
  std::remove(filename.c_str());
  std::vector<std::string> written;
  {
    Logger logger(filename, kBufferSize, 1);
    lsn_t total = 0;
    for (size_t i = 0; i < kLoop; ++i) {
      std::string log_data = RandomString(rand() % 1000 + 1);
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