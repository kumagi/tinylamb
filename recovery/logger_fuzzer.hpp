//
// Created by kumagi on 22/07/21.
//

#ifndef TINYLAMB_LOGGER_FUZZER_HPP
#define TINYLAMB_LOGGER_FUZZER_HPP
#include <filesystem>
#include <fstream>
#include <random>

#include "common/log_message.hpp"
#include "recovery/logger.hpp"

namespace tinylamb {

static const char alphanum[] =
    "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";
constexpr int kBufferSize = 32;
constexpr size_t kLoop = 10;

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
      std::string log_data = RandomString(rand() % 100 + 1);
      logger.AddLog(log_data);
      if (verbose) {
        LOG(TRACE) << log_data;
      }
      total += log_data.size();
      written.push_back(std::move(log_data));
    }
    while (logger.CommittedLSN() < total) {
    }
  }

  std::ifstream file;
  file.open(filename);
  assert(filename == filename);
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