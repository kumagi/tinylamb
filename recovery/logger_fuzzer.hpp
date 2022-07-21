//
// Created by kumagi on 22/07/21.
//

#ifndef TINYLAMB_LOGGER_FUZZER_HPP
#define TINYLAMB_LOGGER_FUZZER_HPP
#include <fstream>
#include <random>

#include "common/log_message.hpp"
#include "recovery/logger.hpp"

namespace tinylamb {

static const char alphanum[] =
    "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";

void Try(const uint64_t seed, bool verbose) {
  std::string filename;
  {  // Write.
    std::mt19937 rand(seed);
    auto RandomString = [&rand](size_t len = 16) {
      std::string ret;
      ret.reserve(len);
      for (size_t i = 0; i < len; ++i) {
        ret.push_back(alphanum[rand() % (sizeof(alphanum) - 1)]);
      }
      return ret;
    };

    filename = RandomString(16) + "-fuzzer.log";
    Logger logger(filename, 280, 1);
    for (size_t i = 0; i < rand() % 1000 + 1000; ++i) {
      std::string log_data = RandomString(rand() % 100 + 1);
      logger.AddLog(log_data);
      if (verbose) {
        LOG(TRACE) << log_data;
      }
    }
  }

  {  // Verify.
    std::mt19937 verify_rand(seed);
    auto RandomString = [&verify_rand](size_t len = 16) {
      std::string ret;
      ret.reserve(len);
      for (size_t i = 0; i < len; ++i) {
        ret.push_back(alphanum[verify_rand() % (sizeof(alphanum) - 1)]);
      }
      return ret;
    };
    std::ifstream file;
    file.open(RandomString(16) + "-fuzzer.log");
    for (size_t i = 0; i < verify_rand() % 1000 + 1000; ++i) {
      std::string expected_log_data = RandomString(verify_rand() % 100 + 1);
      std::string log_data(expected_log_data.size(), ' ');
      file.read(log_data.data(), log_data.size());
      if (verbose) {
        LOG(TRACE) << log_data << " == " << expected_log_data;
      }
      assert(log_data == expected_log_data);
    }
    std::string a(1, ' ');
    file.read(a.data(), 1);
    assert(file.eof());
  }
  std::remove(filename.c_str());
}

}  // namespace tinylamb

#endif  // TINYLAMB_LOGGER_FUZZER_HPP
