/**
 * Copyright 2024 KUMAZAKI Hiroki
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
#include <fcntl.h>
#include <unistd.h>

#include <cstddef>
#include <cstdio>
#include <filesystem>
#include <functional>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/random_string.hpp"
#include "gtest/gtest.h"
#include "index/lsm_detail/cache.hpp"

namespace tinylamb {
class CacheConcurrentTest : public ::testing::Test {
 protected:
  constexpr static int kSize = 1LU * 1024 * 1024;
  void SetUp() override {
    path_ = "cache_concurrent_test-" + RandomString();
    fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
    std::vector<int> value;
    value.resize(kSize);
    for (size_t i = 0; i < kSize; ++i) {
      value[i] = Expected(i);
    }
    ASSERT_EQ(value.size(), kSize);
    size_t remaining = value.size() * sizeof(int);
    size_t written = 0;
    while (0 < remaining) {
      ssize_t wrote = ::write(
          fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
      ASSERT_LT(0, wrote);
      written += wrote;
      remaining -= wrote;
    }
    ASSERT_EQ(written, value.size() * sizeof(int));
    ::fsync(fd_);
    ASSERT_EQ(std::filesystem::file_size(path_), kSize * sizeof(int));
    cache_ = std::make_unique<Cache>(fd_, 128 * 1024);
  }

  void TearDown() override {
    ::close(fd_);
    std::ignore = std::remove(path_.c_str());
  }

  static int Expected(size_t pos) { return kSeed + std::hash<size_t>()(pos); }

  constexpr static int kSeed = 0xdeadbeef;
  int fd_;
  std::filesystem::path path_;
  std::unique_ptr<Cache> cache_;
};

TEST_F(CacheConcurrentTest, ReadTwo) {
  constexpr size_t kThreads = 15;
  std::vector<std::thread> workers;
  workers.reserve(kThreads);
  for (size_t i = 0; i < kThreads; ++i) {
    workers.emplace_back([&, i]() {
      std::mt19937 rand(i);
      for (int j = 0; j < 1000; ++j) {
        size_t pos = rand() % kSize;
        std::string data = cache_->ReadAt(pos * sizeof(int), sizeof(int));
        int data_as_int = *(reinterpret_cast<int*>(data.data()));
        ASSERT_EQ(data_as_int, Expected(pos));
      }
    });
  }
  for (auto& worker : workers) {
    worker.join();
  }
}

TEST_F(CacheConcurrentTest, ReadFifteen) {
  constexpr size_t kThreads = 50;
  std::vector<std::thread> workers;
  workers.reserve(kThreads);
  for (size_t i = 0; i < kThreads; ++i) {
    workers.emplace_back([&, i]() {
      std::mt19937 rand(i);
      for (int j = 0; j < 100; ++j) {
        size_t pos = rand() % kSize;
        std::string data = cache_->ReadAt(pos * sizeof(int), sizeof(int));
        int data_as_int = *(reinterpret_cast<int*>(data.data()));
        if (data_as_int != Expected(pos)) {
          LOG(ERROR) << pos;
        }
        ASSERT_EQ(data_as_int, Expected(pos));
      }
    });
  }
  for (auto& worker : workers) {
    worker.join();
  }
}

}  // namespace tinylamb