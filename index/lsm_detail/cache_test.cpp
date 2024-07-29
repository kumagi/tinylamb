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

#include "index/lsm_detail/cache.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <cstddef>
#include <cstdio>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "common/log_message.hpp"
#include "common/random_string.hpp"
#include "gtest/gtest.h"

namespace tinylamb {

class CacheTest : public ::testing::Test {
 protected:
  void SetUp() override {
    path_ = "cache_test-" + RandomString();
    fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
    size_t kSize = 4L * 1024L * 1024L;
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
    cache_ = std::make_unique<Cache>(fd_, 32 * 1024);
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

TEST_F(CacheTest, one_page) {
  for (int i = 0; i < 1024; ++i) {
    std::string data = cache_->ReadAt(i * sizeof(int), sizeof(int));
    int data_as_int = *(reinterpret_cast<int*>(data.data()));
    ASSERT_EQ(data_as_int, Expected(i));
  }
}

TEST_F(CacheTest, mega_page) {
  for (int i = 0; i < 1024; ++i) {
    std::string data = cache_->ReadAt(i * 1024 * sizeof(int), sizeof(int));
    int data_as_int = *(reinterpret_cast<int*>(data.data()));
    ASSERT_EQ(data_as_int, Expected(i * 1024));
  }
}

TEST_F(CacheTest, mega_pages) {
  for (int i = 0; i < 4; ++i) {
    std::string data =
        cache_->ReadAt(i * 1024 * 1024 * sizeof(int), sizeof(int));
    int data_as_int = *(reinterpret_cast<int*>(data.data()));
    ASSERT_EQ(data_as_int, Expected(i * 1024 * 1024));
  }
}

}  // namespace tinylamb