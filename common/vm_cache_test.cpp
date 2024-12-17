
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

#include "common/vm_cache.hpp"

#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <ostream>
#include <string>
#include <tuple>
#include <vector>

#include "gtest/gtest.h"
#include "random_string.hpp"

namespace tinylamb {
namespace {

class VMCacheTest : public ::testing::Test {
 protected:
  template <typename T>
  std::unique_ptr<VMCache<T>> MakeCache(size_t data_size, size_t offset = 0) {
    path_ = "vm_cache_test-" + RandomString();
    fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
    std::string random = RandomString(offset);
    ssize_t padding_wrote = ::write(fd_, random.data(), random.length());
    EXPECT_EQ(padding_wrote, offset);
    std::vector<T> value;
    value.resize(data_size);
    for (size_t i = 0; i < data_size; ++i) {
      value[i] = Expected<T>(i);
    }
    EXPECT_EQ(value.size(), data_size);
    size_t remaining = value.size() * sizeof(T);
    size_t written = 0;
    while (0 < remaining) {
      ssize_t wrote = ::write(
          fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
      EXPECT_LT(0, wrote);
      written += wrote;
      remaining -= wrote;
    }
    EXPECT_EQ(written, value.size() * sizeof(T));
    ::fsync(fd_);
    EXPECT_EQ(std::filesystem::file_size(path_),
              offset + data_size * sizeof(T));
    return std::make_unique<VMCache<T>>(fd_, data_size * 1024, offset);
  }

  void TearDown() override {
    ::close(fd_);
    std::ignore = std::remove(path_.c_str());
  }

  template <typename T>
  static T Expected(size_t key) {
    return T(kSeed + std::hash<size_t>()(key));
  }

  constexpr static int kSeed = 0;
  // 0xdeadbeef;
  int fd_;
  std::filesystem::path path_;
};

TEST_F(VMCacheTest, one_page) {
  constexpr size_t kCount = 1024;
  auto cache = MakeCache<int32_t>(kCount);
  for (int i = 0; i < 1024; ++i) {
    int32_t data;
    cache->Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, offset) {
  constexpr size_t kCount = 1024;
  for (int i = 1; i < 4096; i += 127) {
    auto cache = MakeCache<int32_t>(kCount, i);
    for (int j = 0; j < 1024; ++j) {
      int32_t data;
      cache->Read(&data, j, 1);
      if (data != Expected<int32_t>(j)) {
        exit(1);
      }
      ASSERT_EQ(data, Expected<int32_t>(j));
    }
  }
}

struct Data {
  int a;
  int b;
  char c;
  Data() = default;
  Data(size_t from)
      : a(from & 0xffffffff), b(from & 0x0000ffff), c(from & 0xff) {}
  bool operator==(const Data& rhs) const {
    return a == rhs.a && b == rhs.b && c == rhs.c;
  }
  friend std::ostream& operator<<(std::ostream& o, const Data& d) {
    o << "{ a: " << d.a << " b: " << d.b << " c: " << (int)d.c << " }\n";
    return o;
  }
};

TEST_F(VMCacheTest, offset_struct) {
  constexpr size_t kCount = 1024;
  for (int i = 1; i < 4096; i += 127) {
    auto cache = MakeCache<Data>(kCount, i);
    for (int j = 0; j < 1024; ++j) {
      Data data;
      cache->Read(&data, j, 1);
      ASSERT_EQ(data, Expected<Data>(j));
    }
  }
}

}  // namespace
}  // namespace tinylamb