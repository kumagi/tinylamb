//
// Created by kumagi on 24/08/10.
//

#ifndef TINYLAMB_CACHE_FUZZER_HPP
#define TINYLAMB_CACHE_FUZZER_HPP

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <map>
#include <random>
#include <string>
#include <vector>

#include "common/constants.hpp"
#include "common/debug.hpp"
#include "common/log_message.hpp"
#include "common/random_string.hpp"
#include "index/lsm_detail/blob_file.hpp"
#include "index/lsm_detail/sorted_run.hpp"
#include "lsm_view.hpp"

namespace tinylamb {

inline uint64_t Generate(size_t offset) { return offset * 19937 + 2147483647; }

static constexpr size_t kFileSize = 8LLU * 1024 * 1024;
inline void Try(const uint64_t seed, bool verbose) {
  if (verbose) {
    LOG(INFO) << "seed: " << seed;
  }
  std::filesystem::path blob_path =
      "cache_fuzzer-" + RandomString(20, false) + ".db";
  std::mt19937 rand(seed);
  const size_t kMemoryPages = rand() % 1024 + 8;
  BlobFile blob(blob_path, kMemoryPages * 4 * 1024, kFileSize);
  std::string expected(kFileSize, '0');
  auto* expected_ptr = reinterpret_cast<uint64_t*>(expected.data());
  for (size_t i = 0; i < kFileSize / sizeof(uint64_t); ++i) {
    expected_ptr[i] = Generate(i);
  }
  blob.Append(expected);
  blob.Flush();
  if (verbose) {
    LOG(TRACE) << "Written " << kFileSize << " bytes";
  }
  for (size_t i = 0; i < 100000; ++i) {
    size_t pos = rand() % (kFileSize - 1);
    size_t size = rand() % 4096 + 1;
    if (kFileSize < size + pos) {
      size -= pos + size - kFileSize;
      assert(kFileSize == size + pos);
    }
    if (verbose) {
      LOG(DEBUG) << "Read: [" << pos << " - " << pos + size << "]";
    }
    std::string actual_piece = blob.ReadAt(pos, size);
    std::string expected_piece(&expected[pos], size);
    if (actual_piece != expected_piece) {
      LOG(FATAL) << "Miss match at " << pos << " size: " << size;
      exit(1);
    }
  }

  std::filesystem::remove_all(blob_path);
}

}  // namespace tinylamb

#endif  // TINYLAMB_CACHE_FUZZER_HPP
