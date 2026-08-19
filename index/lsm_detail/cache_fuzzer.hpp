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
#include <string>
#include <vector>

#include "common/byte_stream.hpp"
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
static constexpr size_t kCachePageSize = 4096;
static constexpr size_t kMaxReads = 512;
// Byte-driven blob cache fuzzer.  Read positions are encoded as a (cache page,
// offset within page) pair so libFuzzer can steer reads across the 4096-byte
// cache page boundaries directly instead of hoping a PRNG lands near one.
inline void Try(const uint8_t* data, size_t size, bool verbose) {
  ByteStream stream(data, size);
  std::filesystem::path blob_path =
      "cache_fuzzer-" + RandomString(20, false) + ".db";
  const size_t kMemoryPages = stream.Pick(1024) + 8;
  BlobFile blob(blob_path, kMemoryPages * kCachePageSize, kFileSize);
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
  for (size_t i = 0; i < kMaxReads && stream.Remaining(); ++i) {
    const size_t page = stream.Pick(kFileSize / kCachePageSize);
    const size_t offset = stream.Pick(kCachePageSize);
    const size_t pos = page * kCachePageSize + offset;
    size_t read_size = stream.Pick(kCachePageSize) + 1;
    if (kFileSize < read_size + pos) {
      read_size -= pos + read_size - kFileSize;
      assert(kFileSize == read_size + pos);
    }
    if (verbose) {
      LOG(DEBUG) << "Read: [" << pos << " - " << pos + read_size << "]";
    }
    std::string actual_piece = blob.ReadAt(pos, read_size);
    std::string expected_piece(&expected[pos], read_size);
    if (actual_piece != expected_piece) {
      LOG(FATAL) << "Miss match at " << pos << " size: " << read_size;
      exit(1);
    }
  }

  std::filesystem::remove_all(blob_path);
}

}  // namespace tinylamb

#endif  // TINYLAMB_CACHE_FUZZER_HPP
