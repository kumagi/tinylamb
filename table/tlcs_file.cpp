/**
 * Copyright 2026 KUMAZAKI Hiroki
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

#include "table/tlcs_file.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cerrno>
#include <charconv>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#ifdef __linux__
#include <sys/mman.h>
#include <unistd.h>
#endif

#include "common/crc32c.hpp"
#include "common/log_message.hpp"
#include "common/memory_budget.hpp"
#include "common/parallel_work.hpp"
#include "common/platform/endian.hpp"
#include "common/platform/file_io.hpp"
#include "common/query_parallelism.hpp"
#include "common/runtime_filter.hpp"
#include "common/serdes.hpp"
#include "common/unwind_guard.hpp"
#include "page/page.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "page/row_page.hpp"
#include "table/full_scan_iterator.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "transaction/transaction.hpp"
#include "type/collation.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {

// The trailing "0001" is a historical label; the authoritative format
// version is TlcsFile::kFormatVersion in the serialized header.
constexpr std::string_view kTlcsMagic = "TLCS0001";
constexpr size_t kTlcsHeaderBytes = 72;
constexpr size_t kWriteBufferCap = size_t{1} << 20;  // 1 MiB
constexpr page_id_t kMaxFlatPageId = 16'777'216ULL;

std::optional<uint64_t> ReadResidentMmapLimitBytes() noexcept {
  const char* env = std::getenv("TINYLAMB_TLCS_MMAP_LIMIT_BYTES");
  if (env == nullptr) {
    return std::nullopt;
  }
  std::string_view text(env);
  const size_t start = text.find_first_not_of(" \t\n\r\v\f");
  if (start == std::string_view::npos) {
    return std::nullopt;
  }
  text.remove_prefix(start);
  if (text.starts_with('+')) {
    text.remove_prefix(1);
  }
  uint64_t value = 0;
  const auto [end, error] =
      std::from_chars(text.data(), text.data() + text.size(), value);
  if (error != std::errc{} || end != text.data() + text.size()) {
    return std::nullopt;
  }
  return value;
}

size_t EffectiveColumnarPoolMaxBytes() noexcept {
  const auto& budget = ProcessMemoryBudget::Global();
  return std::min(budget.ColumnarPoolLimit(),
                  budget.EffectivePoolMaxBytes(MemoryPoolKind::kColumnar));
}

size_t PointReadCacheLimitBytes() noexcept {
  const size_t pool_share =
      std::max<size_t>(size_t{4} << 20, EffectiveColumnarPoolMaxBytes() / 8);
  return std::min<size_t>(size_t{64} << 20, pool_share);
}

void AdviseHugePages(void* ptr, size_t bytes) noexcept {
#if defined(__linux__) && defined(MADV_HUGEPAGE)
  constexpr size_t kMinHugePageHintBytes = size_t{4} << 20;
  if (ptr == nullptr || bytes < kMinHugePageHintBytes) {
    return;
  }
  const long sys_page = ::sysconf(_SC_PAGESIZE);
  const size_t page_size =
      sys_page > 0 ? static_cast<size_t>(sys_page) : size_t{4096};
  const auto addr = reinterpret_cast<uintptr_t>(ptr);
  const uintptr_t aligned = (addr + page_size - 1) & ~(page_size - 1);
  if (aligned >= addr + bytes) {
    return;
  }
  const size_t aligned_len = ((addr + bytes) - aligned) & ~(page_size - 1);
  if (aligned_len >= page_size) {
    (void)::madvise(reinterpret_cast<void*>(aligned), aligned_len,
                    MADV_HUGEPAGE);
  }
#else
  (void)ptr;
  (void)bytes;
#endif
}

// #1111 instrumentation: one count per block-column decode execution, so a
// test can prove repeat encoded reads never re-decode.
std::atomic<uint64_t> g_tlcs_decode_calls{0};

size_t DirCacheLimitBytes() noexcept {
  const size_t pool_share =
      std::max<size_t>(size_t{512} << 10, EffectiveColumnarPoolMaxBytes() / 4);
  return std::min<size_t>(size_t{512} << 20, pool_share);
}

void AppendU8(std::string& dst, uint8_t v) {
  dst.push_back(static_cast<char>(v));
}

void AppendU32(std::string& dst, uint32_t v) {
  const uint32_t le = htole32(v);
  dst.append(reinterpret_cast<const char*>(&le), sizeof(le));
}

void AppendU64(std::string& dst, uint64_t v) {
  const uint64_t le = htole64(v);
  dst.append(reinterpret_cast<const char*>(&le), sizeof(le));
}

void AppendI64(std::string& dst, int64_t v) {
  uint64_t raw = 0;
  std::memcpy(&raw, &v, sizeof(raw));
  AppendU64(dst, raw);
}

void AppendF64(std::string& dst, double v) {
  uint64_t raw = 0;
  std::memcpy(&raw, &v, sizeof(raw));
  AppendU64(dst, raw);
}

bool ReadU8(std::span<const uint8_t> src, size_t* pos, uint8_t* out) {
  if (*pos + 1 > src.size()) {
    return false;
  }
  *out = src[*pos];
  *pos += 1;
  return true;
}

bool ReadU32(std::span<const uint8_t> src, size_t* pos, uint32_t* out) {
  if (*pos + 4 > src.size()) {
    return false;
  }
  uint32_t le = 0;
  std::memcpy(&le, src.data() + *pos, sizeof(le));
  *out = le32toh(le);
  *pos += 4;
  return true;
}

bool ReadU64(std::span<const uint8_t> src, size_t* pos, uint64_t* out) {
  if (*pos + 8 > src.size()) {
    return false;
  }
  uint64_t le = 0;
  std::memcpy(&le, src.data() + *pos, sizeof(le));
  *out = le64toh(le);
  *pos += 8;
  return true;
}

bool ReadI64(std::span<const uint8_t> src, size_t* pos, int64_t* out) {
  uint64_t raw = 0;
  if (!ReadU64(src, pos, &raw)) {
    return false;
  }
  std::memcpy(out, &raw, sizeof(raw));
  return true;
}

bool ReadF64(std::span<const uint8_t> src, size_t* pos, double* out) {
  uint64_t raw = 0;
  if (!ReadU64(src, pos, &raw)) {
    return false;
  }
  std::memcpy(out, &raw, sizeof(raw));
  return true;
}

Status WriteAllFd(int fd, const void* data, size_t len) {
  const auto* ptr = static_cast<const uint8_t*>(data);
  size_t done = 0;
  while (done < len) {
    const ssize_t n = platform::Write(fd, ptr + done, len - done);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      return StatusError(StatusCode::kIOError,
                         "TLCS write failed: " + std::string(strerror(errno)));
    }
    if (n == 0) {
      return StatusError(StatusCode::kIOError, "TLCS write returned 0");
    }
    done += static_cast<size_t>(n);
  }
  return Status::kSuccess;
}

Status PwriteAllFd(int fd, const void* data, size_t len, uint64_t offset) {
  const auto* ptr = static_cast<const uint8_t*>(data);
  size_t done = 0;
  while (done < len) {
    const ssize_t n = platform::Pwrite(fd, ptr + done, len - done,
                                       static_cast<int64_t>(offset + done));
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      return StatusError(StatusCode::kIOError,
                         "tlcs pwrite failed: " + std::string(strerror(errno)));
    }
    if (n == 0) {
      return StatusError(StatusCode::kIOError, "tlcs pwrite returned 0");
    }
    done += static_cast<size_t>(n);
  }
  return Status::kSuccess;
}

Status PreadAllFd(int fd, void* dst, size_t len, uint64_t offset) {
  auto* ptr = static_cast<uint8_t*>(dst);
  size_t done = 0;
  while (done < len) {
    const ssize_t n = platform::Pread(fd, ptr + done, len - done,
                                      static_cast<int64_t>(offset + done));
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      return StatusError(StatusCode::kIOError,
                         "tlcs pread failed: " + std::string(strerror(errno)));
    }
    if (n == 0) {
      return StatusError(StatusCode::kCorrupt, "unexpected EOF reading tlcs");
    }
    done += static_cast<size_t>(n);
  }
  return Status::kSuccess;
}

class BufferedSeqWriter {
 public:
  explicit BufferedSeqWriter(int fd, uint64_t start_offset)
      : fd_(fd), file_offset_(start_offset) {
    buf_.reserve(kWriteBufferCap);
  }

  Status Append(const void* data, size_t len) {
    if (len == 0) {
      return Status::kSuccess;
    }
    const auto* ptr = static_cast<const uint8_t*>(data);
    if (len >= kWriteBufferCap) {
      RETURN_IF_FAIL(Flush());
      RETURN_IF_FAIL(WriteAllFd(fd_, ptr, len));
      file_offset_ += len;
      return Status::kSuccess;
    }
    if (buf_.size() + len > kWriteBufferCap) {
      RETURN_IF_FAIL(Flush());
    }
    buf_.insert(buf_.end(), ptr, ptr + len);
    file_offset_ += len;
    return Status::kSuccess;
  }

  Status AlignTo8() {
    const auto rem = static_cast<size_t>(file_offset_ & 7ULL);
    if (rem == 0) {
      return Status::kSuccess;
    }
    static constexpr std::array<uint8_t, 8> kZeros{};
    return Append(kZeros.data(), 8 - rem);
  }

  Status Flush() {
    if (buf_.empty()) {
      return Status::kSuccess;
    }
    RETURN_IF_FAIL(WriteAllFd(fd_, buf_.data(), buf_.size()));
    buf_.clear();
    return Status::kSuccess;
  }

  [[nodiscard]] uint64_t Offset() const noexcept { return file_offset_; }

 private:
  int fd_{-1};
  uint64_t file_offset_{0};
  std::vector<uint8_t> buf_;
};

uint64_t HashU64(uint64_t z) noexcept {
  z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
  z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
  z ^= z >> 31;
  return z == 0 ? 1ULL : z;
}

uint64_t HashSv(std::string_view sv) noexcept {
  uint64_t h = 0xcbf29ce484222325ULL;
  for (const char raw_ch : sv) {
    h ^= static_cast<unsigned char>(raw_ch);
    h *= 0x100000001b3ULL;
  }
  return h == 0 ? 1ULL : h;
}

double CanonDouble(double d, int64_t* bits_out) noexcept {
  if (d == 0.0) {
    d = 0.0;
  }
  std::memcpy(bits_out, &d, sizeof(d));
  return d;
}

// Per-column incremental <=254 dictionary tracker shared by single-pass stream
// writing. Stops gracefully when a block would overflow 254 distinct entries,
// freezing already-assigned encounter-order codes for prior blocks cleanly.
class OnlineColumnDict {
 public:
  void Init(ValueType type, bool is_unsigned) {
    type_ = type;
    is_unsigned_ = is_unsigned;
    if (is_unsigned && type == ValueType::kInt64) {
      // Signedness differs in Value semantics; keep raw uint64 values.
      overflowed_ = true;
    }
  }

  [[nodiscard]] bool Overflowed() const noexcept { return overflowed_; }
  [[nodiscard]] bool UsedInAnyBlock() const noexcept {
    return used_in_any_block_;
  }
  void MarkUsed() noexcept { used_in_any_block_ = true; }

  // Attempts to append `n` values into `codes_out` (size n). If total distinct
  // values would exceed kMaxEntries (254) or NaN appears, freezes state, marks
  // overflowed_, and returns false so the caller encodes this block natively.
  bool TryEncodeBlockInts(const int64_t* vals, const uint8_t* nulls, size_t n,
                          bool has_null, std::vector<uint8_t>* codes_out) {
    if (overflowed_) {
      return false;
    }
    codes_out->resize(n);
    const size_t saved_size = int_entries_.size();
    bool has_prev = false;
    int64_t prev_val = 0;
    uint8_t prev_code = 0;
    std::vector<std::pair<size_t, IntSlot>> added_slots;

    for (size_t r = 0; r < n; ++r) {
      if (has_null && nulls[r] != 0) {
        (*codes_out)[r] = LSTableColumnDictionary::kNullCode;
        continue;
      }
      const int64_t v = vals[r];
      if (has_prev && v == prev_val) {
        (*codes_out)[r] = prev_code;
        continue;
      }
      const uint64_t h = HashU64(static_cast<uint64_t>(v));
      size_t slot = static_cast<size_t>(h) & kMask;
      uint8_t code = 0;
      while (true) {
        if (int_slots_[slot].hash == 0) {
          if (int_entries_.size() >= LSTableColumnDictionary::kMaxEntries) {
            RollbackInts(saved_size, added_slots);
            overflowed_ = true;
            return false;
          }
          code = static_cast<uint8_t>(int_entries_.size());
          added_slots.emplace_back(slot, int_slots_[slot]);
          int_slots_[slot] = IntSlot{.hash = h, .val = v, .code = code};
          int_entries_.push_back(v);
          break;
        }
        if (int_slots_[slot].val == v) {
          code = int_slots_[slot].code;
          break;
        }
        slot = (slot + 1) & kMask;
      }
      prev_val = v;
      prev_code = code;
      has_prev = true;
      (*codes_out)[r] = code;
    }
    used_in_any_block_ = true;
    return true;
  }

  bool TryEncodeBlockDoubles(const int64_t* raw_bits, const uint8_t* nulls,
                             size_t n, bool has_null,
                             std::vector<uint8_t>* codes_out) {
    if (overflowed_) {
      return false;
    }
    codes_out->resize(n);
    const size_t saved_size = double_entries_.size();
    bool has_prev = false;
    int64_t prev_raw = 0;
    uint8_t prev_code = 0;
    std::vector<std::pair<size_t, IntSlot>> added_slots;

    for (size_t r = 0; r < n; ++r) {
      if (has_null && nulls[r] != 0) {
        (*codes_out)[r] = LSTableColumnDictionary::kNullCode;
        continue;
      }
      const int64_t raw = raw_bits[r];
      if (has_prev && raw == prev_raw) {
        (*codes_out)[r] = prev_code;
        continue;
      }
      double d = 0.0;
      std::memcpy(&d, &raw, sizeof(d));
      if (std::isnan(d)) {
        RollbackDoubles(saved_size, added_slots);
        overflowed_ = true;
        return false;
      }
      int64_t canon = 0;
      d = CanonDouble(d, &canon);
      const uint64_t h = HashU64(static_cast<uint64_t>(canon));
      size_t slot = static_cast<size_t>(h) & kMask;
      uint8_t code = 0;
      while (true) {
        if (int_slots_[slot].hash == 0) {
          if (double_entries_.size() >= LSTableColumnDictionary::kMaxEntries) {
            RollbackDoubles(saved_size, added_slots);
            overflowed_ = true;
            return false;
          }
          code = static_cast<uint8_t>(double_entries_.size());
          added_slots.emplace_back(slot, int_slots_[slot]);
          int_slots_[slot] = IntSlot{.hash = h, .val = canon, .code = code};
          double_entries_.push_back(d);
          break;
        }
        if (int_slots_[slot].val == canon) {
          code = int_slots_[slot].code;
          break;
        }
        slot = (slot + 1) & kMask;
      }
      prev_raw = raw;
      prev_code = code;
      has_prev = true;
      (*codes_out)[r] = code;
    }
    used_in_any_block_ = true;
    return true;
  }

  bool TryEncodeBlockVarChar(const uint8_t* bytes, const uint32_t* ends,
                             const uint8_t* nulls, size_t n, bool has_null,
                             std::vector<uint8_t>* codes_out) {
    if (overflowed_) {
      return false;
    }
    codes_out->resize(n);
    const size_t saved_size = str_entries_.size();
    bool has_prev = false;
    std::string prev_str;
    uint8_t prev_code = 0;
    std::vector<std::pair<size_t, StrSlot>> added_slots;

    for (size_t r = 0; r < n; ++r) {
      if (has_null && nulls[r] != 0) {
        (*codes_out)[r] = LSTableColumnDictionary::kNullCode;
        continue;
      }
      const uint32_t begin = (r == 0) ? 0U : ends[r - 1];
      const uint32_t end = ends[r];
      const std::string_view sv(reinterpret_cast<const char*>(bytes) + begin,
                                end - begin);
      if (has_prev && sv == prev_str) {
        (*codes_out)[r] = prev_code;
        continue;
      }
      const uint64_t h = HashSv(sv);
      size_t slot = static_cast<size_t>(h) & kMask;
      uint8_t code = 0;
      while (true) {
        if (str_slots_[slot].hash == 0) {
          if (str_entries_.size() >= LSTableColumnDictionary::kMaxEntries ||
              sv.size() > (size_t{1} << 20)) {
            RollbackStrings(saved_size, added_slots);
            overflowed_ = true;
            return false;
          }
          code = static_cast<uint8_t>(str_entries_.size());
          added_slots.emplace_back(slot, str_slots_[slot]);
          str_entries_.emplace_back(sv);
          str_slots_[slot] = StrSlot{.hash = h, .code = code};
          break;
        }
        if (str_slots_[slot].hash == h &&
            str_entries_[str_slots_[slot].code] == sv) {
          code = str_slots_[slot].code;
          break;
        }
        slot = (slot + 1) & kMask;
      }
      prev_str.assign(sv);
      prev_code = code;
      has_prev = true;
      (*codes_out)[r] = code;
    }
    used_in_any_block_ = true;
    return true;
  }

  void Finalize(LSTableColumnDictionary* dict_out,
                TlcsDictKind* kind_out) const {
    *dict_out = LSTableColumnDictionary{};
    if (!used_in_any_block_) {
      *kind_out = TlcsDictKind::kNone;
      return;
    }
    *kind_out = overflowed_ ? TlcsDictKind::kPartialFrozen
                            : TlcsDictKind::kCompleteNarrow;
    dict_out->value_type = type_;
    dict_out->active = !overflowed_;

    if (type_ == ValueType::kInt64 || type_ == ValueType::kDate) {
      dict_out->int_entries = int_entries_;
      dict_out->bytewise_equality = !int_entries_.empty();
    } else if (type_ == ValueType::kDouble) {
      dict_out->double_entries = double_entries_;
      dict_out->one_minus_double_entries.resize(double_entries_.size());
      dict_out->one_plus_double_entries.resize(double_entries_.size());
      for (size_t i = 0; i < double_entries_.size(); ++i) {
        dict_out->one_minus_double_entries[i] = 1.0 - double_entries_[i];
        dict_out->one_plus_double_entries[i] = 1.0 + double_entries_[i];
      }
      dict_out->bytewise_equality = !double_entries_.empty();
    } else if (type_ == ValueType::kVarChar) {
      dict_out->entries = str_entries_;
      dict_out->codes_follow_string_order =
          std::adjacent_find(dict_out->entries.begin(), dict_out->entries.end(),
                             [](const std::string& a, const std::string& b) {
                               return CompareVarCharForOrderBy(a, b, false) >=
                                      0;
                             }) == dict_out->entries.end();
      bool all_single = true;
      for (size_t i = 0; i < dict_out->entries.size(); ++i) {
        if (dict_out->entries[i].size() != 1) {
          all_single = false;
        } else {
          dict_out->single_byte_chars[i] =
              static_cast<uint8_t>(dict_out->entries[i][0]);
        }
      }
      dict_out->all_single_byte = all_single && !overflowed_;
      // Compute bytewise Value equality proof (handles interval spellings).
      struct VHash {
        const std::vector<std::string>* e;
        size_t operator()(uint32_t c) const {
          Value v;
          v.type = ValueType::kVarChar;
          v.value.varchar_value = (*e)[c];
          return std::hash<Value>()(v);
        }
      };
      struct VEqual {
        const std::vector<std::string>* e;
        bool operator()(uint32_t a, uint32_t b) const {
          Value va;
          va.type = ValueType::kVarChar;
          va.value.varchar_value = (*e)[a];
          Value vb;
          vb.type = ValueType::kVarChar;
          vb.value.varchar_value = (*e)[b];
          return va == vb;
        }
      };
      std::unordered_set<uint32_t, VHash, VEqual> peers(
          0, VHash{&dict_out->entries}, VEqual{&dict_out->entries});
      peers.reserve(dict_out->entries.size());
      dict_out->bytewise_equality = true;
      for (size_t i = 0; i < dict_out->entries.size(); ++i) {
        if (!peers.insert(static_cast<uint32_t>(i)).second) {
          dict_out->bytewise_equality = false;
          break;
        }
      }
    }
  }

 private:
  static constexpr size_t kTableSize = 512;
  static constexpr size_t kMask = kTableSize - 1;
  struct IntSlot {
    uint64_t hash{0};
    int64_t val{0};
    uint8_t code{0};
  };
  struct StrSlot {
    uint64_t hash{0};
    uint8_t code{0};
  };

  void RollbackInts(size_t saved_size,
                    const std::vector<std::pair<size_t, IntSlot>>& added) {
    for (const auto& it : std::ranges::reverse_view(added)) {
      int_slots_[it.first] = it.second;
    }
    int_entries_.resize(saved_size);
  }

  void RollbackDoubles(size_t saved_size,
                       const std::vector<std::pair<size_t, IntSlot>>& added) {
    for (const auto& it : std::ranges::reverse_view(added)) {
      int_slots_[it.first] = it.second;
    }
    double_entries_.resize(saved_size);
  }

  void RollbackStrings(size_t saved_size,
                       const std::vector<std::pair<size_t, StrSlot>>& added) {
    for (const auto& it : std::ranges::reverse_view(added)) {
      str_slots_[it.first] = it.second;
    }
    str_entries_.resize(saved_size);
  }

  ValueType type_{ValueType::kNull};
  bool is_unsigned_{false};
  bool overflowed_{false};
  bool used_in_any_block_{false};
  std::array<IntSlot, kTableSize> int_slots_{};
  std::array<StrSlot, kTableSize> str_slots_{};
  std::vector<int64_t> int_entries_;
  std::vector<double> double_entries_;
  std::vector<std::string> str_entries_;
};

// Reusable 1-block (<= 32,768 rows) staging accumulator so TLCS compaction
// keeps peak scratch memory tiny even across 600M-row tables.
struct StreamingColSynopsis {
  ValueType type{ValueType::kNull};
  bool is_unsigned{false};
  bool has_null{false};
  bool is_monotonic{false};
  bool is_unique_nonnull{false};
  bool is_identity_1_to_n{false};
  bool have_prev{false};
  int64_t prev{0};
  uint64_t expected_id{1};
  int64_t min_int{std::numeric_limits<int64_t>::max()};
  int64_t max_int{std::numeric_limits<int64_t>::min()};

  void Init(ValueType vt, bool u) noexcept {
    type = vt;
    is_unsigned = u;
    const bool track_int =
        (vt == ValueType::kInt64 || vt == ValueType::kDate) && !u;
    is_monotonic = track_int;
    is_unique_nonnull = track_int;
    is_identity_1_to_n = (vt == ValueType::kInt64) && !u;
  }

  void AddNull() noexcept {
    has_null = true;
    is_monotonic = false;
    is_unique_nonnull = false;
    is_identity_1_to_n = false;
  }

  void AddInt(int64_t v) noexcept {
    if ((type != ValueType::kInt64 && type != ValueType::kDate) ||
        is_unsigned) {
      return;
    }
    min_int = std::min(min_int, v);
    max_int = std::max(max_int, v);
    if (is_monotonic) {
      if (have_prev) {
        if (v < prev) {
          is_monotonic = false;
          is_unique_nonnull = false;
          is_identity_1_to_n = false;
        } else if (v == prev) {
          is_unique_nonnull = false;
          is_identity_1_to_n = false;
        }
      }
      if (is_identity_1_to_n && std::cmp_not_equal(v, expected_id)) {
        is_identity_1_to_n = false;
      }
      ++expected_id;
      prev = v;
      have_prev = true;
    }
  }

  [[nodiscard]] TlcsFile::ColumnSynopsis Finish(
      uint64_t total_rows) const noexcept {
    TlcsFile::ColumnSynopsis out;
    out.has_null = has_null;
    if (total_rows > 0 &&
        (type == ValueType::kInt64 || type == ValueType::kDate) &&
        !is_unsigned) {
      out.is_monotonic = is_monotonic;
      out.is_unique_nonnull = is_unique_nonnull;
      out.is_identity_1_to_n = is_identity_1_to_n;
      out.min_int = min_int;
      out.max_int = max_int;
    }
    return out;
  }
};

struct BlockStaging {
  struct Col {
    ValueType type{ValueType::kNull};
    bool is_unsigned{false};
    std::vector<uint8_t> nulls;
    std::vector<int64_t> ints;
    std::vector<uint8_t> varchar_bytes;
    std::vector<uint32_t> varchar_ends;
    bool has_null{false};
    bool has_min_max{false};
    bool has_double_min_max{false};
    bool has_nan{false};
    bool all_single_byte_varchar{true};
    int64_t min_int{std::numeric_limits<int64_t>::max()};
    int64_t max_int{std::numeric_limits<int64_t>::min()};
    double min_double{std::numeric_limits<double>::infinity()};
    double max_double{-std::numeric_limits<double>::infinity()};

    void ResetForBlock() {
      nulls.clear();
      ints.clear();
      varchar_bytes.clear();
      varchar_ends.clear();
      has_null = false;
      has_min_max = false;
      has_double_min_max = false;
      has_nan = false;
      all_single_byte_varchar = true;
      min_int = std::numeric_limits<int64_t>::max();
      max_int = std::numeric_limits<int64_t>::min();
      min_double = std::numeric_limits<double>::infinity();
      max_double = -std::numeric_limits<double>::infinity();
    }
  };

  uint32_t row_count{0};
  bool has_any_null{false};
  std::vector<Col> cols;

  void Init(const Schema& schema) {
    cols.resize(schema.ColumnCount());
    for (slot_t c = 0; c < schema.ColumnCount(); ++c) {
      cols[c].type = schema.GetColumn(c).Type();
      cols[c].is_unsigned = schema.GetColumn(c).IsUnsigned();
      cols[c].ResetForBlock();
    }
    row_count = 0;
    has_any_null = false;
  }

  void ResetBlock() {
    row_count = 0;
    has_any_null = false;
    for (Col& c : cols) {
      c.ResetForBlock();
    }
  }

  Status AppendRow(const Row& row,
                   std::vector<StreamingColSynopsis>* synopses = nullptr) {
    const auto ncol = static_cast<slot_t>(cols.size());
    if (row.Count() != ncol) {
      return StatusError(StatusCode::kCorrupt,
                         "tlcs writer row column count mismatch");
    }
    for (slot_t c = 0; c < ncol; ++c) {
      Col& col = cols[c];
      const Value& v = row[c];
      if (v.IsNull()) {
        has_any_null = true;
        if (!col.has_null) {
          col.has_null = true;
          col.nulls.assign(row_count, 0);
        }
        col.nulls.push_back(1);
        if (col.type == ValueType::kVarChar) {
          col.all_single_byte_varchar = false;
          col.varchar_ends.push_back(
              static_cast<uint32_t>(col.varchar_bytes.size()));
        } else {
          col.ints.push_back(0);
        }
        if (synopses != nullptr) {
          (*synopses)[c].AddNull();
        }
        continue;
      }
      if (col.has_null) {
        col.nulls.push_back(0);
      }
      switch (col.type) {
        case ValueType::kInt64:
        case ValueType::kDate: {
          const int64_t iv = v.value.int_value;
          col.ints.push_back(iv);
          if (!col.is_unsigned) {
            col.has_min_max = true;
            col.min_int = std::min(col.min_int, iv);
            col.max_int = std::max(col.max_int, iv);
          }
          if (synopses != nullptr) {
            (*synopses)[c].AddInt(iv);
          }
          break;
        }
        case ValueType::kDouble: {
          const double dv = v.value.double_value;
          int64_t bits = 0;
          std::memcpy(&bits, &dv, sizeof(bits));
          col.ints.push_back(bits);
          if (std::isnan(dv)) {
            col.has_nan = true;
          } else {
            col.has_double_min_max = true;
            col.min_double = std::min(col.min_double, dv);
            col.max_double = std::max(col.max_double, dv);
          }
          break;
        }
        case ValueType::kVarChar: {
          const std::string_view sv = v.value.varchar_value;
          if (sv.size() != 1) {
            col.all_single_byte_varchar = false;
          }
          const size_t next_end = col.varchar_bytes.size() + sv.size();
          if (next_end > std::numeric_limits<uint32_t>::max()) {
            return StatusError(StatusCode::kTooBigData,
                               "tlcs block varchar stream exceeds 4 GiB");
          }
          col.varchar_bytes.insert(
              col.varchar_bytes.end(),
              reinterpret_cast<const uint8_t*>(sv.data()),
              reinterpret_cast<const uint8_t*>(sv.data()) + sv.size());
          col.varchar_ends.push_back(static_cast<uint32_t>(next_end));
          break;
        }
        default:
          return StatusError(StatusCode::kNotImplemented,
                             "tlcs unsupported column type");
      }
    }
    ++row_count;
    return Status::kSuccess;
  }

  Status AppendRawRow(std::string_view raw, StreamingStatsCollector* stats,
                      std::vector<StreamingColSynopsis>* synopses) {
    constexpr slot_t kNullBitmapFlag = slot_t{1} << 15;
    if (raw.size() < sizeof(slot_t)) {
      return StatusError(StatusCode::kCorrupt, "truncated row image");
    }
    const char* cursor = raw.data();
    const char* const end = raw.data() + raw.size();
    slot_t encoded_count = 0;
    cursor += DeserializeSlot(cursor, &encoded_count);
    const bool row_has_null = (encoded_count & kNullBitmapFlag) != 0;
    const auto stored_count =
        static_cast<slot_t>(encoded_count & ~kNullBitmapFlag);
    const auto ncol = static_cast<slot_t>(cols.size());
    if (stored_count != ncol) {
      return StatusError(StatusCode::kCorrupt,
                         "tlcs writer row column count mismatch");
    }
    const char* bitmap = nullptr;
    if (row_has_null) {
      const size_t bitmap_bytes = (stored_count + 7) / 8;
      if (std::cmp_less(end - cursor, bitmap_bytes)) {
        return StatusError(StatusCode::kCorrupt, "truncated row null bitmap");
      }
      bitmap = cursor;
      cursor += bitmap_bytes;
    }
    if (stats != nullptr) {
      stats->BeginRow();
    }
    for (slot_t c = 0; c < ncol; ++c) {
      Col& col = cols[c];
      const bool is_null =
          row_has_null &&
          (bitmap[c / 8] & static_cast<char>(1U << (c % 8))) != 0;
      if (is_null) {
        has_any_null = true;
        if (!col.has_null) {
          col.has_null = true;
          col.nulls.assign(row_count, 0);
        }
        col.nulls.push_back(1);
        if (col.type == ValueType::kVarChar) {
          col.all_single_byte_varchar = false;
          col.varchar_ends.push_back(
              static_cast<uint32_t>(col.varchar_bytes.size()));
        } else {
          col.ints.push_back(0);
        }
        if (stats != nullptr) {
          stats->AddNull(c);
        }
        if (synopses != nullptr && c < synopses->size()) {
          (*synopses)[c].AddNull();
        }
        continue;
      }
      if (col.has_null) {
        col.nulls.push_back(0);
      }
      switch (col.type) {
        case ValueType::kInt64:
        case ValueType::kDate: {
          if (std::cmp_less(end - cursor, sizeof(uint64_t))) {
            return StatusError(StatusCode::kCorrupt,
                               "truncated row integer field");
          }
          int64_t iv = 0;
          cursor += DeserializeInteger(cursor, &iv);
          col.ints.push_back(iv);
          if (!col.is_unsigned) {
            col.has_min_max = true;
            col.min_int = std::min(col.min_int, iv);
            col.max_int = std::max(col.max_int, iv);
          }
          if (stats != nullptr) {
            stats->AddInt(c, iv, col.is_unsigned);
          }
          if (synopses != nullptr && c < synopses->size()) {
            (*synopses)[c].AddInt(iv);
          }
          break;
        }
        case ValueType::kDouble: {
          if (std::cmp_less(end - cursor, sizeof(uint64_t))) {
            return StatusError(StatusCode::kCorrupt,
                               "truncated row double field");
          }
          double dv = 0.0;
          cursor += DeserializeDouble(cursor, &dv);
          int64_t bits = 0;
          std::memcpy(&bits, &dv, sizeof(bits));
          col.ints.push_back(bits);
          if (std::isnan(dv)) {
            col.has_nan = true;
          } else {
            col.has_double_min_max = true;
            col.min_double = std::min(col.min_double, dv);
            col.max_double = std::max(col.max_double, dv);
          }
          if (stats != nullptr) {
            stats->AddDouble(c, dv);
          }
          break;
        }
        case ValueType::kVarChar: {
          if (std::cmp_less(end - cursor, sizeof(bin_size_t))) {
            return StatusError(StatusCode::kCorrupt,
                               "truncated row varchar length");
          }
          bin_size_t length = 0;
          cursor += DeserializeSlot(cursor, &length);
          if (std::cmp_less(end - cursor, length)) {
            return StatusError(StatusCode::kCorrupt,
                               "truncated row varchar payload");
          }
          const std::string_view sv(cursor, length);
          cursor += length;
          if (sv.size() != 1) {
            col.all_single_byte_varchar = false;
          }
          const size_t next_end = col.varchar_bytes.size() + sv.size();
          if (next_end > std::numeric_limits<uint32_t>::max()) {
            return StatusError(StatusCode::kTooBigData,
                               "tlcs block varchar stream exceeds 4 GiB");
          }
          col.varchar_bytes.insert(
              col.varchar_bytes.end(),
              reinterpret_cast<const uint8_t*>(sv.data()),
              reinterpret_cast<const uint8_t*>(sv.data()) + sv.size());
          col.varchar_ends.push_back(static_cast<uint32_t>(next_end));
          if (stats != nullptr) {
            stats->AddVarChar(c, sv);
          }
          break;
        }
        default:
          return StatusError(StatusCode::kNotImplemented,
                             "tlcs unsupported column type");
      }
    }
    ++row_count;
    return Status::kSuccess;
  }
};

void Int64BufferToLe64(const int64_t* __restrict src, uint32_t n,
                       uint8_t* __restrict dst) noexcept;

Status FlushStagedBlock(const BlockStaging& stage,
                        std::vector<OnlineColumnDict>& online_dicts,
                        BufferedSeqWriter& writer,
                        std::vector<uint8_t>& scratch_codes,
                        std::vector<uint8_t>& scratch_for,
                        TlcsFile::BlockDirEntry* dir_out) {
  TlcsFile::BlockDirEntry dir;
  dir.pid = 0;
  dir.row_count = stage.row_count;
  dir.has_any_null = stage.has_any_null;
  dir.columns.resize(stage.cols.size());

  const uint32_t n = stage.row_count;
  for (size_t c = 0; c < stage.cols.size(); ++c) {
    const BlockStaging::Col& sc = stage.cols[c];
    TlcsFile::ColumnMeta& meta = dir.columns[c];
    meta.type = sc.type;
    meta.is_unsigned = sc.is_unsigned;
    meta.has_null = sc.has_null;
    meta.has_min_max = sc.has_min_max;
    meta.has_double_min_max = sc.has_double_min_max;
    meta.has_nan = sc.has_nan;
    meta.all_single_byte_varchar =
        sc.type == ValueType::kVarChar && sc.all_single_byte_varchar;
    meta.min_int = sc.min_int;
    meta.max_int = sc.max_int;
    meta.min_double = sc.min_double;
    meta.max_double = sc.max_double;

    if (sc.has_null) {
      RETURN_IF_FAIL(writer.AlignTo8());
      meta.nulls_offset = writer.Offset();
      meta.nulls_size = n;
      RETURN_IF_FAIL(writer.Append(sc.nulls.data(), n));
    }

    const uint8_t* nulls_ptr = sc.has_null ? sc.nulls.data() : nullptr;
    bool encoded_dict = false;
    if (!online_dicts[c].Overflowed()) {
      if (sc.type == ValueType::kInt64 || sc.type == ValueType::kDate) {
        encoded_dict = online_dicts[c].TryEncodeBlockInts(
            sc.ints.data(), nulls_ptr, n, sc.has_null, &scratch_codes);
      } else if (sc.type == ValueType::kDouble) {
        encoded_dict = online_dicts[c].TryEncodeBlockDoubles(
            sc.ints.data(), nulls_ptr, n, sc.has_null, &scratch_codes);
      } else if (sc.type == ValueType::kVarChar) {
        encoded_dict = online_dicts[c].TryEncodeBlockVarChar(
            sc.varchar_bytes.data(), sc.varchar_ends.data(), nulls_ptr, n,
            sc.has_null, &scratch_codes);
      }
    }

    if (encoded_dict) {
      meta.encoding = TlcsColumnEncoding::kNarrowDict;
      uint32_t non_null_const = UINT32_MAX;
      bool same_code = !sc.has_null;
      for (uint32_t r = 0; r < n; ++r) {
        const uint8_t code = scratch_codes[r];
        if (code == LSTableColumnDictionary::kNullCode) {
          same_code = false;
          continue;
        }
        meta.has_code_min_max = true;
        meta.min_code = std::min<uint32_t>(meta.min_code, code);
        meta.max_code = std::max<uint32_t>(meta.max_code, code);
        meta.dict_mask[code >> 6] |= uint64_t{1} << (code & 63);
        if (same_code) {
          if (non_null_const == UINT32_MAX) {
            non_null_const = code;
          } else if (non_null_const != code) {
            same_code = false;
          }
        }
      }
      if (same_code && non_null_const != UINT32_MAX) {
        meta.is_const_code = true;
        meta.const_code = non_null_const;
      }
      RETURN_IF_FAIL(writer.AlignTo8());
      meta.data_offset = writer.Offset();
      meta.data_size = n;
      RETURN_IF_FAIL(writer.Append(scratch_codes.data(), n));
      continue;
    }

    if (sc.type == ValueType::kInt64 || sc.type == ValueType::kDate) {
      meta.encoding = TlcsColumnEncoding::kFor;
      if (!sc.has_min_max || sc.is_unsigned) {
        if (!sc.has_min_max && !sc.is_unsigned) {
          // All NULL rows: zero-width constant placeholder.
          meta.for_width = 0;
          meta.for_base = 0;
        } else {
          meta.for_width = 8;
          meta.for_base = 0;
          RETURN_IF_FAIL(writer.AlignTo8());
          meta.data_offset = writer.Offset();
          meta.data_size = static_cast<uint64_t>(n) * 8ULL;
          scratch_for.resize(static_cast<size_t>(meta.data_size));
          Int64BufferToLe64(sc.ints.data(), n, scratch_for.data());
          RETURN_IF_FAIL(writer.Append(scratch_for.data(), meta.data_size));
        }
        continue;
      }
      const uint64_t span =
          static_cast<uint64_t>(sc.max_int) - static_cast<uint64_t>(sc.min_int);
      meta.for_base = sc.min_int;
      const uint64_t base_u =                    // NOLINT(modernize-use-auto)
          static_cast<uint64_t>(meta.for_base);  // NOLINT(modernize-use-auto)
                                                 // reviewed intentional pattern
      if (span == 0) {
        meta.for_width = 0;
        continue;
      }
      if (span <= 0xFFULL) {
        meta.for_width = 1;
        scratch_for.resize(n);
        for (uint32_t r = 0; r < n; ++r) {
          scratch_for[r] =  // NOLINT(clang-diagnostic-error)
              static_cast<uint8_t>(static_cast<uint64_t>(sc.ints[r]) - base_u);
        }
        RETURN_IF_FAIL(writer.AlignTo8());
        meta.data_offset = writer.Offset();
        meta.data_size = n;
        RETURN_IF_FAIL(writer.Append(scratch_for.data(), n));
      } else if (span <= 0xFFFFULL) {
        meta.for_width = 2;
        scratch_for.resize(static_cast<size_t>(n) * 2);
        for (uint32_t r = 0; r < n; ++r) {
          const auto delta =  // NOLINT(clang-diagnostic-error)
              static_cast<uint16_t>(static_cast<uint64_t>(sc.ints[r]) - base_u);
          const uint16_t le = htole16(delta);
          std::memcpy(scratch_for.data() + (r * 2ULL), &le, sizeof(le));
        }
        RETURN_IF_FAIL(writer.AlignTo8());
        meta.data_offset = writer.Offset();
        meta.data_size = scratch_for.size();
        RETURN_IF_FAIL(writer.Append(scratch_for.data(), scratch_for.size()));
      } else if (span <= 0xFFFFFFFFULL) {
        meta.for_width = 4;
        scratch_for.resize(static_cast<size_t>(n) * 4);
        for (uint32_t r = 0; r < n; ++r) {
          const auto delta =  // NOLINT(clang-diagnostic-error)
              static_cast<uint32_t>(static_cast<uint64_t>(sc.ints[r]) - base_u);
          const uint32_t le = htole32(delta);
          std::memcpy(scratch_for.data() + (r * 4ULL), &le, sizeof(le));
        }
        RETURN_IF_FAIL(writer.AlignTo8());
        meta.data_offset = writer.Offset();
        meta.data_size = scratch_for.size();
        RETURN_IF_FAIL(writer.Append(scratch_for.data(), scratch_for.size()));
      } else {
        meta.for_width = 8;
        meta.for_base = 0;
        RETURN_IF_FAIL(writer.AlignTo8());
        meta.data_offset = writer.Offset();
        meta.data_size = static_cast<uint64_t>(n) * 8ULL;
        scratch_for.resize(static_cast<size_t>(meta.data_size));
        Int64BufferToLe64(sc.ints.data(), n, scratch_for.data());
        RETURN_IF_FAIL(writer.Append(scratch_for.data(), meta.data_size));
      }
    } else if (sc.type == ValueType::kDouble) {
      meta.encoding = TlcsColumnEncoding::kRawDouble;
      meta.for_width = 8;
      RETURN_IF_FAIL(writer.AlignTo8());
      meta.data_offset = writer.Offset();
      meta.data_size = static_cast<uint64_t>(n) * 8ULL;
      scratch_for.resize(static_cast<size_t>(meta.data_size));
      Int64BufferToLe64(sc.ints.data(), n, scratch_for.data());
      RETURN_IF_FAIL(writer.Append(scratch_for.data(), meta.data_size));
    } else if (sc.type == ValueType::kVarChar) {
      meta.encoding = TlcsColumnEncoding::kVarChar;
      if (!sc.varchar_bytes.empty()) {
        RETURN_IF_FAIL(writer.AlignTo8());
        meta.data_offset = writer.Offset();
        meta.data_size = sc.varchar_bytes.size();
        RETURN_IF_FAIL(
            writer.Append(sc.varchar_bytes.data(), sc.varchar_bytes.size()));
      }
      if (!sc.varchar_ends.empty()) {
        RETURN_IF_FAIL(writer.AlignTo8());
        meta.aux_offset = writer.Offset();
        meta.aux_size = sc.varchar_ends.size() * sizeof(uint32_t);
        RETURN_IF_FAIL(writer.Append(sc.varchar_ends.data(), meta.aux_size));
      }
    }
  }

  *dir_out = std::move(dir);
  return Status::kSuccess;
}

std::string EncodeHeaderBytes(uint64_t layout_generation, page_id_t first_pid,
                              uint64_t total_rows, uint32_t block_count,
                              uint32_t column_count, uint64_t directory_offset,
                              uint64_t directory_length,
                              uint32_t directory_crc32c) {
  std::string hdr;
  hdr.reserve(kTlcsHeaderBytes);
  hdr.append(kTlcsMagic);
  AppendU32(hdr, TlcsFile::kFormatVersion);
  AppendU32(hdr, 0U);
  AppendU64(hdr, layout_generation);
  AppendU64(hdr, first_pid);
  AppendU64(hdr, total_rows);
  AppendU32(hdr, block_count);
  AppendU32(hdr, column_count);
  AppendU64(hdr, directory_offset);
  AppendU64(hdr, directory_length);
  AppendU32(hdr, directory_crc32c);
  const uint32_t hdr_crc = Crc32C(hdr.data(), hdr.size());
  AppendU32(hdr, hdr_crc);
  return hdr;
}

void SerializeDirGroupBody(std::span<const TlcsFile::BlockDirEntry> blocks,
                           uint32_t col_count, std::string* out) {
  out->clear();
  for (const TlcsFile::BlockDirEntry& b : blocks) {
    AppendU32(*out, b.row_count);
    AppendU8(*out, b.has_any_null ? 1 : 0);
    for (uint32_t c = 0; c < col_count; ++c) {
      const TlcsFile::ColumnMeta& m = b.columns[c];
      AppendU8(*out, static_cast<uint8_t>(m.encoding));
      AppendU8(*out, m.for_width);
      const uint8_t flags =
          (m.has_null ? 1U : 0U) | (m.has_min_max ? 2U : 0U) |
          (m.has_double_min_max ? 4U : 0U) | (m.has_nan ? 8U : 0U) |
          (m.all_single_byte_varchar ? 16U : 0U) |
          (m.is_const_code ? 32U : 0U) | (m.has_code_min_max ? 64U : 0U);
      AppendU8(*out, flags);
      AppendI64(*out, m.for_base);
      AppendI64(*out, m.min_int);
      AppendI64(*out, m.max_int);
      AppendF64(*out, m.min_double);
      AppendF64(*out, m.max_double);
      AppendU32(*out, m.const_code);
      AppendU32(*out, m.min_code);
      AppendU32(*out, m.max_code);
      for (const uint64_t w : m.dict_mask) {
        AppendU64(*out, w);
      }
      AppendU64(*out, m.nulls_offset);
      AppendU64(*out, m.nulls_size);
      AppendU64(*out, m.data_offset);
      AppendU64(*out, m.data_size);
      AppendU64(*out, m.aux_offset);
      AppendU64(*out, m.aux_size);
    }
  }
}

Status ParseDirGroupBody(std::span<const uint8_t> raw, uint32_t block_count,
                         uint32_t col_count,
                         const std::vector<ValueType>& col_types,
                         const std::vector<uint8_t>& col_unsigned,
                         const TlcsFile::DirGroupMeta& meta,
                         std::vector<page_id_t>* pids_out,
                         std::vector<TlcsFile::BlockDirEntry>* blocks_out) {
  const size_t pid_bytes_len = static_cast<size_t>(block_count) * 8ULL;
  if (raw.size() != pid_bytes_len + meta.body_size) {
    return StatusError(StatusCode::kCorrupt, "tlcs dir group size mismatch");
  }
  const std::span<const uint8_t> pid_span = raw.subspan(0, pid_bytes_len);
  const std::span<const uint8_t> body_span = raw.subspan(pid_bytes_len);
  if (Crc32C(pid_span.data(), pid_span.size()) != meta.pid_crc32c ||
      Crc32C(body_span.data(), body_span.size()) != meta.body_crc32c) {
    return StatusError(StatusCode::kCorrupt,
                       "TLCS dir group checksum mismatch");
  }
  size_t ppos = 0;
  pids_out->resize(block_count);
  for (uint32_t i = 0; i < block_count; ++i) {
    if (!ReadU64(pid_span, &ppos, &(*pids_out)[i])) {
      return StatusError(StatusCode::kCorrupt, "tlcs dir group pid truncated");
    }
  }
  size_t dpos = 0;
  blocks_out->resize(block_count);
  for (uint32_t bi = 0; bi < block_count; ++bi) {
    TlcsFile::BlockDirEntry& b = (*blocks_out)[bi];
    b.pid = (*pids_out)[bi];
    uint8_t any_null = 0;
    if (!ReadU32(body_span, &dpos, &b.row_count) ||
        !ReadU8(body_span, &dpos, &any_null)) {
      return StatusError(StatusCode::kCorrupt, "tlcs block meta truncated");
    }
    b.has_any_null = (any_null != 0);
    b.columns.resize(col_count);
    for (uint32_t c = 0; c < col_count; ++c) {
      TlcsFile::ColumnMeta& m = b.columns[c];
      m.type = col_types[c];
      m.is_unsigned = (col_unsigned[c] != 0);
      uint8_t enc = 0;
      uint8_t fw = 0;
      uint8_t bflags = 0;
      if (!ReadU8(body_span, &dpos, &enc) || !ReadU8(body_span, &dpos, &fw) ||
          !ReadU8(body_span, &dpos, &bflags) ||
          !ReadI64(body_span, &dpos, &m.for_base) ||
          !ReadI64(body_span, &dpos, &m.min_int) ||
          !ReadI64(body_span, &dpos, &m.max_int) ||
          !ReadF64(body_span, &dpos, &m.min_double) ||
          !ReadF64(body_span, &dpos, &m.max_double) ||
          !ReadU32(body_span, &dpos, &m.const_code) ||
          !ReadU32(body_span, &dpos, &m.min_code) ||
          !ReadU32(body_span, &dpos, &m.max_code)) {
        return StatusError(StatusCode::kCorrupt, "tlcs col meta truncated");
      }
      if (enc > static_cast<uint32_t>(TlcsColumnEncoding::kVarChar)) {
        return StatusError(StatusCode::kCorrupt,
                           "tlcs unknown column encoding");
      }
      m.encoding = static_cast<TlcsColumnEncoding>(enc);
      m.for_width = fw;
      m.has_null = (bflags & 1U) != 0;
      m.has_min_max = (bflags & 2U) != 0;
      m.has_double_min_max = (bflags & 4U) != 0;
      m.has_nan = (bflags & 8U) != 0;
      m.all_single_byte_varchar = (bflags & 16U) != 0;
      m.is_const_code = (bflags & 32U) != 0;
      m.has_code_min_max = (bflags & 64U) != 0;
      for (uint64_t& w : m.dict_mask) {
        if (!ReadU64(body_span, &dpos, &w)) {
          return StatusError(StatusCode::kCorrupt, "tlcs mask truncated");
        }
      }
      if (!ReadU64(body_span, &dpos, &m.nulls_offset) ||
          !ReadU64(body_span, &dpos, &m.nulls_size) ||
          !ReadU64(body_span, &dpos, &m.data_offset) ||
          !ReadU64(body_span, &dpos, &m.data_size) ||
          !ReadU64(body_span, &dpos, &m.aux_offset) ||
          !ReadU64(body_span, &dpos, &m.aux_size)) {
        return StatusError(StatusCode::kCorrupt, "tlcs offsets truncated");
      }
    }
  }
  if (dpos != body_span.size()) {
    return StatusError(StatusCode::kCorrupt, "tlcs dir group length mismatch");
  }
  return Status::kSuccess;
}

// A directory group's per-column payload offsets are attacker-controlled bytes
// (the body is CRC-checked but a corrupt producer can recompute the CRC). The
// resident-store builder passes these offsets straight into `std::span` /
// `memcpy` / `Pread` calls writing into buffers sized from `row_count`, so an
// out-of-range offset is a SIGSEGV (mmap) or a heap overflow (paged). Reject
// such spans up front, mirroring the checks in DecodeBlockColumns (which the
// paged block decoder already applies).
bool ColumnPayloadSpansValid(const TlcsFile::DirGroup& group,
                             uint32_t column_count, uint64_t file_size) {
  const auto in_file = [file_size](uint64_t offset, uint64_t size) {
    // Overflow-safe extent check; an empty span may carry a stale offset.
    return size == 0 || (offset <= file_size && size <= file_size - offset);
  };
  for (const TlcsFile::BlockDirEntry& b : group.blocks) {
    const uint64_t n = b.row_count;
    // A producer never writes more than one page's worth of columnar rows.
    // Without this cap a corrupt directory can claim ~4 G rows with tiny
    // (e.g. constant-encoded) payloads and make decoders resize multi-gigabyte
    // vectors from a mere status-bearing read.
    if (n > RowPage::kMaxColumnarRows) {
      return false;
    }
    if (b.columns.size() < column_count) {
      return false;
    }
    for (uint32_t c = 0; c < column_count; ++c) {
      const TlcsFile::ColumnMeta& m = b.columns[c];
      if (m.has_null &&
          (m.nulls_size != n || !in_file(m.nulls_offset, m.nulls_size))) {
        return false;
      }
      switch (m.encoding) {
        case TlcsColumnEncoding::kNarrowDict:
          // One code byte per row (both the resident and paged decoders index
          // exactly `row_count` codes from this span).
          if (m.data_size != n || !in_file(m.data_offset, m.data_size)) {
            return false;
          }
          break;
        case TlcsColumnEncoding::kFor:
          if (m.for_width == 0) {
            break;  // constant/zero-width placeholder carries no payload.
          }
          if ((m.for_width != 1 && m.for_width != 2 && m.for_width != 4 &&
               m.for_width != 8) ||
              m.data_size != n * m.for_width ||
              !in_file(m.data_offset, m.data_size)) {
            return false;
          }
          break;
        case TlcsColumnEncoding::kRawDouble:
          if (m.data_size != n * 8ULL || !in_file(m.data_offset, m.data_size)) {
            return false;
          }
          break;
        case TlcsColumnEncoding::kVarChar:
          if (!in_file(m.data_offset, m.data_size) ||
              m.aux_size != n * sizeof(uint32_t) ||
              !in_file(m.aux_offset, m.aux_size)) {
            return false;
          }
          break;
        case TlcsColumnEncoding::kConstNull:
          break;
        default:
          return false;
      }
    }
  }
  return true;
}

std::string EncodeMetaDirectoryBytes(
    const Schema& schema, const std::vector<LSTableColumnDictionary>& dicts,
    const std::vector<TlcsDictKind>& dict_kinds, uint32_t dir_group_size,
    const std::vector<TlcsFile::DirGroupMeta>& dir_groups,
    std::string_view serialized_column_stats,
    const std::vector<TlcsFile::ColumnSynopsis>& col_synopses,
    uint64_t* stats_rel_offset_out) {
  std::string dir;
  const uint32_t col_count = schema.ColumnCount();
  for (uint32_t c = 0; c < col_count; ++c) {
    AppendU8(dir, static_cast<uint8_t>(schema.GetColumn(c).Type()));
    AppendU8(dir, schema.GetColumn(c).IsUnsigned() ? 1 : 0);
  }
  for (uint32_t c = 0; c < col_count; ++c) {
    const TlcsDictKind kind = dict_kinds[c];
    const LSTableColumnDictionary& d = dicts[c];
    AppendU8(dir, static_cast<uint8_t>(kind));
    AppendU8(dir, static_cast<uint8_t>(d.value_type));
    AppendU8(dir, d.bytewise_equality ? 1 : 0);
    AppendU8(dir, d.codes_follow_string_order ? 1 : 0);
    AppendU8(dir, d.all_single_byte ? 1 : 0);
    if (kind == TlcsDictKind::kNone) {
      AppendU32(dir, 0);
      continue;
    }
    if (d.value_type == ValueType::kInt64 || d.value_type == ValueType::kDate) {
      AppendU32(dir, static_cast<uint32_t>(d.int_entries.size()));
      for (const int64_t v : d.int_entries) {
        AppendI64(dir, v);
      }
    } else if (d.value_type == ValueType::kDouble) {
      AppendU32(dir, static_cast<uint32_t>(d.double_entries.size()));
      for (const double v : d.double_entries) {
        AppendF64(dir, v);
      }
    } else {
      AppendU32(dir, static_cast<uint32_t>(d.entries.size()));
      for (const std::string& s : d.entries) {
        AppendU32(dir, static_cast<uint32_t>(s.size()));
        dir.append(s);
      }
    }
  }
  AppendU32(dir, dir_group_size);
  AppendU32(dir, static_cast<uint32_t>(dir_groups.size()));
  for (const TlcsFile::DirGroupMeta& gm : dir_groups) {
    AppendU64(dir, gm.group_offset);
    AppendU32(dir, gm.block_count);
    AppendU32(dir, gm.pid_crc32c);
    AppendU64(dir, gm.body_size);
    AppendU32(dir, gm.body_crc32c);
    AppendU64(dir, gm.first_pid);
    AppendU64(dir, gm.last_pid);
    AppendU64(dir, gm.min_pid);
    AppendU64(dir, gm.max_pid);
    AppendU8(dir, gm.pids_monotonic ? 1 : 0);
  }
  AppendU32(dir, static_cast<uint32_t>(serialized_column_stats.size()));
  *stats_rel_offset_out = dir.size();
  dir.append(serialized_column_stats);
  for (uint32_t c = 0; c < col_count; ++c) {
    const TlcsFile::ColumnSynopsis syn =
        c < col_synopses.size() ? col_synopses[c] : TlcsFile::ColumnSynopsis{};
    AppendU8(dir, syn.has_null ? 1 : 0);
    AppendU8(dir, syn.is_monotonic ? 1 : 0);
    AppendU8(dir, syn.is_unique_nonnull ? 1 : 0);
    AppendU8(dir, syn.is_identity_1_to_n ? 1 : 0);
    AppendI64(dir, syn.min_int);
    AppendI64(dir, syn.max_int);
  }
  return dir;
}

// TLCS persists 8-byte column payloads little-endian, matching the
// htole16/htole32 narrow FOR paths and AppendU64 for metadata. Convert at the
// format boundary so files are portable across host byte orders.
void Int64BufferToLe64(const int64_t* __restrict src, uint32_t n,
                       uint8_t* __restrict dst) noexcept {
  for (uint32_t r = 0; r < n; ++r) {
    const uint64_t le = htole64(static_cast<uint64_t>(src[r]));
    std::memcpy(dst + (static_cast<size_t>(r) * 8), &le, sizeof(le));
  }
}

void Le64BufferToInt64(const uint8_t* __restrict src, uint32_t n,
                       int64_t* __restrict dst) noexcept {
  if constexpr (std::endian::native == std::endian::little) {
    if (n > 0) {
      std::memcpy(dst, src, static_cast<size_t>(n) * sizeof(int64_t));
    }
    return;
  }
  for (uint32_t r = 0; r < n; ++r) {
    uint64_t v = 0;
    std::memcpy(&v, src + (static_cast<size_t>(r) * 8), sizeof(v));
    dst[r] = static_cast<int64_t>(le64toh(v));
  }
}

void UnpackForToInt64(const TlcsFile::ColumnMeta& meta,
                      std::span<const uint8_t> raw, uint32_t n,
                      int64_t* __restrict dst) noexcept {
  const auto base_u = static_cast<uint64_t>(meta.for_base);
  switch (meta.for_width) {
    case 0:
      for (uint32_t r = 0; r < n; ++r) {
        dst[r] = meta.for_base;
      }
      return;
    case 1:
      for (uint32_t r = 0; r < n; ++r) {
        dst[r] = static_cast<int64_t>(base_u + raw[r]);
      }
      return;
    case 2:
      for (uint32_t r = 0; r < n; ++r) {
        uint16_t v = 0;
        std::memcpy(&v, raw.data() + (static_cast<size_t>(r) * 2), sizeof(v));
        dst[r] = static_cast<int64_t>(base_u + le16toh(v));
      }
      return;
    case 4:
      for (uint32_t r = 0; r < n; ++r) {
        uint32_t v = 0;
        std::memcpy(&v, raw.data() + (static_cast<size_t>(r) * 4), sizeof(v));
        dst[r] = static_cast<int64_t>(base_u + le32toh(v));
      }
      return;
    case 8:
      Le64BufferToInt64(raw.data(), n, dst);
      return;
    default:
      std::memset(dst, 0, static_cast<size_t>(n) * sizeof(int64_t));
      return;
  }
}

void MaterializeDictInts(const TlcsFile::ColumnMeta& meta,
                         const LSTableColumnDictionary& dict,
                         std::span<const uint8_t> codes, uint32_t n,
                         int64_t* __restrict dst) noexcept {
  if (meta.type == ValueType::kDouble) {
    const auto entries = dict.DoubleEntries();
    for (uint32_t r = 0; r < n; ++r) {
      const uint8_t code = codes[r];
      if (code == LSTableColumnDictionary::kNullCode ||
          code >= entries.size()) {
        dst[r] = 0;
      } else {
        std::memcpy(&dst[r], &entries[code], sizeof(int64_t));
      }
    }
    return;
  }
  const auto entries = dict.IntEntries();
  for (uint32_t r = 0; r < n; ++r) {
    const uint8_t code = codes[r];
    if (code == LSTableColumnDictionary::kNullCode || code >= entries.size()) {
      dst[r] = 0;
    } else {
      dst[r] = entries[code];
    }
  }
}

size_t EstimateDecodedBlockBytes(const TlcsFile::DecodedBlock& blk) noexcept {
  size_t total = sizeof(blk) + (blk.column_ids.capacity() * sizeof(slot_t));
  for (const auto& c : blk.columns) {
    total += sizeof(c) + c.nulls.size() +
             (c.int_cells.size() * sizeof(int64_t)) + c.dict_codes.size() +
             c.varchar_bytes.size() +
             (c.varchar_ends.size() * sizeof(uint32_t));
  }
  return total;
}

void BuildEncounterCodesFromFirstRow(LSTableColumnDictionary* dict) {
  dict->encounter_codes.clear();
  std::vector<std::pair<uint64_t, uint32_t>> order;
  order.reserve(dict->code_counts.size());
  for (uint32_t code = 0; code < dict->code_counts.size(); ++code) {
    if (dict->code_counts[code] > 0) {
      order.emplace_back(dict->code_first_row[code], code);
    }
  }
  std::ranges::sort(order);
  dict->encounter_codes.reserve(order.size());
  for (const auto& [first_row, code] : order) {
    dict->encounter_codes.push_back(code);
  }
}

void ComputeZeroColumnBitsetUniqueness(
    uint64_t total_rows, std::span<const LSTableColumnBlock> blocks,
    std::span<const uint32_t> row_counts, TlcsFile::ColumnSynopsis* syn) {
  if (syn->is_unique_nonnull || syn->has_null || total_rows == 0 ||
      syn->min_int > syn->max_int) {
    return;
  }
  const uint64_t span =
      static_cast<uint64_t>(syn->max_int) - static_cast<uint64_t>(syn->min_int);
  if (span > 33'554'432ULL || span + 1 < total_rows) {
    return;
  }
  std::vector<uint64_t> seen_bits(static_cast<size_t>((span >> 6U) + 1U), 0ULL);
  const auto base_u = static_cast<uint64_t>(syn->min_int);
  for (size_t bi = 0; bi < blocks.size(); ++bi) {
    const uint32_t n = row_counts[bi];
    const int64_t* __restrict cells = blocks[bi].int_cells.data();
    for (uint32_t r = 0; r < n; ++r) {
      const uint64_t off = static_cast<uint64_t>(cells[r]) - base_u;
      if (off > span) {
        return;
      }
      const uint64_t mask = uint64_t{1} << (off & 63U);
      uint64_t& word = seen_bits[static_cast<size_t>(off >> 6U)];
      if ((word & mask) != 0) {
        return;
      }
      word |= mask;
    }
  }
  syn->is_unique_nonnull = true;
}

TlcsFile::ColumnSynopsis ComputeColumnSynopsisFromEntry(
    slot_t c, ValueType vt, bool is_unsigned, uint64_t total_rows,
    std::span<const LSTableColumnBlock> blocks,
    std::span<const uint32_t> row_counts) {
  TlcsFile::ColumnSynopsis syn;
  const bool track_int = total_rows > 0 &&
                         (vt == ValueType::kInt64 || vt == ValueType::kDate) &&
                         !is_unsigned;
  bool mono = track_int;
  bool strict_mono = track_int;
  bool identity_1_to_n = strict_mono && vt == ValueType::kInt64;
  int64_t prev = 0;
  bool have_prev = false;
  uint64_t expected_id = 1;
  for (size_t bi = 0; bi < blocks.size(); ++bi) {
    const LSTableColumnBlock& col = blocks[bi];
    const uint32_t n = row_counts[bi];
    if (col.has_null) {
      syn.has_null = true;
      mono = false;
      strict_mono = false;
      identity_1_to_n = false;
    }
    if (col.has_min_max) {
      syn.min_int = std::min(syn.min_int, col.min_int);
      syn.max_int = std::max(syn.max_int, col.max_int);
    }
    if (mono) {
      if (have_prev && col.has_min_max && col.min_int < prev) {
        mono = false;
        strict_mono = false;
        identity_1_to_n = false;
      } else {
        for (uint32_t r = 0; r < n; ++r) {
          const int64_t v = col.int_cells[r];
          if (have_prev) {
            if (v < prev) {
              mono = false;
              strict_mono = false;
              identity_1_to_n = false;
              break;
            }
            if (v == prev) {
              strict_mono = false;
              identity_1_to_n = false;
            }
          }
          if (identity_1_to_n && std::cmp_not_equal(v, expected_id)) {
            identity_1_to_n = false;
          }
          ++expected_id;
          prev = v;
          have_prev = true;
        }
      }
    }
  }
  syn.is_monotonic = mono;
  syn.is_unique_nonnull = strict_mono;
  syn.is_identity_1_to_n = identity_1_to_n;
  if (c == 0 && vt == ValueType::kInt64 && !is_unsigned) {
    ComputeZeroColumnBitsetUniqueness(total_rows, blocks, row_counts, &syn);
  }
  return syn;
}

bool DecodeColumnBlockFromFile(
    int fd, const uint8_t* base, bool using_mmap, size_t row_base,
    const TlcsFile::BlockDirEntry& src_b, const TlcsFile::ColumnMeta& sm,
    const LSTableColumnDictionary& ro_dict, TlcsDictKind dict_kind,
    bool zero_copy_64, LSTableColumnDictionary* active_dict_mut,
    uint8_t* dst_narrow_codes_base, int64_t* dst_ints_base,
    LSTableColumnBlock* dc_out, std::vector<uint8_t>* raw_tmp,
    std::vector<int64_t>* dict_counts, std::vector<uint64_t>* dict_first_row) {
  LSTableColumnBlock& dc = *dc_out;
  dc.type = sm.type;
  dc.is_unsigned = sm.is_unsigned;
  dc.has_null = sm.has_null;
  dc.has_min_max = sm.has_min_max;
  dc.has_double_min_max = sm.has_double_min_max;
  dc.has_nan = sm.has_nan;
  dc.all_single_byte_varchar = sm.all_single_byte_varchar;
  dc.for_width = sm.for_width;
  dc.for_base = sm.for_base;
  dc.min_int = sm.min_int;
  dc.max_int = sm.max_int;
  dc.min_double = sm.min_double;
  dc.max_double = sm.max_double;
  dc.is_const_code = sm.is_const_code;
  dc.has_code_min_max = sm.has_code_min_max;
  dc.const_code = sm.const_code;
  dc.min_code = sm.min_code;
  dc.max_code = sm.max_code;
  dc.dict_mask = sm.dict_mask;

  if (sm.has_null) {
    if (using_mmap) {
      dc.nulls_view =
          std::span<const uint8_t>(base + sm.nulls_offset, sm.nulls_size);
    } else {
      dc.nulls.resize(sm.nulls_size);
      if (sm.nulls_size > 0 &&
          PreadAllFd(fd, dc.nulls.data(), sm.nulls_size, sm.nulls_offset) !=
              Status::kSuccess) {
        return false;
      }
    }
  }

  if (sm.encoding == TlcsColumnEncoding::kNarrowDict &&
      dict_kind == TlcsDictKind::kCompleteNarrow) {
    uint8_t* const base_codes = dst_narrow_codes_base != nullptr
                                    ? dst_narrow_codes_base
                                    : active_dict_mut->narrow_codes.data();
    uint8_t* dst_codes =
        base_codes != nullptr ? base_codes + row_base : nullptr;
    if (using_mmap) {
      if (sm.data_size > 0) {
        std::memcpy(dst_codes, base + sm.data_offset, sm.data_size);
      }
    } else if (sm.data_size > 0 &&
               PreadAllFd(fd, dst_codes, sm.data_size, sm.data_offset) !=
                   Status::kSuccess) {
      return false;
    }
    if (src_b.row_count > 0 && dict_counts != nullptr &&
        !dict_counts->empty()) {
      const size_t null_slot = dict_counts->size() - 1;
      if (!dc.has_null && dc.is_const_code && dc.const_code < null_slot) {
        (*dict_counts)[dc.const_code] += static_cast<int64_t>(src_b.row_count);
        (*dict_first_row)[dc.const_code] =
            std::min<uint64_t>((*dict_first_row)[dc.const_code], row_base);
      } else {
        for (uint32_t r = 0; r < src_b.row_count; ++r) {
          const uint8_t raw = dst_codes[r];
          const size_t slot =
              (raw == LSTableColumnDictionary::kNullCode || raw >= null_slot)
                  ? null_slot
                  : raw;
          ++(*dict_counts)[slot];
          (*dict_first_row)[slot] =
              std::min<uint64_t>((*dict_first_row)[slot], row_base + r);
        }
      }
    }
    dc.dict = active_dict_mut;
    dc.dict_codes = std::span<const uint8_t>(dst_codes, src_b.row_count);
    dc.wide_code_begin = row_base;
    dc.wide_code_count = src_b.row_count;
    if (sm.type != ValueType::kVarChar) {
      int64_t* dst_ints =
          dst_ints_base != nullptr ? dst_ints_base + row_base : nullptr;
      MaterializeDictInts(sm, *active_dict_mut, dc.dict_codes, src_b.row_count,
                          dst_ints);
      dc.int_cells = std::span<const int64_t>(dst_ints, src_b.row_count);
    }
    return true;
  }

  if (sm.encoding == TlcsColumnEncoding::kNarrowDict &&
      dict_kind == TlcsDictKind::kPartialFrozen) {
    raw_tmp->resize(sm.data_size);
    if (using_mmap) {
      if (sm.data_size > 0) {
        std::memcpy(raw_tmp->data(), base + sm.data_offset, sm.data_size);
      }
    } else if (sm.data_size > 0 &&
               PreadAllFd(fd, raw_tmp->data(), sm.data_size, sm.data_offset) !=
                   Status::kSuccess) {
      return false;
    }
    if (sm.type == ValueType::kVarChar) {
      dc.varchar_ends.resize(src_b.row_count);
      for (uint32_t r = 0; r < src_b.row_count; ++r) {
        const uint8_t code = (*raw_tmp)[r];
        if (code != LSTableColumnDictionary::kNullCode &&
            code < ro_dict.entries.size()) {
          const std::string_view sv = ro_dict.entries[code];
          dc.varchar_bytes.insert(
              dc.varchar_bytes.end(),
              reinterpret_cast<const uint8_t*>(sv.data()),
              reinterpret_cast<const uint8_t*>(sv.data()) + sv.size());
        }
        dc.varchar_ends[r] = static_cast<uint32_t>(dc.varchar_bytes.size());
      }
    } else {
      int64_t* dst_ints =
          dst_ints_base != nullptr ? dst_ints_base + row_base : nullptr;
      MaterializeDictInts(sm, ro_dict, *raw_tmp, src_b.row_count, dst_ints);
      dc.int_cells = std::span<const int64_t>(dst_ints, src_b.row_count);
    }
    return true;
  }

  if (sm.type == ValueType::kVarChar) {
    if (using_mmap) {
      dc.varchar_bytes_view =
          std::span<const uint8_t>(base + sm.data_offset, sm.data_size);
      const uint8_t* ends = base + sm.aux_offset;
      if (std::endian::native == std::endian::little &&
          sm.aux_offset % alignof(uint32_t) == 0) {
        dc.varchar_ends_view = std::span<const uint32_t>(
            reinterpret_cast<const uint32_t*>(ends), src_b.row_count);
      } else {
        dc.varchar_ends.resize(src_b.row_count);
        if (src_b.row_count != 0) {
          std::memcpy(dc.varchar_ends.data(), ends,
                      src_b.row_count * sizeof(uint32_t));
        }
        if constexpr (std::endian::native != std::endian::little) {
          for (uint32_t& end : dc.varchar_ends) {
            end = le32toh(end);
          }
        }
      }
    } else {
      dc.varchar_bytes.resize(sm.data_size);
      if (sm.data_size > 0 &&
          PreadAllFd(fd, dc.varchar_bytes.data(), sm.data_size,
                     sm.data_offset) != Status::kSuccess) {
        return false;
      }
      dc.varchar_ends.resize(src_b.row_count);
      if (sm.aux_size > 0 && PreadAllFd(fd, dc.varchar_ends.data(), sm.aux_size,
                                        sm.aux_offset) != Status::kSuccess) {
        return false;
      }
      if constexpr (std::endian::native != std::endian::little) {
        for (uint32_t& end : dc.varchar_ends) {
          end = le32toh(end);
        }
      }
    }
  } else if (zero_copy_64) {
    dc.int_cells = std::span<const int64_t>(
        reinterpret_cast<const int64_t*>(base + sm.data_offset),
        src_b.row_count);
  } else {
    int64_t* dst_ints =
        dst_ints_base != nullptr ? dst_ints_base + row_base : nullptr;
    if (sm.encoding == TlcsColumnEncoding::kFor) {
      if (sm.for_width == 0) {
        UnpackForToInt64(sm, {}, src_b.row_count, dst_ints);
      } else if (using_mmap) {
        UnpackForToInt64(
            sm, std::span<const uint8_t>(base + sm.data_offset, sm.data_size),
            src_b.row_count, dst_ints);
      } else {
        raw_tmp->resize(sm.data_size);
        if (sm.data_size > 0 &&
            PreadAllFd(fd, raw_tmp->data(), sm.data_size, sm.data_offset) !=
                Status::kSuccess) {
          return false;
        }
        UnpackForToInt64(sm, *raw_tmp, src_b.row_count, dst_ints);
      }
    } else if (sm.encoding == TlcsColumnEncoding::kRawDouble) {
      if (using_mmap && std::endian::native == std::endian::little) {
        if (sm.data_size > 0) {
          std::memcpy(dst_ints, base + sm.data_offset, sm.data_size);
        }
      } else if (!using_mmap && std::endian::native == std::endian::little) {
        if (sm.data_size > 0 &&
            PreadAllFd(fd, dst_ints, sm.data_size, sm.data_offset) !=
                Status::kSuccess) {
          return false;
        }
      } else {
        raw_tmp->resize(sm.data_size);
        if (using_mmap) {
          if (sm.data_size > 0) {
            std::memcpy(raw_tmp->data(), base + sm.data_offset, sm.data_size);
          }
        } else if (sm.data_size > 0 &&
                   PreadAllFd(fd, raw_tmp->data(), sm.data_size,
                              sm.data_offset) != Status::kSuccess) {
          return false;
        }
        Le64BufferToInt64(raw_tmp->data(), src_b.row_count, dst_ints);
      }
    } else if (sm.encoding == TlcsColumnEncoding::kConstNull) {
      if (src_b.row_count > 0 && dst_ints != nullptr) {
        std::memset(dst_ints, 0,
                    static_cast<size_t>(src_b.row_count) * sizeof(int64_t));
      }
    }
    dc.int_cells = std::span<const int64_t>(dst_ints, src_b.row_count);
  }
  return true;
}

void CopyBlockColumnMeta(const TlcsFile::ColumnMeta& sm,
                         LSTableColumnBlock* dc_out) noexcept {
  LSTableColumnBlock& dc = *dc_out;
  dc.type = sm.type;
  dc.is_unsigned = sm.is_unsigned;
  dc.has_null = sm.has_null;
  dc.has_min_max = sm.has_min_max;
  dc.has_double_min_max = sm.has_double_min_max;
  dc.has_nan = sm.has_nan;
  dc.all_single_byte_varchar = sm.all_single_byte_varchar;
  dc.for_width = sm.for_width;
  dc.for_base = sm.for_base;
  dc.min_int = sm.min_int;
  dc.max_int = sm.max_int;
  dc.min_double = sm.min_double;
  dc.max_double = sm.max_double;
  dc.is_const_code = sm.is_const_code;
  dc.has_code_min_max = sm.has_code_min_max;
  dc.const_code = sm.const_code;
  dc.min_code = sm.min_code;
  dc.max_code = sm.max_code;
  dc.dict_mask = sm.dict_mask;
}

bool ReuseLazyDecodedBlock(
    size_t row_base, const TlcsFile::BlockDirEntry& src_b,
    const TlcsFile::ColumnMeta& sm, TlcsDictKind dict_kind, bool using_mmap,
    const uint8_t* base, const LSTableColumnBlock& cached,
    LSTableColumnDictionary* active_dict_mut, uint8_t* dst_narrow_codes_base,
    int64_t* dst_ints_base, LSTableColumnBlock* dc_out,
    std::vector<int64_t>* dict_counts, std::vector<uint64_t>* dict_first_row) {
  CopyBlockColumnMeta(sm, dc_out);
  LSTableColumnBlock& dc = *dc_out;
  if (sm.has_null) {
    if (using_mmap) {
      dc.nulls_view =
          std::span<const uint8_t>(base + sm.nulls_offset, sm.nulls_size);
    } else if (sm.nulls_size > 0) {
      const auto ns = cached.Nulls();
      if (ns.size() < sm.nulls_size) {
        return false;
      }
      dc.nulls.assign(ns.begin(), ns.begin() + sm.nulls_size);
    }
  }
  if (sm.encoding == TlcsColumnEncoding::kNarrowDict &&
      dict_kind == TlcsDictKind::kCompleteNarrow) {
    uint8_t* const base_codes = dst_narrow_codes_base != nullptr
                                    ? dst_narrow_codes_base
                                    : active_dict_mut->narrow_codes.data();
    uint8_t* dst_codes =
        base_codes != nullptr ? base_codes + row_base : nullptr;
    if (src_b.row_count > 0) {
      if (cached.dict_codes.size() < src_b.row_count || dst_codes == nullptr) {
        return false;
      }
      std::memcpy(dst_codes, cached.dict_codes.data(), src_b.row_count);
      if (dict_counts != nullptr && !dict_counts->empty()) {
        const size_t null_slot = dict_counts->size() - 1;
        if (!dc.has_null && dc.is_const_code && dc.const_code < null_slot) {
          (*dict_counts)[dc.const_code] +=
              static_cast<int64_t>(src_b.row_count);
          (*dict_first_row)[dc.const_code] =
              std::min<uint64_t>((*dict_first_row)[dc.const_code], row_base);
        } else {
          for (uint32_t r = 0; r < src_b.row_count; ++r) {
            const uint8_t raw = dst_codes[r];
            const size_t slot =
                (raw == LSTableColumnDictionary::kNullCode || raw >= null_slot)
                    ? null_slot
                    : raw;
            ++(*dict_counts)[slot];
            (*dict_first_row)[slot] =
                std::min<uint64_t>((*dict_first_row)[slot], row_base + r);
          }
        }
      }
    }
    dc.dict = active_dict_mut;
    dc.dict_codes = std::span<const uint8_t>(dst_codes, src_b.row_count);
    dc.wide_code_begin = row_base;
    dc.wide_code_count = src_b.row_count;
    if (sm.type != ValueType::kVarChar) {
      int64_t* dst_ints =
          dst_ints_base != nullptr ? dst_ints_base + row_base : nullptr;
      if (src_b.row_count > 0) {
        if (cached.int_cells.size() < src_b.row_count || dst_ints == nullptr) {
          return false;
        }
        std::memcpy(dst_ints, cached.int_cells.data(),
                    static_cast<size_t>(src_b.row_count) * sizeof(int64_t));
      }
      dc.int_cells = std::span<const int64_t>(dst_ints, src_b.row_count);
    }
    return true;
  }
  if (sm.type == ValueType::kVarChar) {
    if (sm.encoding == TlcsColumnEncoding::kConstNull) {
      return true;
    }
    if (using_mmap && sm.encoding == TlcsColumnEncoding::kVarChar &&
        std::endian::native == std::endian::little &&
        sm.aux_offset % alignof(uint32_t) == 0) {
      dc.varchar_bytes_view =
          std::span<const uint8_t>(base + sm.data_offset, sm.data_size);
      dc.varchar_ends_view = std::span<const uint32_t>(
          reinterpret_cast<const uint32_t*>(base + sm.aux_offset),
          src_b.row_count);
      return true;
    }
    const auto vb = cached.VarcharBytes();
    const auto ve = cached.VarcharEnds();
    if (ve.size() < src_b.row_count) {
      return false;
    }
    dc.varchar_bytes.assign(vb.begin(), vb.end());
    dc.varchar_ends.assign(ve.begin(), ve.begin() + src_b.row_count);
    return true;
  }
  int64_t* dst_ints =
      dst_ints_base != nullptr ? dst_ints_base + row_base : nullptr;
  if (src_b.row_count > 0) {
    if (cached.int_cells.size() < src_b.row_count || dst_ints == nullptr) {
      return false;
    }
    std::memcpy(dst_ints, cached.int_cells.data(),
                static_cast<size_t>(src_b.row_count) * sizeof(int64_t));
  }
  dc.int_cells = std::span<const int64_t>(dst_ints, src_b.row_count);
  return true;
}

Status FillRowFromDecodedBlock(const TlcsFile::DecodedBlock& blk, uint32_t row,
                               const Schema& schema, Row* out) {
  if (row >= blk.row_count) {
    return Status::kNotExists;
  }
  const slot_t ncols = schema.ColumnCount();
  if (blk.columns.size() != ncols) {
    return StatusError(StatusCode::kCorrupt,
                       "tlcs decoded block column count mismatch");
  }
  out->values_.resize(ncols);
  for (slot_t c = 0; c < ncols; ++c) {
    const TlcsFile::DecodedColumnChunk& col = blk.columns[c];
    if (col.IsNull(row)) {
      out->values_[c] = Value();
      continue;
    }
    switch (col.type) {
      case ValueType::kInt64:
        out->values_[c] = Value(col.int_cells[row]);
        if (col.is_unsigned) {
          out->values_[c].SetUnsigned();
        }
        break;
      case ValueType::kDate:
        out->values_[c] = Value::DateFromDays(col.int_cells[row]);
        break;
      case ValueType::kDouble: {
        double d = 0.0;
        std::memcpy(&d, &col.int_cells[row], sizeof(d));
        out->values_[c] = Value(d);
        break;
      }
      case ValueType::kVarChar:
        out->values_[c] = Value(std::string(col.VarcharAt(row)));
        break;
      default:
        out->values_[c] = Value();
        break;
    }
  }
  return Status::kSuccess;
}

}  // namespace

TlcsFile::~TlcsFile() {
  cache_reclaim_hook_.Reset();
  if (cache_reclaim_bridge_) {
    std::scoped_lock lk(cache_reclaim_bridge_->mu);
    cache_reclaim_bridge_->self = nullptr;
  }
  {
    std::scoped_lock lk(cache_mu_);
    if (cache_bytes_ > 0) {
      ProcessMemoryBudget::Global().Release(MemoryPoolKind::kColumnar,
                                            cache_bytes_);
      cache_bytes_ = 0;
    }
    cache_map_.clear();
    cache_lru_.clear();
    if (encoded_cache_bytes_ > 0) {
      ProcessMemoryBudget::Global().Release(MemoryPoolKind::kColumnar,
                                            encoded_cache_bytes_);
      encoded_cache_bytes_ = 0;
    }
    encoded_cache_map_.clear();
    encoded_cache_lru_.clear();
  }
  {
    std::scoped_lock lk(dir_cache_mu_);
    const size_t total_dir = dir_cache_bytes_ + eager_dir_reserved_bytes_;
    if (total_dir > 0) {
      ProcessMemoryBudget::Global().Release(MemoryPoolKind::kColumnar,
                                            total_dir);
      dir_cache_bytes_ = 0;
      eager_dir_reserved_bytes_ = 0;
    }
    dir_cache_map_.clear();
    dir_cache_lru_.clear();
    eager_dir_groups_.clear();
    flat_page_to_block_.clear();
    page_to_block_.clear();
  }
  {
    std::scoped_lock lk(resident_mu_);
    resident_store_.reset();
    resident_mmap_.reset();
  }
  {
    std::scoped_lock lk(varchar_bounds_mu_);
    if (varchar_bounds_bytes_ > 0) {
      ProcessMemoryBudget::Global().Release(MemoryPoolKind::kColumnar,
                                            varchar_bounds_bytes_);
      varchar_bounds_bytes_ = 0;
    }
    varchar_bounds_cache_.clear();
  }
  if (fd_ >= 0) {
    platform::CloseFile(fd_);
    fd_ = -1;
  }
  if (meta_reserved_bytes_ > 0) {
    if (meta_emergency_) {
      ProcessMemoryBudget::Global().ReleaseEmergency(MemoryPoolKind::kColumnar,
                                                     meta_reserved_bytes_);
    } else {
      ProcessMemoryBudget::Global().Release(MemoryPoolKind::kColumnar,
                                            meta_reserved_bytes_);
    }
    meta_reserved_bytes_ = 0;
  }
}

size_t TlcsFile::EvictDecodedCacheForReclaim(
    size_t target_bytes) const noexcept {
  if (target_bytes == 0) {
    return 0;
  }
  size_t freed = 0;
  {
    std::unique_lock<std::mutex> lk(cache_mu_, std::try_to_lock);
    if (lk.owns_lock() && cache_bytes_ > 0) {
      size_t cache_freed = 0;
      while (freed + cache_freed < target_bytes && !cache_lru_.empty()) {
        const uint32_t bidx = cache_lru_.front();
        cache_lru_.pop_front();
        auto it = cache_map_.find(bidx);
        if (it == cache_map_.end()) {
          continue;
        }
        const size_t vb = it->second.bytes;
        cache_freed += vb;
        cache_bytes_ = cache_bytes_ > vb ? cache_bytes_ - vb : 0;
        cache_map_.erase(it);
      }
      if (cache_freed > 0) {
        ProcessMemoryBudget::Global().Release(MemoryPoolKind::kColumnar,
                                              cache_freed);
        freed += cache_freed;
      }
      size_t encoded_freed = 0;
      while (freed + encoded_freed < target_bytes &&
             !encoded_cache_lru_.empty()) {
        const uint32_t bidx = encoded_cache_lru_.front();
        encoded_cache_lru_.pop_front();
        auto it = encoded_cache_map_.find(bidx);
        if (it == encoded_cache_map_.end()) {
          continue;
        }
        const size_t vb = it->second.bytes;
        encoded_freed += vb;
        encoded_cache_bytes_ =
            encoded_cache_bytes_ > vb ? encoded_cache_bytes_ - vb : 0;
        encoded_cache_map_.erase(it);
      }
      if (encoded_freed > 0) {
        ProcessMemoryBudget::Global().Release(MemoryPoolKind::kColumnar,
                                              encoded_freed);
        freed += encoded_freed;
      }
    }
  }
  if (freed < target_bytes) {
    std::vector<std::shared_ptr<const DirGroup>> dropped_eager;
    std::vector<std::shared_ptr<const DirGroup>> dropped_cached;
    std::vector<uint32_t> dropped_flat;
    std::unordered_map<page_id_t, uint32_t> dropped_map;
    size_t dir_freed = 0;
    {
      std::unique_lock<std::mutex> lk(dir_cache_mu_, std::try_to_lock);
      if (lk.owns_lock()) {
        while (freed + dir_freed < target_bytes && !dir_cache_lru_.empty()) {
          const uint32_t gidx = dir_cache_lru_.front();
          dir_cache_lru_.pop_front();
          auto it = dir_cache_map_.find(gidx);
          if (it == dir_cache_map_.end()) {
            continue;
          }
          const size_t vb = it->second.bytes;
          dir_freed += vb;
          dir_cache_bytes_ = dir_cache_bytes_ > vb ? dir_cache_bytes_ - vb : 0;
          dropped_cached.push_back(std::move(it->second.group));
          dir_cache_map_.erase(it);
        }
        if (freed + dir_freed < target_bytes &&
            (!eager_dir_groups_.empty() || !flat_page_to_block_.empty() ||
             !page_to_block_.empty())) {
          dropped_eager = std::move(eager_dir_groups_);
          dropped_flat = std::move(flat_page_to_block_);
          dropped_map = std::move(page_to_block_);
          eager_dir_groups_.clear();
          flat_page_to_block_.clear();
          page_to_block_.clear();
          dir_freed += eager_dir_reserved_bytes_;
          eager_dir_reserved_bytes_ = 0;
        }
      }
    }
    dropped_eager.clear();
    dropped_cached.clear();
    dropped_flat.clear();
    dropped_map.clear();
    if (dir_freed > 0) {
      ProcessMemoryBudget::Global().Release(MemoryPoolKind::kColumnar,
                                            dir_freed);
      freed += dir_freed;
    }
  }
  if (freed < target_bytes) {
    std::shared_ptr<const LSTableStore> old_resident;
    std::shared_ptr<const platform::MemoryMap> old_mmap;
    {
      std::unique_lock<std::mutex> lk(resident_mu_, std::try_to_lock);
      if (lk.owns_lock() &&
          (resident_store_ != nullptr || resident_mmap_ != nullptr)) {
        old_resident = std::move(resident_store_);
        old_mmap = std::move(resident_mmap_);
      }
    }
    if (old_resident != nullptr || old_mmap != nullptr) {
      const size_t before = ProcessMemoryBudget::Global().ColumnarUsed();
      old_resident.reset();
      old_mmap.reset();
      const size_t after = ProcessMemoryBudget::Global().ColumnarUsed();
      if (before > after) {
        freed += (before - after);
      }
    }
  }
  if (freed < target_bytes) {
    std::unordered_map<slot_t,
                       std::shared_ptr<const std::vector<VarcharBlockBound>>>
        dropped_vb;
    size_t vb_freed = 0;
    {
      std::unique_lock<std::mutex> lk(varchar_bounds_mu_, std::try_to_lock);
      if (lk.owns_lock() && varchar_bounds_bytes_ > 0) {
        dropped_vb.swap(varchar_bounds_cache_);
        vb_freed = varchar_bounds_bytes_;
        varchar_bounds_bytes_ = 0;
      }
    }
    dropped_vb.clear();
    if (vb_freed > 0) {
      ProcessMemoryBudget::Global().Release(MemoryPoolKind::kColumnar,
                                            vb_freed);
      freed += vb_freed;
    }
  }
  return freed;
}

StatusOr<std::shared_ptr<const TlcsFile::DirGroup>>
TlcsFile::LoadDirGroupFromFd(uint32_t group_idx) const {
  if (group_idx >= dir_groups_.size()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "tlcs dir group index out of range");
  }
  const DirGroupMeta& gm = dir_groups_[group_idx];
  const uint64_t total_len =
      (static_cast<uint64_t>(gm.block_count) * 8ULL) + gm.body_size;
  if (gm.group_offset > file_size_ ||
      total_len > file_size_ - gm.group_offset) {
    return StatusError(StatusCode::kCorrupt, "tlcs dir group out of bounds");
  }
  std::shared_ptr<const platform::MemoryMap> mmap_copy;
  {
    std::scoped_lock lk(resident_mu_);
    mmap_copy = resident_mmap_;
  }
  auto group = std::make_shared<DirGroup>();
  if (mmap_copy != nullptr && mmap_copy->IsValid()) {
    const std::span<const uint8_t> raw(mmap_copy->Data() + gm.group_offset,
                                       total_len);
    RETURN_IF_FAIL(ParseDirGroupBody(raw, gm.block_count, column_count_,
                                     col_types_, col_unsigned_, gm,
                                     &group->pids, &group->blocks));
  } else {
    std::vector<uint8_t> raw(total_len);
    RETURN_IF_FAIL(PreadAllFd(fd_, raw.data(), total_len, gm.group_offset));
    RETURN_IF_FAIL(ParseDirGroupBody(raw, gm.block_count, column_count_,
                                     col_types_, col_unsigned_, gm,
                                     &group->pids, &group->blocks));
  }
  if (!ColumnPayloadSpansValid(*group, column_count_, file_size_)) {
    return StatusError(StatusCode::kCorrupt,
                       "tlcs column payload out of bounds");
  }
  return std::shared_ptr<const DirGroup>(std::move(group));
}

StatusOr<std::shared_ptr<const TlcsFile::DirGroup>> TlcsFile::GetDirGroup(
    uint32_t group_idx) const {
  if (group_idx >= dir_groups_.size()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "tlcs dir group index out of range");
  }
  {
    std::scoped_lock lk(dir_cache_mu_);
    if (group_idx < eager_dir_groups_.size() &&
        eager_dir_groups_[group_idx] != nullptr) {
      return eager_dir_groups_[group_idx];
    }
    auto it = dir_cache_map_.find(group_idx);
    if (it != dir_cache_map_.end()) {
      dir_cache_lru_.splice(dir_cache_lru_.end(), dir_cache_lru_,
                            it->second.lru_it);
      return it->second.group;
    }
  }
  ASSIGN_OR_RETURN(std::shared_ptr<const DirGroup>, loaded,
                   LoadDirGroupFromFd(group_idx));
  const size_t entry_bytes =
      sizeof(DirGroup) + (loaded->pids.size() * sizeof(page_id_t)) +
      (loaded->blocks.size() *
       (sizeof(BlockDirEntry) + column_count_ * sizeof(ColumnMeta)));
  const size_t cache_cap = DirCacheLimitBytes();
  std::scoped_lock lk(dir_cache_mu_);
  if (group_idx < eager_dir_groups_.size() &&
      eager_dir_groups_[group_idx] != nullptr) {
    return eager_dir_groups_[group_idx];
  }
  if (auto it = dir_cache_map_.find(group_idx); it != dir_cache_map_.end()) {
    dir_cache_lru_.splice(dir_cache_lru_.end(), dir_cache_lru_,
                          it->second.lru_it);
    return it->second.group;
  }
  if (entry_bytes <= cache_cap) {
    while (dir_cache_bytes_ + entry_bytes > cache_cap &&
           !dir_cache_lru_.empty()) {
      const uint32_t victim = dir_cache_lru_.front();
      dir_cache_lru_.pop_front();
      auto vit = dir_cache_map_.find(victim);
      if (vit != dir_cache_map_.end()) {
        const size_t vb = vit->second.bytes;
        dir_cache_map_.erase(vit);
        dir_cache_bytes_ = dir_cache_bytes_ > vb ? dir_cache_bytes_ - vb : 0;
        ProcessMemoryBudget::Global().Release(MemoryPoolKind::kColumnar, vb);
      }
    }
    if (ProcessMemoryBudget::Global().TryReserve(MemoryPoolKind::kColumnar,
                                                 entry_bytes)) {
      dir_cache_lru_.push_back(group_idx);
      auto lru_it = std::prev(dir_cache_lru_.end());
      dir_cache_map_.emplace(group_idx, CachedDirGroup{.group = loaded,
                                                       .bytes = entry_bytes,
                                                       .lru_it = lru_it});
      dir_cache_bytes_ += entry_bytes;
    }
  }
  return loaded;
}

Status TlcsFile::LoadDirGroupPids(uint32_t group_idx,
                                  std::vector<page_id_t>* pids_out) const {
  if (group_idx >= dir_groups_.size()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "tlcs dir group index out of range");
  }
  const DirGroupMeta& gm = dir_groups_[group_idx];
  const uint64_t pid_bytes = static_cast<uint64_t>(gm.block_count) * 8ULL;
  if (gm.group_offset > file_size_ ||
      pid_bytes > file_size_ - gm.group_offset) {
    return StatusError(StatusCode::kCorrupt, "tlcs dir group out of bounds");
  }
  std::vector<uint8_t> raw(static_cast<size_t>(pid_bytes));
  RETURN_IF_FAIL(PreadAllFd(fd_, raw.data(), raw.size(), gm.group_offset));
  if (Crc32C(raw.data(), raw.size()) != gm.pid_crc32c) {
    return StatusError(StatusCode::kCorrupt, "tlcs pid checksum mismatch");
  }
  pids_out->resize(gm.block_count);
  size_t dpos = 0;
  for (uint32_t i = 0; i < gm.block_count; ++i) {
    if (!ReadU64(raw, &dpos, &(*pids_out)[i])) {
      return StatusError(StatusCode::kCorrupt, "tlcs pid span truncated");
    }
  }
  return Status::kSuccess;
}

bool TlcsFile::TryBuildPidIndex() const noexcept {
  {
    std::scoped_lock lk(dir_cache_mu_);
    if (!flat_page_to_block_.empty() || !page_to_block_.empty()) {
      return true;
    }
    if (dir_groups_.empty() || block_count_ == 0) {
      return false;
    }
  }
  page_id_t max_pid = 0;
  for (const DirGroupMeta& gm : dir_groups_) {
    max_pid = std::max(max_pid, gm.max_pid);
  }
  const bool use_flat = max_pid < kMaxFlatPageId;
  const size_t bytes =
      use_flat ? (static_cast<size_t>(max_pid) + 1) * sizeof(uint32_t)
               : static_cast<size_t>(block_count_) * 32;
  const size_t cap =
      std::min<size_t>(size_t{4} << 20, EffectiveColumnarPoolMaxBytes() / 32);
  if (bytes == 0 || bytes > cap) {
    return false;
  }
  std::vector<uint32_t> flat;
  std::unordered_map<page_id_t, uint32_t> map;
  try {
    if (use_flat) {
      flat.assign(static_cast<size_t>(max_pid) + 1,
                  LSTableStore::kInvalidBlockIndex);
    } else {
      map.reserve(block_count_);
    }
    std::vector<page_id_t> pids;
    uint32_t bidx = 0;
    for (uint32_t g = 0; g < dir_groups_.size(); ++g) {
      if (LoadDirGroupPids(g, &pids) != Status::kSuccess) {
        return false;
      }
      for (const page_id_t pid : pids) {
        if (use_flat) {
          if (pid < flat.size() &&
              flat[pid] == LSTableStore::kInvalidBlockIndex) {
            flat[pid] = bidx;
          }
        } else {
          map.emplace(pid, bidx);  // first occurrence wins, as in the scan.
        }
        ++bidx;
      }
    }
  } catch (...) {
    return false;
  }
  // Reserve outside the dir-cache lock: a reclaim may call back into
  // EvictDecodedCacheForReclaim which needs that lock.
  if (!ProcessMemoryBudget::Global().TryReserveWithReclaim(
          MemoryPoolKind::kColumnar, bytes)) {
    return false;
  }
  {
    std::scoped_lock lk(dir_cache_mu_);
    if (!flat_page_to_block_.empty() || !page_to_block_.empty()) {
      // Another thread built the index first; drop our duplicate reservation.
      ProcessMemoryBudget::Global().Release(MemoryPoolKind::kColumnar, bytes);
      return true;
    }
    flat_page_to_block_ = std::move(flat);
    page_to_block_ = std::move(map);
    eager_dir_reserved_bytes_ += bytes;
  }
  return true;
}

uint32_t TlcsFile::FindBlockIndex(page_id_t pid) const noexcept {
  {
    std::scoped_lock lk(dir_cache_mu_);
    if (!flat_page_to_block_.empty() || !page_to_block_.empty()) {
      if (pid < flat_page_to_block_.size()) {
        return flat_page_to_block_[pid];
      }
      const auto it = page_to_block_.find(pid);
      return it != page_to_block_.end() ? it->second
                                        : LSTableStore::kInvalidBlockIndex;
    }
  }
  if (dir_groups_.empty()) {
    return LSTableStore::kInvalidBlockIndex;
  }
  if (groups_monotonic_pids_) {
    const auto git =
        std::lower_bound(dir_groups_.begin(), dir_groups_.end(), pid,
                         [](const DirGroupMeta& gm, page_id_t target) {
                           return gm.last_pid < target;
                         });
    if (git == dir_groups_.end() || pid < git->first_pid) {
      return LSTableStore::kInvalidBlockIndex;
    }
    const auto gidx =
        static_cast<uint32_t>(std::distance(dir_groups_.begin(), git));
    StatusOr<std::shared_ptr<const DirGroup>> grp_or = GetDirGroup(gidx);
    if (!grp_or.HasValue()) {
      return LSTableStore::kInvalidBlockIndex;
    }
    const auto& pids = grp_or.Value()->pids;
    const auto pit = std::lower_bound(pids.begin(), pids.end(), pid);
    if (pit == pids.end() || *pit != pid) {
      return LSTableStore::kInvalidBlockIndex;
    }
    return (gidx * dir_group_size_) +
           static_cast<uint32_t>(std::distance(pids.begin(), pit));
  }
  // Overlapping pid ranges (reused/duplicate stub pids): the per-group
  // min/max filter cannot prune. Build a compact global index once so that
  // repeated point lookups are O(1) instead of re-parsing whole dir groups.
  if (TryBuildPidIndex()) {
    std::scoped_lock lk(dir_cache_mu_);
    if (!flat_page_to_block_.empty() || !page_to_block_.empty()) {
      if (pid < flat_page_to_block_.size()) {
        return flat_page_to_block_[pid];
      }
      const auto it = page_to_block_.find(pid);
      return it != page_to_block_.end() ? it->second
                                        : LSTableStore::kInvalidBlockIndex;
    }
  }
  // The index may be unavailable or reclaimed before we reacquire the lock.
  // Read only pid spans instead of parsing each candidate's full group.
  std::vector<page_id_t> pids;
  for (uint32_t gidx = 0; gidx < dir_groups_.size(); ++gidx) {
    const DirGroupMeta& gm = dir_groups_[gidx];
    if (pid < gm.min_pid || pid > gm.max_pid) {
      continue;
    }
    try {
      if (LoadDirGroupPids(gidx, &pids) != Status::kSuccess) {
        continue;
      }
    } catch (...) {
      continue;
    }
    for (uint32_t li = 0; li < pids.size(); ++li) {
      if (pids[li] == pid) {
        return (gidx * dir_group_size_) + li;
      }
    }
  }
  return LSTableStore::kInvalidBlockIndex;
}

StatusOr<uint32_t> TlcsFile::BlockRowCount(uint32_t idx) const {
  if (idx >= block_count_ || dir_group_size_ == 0) {
    return StatusError(StatusCode::kInvalidArgument,
                       "tlcs block index out of range");
  }
  const uint32_t gidx = idx / dir_group_size_;
  const uint32_t lidx = idx % dir_group_size_;
  ASSIGN_OR_RETURN(std::shared_ptr<const DirGroup>, grp, GetDirGroup(gidx));
  if (lidx >= grp->blocks.size()) {
    return StatusError(StatusCode::kCorrupt,
                       "tlcs local block index out of range");
  }
  return grp->blocks[lidx].row_count;
}

bool TlcsFile::BlockAllRowsMatchPeek(
    uint32_t block_idx,
    const std::vector<IntegerPeekCompare>& peek_compares) const noexcept {
  if (block_idx >= block_count_ || dir_group_size_ == 0) {
    return false;
  }
  StatusOr<std::shared_ptr<const DirGroup>> grp_or =
      GetDirGroup(block_idx / dir_group_size_);
  if (!grp_or.HasValue()) {
    return false;
  }
  const uint32_t lidx = block_idx % dir_group_size_;
  if (lidx >= grp_or.Value()->blocks.size()) {
    return false;
  }
  const BlockDirEntry& blk = grp_or.Value()->blocks[lidx];
  for (const IntegerPeekCompare& cmp : peek_compares) {
    if (cmp.column >= blk.columns.size()) {
      return false;
    }
    const ColumnMeta& col = blk.columns[cmp.column];
    if (col.has_null || !col.has_min_max ||
        (col.type != ValueType::kInt64 && col.type != ValueType::kDate)) {
      return false;
    }
    switch (cmp.op) {
      case BinaryOperation::kEquals:
        if (col.min_int != cmp.constant || col.max_int != cmp.constant) {
          return false;
        }
        break;
      case BinaryOperation::kNotEquals:
        if (cmp.constant >= col.min_int && cmp.constant <= col.max_int) {
          return false;
        }
        break;
      case BinaryOperation::kLessThan:
        if (col.max_int >= cmp.constant) {
          return false;
        }
        break;
      case BinaryOperation::kLessThanEquals:
        if (col.max_int > cmp.constant) {
          return false;
        }
        break;
      case BinaryOperation::kGreaterThan:
        if (col.min_int <= cmp.constant) {
          return false;
        }
        break;
      case BinaryOperation::kGreaterThanEquals:
        if (col.min_int < cmp.constant) {
          return false;
        }
        break;
      default:
        return false;
    }
  }
  return true;
}

bool TlcsFile::BlockIntBounds(uint32_t block_idx, slot_t col, int64_t* min_out,
                              int64_t* max_out) const noexcept {
  if (block_idx >= block_count_ || dir_group_size_ == 0 || min_out == nullptr ||
      max_out == nullptr) {
    return false;
  }
  StatusOr<std::shared_ptr<const DirGroup>> grp_or =
      GetDirGroup(block_idx / dir_group_size_);
  if (!grp_or.HasValue()) {
    return false;
  }
  const uint32_t lidx = block_idx % dir_group_size_;
  if (lidx >= grp_or.Value()->blocks.size()) {
    return false;
  }
  const BlockDirEntry& blk = grp_or.Value()->blocks[lidx];
  if (blk.row_count == 0 || col >= blk.columns.size()) {
    return false;
  }
  const ColumnMeta& cm = blk.columns[col];
  if (!cm.has_min_max ||
      (cm.type != ValueType::kInt64 && cm.type != ValueType::kDate) ||
      cm.max_int < cm.min_int) {
    return false;
  }
  *min_out = cm.min_int;
  *max_out = cm.max_int;
  return true;
}

bool TlcsFile::BlockMatchesZoneMaps(
    uint32_t block_idx, const std::vector<IntegerPeekCompare>* peek_compares,
    const RuntimeFilterGroup* runtime_filter,
    std::optional<slot_t> runtime_filter_column) const noexcept {
  if (block_idx >= block_count_ || dir_group_size_ == 0) {
    return false;
  }
  StatusOr<std::shared_ptr<const DirGroup>> grp_or =
      GetDirGroup(block_idx / dir_group_size_);
  if (!grp_or.HasValue()) {
    // Conservatively treat directory I/O failure as non-pruned so the
    // subsequent DecodeBlockColumns surfaces the Status cleanly.
    return true;
  }
  const uint32_t lidx = block_idx % dir_group_size_;
  if (lidx >= grp_or.Value()->blocks.size()) {
    return false;
  }
  const BlockDirEntry& blk = grp_or.Value()->blocks[lidx];
  if (blk.row_count == 0) {
    return false;
  }
  if (peek_compares != nullptr) {
    for (const IntegerPeekCompare& cmp : *peek_compares) {
      if (cmp.column >= blk.columns.size()) {
        continue;
      }
      const ColumnMeta& col = blk.columns[cmp.column];
      if (col.type != ValueType::kInt64 && col.type != ValueType::kDate) {
        return false;
      }
      if (!col.has_min_max) {
        return false;
      }
      switch (cmp.op) {
        case BinaryOperation::kEquals:
          if (cmp.constant < col.min_int || cmp.constant > col.max_int) {
            return false;
          }
          if (col.encoding == TlcsColumnEncoding::kNarrowDict &&
              dict_kinds_[cmp.column] != TlcsDictKind::kNone) {
            const auto entries = dicts_[cmp.column].IntEntries();
            auto it = std::ranges::find(entries, cmp.constant);
            if (it == entries.end()) {
              return false;
            }
            const auto code =
                static_cast<size_t>(std::distance(entries.begin(), it));
            if ((col.dict_mask[code >> 6] & (uint64_t{1} << (code & 63))) ==
                0) {
              return false;
            }
          }
          break;
        case BinaryOperation::kNotEquals:
          if (!col.has_null && col.min_int == cmp.constant &&
              col.max_int == cmp.constant) {
            return false;
          }
          break;
        case BinaryOperation::kLessThan:
          if (col.min_int >= cmp.constant) {
            return false;
          }
          break;
        case BinaryOperation::kLessThanEquals:
          if (col.min_int > cmp.constant) {
            return false;
          }
          break;
        case BinaryOperation::kGreaterThan:
          if (col.max_int <= cmp.constant) {
            return false;
          }
          break;
        case BinaryOperation::kGreaterThanEquals:
          if (col.max_int < cmp.constant) {
            return false;
          }
          break;
        default:
          break;
      }
    }
  }
  if (runtime_filter != nullptr && runtime_filter_column.has_value() &&
      *runtime_filter_column < blk.columns.size()) {
    if (runtime_filter->RejectsAll()) {
      return false;
    }
    const ColumnMeta& col = blk.columns[*runtime_filter_column];
    if (col.type == ValueType::kInt64 || col.type == ValueType::kDate) {
      if (!col.has_min_max) {
        return false;
      }
      if (runtime_filter->RejectRange(col.min_int, col.max_int)) {
        return false;
      }
    }
  }
  return true;
}

Status TlcsFile::CollectPageIds(std::vector<page_id_t>* out) const {
  if (out == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "null out in TlcsFile::CollectPageIds");
  }
  out->clear();
  out->reserve(block_count_);
  for (uint32_t g = 0; g < dir_groups_.size(); ++g) {
    ASSIGN_OR_RETURN(std::shared_ptr<const DirGroup>, grp, GetDirGroup(g));
    for (const BlockDirEntry& blk : grp->blocks) {
      out->push_back(blk.pid);
    }
  }
  return Status::kSuccess;
}

StatusOr<std::shared_ptr<const TlcsFile::IntFrequencies>>
TlcsFile::GetOrComputeIntFrequencies(slot_t col, const Schema& schema,
                                     bool allow_dense_range) const {
  if (col >= column_count_ || col >= schema.ColumnCount() ||
      col_types_[col] != ValueType::kInt64 || col_unsigned_[col] != 0 ||
      block_count_ == 0) {
    return nullptr;
  }
  const bool narrow = dict_kinds_[col] == TlcsDictKind::kCompleteNarrow;
  if (!narrow && !allow_dense_range) {
    return nullptr;
  }
  {
    std::scoped_lock lock(cache_mu_);
    if (const auto it = int_frequencies_.find(col);
        it != int_frequencies_.end()) {
      return it->second;
    }
  }

  struct Histogram {
    ProcessMemoryCharge charge{MemoryPoolKind::kColumnar};
    IntFrequencies data;
  };
  size_t n = dicts_[col].IntEntryCount();
  int64_t first = 0;
  if (!narrow) {
    int64_t low = INT64_MAX;
    int64_t high = INT64_MIN;
    for (uint32_t group = 0; group < DirGroupCount(); ++group) {
      ASSIGN_OR_RETURN(std::shared_ptr<const DirGroup>, directory,
                       GetDirGroup(group));
      for (const auto& block : directory->blocks) {
        const auto& meta = block.columns[col];
        if (block.row_count == 0 ||
            meta.encoding == TlcsColumnEncoding::kConstNull) {
          continue;
        }
        if (!meta.has_min_max || meta.min_int > meta.max_int) {
          return nullptr;
        }
        low = std::min(low, meta.min_int);
        high = std::max(high, meta.max_int);
      }
    }
    constexpr uint64_t kMaxDenseValues = uint64_t{1} << 18;
    const uint64_t distance =
        static_cast<uint64_t>(high) - static_cast<uint64_t>(low);
    if (low > high || distance >= kMaxDenseValues) {
      return nullptr;
    }
    first = low;
    n = static_cast<size_t>(distance) + 1;
  }
  auto histogram = std::make_shared<Histogram>();
  if (n == 0 || (narrow && n > LSTableColumnDictionary::kMaxEntries) ||
      !histogram->charge.TryAdd((n * sizeof(uint64_t)) + sizeof(Histogram))) {
    return nullptr;
  }
  histogram->data.counts.resize(n);
  if (!narrow) {
    histogram->data.first_value = first;
  }
  const size_t workers = std::max<size_t>(
      1, std::min<size_t>(
             {QueryParallelWorkerLimit(std::thread::hardware_concurrency()),
              block_count_, 8}));
  ProcessMemoryCharge partial_charge{MemoryPoolKind::kColumnar};
  if (!partial_charge.TryAdd(workers * n * sizeof(uint64_t))) {
    return nullptr;
  }
  std::vector<std::vector<uint64_t>> partial(workers, std::vector<uint64_t>(n));
  std::vector<Status> errors(workers, Status::kSuccess);
  std::vector<uint8_t> unsupported(workers, 0);
  std::atomic<size_t> next{0};
  const std::array columns{col};
  RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t worker) {
    DecodedBlock block;
    std::vector<uint8_t> scratch;
    for (;;) {
      const size_t b = next.fetch_add(1, std::memory_order_relaxed);
      if (b >= block_count_) {
        return;
      }
      auto status = DecodeBlockColumnsWithOptions(static_cast<uint32_t>(b),
                                                  columns, schema, &block,
                                                  !narrow, &scratch, {}, true);
      if (!status.ok()) {
        errors[worker] = std::move(status);
        return;
      }
      if (block.row_count == 0) {
        continue;
      }
      const auto& ch = block.columns[0];
      for (uint32_t row = 0; row < block.row_count; ++row) {
        if (ch.IsNull(row)) {
          continue;
        }
        if (narrow && !ch.dict_encoded) {
          unsupported[worker] = 1;
          return;
        }
        // Lean: dense_frequency_offset. Unsigned subtraction yields the
        // signed distance between the key and the bounded interval start.
        const uint64_t index = narrow
                                   ? ch.dict_codes[row]
                                   : static_cast<uint64_t>(ch.int_cells[row]) -
                                         static_cast<uint64_t>(first);
        if (index >= n) {
          unsupported[worker] = 1;
          return;
        }
        ++partial[worker][index];
      }
    }
  }));
  for (const auto& status : errors) {
    RETURN_IF_FAIL(status);
  }
  if (std::ranges::any_of(unsupported, [](uint8_t b) { return b != 0; })) {
    return nullptr;
  }
  for (const auto& counts : partial) {
    for (size_t i = 0; i < n; ++i) {
      histogram->data.counts[i] += counts[i];
    }
  }
  const std::shared_ptr<const IntFrequencies> summary(histogram,
                                                      &histogram->data);
  std::scoped_lock lock(cache_mu_);
  return int_frequencies_.try_emplace(col, summary).first->second;
}

std::shared_ptr<const std::vector<TlcsFile::VarcharBlockBound>>
TlcsFile::GetCachedVarcharBlockBounds(slot_t col_idx) const noexcept {
  std::scoped_lock lk(varchar_bounds_mu_);
  const auto it = varchar_bounds_cache_.find(col_idx);
  return it != varchar_bounds_cache_.end() ? it->second : nullptr;
}

StatusOr<std::shared_ptr<const std::vector<TlcsFile::VarcharBlockBound>>>
TlcsFile::GetOrComputeVarcharBlockBounds(slot_t col_idx,
                                         const Schema& schema) const {
  {
    std::scoped_lock lk(varchar_bounds_mu_);
    if (auto it = varchar_bounds_cache_.find(col_idx);
        it != varchar_bounds_cache_.end()) {
      return it->second;
    }
  }
  if (col_idx >= column_count_ || col_idx >= schema.ColumnCount() ||
      schema.GetColumn(col_idx).Type() != ValueType::kVarChar) {
    return StatusError(StatusCode::kInvalidArgument,
                       "invalid column for TlcsFile varchar bounds");
  }
  if (block_count_ == 0) {
    return std::make_shared<const std::vector<VarcharBlockBound>>();
  }
  const size_t reserve_bytes =
      static_cast<size_t>(block_count_) * sizeof(VarcharBlockBound);
  if (!ProcessMemoryBudget::Global().TryReserve(MemoryPoolKind::kColumnar,
                                                reserve_bytes)) {
    return std::shared_ptr<const std::vector<VarcharBlockBound>>{};
  }
  UnwindGuard release_reservation([reserve_bytes] {
    ProcessMemoryBudget::Global().Release(MemoryPoolKind::kColumnar,
                                          reserve_bytes);
  });
  const uint32_t dg_count = DirGroupCount();
  const uint32_t dg_size = DirGroupSize();
  std::vector<std::shared_ptr<const DirGroup>> dir_groups(dg_count);
  for (uint32_t g = 0; g < dg_count; ++g) {
    auto grp_or = GetDirGroup(g);
    if (!grp_or.HasValue()) {
      return grp_or.GetStatus();
    }
    dir_groups[g] = grp_or.MoveValue();
  }

  auto bounds = std::make_shared<std::vector<VarcharBlockBound>>(block_count_);
  const size_t workers = std::max<size_t>(
      1, std::min<size_t>(
             {QueryParallelWorkerLimit(std::thread::hardware_concurrency()),
              block_count_, size_t{16}}));
  std::vector<Status> worker_status(workers, Status::kSuccess);
  std::atomic<uint32_t> next_b{0};
  const std::array<slot_t, 1> col_ids = {col_idx};

  // codes_follow_string_order uses the canonical ORDER BY comparator, which
  // re-keys ENUM spellings below printable bytes. Only reuse dictionary codes
  // for byte-order pruning prefixes when both orders actually agree.
  auto is_canonical_space_utc = [](std::string_view s) noexcept -> bool {
    if ((s.size() != 19 &&
         (s.size() != 22 || s[19] != '+' || s[20] != '0' || s[21] != '0')) ||
        s[4] != '-' || s[7] != '-' || s[10] != ' ' || s[13] != ':' ||
        s[16] != ':') {
      return false;
    }
    auto d = [&](size_t idx) -> int {
      const char c = s[idx];
      return (c >= '0' && c <= '9') ? (c - '0') : -1;
    };
    const int y0 = d(0), y1 = d(1), y2 = d(2), y3 = d(3);
    const int m0 = d(5), m1 = d(6), d0 = d(8), d1 = d(9);
    const int h0 = d(11), h1 = d(12), mn0 = d(14), mn1 = d(15);
    const int s0 = d(17), s1 = d(18);
    if ((y0 | y1 | y2 | y3 | m0 | m1 | d0 | d1 | h0 | h1 | mn0 | mn1 | s0 |
         s1) < 0) {
      return false;
    }
    const int year = (y0 * 1000) + (y1 * 100) + (y2 * 10) + y3;
    const int month = (m0 * 10) + m1;
    const int day = (d0 * 10) + d1;
    const int hh = (h0 * 10) + h1;
    const int mm = (mn0 * 10) + mn1;
    const int ss = (s0 * 10) + s1;
    if (year < 1 || month < 1 || month > 12 || day < 1 || hh > 23 || mm > 59 ||
        ss > 59) {
      return false;
    }
    static constexpr std::array<int, 12> kDays = {31, 28, 31, 30, 31, 30,
                                                  31, 31, 30, 31, 30, 31};
    int max_day = kDays[static_cast<size_t>(month - 1)];
    if (month == 2 &&
        ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0))) {
      max_day = 29;
    }
    return day <= max_day;
  };

  bool dict_byte_sorted = false;
  bool dict_all_canonical_utc = false;
  const LSTableColumnDictionary& bound_dict = dicts_[col_idx];
  if (dict_kinds_[col_idx] == TlcsDictKind::kCompleteNarrow) {
    const size_t n = bound_dict.StringEntryCount();
    dict_all_canonical_utc = (n > 0);
    for (size_t k = 0; k < n; ++k) {
      if (!is_canonical_space_utc(bound_dict.StringEntryAt(k))) {
        dict_all_canonical_utc = false;
        break;
      }
    }
    if (bound_dict.codes_follow_string_order) {
      dict_byte_sorted = true;
      for (size_t k = 1; k < n; ++k) {
        if (!(bound_dict.StringEntryAt(k - 1) < bound_dict.StringEntryAt(k))) {
          dict_byte_sorted = false;
          break;
        }
      }
    }
  }

  // Lean proof: lean/TinylambOpt/ClickBenchRound2.lean
  //   (bottom_exclusion_tightens_min_bound_sound,
  //    interior_exclusion_min_bound_unsound_bug)
  auto set_bounds = [](VarcharBlockBound& out, std::string_view min_str,
                       std::string_view max_str, bool has_nonempty,
                       std::string_view nonempty_min_str, bool has_null,
                       bool any_non_null, bool all_canonical_utc) {
    out.has_null = has_null;
    if (!any_non_null) {
      out.is_const_null = true;
      out.has_null = true;
      out.all_canonical_utc_ts = true;
      return;
    }
    out.has_min_max = true;
    out.all_canonical_utc_ts = all_canonical_utc;
    out.min_len = static_cast<uint8_t>(std::min<size_t>(24, min_str.size()));
    out.min_truncated = min_str.size() > 24;
    if (out.min_len > 0) {
      std::memcpy(out.min_prefix.data(), min_str.data(), out.min_len);
    }
    out.max_len = static_cast<uint8_t>(std::min<size_t>(24, max_str.size()));
    out.max_truncated = max_str.size() > 24;
    if (out.max_len > 0) {
      std::memcpy(out.max_prefix.data(), max_str.data(), out.max_len);
    }
    out.has_nonempty_min = has_nonempty;
    if (has_nonempty) {
      out.nonempty_min_len =
          static_cast<uint8_t>(std::min<size_t>(24, nonempty_min_str.size()));
      out.nonempty_min_truncated = nonempty_min_str.size() > 24;
      if (out.nonempty_min_len > 0) {
        std::memcpy(out.nonempty_min_prefix.data(), nonempty_min_str.data(),
                    out.nonempty_min_len);
      }
    }
  };

  auto worker_fn = [&](size_t tid) {
    DecodedBlock blk;
    std::vector<uint8_t> io_scratch;
    for (;;) {
      const uint32_t b = next_b.fetch_add(1, std::memory_order_relaxed);
      if (b >= block_count_) {
        break;
      }
      const ColumnMeta& meta =
          dir_groups[b / dg_size]->blocks[b % dg_size].columns[col_idx];
      VarcharBlockBound& out = (*bounds)[b];
      if (meta.encoding == TlcsColumnEncoding::kConstNull) {
        out.is_const_null = true;
        out.has_null = true;
        out.all_canonical_utc_ts = true;
        continue;
      }
      // For a string-ordered narrow dictionary the block's min/max strings
      // are exactly the dictionary entries at its min/max codes. Deriving
      // them here avoids re-reading and unpacking the whole varchar column
      // just to build 24-byte pruning prefixes.
      const LSTableColumnDictionary& dict = dicts_[col_idx];
      if (meta.encoding == TlcsColumnEncoding::kNarrowDict &&
          dict_kinds_[col_idx] == TlcsDictKind::kCompleteNarrow &&
          meta.has_code_min_max && dict.codes_follow_string_order &&
          dict_byte_sorted &&
          static_cast<size_t>(meta.min_code) < dict.StringEntryCount() &&
          static_cast<size_t>(meta.max_code) < dict.StringEntryCount()) {
        const std::string_view min_sv = dict.StringEntryAt(meta.min_code);
        const std::string_view max_sv = dict.StringEntryAt(meta.max_code);
        bool has_ne = !min_sv.empty();
        std::string_view ne_sv = min_sv;
        if (!has_ne && meta.max_code > meta.min_code) {
          has_ne = true;
          ne_sv = dict.StringEntryAt(meta.min_code + 1U);
        }
        set_bounds(out, min_sv, max_sv, has_ne, ne_sv, meta.has_null,
                   /*any_non_null=*/true, dict_all_canonical_utc);
        continue;
      }
      Status st = DecodeBlockColumnsWithOptions(b, col_ids, schema, &blk, true,
                                                &io_scratch, {});
      if (st != Status::kSuccess) {
        worker_status[tid] = std::move(st);
        return;
      }
      const DecodedColumnChunk& ch = blk.columns[0];
      out.has_null = ch.has_null;
      std::string min_str;
      std::string max_str;
      std::string nonempty_min_str;
      bool any_non_null = false;
      bool any_nonempty = false;
      bool all_canonical = true;
      for (uint32_t r = 0; r < blk.row_count; ++r) {
        if (ch.has_null && ch.IsNull(r)) {
          out.has_null = true;
          continue;
        }
        const std::string_view sv = ch.VarcharAt(r);
        if (all_canonical && !is_canonical_space_utc(sv)) {
          all_canonical = false;
        }
        if (!any_non_null) {
          min_str.assign(sv);
          max_str.assign(sv);
          any_non_null = true;
        } else {
          if (sv < std::string_view(min_str)) {
            min_str.assign(sv);
          }
          if (sv > std::string_view(max_str)) {
            max_str.assign(sv);
          }
        }
        if (!sv.empty()) {
          if (!any_nonempty || sv < std::string_view(nonempty_min_str)) {
            nonempty_min_str.assign(sv);
            any_nonempty = true;
          }
        }
      }
      set_bounds(out, min_str, max_str, any_nonempty, nonempty_min_str,
                 out.has_null, any_non_null, all_canonical);
    }
  };
  RETURN_IF_FAIL(RunParallelTasks(workers, worker_fn));
  for (const Status& st : worker_status) {
    RETURN_IF_FAIL(st);
  }
  std::shared_ptr<const std::vector<VarcharBlockBound>> published = bounds;
  {
    std::scoped_lock lk(varchar_bounds_mu_);
    auto [it, inserted] =
        varchar_bounds_cache_.emplace(col_idx, std::move(bounds));
    if (inserted) {
      varchar_bounds_bytes_ += reserve_bytes;
      release_reservation.Disarm();
    } else {
      published = it->second;
    }
  }
  return published;
}

Status TlcsFile::DecodeBlockColumns(uint32_t block_idx,
                                    std::span<const slot_t> column_ids,
                                    const Schema& schema, DecodedBlock* out,
                                    bool materialize_dict_ints,
                                    std::vector<uint8_t>* io_scratch) const {
  return DecodeBlockColumnsWithOptions(block_idx, column_ids, schema, out,
                                       materialize_dict_ints, io_scratch, {});
}

// Lean proof: lean/TinylambOpt/TpchSf10VectorizedExec.lean
//   (gather_decode_commutes)
bool TlcsFile::TryGatherMmapFixedColumns(uint32_t block_idx,
                                         std::span<const slot_t> column_ids,
                                         std::span<const uint32_t> rows,
                                         const Schema& schema,
                                         DecodedBlock* out) const {
  if constexpr (std::endian::native != std::endian::little) {
    return false;
  }
  if (block_idx >= block_count_ || dir_group_size_ == 0 || out == nullptr ||
      rows.empty()) {
    return false;
  }
  decltype(resident_mmap_) mmap_copy;
  {
    std::scoped_lock lk(resident_mu_);
    mmap_copy = resident_mmap_;
  }
  if (mmap_copy == nullptr || !mmap_copy->IsValid()) {
    return false;
  }
  const uint8_t* const mmap_data = mmap_copy->Data();
  StatusOr<std::shared_ptr<const DirGroup>> grp_or =
      GetDirGroup(block_idx / dir_group_size_);
  if (!grp_or.HasValue()) {
    return false;
  }
  const std::shared_ptr<const DirGroup>& grp = grp_or.Value();
  const uint32_t lidx = block_idx % dir_group_size_;
  if (lidx >= grp->blocks.size()) {
    return false;
  }
  const auto& blk = grp->blocks[lidx];
  const uint32_t n = blk.row_count;
  // Every gather below indexes the block's null/data arrays with rows[j], so
  // a stale or corrupt row position must be rejected before any mmap read.
  for (const uint32_t row : rows) {
    if (row >= n) {
      return false;
    }
  }
  // Validate all requested columns are fixed-width random-access encodings.
  for (const slot_t cid : column_ids) {
    if (cid >= schema.ColumnCount() || cid >= blk.columns.size()) {
      return false;
    }
    const ColumnMeta& meta = blk.columns[cid];
    if (meta.encoding == TlcsColumnEncoding::kVarChar) {
      return false;
    }
    if (meta.encoding == TlcsColumnEncoding::kNarrowDict &&
        meta.type == ValueType::kVarChar) {
      return false;
    }
    if (meta.has_null && (meta.nulls_size != static_cast<uint64_t>(n) ||
                          meta.nulls_offset > file_size_ ||
                          meta.nulls_size > file_size_ - meta.nulls_offset)) {
      return false;
    }
    if (meta.data_size > 0 &&
        (meta.data_offset > file_size_ ||
         meta.data_size > file_size_ - meta.data_offset)) {
      return false;
    }
  }
  const size_t k = rows.size();
  out->row_count = static_cast<uint32_t>(k);
  out->columns.resize(column_ids.size());
  for (size_t i = 0; i < column_ids.size(); ++i) {
    const slot_t cid = column_ids[i];
    const ColumnMeta& meta = blk.columns[cid];
    DecodedColumnChunk& chunk = out->columns[i];
    chunk.type = meta.type;
    chunk.is_unsigned = meta.is_unsigned;
    chunk.has_null = meta.has_null;
    chunk.has_min_max = meta.has_min_max;
    chunk.has_double_min_max = meta.has_double_min_max;
    chunk.has_nan = meta.has_nan;
    chunk.row_count = static_cast<uint32_t>(k);
    chunk.dict = nullptr;
    chunk.dict_encoded = false;
    chunk.dict_codes.clear();
    chunk.varchar_bytes.clear();
    chunk.varchar_ends.clear();

    if (meta.has_null) {
      const uint8_t* const np = mmap_data + meta.nulls_offset;
      chunk.nulls.resize(k);
      uint8_t* const dnp = chunk.nulls.data();
      for (size_t j = 0; j < k; ++j) {
        dnp[j] = np[rows[j]];
      }
    } else {
      chunk.nulls.clear();
    }

    chunk.int_cells.resize(k);
    int64_t* const dst = chunk.int_cells.data();
    const uint8_t* const dp = mmap_data + meta.data_offset;
    switch (meta.encoding) {
      case TlcsColumnEncoding::kRawDouble: {
        if (meta.data_size != static_cast<uint64_t>(n) * sizeof(int64_t)) {
          return false;
        }
        for (size_t j = 0; j < k; ++j) {
          std::memcpy(&dst[j], dp + (static_cast<size_t>(rows[j]) * 8U), 8U);
        }
        break;
      }
      case TlcsColumnEncoding::kFor: {
        const auto base = static_cast<uint64_t>(meta.for_base);
        if (meta.for_width == 0) {
          std::fill_n(dst, k, meta.for_base);
        } else if (meta.for_width == 1) {
          if (meta.data_size != static_cast<uint64_t>(n)) {
            return false;
          }
          for (size_t j = 0; j < k; ++j) {
            dst[j] = static_cast<int64_t>(base + dp[rows[j]]);
          }
        } else if (meta.for_width == 2) {
          if (meta.data_size != static_cast<uint64_t>(n) * 2U) {
            return false;
          }
          for (size_t j = 0; j < k; ++j) {
            uint16_t d = 0;
            std::memcpy(&d, dp + (static_cast<size_t>(rows[j]) * 2U), 2U);
            dst[j] = static_cast<int64_t>(base + d);
          }
        } else if (meta.for_width == 4) {
          if (meta.data_size != static_cast<uint64_t>(n) * 4U) {
            return false;
          }
          for (size_t j = 0; j < k; ++j) {
            uint32_t d = 0;
            std::memcpy(&d, dp + (static_cast<size_t>(rows[j]) * 4U), 4U);
            dst[j] = static_cast<int64_t>(base + d);
          }
        } else if (meta.for_width == 8) {
          if (meta.data_size != static_cast<uint64_t>(n) * 8U) {
            return false;
          }
          for (size_t j = 0; j < k; ++j) {
            std::memcpy(&dst[j], dp + (static_cast<size_t>(rows[j]) * 8U), 8U);
          }
        } else {
          return false;
        }
        break;
      }
      case TlcsColumnEncoding::kNarrowDict: {
        if (meta.data_size != static_cast<uint64_t>(n)) {
          return false;
        }
        if (meta.type == ValueType::kDouble) {
          const auto entries = dicts_[cid].DoubleEntries();
          for (size_t j = 0; j < k; ++j) {
            const uint8_t code = dp[rows[j]];
            if (code == LSTableColumnDictionary::kNullCode ||
                code >= entries.size()) {
              dst[j] = 0;
            } else {
              std::memcpy(&dst[j], &entries[code], sizeof(int64_t));
            }
          }
        } else {
          const auto entries = dicts_[cid].IntEntries();
          for (size_t j = 0; j < k; ++j) {
            const uint8_t code = dp[rows[j]];
            dst[j] = (code == LSTableColumnDictionary::kNullCode ||
                      code >= entries.size())
                         ? 0
                         : entries[code];
          }
        }
        break;
      }
      default:
        return false;
    }
  }
  return true;
}

Status TlcsFile::DecodeBlockColumnsWithOptions(
    uint32_t block_idx, std::span<const slot_t> column_ids,
    const Schema& schema, DecodedBlock* out, bool materialize_dict_ints,
    std::vector<uint8_t>* io_scratch,
    std::span<const uint8_t> varchar_lengths_only,
    bool read_projected_streams_directly,
    std::span<const uint32_t> varchar_rows) const {
  if (block_idx >= block_count_ || dir_group_size_ == 0 || out == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "tlcs decode invalid block index");
  }
  g_tlcs_decode_calls.fetch_add(1, std::memory_order_relaxed);
  ASSIGN_OR_RETURN(std::shared_ptr<const DirGroup>, grp,
                   GetDirGroup(block_idx / dir_group_size_));
  const uint32_t lidx = block_idx % dir_group_size_;
  if (lidx >= grp->blocks.size()) {
    return StatusError(StatusCode::kCorrupt,
                       "tlcs decode local block index out of range");
  }
  const BlockDirEntry& blk = grp->blocks[lidx];
  out->block_index = block_idx;
  out->pid = blk.pid;
  out->row_count = blk.row_count;
  out->column_ids.assign(column_ids.begin(), column_ids.end());
  out->columns.resize(column_ids.size());
  if (column_ids.empty() || blk.row_count == 0) {
    return Status::kSuccess;
  }
  if (blk.row_count > RowPage::kMaxColumnarRows) {
    return StatusError(StatusCode::kCorrupt,
                       "tlcs block row count out of range");
  }

  std::vector<uint8_t> local_scratch;
  std::vector<uint8_t>& raw =
      io_scratch != nullptr ? *io_scratch : local_scratch;
  const uint32_t n = blk.row_count;
  std::shared_ptr<const platform::MemoryMap> mmap_copy;
  {
    std::scoped_lock lk(resident_mu_);
    mmap_copy = resident_mmap_;
  }
  const bool have_mmap = mmap_copy != nullptr && mmap_copy->IsValid();
  const uint8_t* mmap_data = have_mmap ? mmap_copy->Data() : nullptr;
  if (!varchar_rows.empty()) {
    if (varchar_rows.back() >= n ||
        std::ranges::adjacent_find(varchar_rows, std::greater_equal<>{}) !=
            varchar_rows.end()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "tlcs varchar row ordinals out of range or unordered");
    }
    read_projected_streams_directly = true;
  }

  // A block's column payloads are written contiguously (only AlignTo8 padding
  // between them), but slicing them with one pread per nullmap/data/aux region
  // costs 20-30 syscalls for a ten-column block.  Fetch the union of the
  // requested columns' payload ranges in ONE pread and slice it in memory.
  // A narrow projection can instead read the individual streams directly
  // into their decoder buffers. This avoids any gaps between projected
  // columns, and avoids allocating and copying through the union window.
  // Lean proof: lean/TinylambOpt/QuotientFusion.lean (direct_projection_read)
  std::vector<uint8_t> io_window;
  uint64_t window_begin = std::numeric_limits<uint64_t>::max();
  uint64_t window_end = 0;
  if (!have_mmap && !read_projected_streams_directly) {
    uint64_t projected_bytes = 0;
    uint64_t streams = 0;
    for (size_t i = 0; i < column_ids.size(); ++i) {
      const slot_t cid = column_ids[i];
      if (cid >= blk.columns.size()) {
        continue;  // the decode loop below reports the out-of-range column
      }
      const ColumnMeta& m = blk.columns[cid];
      const auto include = [&](uint64_t offset, uint64_t size) {
        if (size == 0 || offset > file_size_ || size > file_size_ - offset) {
          return;  // read_slice() reports the out-of-bounds slice
        }
        window_begin = std::min(window_begin, offset);
        window_end = std::max(window_end, offset + size);
        const uint64_t room = UINT64_MAX - projected_bytes;
        projected_bytes += std::min(size, room);
        ++streams;
      };
      if (m.has_null) {
        include(m.nulls_offset, m.nulls_size);
      }
      const bool lengths_only = m.encoding == TlcsColumnEncoding::kVarChar &&
                                i < varchar_lengths_only.size() &&
                                varchar_lengths_only[i] != 0;
      if (!lengths_only) {
        include(m.data_offset, m.data_size);
      }
      include(m.aux_offset, m.aux_size);
    }
    if (window_begin < window_end) {
      static const bool adaptive =
          std::getenv("TINYLAMB_DISABLE_ADAPTIVE_TLCS_READ") == nullptr;
      const uint64_t window_bytes = window_end - window_begin;
      // Charge each additional system call one page of bytes. Large gaps
      // between requested streams can dwarf this cost even on a warm cache.
      constexpr uint64_t kExtraReadCostBytes = 4096;
      constexpr uint64_t kDirectStreamBytes = 65536;
      read_projected_streams_directly =
          adaptive &&
          (streams == 1 || projected_bytes / streams >= kDirectStreamBytes ||
           (window_bytes >= projected_bytes &&
            window_bytes - projected_bytes >
                (streams - 1) * kExtraReadCostBytes));
      if (!read_projected_streams_directly) {
        io_window.resize(static_cast<size_t>(window_bytes));
        RETURN_IF_FAIL(
            PreadAllFd(fd_, io_window.data(), io_window.size(), window_begin));
      }
    }
  }

  auto read_slice = [&](uint64_t offset, uint64_t size, void* dst) -> Status {
    if (size == 0) {
      return Status::kSuccess;
    }
    if (offset > file_size_ || size > file_size_ - offset) {
      return StatusError(StatusCode::kCorrupt, "tlcs stream out of bounds");
    }
    if (have_mmap) {
      std::memcpy(dst, mmap_data + offset, size);
      return Status::kSuccess;
    }
    if (read_projected_streams_directly) {
      return PreadAllFd(fd_, dst, size, offset);
    }
    if (offset < window_begin || offset - window_begin > io_window.size() ||
        size > io_window.size() - (offset - window_begin)) {
      return StatusError(StatusCode::kCorrupt,
                         "tlcs stream outside block window");
    }
    std::memcpy(dst, io_window.data() + (offset - window_begin), size);
    return Status::kSuccess;
  };
  const auto read_raw64 = [&](const ColumnMeta& meta, int64_t* dst) -> Status {
    if (meta.data_size != static_cast<uint64_t>(n) * sizeof(int64_t)) {
      return StatusError(StatusCode::kCorrupt, "tlcs 64-bit size mismatch");
    }
    if constexpr (std::endian::native == std::endian::little) {
      return read_slice(meta.data_offset, meta.data_size, dst);
    } else {
      raw.resize(meta.data_size);
      RETURN_IF_FAIL(read_slice(meta.data_offset, meta.data_size, raw.data()));
      Le64BufferToInt64(raw.data(), n, dst);
      return Status::kSuccess;
    }
  };

  for (size_t i = 0; i < column_ids.size(); ++i) {
    const slot_t cid = column_ids[i];
    if (cid >= schema.ColumnCount() || cid >= blk.columns.size()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "tlcs column id out of range");
    }
    const ColumnMeta& meta = blk.columns[cid];
    DecodedColumnChunk& chunk = out->columns[i];
    chunk.type = meta.type;
    chunk.is_unsigned = meta.is_unsigned;
    chunk.has_null = meta.has_null;
    chunk.has_min_max = meta.has_min_max;
    chunk.has_double_min_max = meta.has_double_min_max;
    chunk.has_nan = meta.has_nan;
    chunk.row_count = n;
    chunk.min_int = meta.min_int;
    chunk.max_int = meta.max_int;
    chunk.min_double = meta.min_double;
    chunk.max_double = meta.max_double;
    chunk.dict = nullptr;
    chunk.dict_encoded = false;

    if (meta.has_null) {
      if (meta.nulls_size != static_cast<uint64_t>(n)) {
        return StatusError(StatusCode::kCorrupt,
                           "tlcs null bitmap size mismatch");
      }
      chunk.nulls.resize(n);
      RETURN_IF_FAIL(
          read_slice(meta.nulls_offset, meta.nulls_size, chunk.nulls.data()));
    } else {
      chunk.nulls.clear();
    }

    switch (meta.encoding) {
      case TlcsColumnEncoding::kNarrowDict: {
        chunk.dict = &dicts_[cid];
        chunk.dict_encoded = true;
        if (meta.data_size != static_cast<uint64_t>(n)) {
          return StatusError(StatusCode::kCorrupt,
                             "tlcs dict code size mismatch");
        }
        chunk.dict_codes.resize(n);
        RETURN_IF_FAIL(read_slice(meta.data_offset, meta.data_size,
                                  chunk.dict_codes.data()));
        if (materialize_dict_ints && meta.type != ValueType::kVarChar) {
          chunk.int_cells.resize(n);
          MaterializeDictInts(meta, dicts_[cid], chunk.dict_codes, n,
                              chunk.int_cells.data());
        } else {
          chunk.int_cells.clear();
        }
        chunk.varchar_bytes.clear();
        chunk.varchar_ends.clear();
        break;
      }
      case TlcsColumnEncoding::kFor: {
        chunk.dict_codes.clear();
        chunk.varchar_bytes.clear();
        chunk.varchar_ends.clear();
        chunk.int_cells.resize(n);
        if (meta.for_width == 0) {
          UnpackForToInt64(meta, {}, n, chunk.int_cells.data());
        } else if (meta.for_width == 8) {
          RETURN_IF_FAIL(read_raw64(meta, chunk.int_cells.data()));
        } else {
          if ((meta.for_width != 1 && meta.for_width != 2 &&
               meta.for_width != 4) ||
              meta.data_size != static_cast<uint64_t>(n) * meta.for_width) {
            return StatusError(StatusCode::kCorrupt,
                               "tlcs narrow FOR column size mismatch");
          }
          raw.resize(meta.data_size);
          RETURN_IF_FAIL(
              read_slice(meta.data_offset, meta.data_size, raw.data()));
          UnpackForToInt64(meta, raw, n, chunk.int_cells.data());
        }
        break;
      }
      case TlcsColumnEncoding::kRawDouble: {
        chunk.dict_codes.clear();
        chunk.varchar_bytes.clear();
        chunk.varchar_ends.clear();
        chunk.int_cells.resize(n);
        RETURN_IF_FAIL(read_raw64(meta, chunk.int_cells.data()));
        break;
      }
      case TlcsColumnEncoding::kVarChar: {
        chunk.dict_codes.clear();
        chunk.int_cells.clear();
        chunk.varchar_ends.resize(n);
        if (meta.aux_size != static_cast<uint64_t>(n) * sizeof(uint32_t)) {
          return StatusError(StatusCode::kCorrupt,
                             "tlcs varchar ends size mismatch");
        }
        RETURN_IF_FAIL(read_slice(meta.aux_offset, meta.aux_size,
                                  chunk.varchar_ends.data()));
        const bool lengths_only =
            i < varchar_lengths_only.size() && varchar_lengths_only[i] != 0;
        if (lengths_only) {
          chunk.varchar_bytes.clear();
        } else if (!varchar_rows.empty() &&
                   (have_mmap ||
                    varchar_rows.size() <= meta.data_size / 4096) &&
                   chunk.varchar_ends.back() <= meta.data_size &&
                   std::ranges::is_sorted(chunk.varchar_ends)) {
          if (meta.data_size > 0 &&
              (meta.data_offset > file_size_ ||
               meta.data_size > file_size_ - meta.data_offset)) {
            return StatusError(StatusCode::kCorrupt,
                               "tlcs stream out of bounds");
          }
          // Pack selected strings in row order and make the unselected
          // ranges empty. Lean: sparse_varchar_projection.
          const std::vector<uint32_t> source_ends = chunk.varchar_ends;
          chunk.varchar_bytes.clear();
          uint32_t next_row = 0;
          for (const uint32_t row : varchar_rows) {
            const uint32_t begin = row == 0 ? 0 : source_ends[row - 1];
            const uint32_t end = source_ends[row];
            const uint32_t len = end - begin;
            const size_t packed = chunk.varchar_bytes.size();
            std::fill_n(chunk.varchar_ends.begin() + next_row, row - next_row,
                        static_cast<uint32_t>(packed));
            if (len != 0) {
              chunk.varchar_bytes.resize(packed + len);
              RETURN_IF_FAIL(read_slice(meta.data_offset + begin, len,
                                        chunk.varchar_bytes.data() + packed));
            }
            chunk.varchar_ends[row] = static_cast<uint32_t>(packed + len);
            next_row = row + 1;
          }
          std::fill(chunk.varchar_ends.begin() + next_row,
                    chunk.varchar_ends.end(),
                    static_cast<uint32_t>(chunk.varchar_bytes.size()));
        } else {
          chunk.varchar_bytes.resize(meta.data_size);
          if (meta.data_size > 0) {
            RETURN_IF_FAIL(read_slice(meta.data_offset, meta.data_size,
                                      chunk.varchar_bytes.data()));
          }
        }
        break;
      }
      case TlcsColumnEncoding::kConstNull: {
        chunk.dict_codes.clear();
        chunk.varchar_bytes.clear();
        chunk.varchar_ends.clear();
        chunk.int_cells.assign(n, 0);
        break;
      }
      default:
        return StatusError(StatusCode::kCorrupt,
                           "tlcs unknown column encoding");
    }
  }
  return Status::kSuccess;
}

StatusOr<std::shared_ptr<const TlcsFile::DecodedBlock>>
TlcsFile::GetOrLoadCachedFullBlock(uint32_t block_idx,
                                   const Schema& schema) const {
  {
    std::scoped_lock lk(cache_mu_);
    auto it = cache_map_.find(block_idx);
    if (it != cache_map_.end()) {
      cache_lru_.splice(cache_lru_.end(), cache_lru_, it->second.lru_it);
      return it->second.block;
    }
  }
  std::vector<slot_t> all_cols(schema.ColumnCount());
  for (slot_t c = 0; c < schema.ColumnCount(); ++c) {
    all_cols[c] = c;
  }
  auto blk = std::make_shared<DecodedBlock>();
  RETURN_IF_FAIL(
      DecodeBlockColumns(block_idx, all_cols, schema, blk.get(), true));
  const size_t entry_bytes = EstimateDecodedBlockBytes(*blk);
  const size_t cache_cap = PointReadCacheLimitBytes();
  std::shared_ptr<const DecodedBlock> cblk = std::move(blk);

  std::scoped_lock lk(cache_mu_);
  if (auto it = cache_map_.find(block_idx); it != cache_map_.end()) {
    cache_lru_.splice(cache_lru_.end(), cache_lru_, it->second.lru_it);
    return it->second.block;
  }
  if (entry_bytes <= cache_cap) {
    while (cache_bytes_ + entry_bytes > cache_cap && !cache_lru_.empty()) {
      const uint32_t victim = cache_lru_.front();
      cache_lru_.pop_front();
      auto vit = cache_map_.find(victim);
      if (vit != cache_map_.end()) {
        const size_t vb = vit->second.bytes;
        cache_map_.erase(vit);
        cache_bytes_ = cache_bytes_ > vb ? cache_bytes_ - vb : 0;
        ProcessMemoryBudget::Global().Release(MemoryPoolKind::kColumnar, vb);
      }
    }
    if (ProcessMemoryBudget::Global().TryReserve(MemoryPoolKind::kColumnar,
                                                 entry_bytes)) {
      cache_lru_.push_back(block_idx);
      auto lru_it = std::prev(cache_lru_.end());
      cache_map_.emplace(
          block_idx,
          CachedBlock{.block = cblk, .bytes = entry_bytes, .lru_it = lru_it});
      cache_bytes_ += entry_bytes;
    }
  }
  return cblk;
}

StatusOr<std::shared_ptr<const std::vector<std::string>>>
TlcsFile::GetOrLoadCachedEncodedRows(uint32_t block_idx,
                                     const Schema& schema) const {
  {
    std::scoped_lock lk(cache_mu_);
    auto it = encoded_cache_map_.find(block_idx);
    if (it != encoded_cache_map_.end()) {
      encoded_cache_lru_.splice(encoded_cache_lru_.end(), encoded_cache_lru_,
                                it->second.lru_it);
      return it->second.rows;
    }
  }
  using CachedBlockPtr = std::shared_ptr<const DecodedBlock>;
  ASSIGN_OR_RETURN(CachedBlockPtr, blk,
                   GetOrLoadCachedFullBlock(block_idx, schema));
  auto rows = std::make_shared<std::vector<std::string>>();
  rows->reserve(blk->row_count);
  size_t payload_bytes = 0;
  Row row;
  for (uint32_t r = 0; r < blk->row_count; ++r) {
    RETURN_IF_FAIL(FillRowFromDecodedBlock(*blk, r, schema, &row));
    std::string& dst = rows->emplace_back();
    dst.resize(row.Size());
    row.Serialize(dst.data());
    payload_bytes += dst.size();
  }
  const size_t entry_bytes =
      payload_bytes + (rows->size() * sizeof(std::string)) + 64;
  const size_t cache_cap = PointReadCacheLimitBytes();
  std::shared_ptr<const std::vector<std::string>> crows = std::move(rows);
  if (entry_bytes <= cache_cap) {
    std::scoped_lock lk(cache_mu_);
    if (auto it = encoded_cache_map_.find(block_idx);
        it != encoded_cache_map_.end()) {
      encoded_cache_lru_.splice(encoded_cache_lru_.end(), encoded_cache_lru_,
                                it->second.lru_it);
      return it->second.rows;
    }
    while (encoded_cache_bytes_ + entry_bytes > cache_cap &&
           !encoded_cache_lru_.empty()) {
      const uint32_t victim = encoded_cache_lru_.front();
      encoded_cache_lru_.pop_front();
      auto vit = encoded_cache_map_.find(victim);
      if (vit != encoded_cache_map_.end()) {
        const size_t vb = vit->second.bytes;
        encoded_cache_map_.erase(vit);
        encoded_cache_bytes_ =
            encoded_cache_bytes_ > vb ? encoded_cache_bytes_ - vb : 0;
        ProcessMemoryBudget::Global().Release(MemoryPoolKind::kColumnar, vb);
      }
    }
    if (ProcessMemoryBudget::Global().TryReserve(MemoryPoolKind::kColumnar,
                                                 entry_bytes)) {
      encoded_cache_lru_.push_back(block_idx);
      auto lru_it = std::prev(encoded_cache_lru_.end());
      encoded_cache_map_.emplace(block_idx, CachedEncodedBlock{
                                                .rows = crows,
                                                .bytes = entry_bytes,
                                                .lru_it = lru_it,
                                            });
      encoded_cache_bytes_ += entry_bytes;
    }
  }
  return crows;
}

Status TlcsFile::DecodeSingleRowByBlockIndex(uint32_t block_idx,
                                             uint32_t row_idx,
                                             const Schema& schema,
                                             Row* out) const {
  if (block_idx >= block_count_ || dir_group_size_ == 0 || out == nullptr) {
    return Status::kNotExists;
  }
  ASSIGN_OR_RETURN(std::shared_ptr<const DirGroup>, grp,
                   GetDirGroup(block_idx / dir_group_size_));
  const uint32_t lidx = block_idx % dir_group_size_;
  if (lidx >= grp->blocks.size()) {
    return StatusError(StatusCode::kCorrupt,
                       "tlcs decode local block index out of range");
  }
  const BlockDirEntry& src_b = grp->blocks[lidx];
  if (row_idx >= src_b.row_count ||
      src_b.columns.size() < schema.ColumnCount()) {
    return Status::kNotExists;
  }

  std::shared_ptr<const platform::MemoryMap> mmap_copy;
  {
    std::scoped_lock lk(resident_mu_);
    mmap_copy = resident_mmap_;
  }
  const bool have_mmap = mmap_copy != nullptr && mmap_copy->IsValid();
  const uint8_t* mmap_data = have_mmap ? mmap_copy->Data() : nullptr;
  auto read_slice = [&](uint64_t offset, uint64_t size, void* dst) -> Status {
    if (size == 0) {
      return Status::kSuccess;
    }
    if (offset > file_size_ || size > file_size_ - offset) {
      return StatusError(StatusCode::kCorrupt, "tlcs stream out of bounds");
    }
    if (have_mmap) {
      std::memcpy(dst, mmap_data + offset, size);
      return Status::kSuccess;
    }
    return PreadAllFd(fd_, dst, size, offset);
  };

  DecodedBlock blk;
  blk.block_index = block_idx;
  blk.pid = src_b.pid;
  blk.row_count = 1;
  blk.columns.resize(schema.ColumnCount());

  for (slot_t c = 0; c < schema.ColumnCount(); ++c) {
    const ColumnMeta& meta = src_b.columns[c];
    DecodedColumnChunk& chunk = blk.columns[c];
    chunk.type = meta.type;
    chunk.is_unsigned = meta.is_unsigned;
    chunk.has_null = meta.has_null;
    chunk.has_min_max = meta.has_min_max;
    chunk.has_double_min_max = meta.has_double_min_max;
    chunk.has_nan = meta.has_nan;
    chunk.row_count = 1;
    chunk.min_int = meta.min_int;
    chunk.max_int = meta.max_int;
    chunk.min_double = meta.min_double;
    chunk.max_double = meta.max_double;

    if (meta.has_null) {
      if (meta.nulls_size != src_b.row_count) {
        return StatusError(StatusCode::kCorrupt,
                           "tlcs null bitmap size mismatch");
      }
      chunk.nulls.resize(1);
      RETURN_IF_FAIL(
          read_slice(meta.nulls_offset + row_idx, 1, chunk.nulls.data()));
    }

    switch (meta.encoding) {
      case TlcsColumnEncoding::kNarrowDict: {
        if (meta.data_size != src_b.row_count) {
          return StatusError(StatusCode::kCorrupt,
                             "tlcs dict code size mismatch");
        }
        chunk.dict = &dicts_[c];
        chunk.dict_encoded = true;
        chunk.dict_codes.resize(1);
        RETURN_IF_FAIL(
            read_slice(meta.data_offset + row_idx, 1, chunk.dict_codes.data()));
        if (meta.type != ValueType::kVarChar) {
          chunk.int_cells.resize(1);
          MaterializeDictInts(meta, dicts_[c], chunk.dict_codes, 1,
                              chunk.int_cells.data());
        }
        break;
      }
      case TlcsColumnEncoding::kFor: {
        chunk.int_cells.resize(1);
        const auto base_u = static_cast<uint64_t>(meta.for_base);
        switch (meta.for_width) {
          case 0:
            chunk.int_cells[0] = meta.for_base;
            break;
          case 1: {
            uint8_t b = 0;
            RETURN_IF_FAIL(read_slice(meta.data_offset + row_idx, 1, &b));
            chunk.int_cells[0] = static_cast<int64_t>(base_u + b);
            break;
          }
          case 2: {
            uint16_t b = 0;
            RETURN_IF_FAIL(read_slice(
                // NOLINTNEXTLINE(readability-math-missing-parentheses)
                // precedence is unambiguous
                meta.data_offset + static_cast<uint64_t>(row_idx) * 2,
                2,     // NOLINT(readability-math-missing-parentheses)
                &b));  // NOLINT(readability-math-missing-parentheses) reviewed
                       // intentional pattern
            chunk.int_cells[0] = static_cast<int64_t>(base_u + le16toh(b));
            break;
          }
          case 4: {
            uint32_t b = 0;
            RETURN_IF_FAIL(read_slice(
                // NOLINTNEXTLINE(readability-math-missing-parentheses)
                // precedence is unambiguous
                meta.data_offset + static_cast<uint64_t>(row_idx) * 4,
                4,     // NOLINT(readability-math-missing-parentheses)
                &b));  // NOLINT(readability-math-missing-parentheses) reviewed
                       // intentional pattern
            chunk.int_cells[0] = static_cast<int64_t>(base_u + le32toh(b));
            break;
          }
          case 8: {
            uint64_t v = 0;
            RETURN_IF_FAIL(read_slice(
                // NOLINTNEXTLINE(readability-math-missing-parentheses)
                // precedence is unambiguous
                meta.data_offset + static_cast<uint64_t>(row_idx) * 8,
                8,     // NOLINT(readability-math-missing-parentheses)
                &v));  // NOLINT(readability-math-missing-parentheses) reviewed
                       // intentional pattern
            chunk.int_cells[0] = static_cast<int64_t>(le64toh(v));
            break;
          }
          default:
            return StatusError(StatusCode::kCorrupt, "tlcs FOR width mismatch");
        }
        break;
      }
      case TlcsColumnEncoding::kRawDouble: {
        chunk.int_cells.resize(1);
        uint64_t v = 0;
        RETURN_IF_FAIL(read_slice(
            // NOLINTNEXTLINE(readability-math-missing-parentheses) precedence
            // is unambiguous
            meta.data_offset + static_cast<uint64_t>(row_idx) * 8,
            8,     // NOLINT(readability-math-missing-parentheses)
            &v));  // NOLINT(readability-math-missing-parentheses) reviewed
                   // intentional pattern
        chunk.int_cells[0] = static_cast<int64_t>(le64toh(v));
        break;
      }
      case TlcsColumnEncoding::kVarChar: {
        if (meta.aux_size !=
            static_cast<uint64_t>(src_b.row_count) * sizeof(uint32_t)) {
          return StatusError(StatusCode::kCorrupt,
                             "tlcs varchar ends size mismatch");
        }
        uint32_t end = 0;
        uint32_t begin = 0;
        RETURN_IF_FAIL(read_slice(
            // NOLINTNEXTLINE(readability-math-missing-parentheses) precedence
            // is unambiguous
            meta.aux_offset + static_cast<uint64_t>(row_idx) * 4,
            4,       // NOLINT(readability-math-missing-parentheses)
            &end));  // NOLINT(readability-math-missing-parentheses) reviewed
                     // intentional pattern
        if (row_idx > 0) {
          RETURN_IF_FAIL(read_slice(
              // NOLINTNEXTLINE(readability-math-missing-parentheses) precedence
              // is unambiguous
              meta.aux_offset +
                  static_cast<uint64_t>(row_idx - 1) *
                      4,  // NOLINT(readability-math-missing-parentheses)
              4,  // NOLINT(readability-math-missing-parentheses) reviewed
                  // intentional pattern
              &begin));
        }
        end = le32toh(end);
        begin = le32toh(begin);
        if (end < begin || end > meta.data_size) {
          return StatusError(StatusCode::kCorrupt,
                             "tlcs varchar bounds mismatch");
        }
        chunk.varchar_bytes.resize(end - begin);
        RETURN_IF_FAIL(read_slice(meta.data_offset + begin, end - begin,
                                  chunk.varchar_bytes.data()));
        chunk.varchar_ends.push_back(end - begin);
        break;
      }
      case TlcsColumnEncoding::kConstNull: {
        chunk.int_cells.assign(1, 0);
        break;
      }
      default:
        return StatusError(StatusCode::kCorrupt,
                           "tlcs unknown column encoding");
    }
  }
  return FillRowFromDecodedBlock(blk, 0, schema, out);
}

Status TlcsFile::ReadRowByBlockIndex(uint32_t block_idx, uint32_t row_idx,
                                     const Schema& schema, Row* out) const {
  if (block_idx >= block_count_) {
    return Status::kNotExists;
  }
  return DecodeSingleRowByBlockIndex(block_idx, row_idx, schema, out);
}

Status TlcsFile::ReadRowByPid(page_id_t pid, slot_t slot, const Schema& schema,
                              Row* out) const {
  const uint32_t block_idx = FindBlockIndex(pid);
  if (block_idx == LSTableStore::kInvalidBlockIndex) {
    return Status::kNotExists;
  }
  return ReadRowByBlockIndex(block_idx, slot, schema, out);
}

void TlcsFile::ResetDecodeCallsForTest() {
  g_tlcs_decode_calls.store(0, std::memory_order_relaxed);
}

uint64_t TlcsFile::DecodeCallsForTest() {
  return g_tlcs_decode_calls.load(std::memory_order_relaxed);
}

uint64_t TlcsFile::CachedEncodedBlocksForTest() const {
  std::scoped_lock lk(cache_mu_);
  return encoded_cache_map_.size();
}

StatusOr<std::shared_ptr<const std::vector<std::string>>>
TlcsFile::GetCachedEncodedBlockByPid(page_id_t pid,
                                     const Schema& schema) const {
  const uint32_t block_idx = FindBlockIndex(pid);
  if (block_idx == LSTableStore::kInvalidBlockIndex) {
    return Status::kNotExists;
  }
  return GetOrLoadCachedEncodedRows(block_idx, schema);
}

Status TlcsFile::ReadEncodedRowByPid(page_id_t pid, slot_t slot,
                                     const Schema& schema,
                                     std::string* out) const {
  const uint32_t block_idx = FindBlockIndex(pid);
  if (block_idx == LSTableStore::kInvalidBlockIndex) {
    return Status::kNotExists;
  }
  // #1111: serve from the block's cached encoded image instead of decoding
  // the row into typed values and re-serializing on every resolve.
  using EncodedRowsPtr = std::shared_ptr<const std::vector<std::string>>;
  ASSIGN_OR_RETURN(EncodedRowsPtr, rows,
                   GetOrLoadCachedEncodedRows(block_idx, schema));
  if (slot >= rows->size()) {
    return Status::kNotExists;
  }
  *out = (*rows)[slot];
  return Status::kSuccess;
}

Status TlcsFile::DecodeBlockEncodedRowsByPid(
    page_id_t pid, const Schema& schema, std::vector<std::string>* out) const {
  const uint32_t block_idx = FindBlockIndex(pid);
  if (block_idx == LSTableStore::kInvalidBlockIndex) {
    return Status::kNotExists;
  }
  using EncodedRowsPtr = std::shared_ptr<const std::vector<std::string>>;
  ASSIGN_OR_RETURN(EncodedRowsPtr, rows,
                   GetOrLoadCachedEncodedRows(block_idx, schema));
  out->assign(rows->begin(), rows->end());
  return Status::kSuccess;
}

Status TlcsFile::ForEachBlockRow(
    const Schema& schema,
    const std::function<Status(page_id_t pid, slot_t slot, const Row& row)>& fn)
    const {
  std::vector<slot_t> all_cols(schema.ColumnCount());
  for (slot_t c = 0; c < schema.ColumnCount(); ++c) {
    all_cols[c] = c;
  }
  DecodedBlock blk;
  Row row;
  for (uint32_t block_idx = 0; block_idx < block_count_; ++block_idx) {
    RETURN_IF_FAIL(DecodeBlockColumns(block_idx, all_cols, schema, &blk, true));
    for (uint32_t r = 0; r < blk.row_count; ++r) {
      RETURN_IF_FAIL(FillRowFromDecodedBlock(blk, r, schema, &row));
      RETURN_IF_FAIL(fn(blk.pid, static_cast<slot_t>(r), row));
    }
  }
  return Status::kSuccess;
}

Status TlcsFile::ForEachBlockRowByPid(
    page_id_t pid, const Schema& schema,
    const std::function<Status(slot_t slot, const Row& row)>& fn) const {
  const uint32_t block_idx = FindBlockIndex(pid);
  if (block_idx == LSTableStore::kInvalidBlockIndex) {
    return Status::kNotExists;
  }
  std::vector<slot_t> all_cols(schema.ColumnCount());
  for (slot_t c = 0; c < schema.ColumnCount(); ++c) {
    all_cols[c] = c;
  }
  DecodedBlock blk;
  RETURN_IF_FAIL(DecodeBlockColumns(block_idx, all_cols, schema, &blk, true));
  Row row;
  for (uint32_t r = 0; r < blk.row_count; ++r) {
    RETURN_IF_FAIL(FillRowFromDecodedBlock(blk, r, schema, &row));
    RETURN_IF_FAIL(fn(static_cast<slot_t>(r), row));
  }
  return Status::kSuccess;
}

bool TlcsFile::LoadAllDirGroups(
    std::vector<std::shared_ptr<const DirGroup>>* groups_out) const {
  groups_out->clear();
  groups_out->reserve(dir_groups_.size());
  uint64_t dir_rows = 0;
  for (uint32_t g = 0; g < dir_groups_.size(); ++g) {
    StatusOr<std::shared_ptr<const DirGroup>> grp_or = GetDirGroup(g);
    if (!grp_or.HasValue()) {
      return false;
    }
    for (const BlockDirEntry& b : grp_or.Value()->blocks) {
      dir_rows += b.row_count;
    }
    groups_out->push_back(std::move(grp_or.Value()));
  }
  return dir_rows == total_rows_;
}

TlcsFile::ColumnDecodePlan TlcsFile::PlanColumnDecode(
    slot_t c, bool using_mmap,
    const std::vector<std::shared_ptr<const DirGroup>>& groups) const {
  ColumnDecodePlan plan;
  plan.heap_bytes =
      sizeof(LSTableColumnCacheEntry) +
      (static_cast<uint64_t>(block_count_) * sizeof(LSTableColumnBlock));
  const ValueType vt = col_types_[c];
  if (vt != ValueType::kVarChar) {
    // Always materialize a contiguous integer_column for per-column cache
    // entries so numeric fast paths (IntegerRuns/IntegerRunBounds and direct
    // scans) see one dense vector instead of strided mmap slices.
    plan.zero_copy_64 = false;
    plan.need_int_vec = true;
    plan.heap_bytes += total_rows_ * sizeof(int64_t);
  }
  if (dict_kinds_[c] == TlcsDictKind::kCompleteNarrow) {
    plan.heap_bytes += total_rows_;
  }
  for (const auto& grp : groups) {
    for (const BlockDirEntry& b : grp->blocks) {
      const ColumnMeta& m = b.columns[c];
      if (!using_mmap) {
        plan.heap_bytes += m.nulls_size;
        if (vt == ValueType::kVarChar) {
          if (m.encoding == TlcsColumnEncoding::kVarChar) {
            plan.heap_bytes += m.data_size + m.aux_size;
          } else if (m.encoding == TlcsColumnEncoding::kNarrowDict &&
                     dict_kinds_[c] == TlcsDictKind::kPartialFrozen) {
            plan.heap_bytes +=
                m.data_size +
                (static_cast<uint64_t>(b.row_count) * sizeof(uint32_t));
          }
        }
      } else {
        plan.mapped_bytes += m.nulls_size;
        if (vt == ValueType::kVarChar &&
            m.encoding == TlcsColumnEncoding::kVarChar) {
          plan.mapped_bytes += m.data_size + m.aux_size;
        }
      }
    }
  }
  return plan;
}

std::shared_ptr<const platform::MemoryMap> TlcsFile::EnsureResidentMmap()
    const {
  {
    std::scoped_lock lk(resident_mu_);
    if (resident_mmap_ != nullptr && resident_mmap_->IsValid()) {
      return resident_mmap_;
    }
  }
  const auto& budget = ProcessMemoryBudget::Global();
  if (budget.AlwaysSpillEnabled() || budget.UnderMemoryPressure() ||
      file_size_ == 0) {
    return nullptr;
  }
  const size_t effective_col_max = EffectiveColumnarPoolMaxBytes();
  const uint64_t pool_half = effective_col_max / 2;
  const auto env_limit = ReadResidentMmapLimitBytes();
  const uint64_t mmap_cap = std::min(
      pool_half, env_limit.value_or(kDefaultResidentMmapLimitBytes));
  const bool env_override = env_limit.has_value();
  const uint64_t map_only_cap =
      env_override
          ? mmap_cap
          : (budget.Limit() == 0
                 ? kEagerResidentFileBytes
                 : std::min<uint64_t>(
                       kEagerResidentFileBytes,
                       std::max<uint64_t>(mmap_cap, budget.Limit() / 2)));
  if (mmap_cap == 0 || file_size_ > map_only_cap) {
    return nullptr;
  }
  auto mmap = std::make_shared<platform::MemoryMap>();
  if (mmap->Open(path_) != 0 || !mmap->IsValid()) {
    return nullptr;
  }
  std::scoped_lock lk(resident_mu_);
  if (resident_mmap_ != nullptr && resident_mmap_->IsValid()) {
    return resident_mmap_;
  }
  resident_mmap_ = std::move(mmap);
  return resident_mmap_;
}

size_t TlcsFile::EstimateColumnsCacheBytes(
    const Schema& schema, const std::vector<slot_t>& columns) const {
  if (schema.ColumnCount() != column_count_ ||
      !LSTableStore::SupportsSchema(schema)) {
    return 0;
  }
  std::vector<std::shared_ptr<const DirGroup>> groups;
  if (!LoadAllDirGroups(&groups)) {
    return 0;
  }
  bool using_mmap = false;
  {
    std::scoped_lock lk(resident_mu_);
    using_mmap = resident_mmap_ != nullptr && resident_mmap_->IsValid();
  }
  uint64_t total = 0;
  for (const slot_t c : columns) {
    if (c < column_count_) {
      const ColumnDecodePlan plan = PlanColumnDecode(c, using_mmap, groups);
      total += plan.mapped_bytes + plan.heap_bytes;
    }
  }
  return static_cast<size_t>(
      std::min<uint64_t>(total, std::numeric_limits<size_t>::max()));
}

std::shared_ptr<const LSTableBlockLayout> TlcsFile::BuildBlockLayout(
    const Schema& schema, uint64_t generation, bool* capacity_rejected) const {
  if (capacity_rejected != nullptr) {
    *capacity_rejected = false;
  }
  if (schema.ColumnCount() != column_count_ ||
      !LSTableStore::SupportsSchema(schema)) {
    return nullptr;
  }
  std::vector<std::shared_ptr<const DirGroup>> groups;
  if (!LoadAllDirGroups(&groups)) {
    return nullptr;
  }
  std::shared_ptr<const platform::MemoryMap> mmap_copy = EnsureResidentMmap();
  const uint8_t* base = mmap_copy != nullptr && mmap_copy->IsValid()
                            ? mmap_copy->Data()
                            : nullptr;
  const bool using_mmap = base != nullptr;

  auto layout = std::make_shared<LSTableBlockLayout>();
  layout->generation = generation;
  layout->first_pid = first_pid_;
  layout->is_persistent_tlcs = true;
  layout->total_rows = total_rows_;
  layout->backing_mmap = using_mmap ? mmap_copy : nullptr;
  if (using_mmap && stats_file_size_ > 0 &&
      stats_file_offset_ + stats_file_size_ <= mmap_copy->Size()) {
    layout->serialized_column_stats = std::string_view(
        reinterpret_cast<const char*>(base + stats_file_offset_),
        stats_file_size_);
  } else if (!serialized_column_stats_.empty()) {
    layout->owned_column_stats = serialized_column_stats_;
    layout->serialized_column_stats = layout->owned_column_stats;
  }

  layout->page_ids.reserve(block_count_);
  layout->block_row_counts.reserve(block_count_);
  page_id_t max_pid = 0;
  for (const auto& grp : groups) {
    for (const BlockDirEntry& src_b : grp->blocks) {
      layout->page_ids.push_back(src_b.pid);
      layout->block_row_counts.push_back(src_b.row_count);
      max_pid = std::max(max_pid, src_b.pid);
    }
  }

  layout->memory_charge =
      std::make_shared<ProcessMemoryCharge>(MemoryPoolKind::kColumnar);
  const size_t base_bytes =
      sizeof(LSTableBlockLayout) +
      (layout->page_ids.size() * sizeof(page_id_t)) +
      (layout->block_row_counts.size() * sizeof(uint32_t));
  if (!layout->memory_charge->TryAddWithReclaim(base_bytes)) {
    if (capacity_rejected != nullptr) {
      *capacity_rejected = true;
    }
    return nullptr;
  }
  if (max_pid < kMaxFlatPageId && !layout->page_ids.empty()) {
    const size_t flat_bytes =
        static_cast<size_t>(max_pid + 1) * sizeof(uint32_t);
    if (layout->memory_charge->TryAddWithReclaim(flat_bytes)) {
      layout->flat_page_to_block.assign(max_pid + 1,
                                        LSTableStore::kInvalidBlockIndex);
      for (size_t i = 0; i < layout->page_ids.size(); ++i) {
        layout->flat_page_to_block[layout->page_ids[i]] =
            static_cast<uint32_t>(i);
      }
    }
  }
  if (layout->flat_page_to_block.empty() && !layout->page_ids.empty()) {
    const size_t map_bytes = layout->page_ids.size() * 32ULL;
    if (!layout->memory_charge->TryAddWithReclaim(map_bytes)) {
      if (capacity_rejected != nullptr) {
        *capacity_rejected = true;
      }
      return nullptr;
    }
    layout->page_to_block.reserve(layout->page_ids.size());
    for (size_t i = 0; i < layout->page_ids.size(); ++i) {
      layout->page_to_block.emplace(layout->page_ids[i], i);
    }
  }
  return layout;
}

std::vector<std::shared_ptr<const LSTableColumnCacheEntry>>
TlcsFile::BuildColumnCacheEntries(
    const Schema& schema, uint64_t generation,
    const std::vector<slot_t>& columns, uint64_t effective_max_bytes,
    std::vector<slot_t>* rejected_cols,
    const std::vector<std::shared_ptr<const LSTableColumnCacheEntry>>*
        existing_lazy) const {
  std::vector<std::shared_ptr<const LSTableColumnCacheEntry>> result(
      columns.size());
  if (columns.empty()) {
    return result;
  }
  if (schema.ColumnCount() != column_count_ ||
      !LSTableStore::SupportsSchema(schema)) {
    return result;
  }
  for (const slot_t c : columns) {
    if (c >= column_count_) {
      return result;
    }
  }
  std::vector<std::shared_ptr<const DirGroup>> groups;
  if (!LoadAllDirGroups(&groups)) {
    return result;
  }
  std::shared_ptr<const platform::MemoryMap> mmap_copy = EnsureResidentMmap();
  const uint8_t* base = mmap_copy != nullptr && mmap_copy->IsValid()
                            ? mmap_copy->Data()
                            : nullptr;
  const bool using_mmap = base != nullptr;

  std::vector<const BlockDirEntry*> blk_ptrs;
  std::vector<size_t> blk_row_bases;
  std::vector<uint32_t> blk_row_counts;
  blk_ptrs.reserve(block_count_);
  blk_row_bases.reserve(block_count_);
  blk_row_counts.reserve(block_count_);
  size_t running_rows = 0;
  for (const auto& grp : groups) {
    for (const BlockDirEntry& src_b : grp->blocks) {
      blk_ptrs.push_back(&src_b);
      blk_row_bases.push_back(running_rows);
      blk_row_counts.push_back(src_b.row_count);
      running_rows += src_b.row_count;
    }
  }

  std::vector<std::shared_ptr<LSTableColumnCacheEntry>> mutable_entries(
      columns.size());
  std::vector<ColumnDecodePlan> plans(columns.size());
  std::vector<size_t> active_indices;
  active_indices.reserve(columns.size());
  const auto& budget = ProcessMemoryBudget::Global();
  const size_t col_limit =
      std::min(budget.ColumnarPoolLimit(),
               budget.EffectivePoolMaxBytes(MemoryPoolKind::kColumnar));
  for (size_t i = 0; i < columns.size(); ++i) {
    const slot_t c = columns[i];
    const ColumnDecodePlan plan = PlanColumnDecode(c, using_mmap, groups);
    plans[i] = plan;
    if (plan.mapped_bytes > effective_max_bytes ||
        plan.heap_bytes > effective_max_bytes) {
      if (rejected_cols != nullptr) {
        rejected_cols->push_back(c);
      }
      continue;
    }
    uint64_t total_reserve = plan.mapped_bytes + plan.heap_bytes;
    auto entry = std::make_shared<LSTableColumnCacheEntry>();
    entry->generation = generation;
    entry->col_index = c;
    entry->type = col_types_[c];
    entry->is_unsigned = col_unsigned_[c] != 0;
    entry->backing_mmap = using_mmap ? mmap_copy : nullptr;
    entry->memory_charge =
        std::make_shared<ProcessMemoryCharge>(MemoryPoolKind::kColumnar);
    if (total_reserve > 0 &&
        !entry->memory_charge->TryAddWithReclaim(total_reserve)) {
      // Only permanently mark the column rejected when it exceeds the entire
      // pool limit; transient contention should remain retryable later.
      if (rejected_cols != nullptr && col_limit > 0 &&
          total_reserve > col_limit) {
        rejected_cols->push_back(c);
      }
      continue;
    }
    entry->blocks.resize(blk_ptrs.size());
    if (plan.need_int_vec && total_rows_ > 0) {
      if (total_rows_ >= 65536) {
        entry->integer_column_raw =
            std::make_unique_for_overwrite<int64_t[]>(total_rows_);
        entry->integer_column_raw_size = total_rows_;
        AdviseHugePages(entry->integer_column_raw.get(),
                        total_rows_ * sizeof(int64_t));
      } else {
        entry->integer_column.resize(total_rows_);
      }
    }
    if (dict_kinds_[c] == TlcsDictKind::kCompleteNarrow) {
      entry->dictionary = dicts_[c];
      if (total_rows_ > 0) {
        if (total_rows_ >= 65536) {
          entry->narrow_codes_raw =
              std::make_unique_for_overwrite<uint8_t[]>(total_rows_);
          entry->narrow_codes_raw_size = total_rows_;
          AdviseHugePages(entry->narrow_codes_raw.get(), total_rows_);
          entry->dictionary.narrow_codes_view = {entry->narrow_codes_raw.get(),
                                                 total_rows_};
        } else {
          entry->dictionary.narrow_codes.resize(total_rows_);
        }
      }
      if (total_rows_ <= static_cast<uint64_t>(INT64_MAX)) {
        auto& dict = entry->dictionary;
        const size_t entry_count = dict.value_type == ValueType::kVarChar
                                       ? dict.StringEntryCount()
                                       : (dict.value_type == ValueType::kDouble
                                              ? dict.DoubleEntryCount()
                                              : dict.IntEntryCount());
        if (entry_count <= LSTableColumnDictionary::kMaxEntries) {
          const size_t summary_bytes =
              (entry_count + 1) *
              (sizeof(int64_t) + sizeof(uint64_t) + sizeof(uint32_t));
          if (entry->memory_charge->TryAddWithReclaim(summary_bytes)) {
            dict.code_counts.assign(entry_count + 1, 0);
            dict.code_first_row.assign(entry_count + 1, UINT64_MAX);
            total_reserve += summary_bytes;
          }
        }
      }
    }
    entry->estimated_bytes = static_cast<size_t>(total_reserve);
    mutable_entries[i] = std::move(entry);
    active_indices.push_back(i);
  }

  if (active_indices.empty()) {
    return result;
  }

  const size_t nblocks = blk_ptrs.size();
  const size_t workers =
      (nblocks >= 4 && total_rows_ >= 65536)
          ? std::min<size_t>(
                nblocks,
                std::max<size_t>(1, QueryParallelWorkerLimit(
                                        std::thread::hardware_concurrency())))
          : 1;

  struct WorkerSummary {
    std::vector<int64_t> counts;
    std::vector<uint64_t> first_row;
  };
  // Per-(worker, active_col) dictionary counters so parallel stripe decodes
  // stay lock-free without false sharing.
  std::vector<std::vector<WorkerSummary>> worker_summaries(workers);
  for (size_t w = 0; w < workers; ++w) {
    worker_summaries[w].resize(active_indices.size());
    for (size_t ai = 0; ai < active_indices.size(); ++ai) {
      const auto& dict = mutable_entries[active_indices[ai]]->dictionary;
      if (!dict.code_counts.empty()) {
        worker_summaries[w][ai].counts.assign(dict.code_counts.size(), 0);
        worker_summaries[w][ai].first_row.assign(dict.code_counts.size(),
                                                 UINT64_MAX);
      }
    }
  }

  std::atomic<bool> io_failed{false};
  const Status par_status = RunParallelTasks(workers, [&](size_t w) -> Status {
    const size_t b_begin = (nblocks * w) / workers;
    const size_t b_end = (nblocks * (w + 1)) / workers;
    std::vector<uint8_t> raw_tmp;
    for (size_t bi = b_begin; bi < b_end; ++bi) {
      if (io_failed.load(std::memory_order_relaxed)) {
        return Status::kSuccess;
      }
      const BlockDirEntry& src_b = *blk_ptrs[bi];
      const size_t row_base = blk_row_bases[bi];
      for (size_t ai = 0; ai < active_indices.size(); ++ai) {
        const size_t idx = active_indices[ai];
        LSTableColumnCacheEntry& entry = *mutable_entries[idx];
        const slot_t c = entry.col_index;
        LSTableColumnDictionary* active_dict =
            dict_kinds_[c] == TlcsDictKind::kCompleteNarrow ? &entry.dictionary
                                                            : nullptr;
        uint8_t* dst_codes = entry.narrow_codes_raw.get();
        int64_t* dst_ints =
            entry.integer_column_raw != nullptr
                ? entry.integer_column_raw.get()
                : (entry.integer_column.empty() ? nullptr
                                                : entry.integer_column.data());
        WorkerSummary& ws = worker_summaries[w][ai];
        std::vector<int64_t>* d_counts =
            ws.counts.empty() ? nullptr : &ws.counts;
        std::vector<uint64_t>* d_first =
            ws.first_row.empty() ? nullptr : &ws.first_row;
        const LSTableColumnCacheEntry* lazy_src =
            (existing_lazy != nullptr && idx < existing_lazy->size() &&
             (*existing_lazy)[idx] != nullptr &&
             (*existing_lazy)[idx]->is_block_lazy &&
             (*existing_lazy)[idx]->lazy_state != nullptr &&
             (*existing_lazy)[idx]->lazy_state->lazy_blocks != nullptr &&
             bi < (*existing_lazy)[idx]->blocks.size())
                ? (*existing_lazy)[idx].get()
                : nullptr;
        const LSTableColumnBlock* lazy_blk =
            lazy_src != nullptr ? lazy_src->lazy_state->lazy_blocks[bi].load(
                                      std::memory_order_acquire)
                                : nullptr;
        if (lazy_blk != nullptr &&
            ReuseLazyDecodedBlock(row_base, src_b, src_b.columns[c],
                                  dict_kinds_[c], using_mmap, base, *lazy_blk,
                                  active_dict, dst_codes, dst_ints,
                                  &entry.blocks[bi], d_counts, d_first)) {
          continue;
        }
        if (!DecodeColumnBlockFromFile(
                fd_, base, using_mmap, row_base, src_b, src_b.columns[c],
                dicts_[c], dict_kinds_[c], plans[idx].zero_copy_64, active_dict,
                dst_codes, dst_ints, &entry.blocks[bi], &raw_tmp, d_counts,
                d_first)) {
          io_failed.store(true, std::memory_order_relaxed);
          return Status::kIOError;
        }
      }
    }
    return Status::kSuccess;
  });
  if (par_status != Status::kSuccess ||
      io_failed.load(std::memory_order_relaxed)) {
    return result;
  }

  for (size_t ai = 0; ai < active_indices.size(); ++ai) {
    const size_t idx = active_indices[ai];
    LSTableColumnCacheEntry& entry = *mutable_entries[idx];
    const slot_t c = entry.col_index;
    if (!entry.dictionary.code_counts.empty()) {
      auto& dict = entry.dictionary;
      const size_t ncodes = dict.code_counts.size();
      for (size_t w = 0; w < workers; ++w) {
        const WorkerSummary& ws = worker_summaries[w][ai];
        for (size_t s = 0; s < ncodes; ++s) {
          dict.code_counts[s] += ws.counts[s];
          dict.code_first_row[s] =
              std::min(dict.code_first_row[s], ws.first_row[s]);
        }
      }
      BuildEncounterCodesFromFirstRow(&dict);
    }

    ColumnSynopsis syn = has_col_synopses_ && c < col_synopses_.size()
                             ? col_synopses_[c]
                             : ComputeColumnSynopsisFromEntry(
                                   c, col_types_[c], col_unsigned_[c] != 0,
                                   total_rows_, entry.blocks, blk_row_counts);
    if (has_col_synopses_ && c == 0 && col_types_[c] == ValueType::kInt64 &&
        col_unsigned_[c] == 0) {
      ComputeZeroColumnBitsetUniqueness(total_rows_, entry.blocks,
                                        blk_row_counts, &syn);
    }
    entry.has_null = syn.has_null;
    entry.is_monotonic = syn.is_monotonic;
    entry.is_unique_nonnull = syn.is_unique_nonnull;
    entry.is_identity_1_to_n = syn.is_identity_1_to_n;
    entry.min_int = syn.min_int;
    entry.max_int = syn.max_int;
    result[idx] = std::move(mutable_entries[idx]);
  }
  return result;
}

std::vector<std::shared_ptr<const LSTableColumnCacheEntry>>
TlcsFile::BuildLazyColumnCacheEntries(
    const Schema& schema, uint64_t generation,
    const std::vector<slot_t>& columns) const {
  std::vector<std::shared_ptr<const LSTableColumnCacheEntry>> result(
      columns.size());
  if (columns.empty()) {
    return result;
  }
  if (schema.ColumnCount() != column_count_ ||
      !LSTableStore::SupportsSchema(schema)) {
    return result;
  }
  for (const slot_t c : columns) {
    if (c >= column_count_) {
      return result;
    }
  }
  std::vector<std::shared_ptr<const DirGroup>> groups;
  if (!LoadAllDirGroups(&groups)) {
    return result;
  }
  std::shared_ptr<const platform::MemoryMap> mmap_copy = EnsureResidentMmap();
  const uint8_t* base = mmap_copy != nullptr && mmap_copy->IsValid()
                            ? mmap_copy->Data()
                            : nullptr;
  const bool using_mmap = base != nullptr;

  std::vector<const BlockDirEntry*> blk_ptrs;
  blk_ptrs.reserve(block_count_);
  for (const auto& grp : groups) {
    for (const BlockDirEntry& src_b : grp->blocks) {
      blk_ptrs.push_back(&src_b);
    }
  }
  const size_t nblocks = blk_ptrs.size();
  const size_t meta_bytes =
      sizeof(LSTableColumnCacheEntry) + sizeof(LSTableLazyColumnState) +
      (nblocks * (sizeof(LSTableColumnBlock) + sizeof(LSTableLazyBlockMeta) +
                  (sizeof(void*) * 2)));
  std::shared_ptr<const TlcsFile> self_tlcs = shared_from_this();

  for (size_t i = 0; i < columns.size(); ++i) {
    const slot_t c = columns[i];
    auto entry = std::make_shared<LSTableColumnCacheEntry>();
    entry->generation = generation;
    entry->col_index = c;
    entry->type = col_types_[c];
    entry->is_unsigned = col_unsigned_[c] != 0;
    entry->is_block_lazy = true;
    entry->backing_mmap = using_mmap ? mmap_copy : nullptr;
    entry->memory_charge =
        std::make_shared<ProcessMemoryCharge>(MemoryPoolKind::kColumnar);
    if (!entry->memory_charge->TryAddWithReclaim(meta_bytes)) {
      continue;
    }
    auto lazy = std::make_shared<LSTableLazyColumnState>();
    lazy->backing_tlcs = self_tlcs;
    lazy->lazy_bytes_used = std::make_shared<std::atomic<size_t>>(0);
    lazy->lazy_blocks =
        std::make_unique<std::atomic<const LSTableColumnBlock*>[]>(nblocks);
    lazy->lazy_owners.resize(nblocks);
    lazy->block_meta.resize(nblocks);
    if (dict_kinds_[c] == TlcsDictKind::kCompleteNarrow) {
      entry->dictionary = dicts_[c];
    }
    entry->blocks.resize(nblocks);
    bool meta_complete = true;
    bool syn_has_null = false;
    int64_t syn_min = std::numeric_limits<int64_t>::max();
    int64_t syn_max = std::numeric_limits<int64_t>::min();
    for (size_t bi = 0; bi < nblocks; ++bi) {
      const BlockDirEntry& src_b = *blk_ptrs[bi];
      const ColumnMeta& sm = src_b.columns[c];
      LSTableColumnBlock& dc = entry->blocks[bi];
      CopyBlockColumnMeta(sm, &dc);
      if (sm.encoding == TlcsColumnEncoding::kNarrowDict &&
          dict_kinds_[c] == TlcsDictKind::kCompleteNarrow) {
        dc.dict = &entry->dictionary;
      }
      if (sm.data_size <= std::numeric_limits<uint32_t>::max() &&
          sm.aux_size <= std::numeric_limits<uint32_t>::max() &&
          sm.nulls_size <= std::numeric_limits<uint32_t>::max()) {
        LSTableLazyBlockMeta& bm = lazy->block_meta[bi];
        bm.data_offset = sm.data_offset;
        bm.aux_offset = sm.aux_offset;
        bm.nulls_offset = sm.nulls_offset;
        bm.data_size = static_cast<uint32_t>(sm.data_size);
        bm.aux_size = static_cast<uint32_t>(sm.aux_size);
        bm.nulls_size = static_cast<uint32_t>(sm.nulls_size);
        bm.row_count = src_b.row_count;
        bm.for_base = sm.for_base;
        bm.encoding = static_cast<uint8_t>(sm.encoding);
        bm.for_width = sm.for_width;
        bm.has_null = sm.has_null;
      } else {
        meta_complete = false;
      }
      syn_has_null |= dc.has_null;
      if (dc.has_min_max) {
        syn_min = std::min(syn_min, dc.min_int);
        syn_max = std::max(syn_max, dc.max_int);
      }
    }
    if (!meta_complete) {
      lazy->block_meta.clear();
    }
    entry->lazy_state = std::move(lazy);
    if (has_col_synopses_ && c < col_synopses_.size()) {
      const ColumnSynopsis& syn = col_synopses_[c];
      entry->has_null = syn.has_null;
      entry->is_monotonic = syn.is_monotonic;
      entry->is_unique_nonnull = syn.is_unique_nonnull;
      entry->is_identity_1_to_n = syn.is_identity_1_to_n;
      entry->min_int = syn.min_int;
      entry->max_int = syn.max_int;
    } else {
      entry->has_null = syn_has_null;
      entry->is_monotonic = false;
      entry->is_unique_nonnull = false;
      entry->is_identity_1_to_n = false;
      entry->min_int = syn_min;
      entry->max_int = syn_max;
    }
    entry->estimated_bytes = meta_bytes;
    result[i] = std::move(entry);
  }
  return result;
}

bool TlcsFile::DecodeSingleColumnBlockForCache(
    uint32_t block_idx, slot_t col, const LSTableColumnCacheEntry& entry,
    std::unique_ptr<LSTableLazyDecodedBlock>* out) const {
  if (out == nullptr || block_idx >= block_count_ || col >= column_count_ ||
      block_idx >= entry.blocks.size()) {
    return false;
  }
  ColumnMeta local_sm;
  const ColumnMeta* sm_ptr = nullptr;
  uint32_t n = 0;
  std::shared_ptr<const DirGroup> grp_guard;
  if (entry.lazy_state != nullptr &&
      block_idx < entry.lazy_state->block_meta.size()) {
    const LSTableLazyBlockMeta& bm = entry.lazy_state->block_meta[block_idx];
    local_sm.type = entry.type;
    local_sm.is_unsigned = entry.is_unsigned;
    local_sm.encoding = static_cast<TlcsColumnEncoding>(bm.encoding);
    local_sm.for_width = bm.for_width;
    local_sm.has_null = bm.has_null;
    local_sm.for_base = bm.for_base;
    local_sm.data_offset = bm.data_offset;
    local_sm.data_size = bm.data_size;
    local_sm.aux_offset = bm.aux_offset;
    local_sm.aux_size = bm.aux_size;
    local_sm.nulls_offset = bm.nulls_offset;
    local_sm.nulls_size = bm.nulls_size;
    sm_ptr = &local_sm;
    n = bm.row_count;
  } else {
    if (dir_group_size_ == 0) {
      return false;
    }
    const uint32_t grp_idx = block_idx / dir_group_size_;
    const uint32_t local_idx = block_idx % dir_group_size_;
    StatusOr<std::shared_ptr<const DirGroup>> grp_or = GetDirGroup(grp_idx);
    if (!grp_or.HasValue() || grp_or.Value() == nullptr ||
        local_idx >= grp_or.Value()->blocks.size()) {
      return false;
    }
    grp_guard = std::move(grp_or.Value());
    const BlockDirEntry& src_b = grp_guard->blocks[local_idx];
    if (col >= src_b.columns.size()) {
      return false;
    }
    sm_ptr = &src_b.columns[col];
    n = src_b.row_count;
  }
  const ColumnMeta& sm = *sm_ptr;
  // File-controlled metadata: every consumer below trusts these sizes
  // against the row count, so reject payloads that cannot hold n rows of the
  // declared encoding before any span, Pread or memcpy touches them.
  const auto row_bytes = static_cast<size_t>(n);
  switch (sm.encoding) {
    case TlcsColumnEncoding::kNarrowDict:
      if (sm.data_size < row_bytes) {
        return false;
      }
      break;
    case TlcsColumnEncoding::kFor:
      if (sm.for_width > 0 && sm.data_size != row_bytes * sm.for_width) {
        return false;
      }
      break;
    case TlcsColumnEncoding::kRawDouble:
      if (sm.data_size != row_bytes * sizeof(int64_t)) {
        return false;
      }
      break;
    default:
      break;
  }
  const uint8_t* base =
      (entry.backing_mmap != nullptr && entry.backing_mmap->IsValid())
          ? entry.backing_mmap->Data()
          : nullptr;
  const bool using_mmap = base != nullptr;
  alignas(8) uint8_t stack_buf[8192];
  std::vector<uint8_t> heap_tmp;
  auto read_data_span = [&](std::span<const uint8_t>* span_out) -> bool {
    if (sm.data_size == 0) {
      *span_out = {};
      return true;
    }
    if (using_mmap) {
      *span_out = std::span<const uint8_t>(base + sm.data_offset, sm.data_size);
      return true;
    }
    uint8_t* dst = stack_buf;
    if (sm.data_size > sizeof(stack_buf)) {
      heap_tmp.resize(sm.data_size);
      dst = heap_tmp.data();
    }
    if (PreadAllFd(fd_, dst, sm.data_size, sm.data_offset) !=
        Status::kSuccess) {
      return false;
    }
    *span_out = std::span<const uint8_t>(dst, sm.data_size);
    return true;
  };

  auto decoded = std::make_unique<LSTableLazyDecodedBlock>();
  if (using_mmap) {
    decoded->backing_mmap = entry.backing_mmap;
  }
  decoded->block = entry.blocks[block_idx];
  LSTableColumnBlock& dc = decoded->block;
  size_t bytes = sizeof(LSTableLazyDecodedBlock);

  if (sm.has_null) {
    if (using_mmap) {
      dc.nulls_view =
          std::span<const uint8_t>(base + sm.nulls_offset, sm.nulls_size);
    } else if (sm.nulls_size > 0) {
      decoded->owned_nulls =
          std::make_unique_for_overwrite<uint8_t[]>(sm.nulls_size);
      if (PreadAllFd(fd_, decoded->owned_nulls.get(), sm.nulls_size,
                     sm.nulls_offset) != Status::kSuccess) {
        return false;
      }
      dc.nulls_view = {decoded->owned_nulls.get(), sm.nulls_size};
      bytes += sm.nulls_size;
    }
  }

  if (sm.encoding == TlcsColumnEncoding::kNarrowDict &&
      dict_kinds_[col] == TlcsDictKind::kCompleteNarrow) {
    if (using_mmap) {
      dc.dict_codes =
          std::span<const uint8_t>(base + sm.data_offset, sm.data_size);
    } else if (sm.data_size > 0) {
      decoded->owned_dict_codes =
          std::make_unique_for_overwrite<uint8_t[]>(sm.data_size);
      if (PreadAllFd(fd_, decoded->owned_dict_codes.get(), sm.data_size,
                     sm.data_offset) != Status::kSuccess) {
        return false;
      }
      dc.dict_codes = {decoded->owned_dict_codes.get(), sm.data_size};
      bytes += sm.data_size;
    }
    dc.dict = &entry.dictionary;
    dc.wide_code_begin = 0;
    dc.wide_code_count = n;
    if (sm.type != ValueType::kVarChar && n > 0) {
      decoded->owned_ints = std::make_unique_for_overwrite<int64_t[]>(n);
      MaterializeDictInts(sm, entry.dictionary, dc.dict_codes, n,
                          decoded->owned_ints.get());
      dc.int_cells = {decoded->owned_ints.get(), n};
      bytes += static_cast<size_t>(n) * sizeof(int64_t);
    }
  } else if (sm.encoding == TlcsColumnEncoding::kNarrowDict &&
             dict_kinds_[col] == TlcsDictKind::kPartialFrozen) {
    std::span<const uint8_t> raw_codes;
    if (!read_data_span(&raw_codes)) {
      return false;
    }
    if (sm.type == ValueType::kVarChar) {
      decoded->owned_varchar_ends.resize(n);
      const auto& ro_dict = dicts_[col];
      for (uint32_t r = 0; r < n; ++r) {
        const uint8_t code = raw_codes[r];
        if (code != LSTableColumnDictionary::kNullCode &&
            code < ro_dict.entries.size()) {
          const std::string_view sv = ro_dict.entries[code];
          decoded->owned_varchar_bytes.insert(
              decoded->owned_varchar_bytes.end(),
              reinterpret_cast<const uint8_t*>(sv.data()),
              reinterpret_cast<const uint8_t*>(sv.data()) + sv.size());
        }
        decoded->owned_varchar_ends[r] =
            static_cast<uint32_t>(decoded->owned_varchar_bytes.size());
      }
      dc.varchar_bytes_view = decoded->owned_varchar_bytes;
      dc.varchar_ends_view = decoded->owned_varchar_ends;
      bytes += decoded->owned_varchar_bytes.size() +
               (decoded->owned_varchar_ends.size() * sizeof(uint32_t));
    } else if (n > 0) {
      decoded->owned_ints = std::make_unique_for_overwrite<int64_t[]>(n);
      MaterializeDictInts(sm, dicts_[col], raw_codes, n,
                          decoded->owned_ints.get());
      dc.int_cells = {decoded->owned_ints.get(), n};
      bytes += static_cast<size_t>(n) * sizeof(int64_t);
    }
  } else if (sm.encoding == TlcsColumnEncoding::kConstNull &&
             sm.type == ValueType::kVarChar) {
    // Const-null VARCHAR blocks carry nulls_view only and zero variable bytes.
  } else if (sm.encoding == TlcsColumnEncoding::kVarChar) {
    if (sm.aux_size != row_bytes * sizeof(uint32_t)) {
      return false;
    }
    const uint8_t* aux_ptr = using_mmap ? (base + sm.aux_offset) : nullptr;
    const bool mmap_aligned32 =
        aux_ptr != nullptr &&
        (reinterpret_cast<uintptr_t>(aux_ptr) & (alignof(uint32_t) - 1)) == 0;
    if (using_mmap) {
      dc.varchar_bytes_view =
          std::span<const uint8_t>(base + sm.data_offset, sm.data_size);
      if constexpr (std::endian::native == std::endian::little) {
        if (mmap_aligned32) {
          dc.varchar_ends_view = std::span<const uint32_t>(
              reinterpret_cast<const uint32_t*>(aux_ptr), n);
        } else {
          decoded->owned_varchar_ends.resize(n);
          if (n > 0) {
            std::memcpy(decoded->owned_varchar_ends.data(), aux_ptr,
                        static_cast<size_t>(n) * sizeof(uint32_t));
          }
          dc.varchar_ends_view = decoded->owned_varchar_ends;
          bytes += static_cast<size_t>(n) * sizeof(uint32_t);
        }
      } else {
        decoded->owned_varchar_ends.resize(n);
        for (uint32_t r = 0; r < n; ++r) {
          uint32_t raw_end = 0;
          std::memcpy(&raw_end, aux_ptr + (static_cast<size_t>(r) * 4), 4);
          decoded->owned_varchar_ends[r] = le32toh(raw_end);
        }
        dc.varchar_ends_view = decoded->owned_varchar_ends;
        bytes += static_cast<size_t>(n) * sizeof(uint32_t);
      }
    } else {
      decoded->owned_varchar_bytes.resize(sm.data_size);
      if (sm.data_size > 0 &&
          PreadAllFd(fd_, decoded->owned_varchar_bytes.data(), sm.data_size,
                     sm.data_offset) != Status::kSuccess) {
        return false;
      }
      if (sm.aux_size != row_bytes * sizeof(uint32_t)) {
        return false;
      }
      decoded->owned_varchar_ends.resize(n);
      if (sm.aux_size > 0 &&
          PreadAllFd(fd_, decoded->owned_varchar_ends.data(), sm.aux_size,
                     sm.aux_offset) != Status::kSuccess) {
        return false;
      }
      if constexpr (std::endian::native != std::endian::little) {
        for (uint32_t& end : decoded->owned_varchar_ends) {
          end = le32toh(end);
        }
      }
      dc.varchar_bytes_view = decoded->owned_varchar_bytes;
      dc.varchar_ends_view = decoded->owned_varchar_ends;
      bytes += decoded->owned_varchar_bytes.size() +
               (decoded->owned_varchar_ends.size() * sizeof(uint32_t));
    }
  } else if (n > 0) {
    const uint8_t* mmap_src = using_mmap ? (base + sm.data_offset) : nullptr;
    const bool mmap_aligned64 =
        mmap_src != nullptr &&
        (reinterpret_cast<uintptr_t>(mmap_src) & (alignof(int64_t) - 1)) == 0;
    if constexpr (std::endian::native == std::endian::little) {
      if (mmap_aligned64 &&
          ((sm.encoding == TlcsColumnEncoding::kFor && sm.for_width == 8) ||
           sm.encoding == TlcsColumnEncoding::kRawDouble) &&
          sm.data_size == static_cast<uint64_t>(n) * sizeof(int64_t)) {
        dc.int_cells = {reinterpret_cast<const int64_t*>(mmap_src), n};
      }
    }
    if (dc.int_cells.empty()) {
      decoded->owned_ints = std::make_unique_for_overwrite<int64_t[]>(n);
      int64_t* dst_ints = decoded->owned_ints.get();
      if (sm.encoding == TlcsColumnEncoding::kFor) {
        if (sm.for_width == 0) {
          UnpackForToInt64(sm, {}, n, dst_ints);
        } else if (using_mmap) {
          if (sm.for_width == 8) {
            Le64BufferToInt64(mmap_src, n, dst_ints);
          } else {
            UnpackForToInt64(sm,
                             std::span<const uint8_t>(mmap_src, sm.data_size),
                             n, dst_ints);
          }
        } else if (sm.for_width == 8 &&
                   std::endian::native == std::endian::little) {
          if (sm.data_size > 0 &&
              PreadAllFd(fd_, dst_ints, sm.data_size, sm.data_offset) !=
                  Status::kSuccess) {
            return false;
          }
        } else {
          std::span<const uint8_t> raw_span;
          if (!read_data_span(&raw_span)) {
            return false;
          }
          UnpackForToInt64(sm, raw_span, n, dst_ints);
        }
      } else if (sm.encoding == TlcsColumnEncoding::kRawDouble) {
        if (using_mmap && std::endian::native == std::endian::little) {
          if (sm.data_size > 0) {
            std::memcpy(dst_ints, mmap_src, sm.data_size);
          }
        } else if (!using_mmap && std::endian::native == std::endian::little) {
          if (sm.data_size > 0 &&
              PreadAllFd(fd_, dst_ints, sm.data_size, sm.data_offset) !=
                  Status::kSuccess) {
            return false;
          }
        } else {
          std::span<const uint8_t> raw_span;
          if (!read_data_span(&raw_span)) {
            return false;
          }
          Le64BufferToInt64(raw_span.data(), n, dst_ints);
        }
      } else if (sm.encoding == TlcsColumnEncoding::kConstNull) {
        std::memset(dst_ints, 0, static_cast<size_t>(n) * sizeof(int64_t));
      }
      dc.int_cells = {dst_ints, n};
      bytes += static_cast<size_t>(n) * sizeof(int64_t);
    }
  }

  decoded->charged_bytes = bytes;
  *out = std::move(decoded);
  return true;
}

std::shared_ptr<LSTableStore> TlcsFile::BuildResidentStoreInternal(
    const Schema& schema, const std::vector<slot_t>* projection,
    uint64_t max_bytes, const std::shared_ptr<const platform::MemoryMap>& mmap,
    bool* capacity_rejected) const {
  if (capacity_rejected != nullptr) {
    *capacity_rejected = false;
  }
  if (schema.ColumnCount() != column_count_ ||
      !LSTableStore::SupportsSchema(schema)) {
    return nullptr;
  }
  std::vector<std::shared_ptr<const DirGroup>> groups;
  if (!LoadAllDirGroups(&groups)) {
    return nullptr;
  }

  std::vector<uint8_t> loaded;
  std::vector<size_t> col_offsets;
  if (projection != nullptr) {
    loaded.assign(column_count_, 0);
    for (const slot_t c : *projection) {
      if (c >= column_count_) {
        return nullptr;
      }
      loaded[c] = 1;
    }
    col_offsets.assign(column_count_, SIZE_MAX);
    size_t off = 0;
    for (slot_t c = 0; c < column_count_; ++c) {
      if (loaded[c] != 0) {
        col_offsets[c] = off++;
      }
    }
  }
  auto has_col = [&](slot_t c) {
    // NOLINTNEXTLINE(clang-analyzer-cplusplus.Move)
    return loaded.empty() || (c < loaded.size() && loaded[c] != 0);
  };

  const uint8_t* base =
      mmap != nullptr && mmap->IsValid() ? mmap->Data() : nullptr;
  const bool using_mmap = base != nullptr;

  // Determine memory footprint and integer expansion needs.
  std::vector<bool> need_int_vec(column_count_, false);
  std::vector<bool> zero_copy_64(column_count_, false);
  uint64_t alloc_bytes =
      static_cast<uint64_t>(block_count_) *
      (sizeof(LSTablePageBlock) + column_count_ * sizeof(LSTableColumnBlock));
  for (slot_t c = 0; c < column_count_; ++c) {
    if (!has_col(c)) {
      continue;
    }
    const ValueType vt = col_types_[c];
    if (vt != ValueType::kVarChar) {
      // Materialize a contiguous integer_column for numeric columns even on
      // small resident tables so IntegerRuns/IntegerRunBounds and direct
      // scans see one dense vector instead of strided mmap slices.
      zero_copy_64[c] = false;
      need_int_vec[c] = true;
      alloc_bytes += total_rows_ * sizeof(int64_t);
    }
    if (dict_kinds_[c] == TlcsDictKind::kCompleteNarrow) {
      alloc_bytes += total_rows_;
    }
    if (!using_mmap) {
      for (const auto& grp : groups) {
        for (const BlockDirEntry& b : grp->blocks) {
          const ColumnMeta& m = b.columns[c];
          alloc_bytes += m.nulls_size + m.data_size + m.aux_size;
        }
      }
    }
  }
  // Evaluate the mapped FILE bytes and the expanded RESIDENT bytes against
  // the cap separately. Summing them double-counts the file-backed mapping
  // (which is demand-paged, not resident heap) against the expansion budget
  // and moves the mmap cliff to half the documented limit for files whose
  // non-raw64 integer columns need an int64 expansion vector. The total is
  // still charged to the columnar pool below; the cap here only decides
  // resident-mmap vs paged mode, matching the TlcsFile class contract.
  const uint64_t mapped_bytes = using_mmap ? file_size_ : 0;
  if (mapped_bytes > max_bytes || alloc_bytes > max_bytes) {
    if (capacity_rejected != nullptr) {
      *capacity_rejected = true;
    }
    return nullptr;
  }
  const uint64_t total_reserve = mapped_bytes + alloc_bytes;
  auto mem_charge =
      std::make_shared<ProcessMemoryCharge>(MemoryPoolKind::kColumnar);
  if (total_reserve > 0 && !mem_charge->TryAddWithReclaim(total_reserve)) {
    return nullptr;
  }

  auto store = std::make_shared<LSTableStore>();
  store->memory_charge = std::move(mem_charge);
  store->layout_generation = layout_generation_;
  store->first_pid = first_pid_;
  store->total_rows = total_rows_;
  store->loaded_columns = std::move(loaded);
  store->column_offsets = std::move(col_offsets);
  store->dictionaries.resize(column_count_);
  store->integer_columns.resize(column_count_);
  store->is_persistent_tlcs = true;
  store->backing_mmap = mmap;

  if (using_mmap && stats_file_size_ > 0 &&
      stats_file_offset_ + stats_file_size_ <= mmap->Size()) {
    store->serialized_column_stats = std::string_view(
        reinterpret_cast<const char*>(base + stats_file_offset_),
        stats_file_size_);
  }

  for (slot_t c = 0; c < column_count_; ++c) {
    if (!has_col(c)) {
      continue;
    }
    if (dict_kinds_[c] == TlcsDictKind::kCompleteNarrow) {
      store->dictionaries[c] = dicts_[c];
      store->dictionaries[c].narrow_codes.resize(total_rows_);
      if (total_rows_ <= static_cast<uint64_t>(INT64_MAX)) {
        auto& dict = store->dictionaries[c];
        const size_t entry_count = dict.value_type == ValueType::kVarChar
                                       ? dict.StringEntryCount()
                                       : (dict.value_type == ValueType::kDouble
                                              ? dict.DoubleEntryCount()
                                              : dict.IntEntryCount());
        if (entry_count <= LSTableColumnDictionary::kMaxEntries) {
          const size_t summary_bytes =
              (entry_count + 1) *
              (sizeof(int64_t) + sizeof(uint64_t) + sizeof(uint32_t));
          if (store->memory_charge->TryAddWithReclaim(summary_bytes)) {
            dict.code_counts.assign(entry_count + 1, 0);
            dict.code_first_row.assign(entry_count + 1, UINT64_MAX);
            dict.encounter_codes.reserve(entry_count + 1);
          }
        }
      }
    }
    if (need_int_vec[c]) {
      store->integer_columns[c].resize(total_rows_);
    }
  }

  store->page_ids.reserve(block_count_);
  store->blocks.resize(block_count_);
  std::vector<uint8_t> raw_tmp;
  size_t row_base = 0;
  size_t bi = 0;

  for (const auto& grp : groups) {
    for (const BlockDirEntry& src_b : grp->blocks) {
      LSTablePageBlock& dst_b = store->blocks[bi++];
      dst_b.page_id = src_b.pid;
      dst_b.row_count = src_b.row_count;
      dst_b.has_any_null = src_b.has_any_null;
      dst_b.identity_slots = true;
      store->page_ids.push_back(src_b.pid);
      if (store->column_offsets.empty()) {
        dst_b.columns.resize(column_count_);
      } else {
        size_t kept = 0;
        for (slot_t c = 0; c < column_count_; ++c) {
          if (store->HasColumn(c)) {
            ++kept;
          }
        }
        dst_b.columns.Project(store->column_offsets, kept);
      }

      for (slot_t c = 0; c < column_count_; ++c) {
        if (!store->HasColumn(c)) {
          continue;
        }
        const ColumnMeta& sm = src_b.columns[c];
        LSTableColumnBlock& dc = dst_b.columns[c];
        LSTableColumnDictionary* active_dict =
            dict_kinds_[c] == TlcsDictKind::kCompleteNarrow
                ? &store->dictionaries[c]
                : nullptr;
        int64_t* dst_ints = store->integer_columns[c].empty()
                                ? nullptr
                                : store->integer_columns[c].data();
        std::vector<int64_t>* d_counts =
            active_dict != nullptr && !active_dict->code_counts.empty()
                ? &active_dict->code_counts
                : nullptr;
        std::vector<uint64_t>* d_first =
            active_dict != nullptr && !active_dict->code_first_row.empty()
                ? &active_dict->code_first_row
                : nullptr;
        if (!DecodeColumnBlockFromFile(
                fd_, base, using_mmap, row_base, src_b, sm, dicts_[c],
                dict_kinds_[c], zero_copy_64[c], active_dict, nullptr, dst_ints,
                &dc, &raw_tmp, d_counts, d_first)) {
          return nullptr;
        }
      }
      row_base += src_b.row_count;
    }
  }
  for (slot_t c = 0; c < column_count_; ++c) {
    if (store->HasColumn(c) &&
        dict_kinds_[c] == TlcsDictKind::kCompleteNarrow &&
        !store->dictionaries[c].code_counts.empty()) {
      BuildEncounterCodesFromFirstRow(&store->dictionaries[c]);
    }
  }

  // Page index maps + table-wide integer min/max synopses.
  page_id_t max_pid = 0;
  for (const page_id_t pid : store->page_ids) {
    max_pid = std::max(max_pid, pid);
  }
  if (max_pid < kMaxFlatPageId && !store->page_ids.empty()) {
    const size_t flat_bytes =
        static_cast<size_t>(max_pid + 1) * sizeof(uint32_t);
    if (store->memory_charge->TryAddWithReclaim(flat_bytes)) {
      store->flat_page_to_block.assign(max_pid + 1,
                                       LSTableStore::kInvalidBlockIndex);
      for (size_t i = 0; i < store->page_ids.size(); ++i) {
        store->flat_page_to_block[store->page_ids[i]] =
            static_cast<uint32_t>(i);
      }
    }
  }
  if (store->flat_page_to_block.empty() && !store->page_ids.empty()) {
    const size_t map_bytes = store->page_ids.size() * 32;
    if (!store->memory_charge->TryAddWithReclaim(map_bytes)) {
      return nullptr;
    }
    store->page_to_block.reserve(store->page_ids.size());
    for (size_t i = 0; i < store->page_ids.size(); ++i) {
      store->page_to_block.emplace(store->page_ids[i], i);
    }
  }

  store->column_has_null.assign(column_count_, 0);
  store->column_is_monotonic.assign(column_count_, 0);
  store->column_is_unique_nonnull.assign(column_count_, 0);
  store->column_is_identity_1_to_n.assign(column_count_, 0);
  store->column_min_int.assign(column_count_,
                               std::numeric_limits<int64_t>::max());
  store->column_max_int.assign(column_count_,
                               std::numeric_limits<int64_t>::min());
  for (slot_t c = 0; c < column_count_; ++c) {
    if (!store->HasColumn(c)) {
      continue;
    }
    if (has_col_synopses_ && c < col_synopses_.size()) {
      const ColumnSynopsis& syn = col_synopses_[c];
      store->column_has_null[c] = syn.has_null ? 1 : 0;
      store->column_is_monotonic[c] = syn.is_monotonic ? 1 : 0;
      store->column_is_unique_nonnull[c] = syn.is_unique_nonnull ? 1 : 0;
      store->column_is_identity_1_to_n[c] = syn.is_identity_1_to_n ? 1 : 0;
      store->column_min_int[c] = syn.min_int;
      store->column_max_int[c] = syn.max_int;
      if (!syn.is_unique_nonnull && c == 0 && !syn.has_null &&
          store->total_rows > 0 && col_types_[c] == ValueType::kInt64 &&
          col_unsigned_[c] == 0 && syn.min_int <= syn.max_int) {
        const uint64_t span = static_cast<uint64_t>(syn.max_int) -
                              static_cast<uint64_t>(syn.min_int);
        if (span <= 33'554'432ULL && span + 1 >= store->total_rows) {
          std::vector<uint64_t> seen_bits(
              static_cast<size_t>((span >> 6U) + 1U), 0ULL);
          const auto base_u = static_cast<uint64_t>(syn.min_int);
          bool all_unique = true;
          for (const LSTablePageBlock& blk : store->blocks) {
            const int64_t* __restrict cells = blk.columns[c].int_cells.data();
            for (uint32_t r = 0; r < blk.row_count; ++r) {
              const uint64_t off = static_cast<uint64_t>(cells[r]) - base_u;
              if (off > span) {
                all_unique = false;
                break;
              }
              const uint64_t mask = uint64_t{1} << (off & 63U);
              uint64_t& word = seen_bits[static_cast<size_t>(off >> 6U)];
              if ((word & mask) != 0) {
                all_unique = false;
                break;
              }
              word |= mask;
            }
            if (!all_unique) {
              break;
            }
          }
          store->column_is_unique_nonnull[c] = all_unique ? 1 : 0;
        }
      }
      continue;
    }
    bool any_null = false;
    bool mono = store->total_rows > 0 &&
                (col_types_[c] == ValueType::kInt64 ||
                 col_types_[c] == ValueType::kDate) &&
                col_unsigned_[c] == 0;
    bool strict_mono = mono;
    bool identity_1_to_n = strict_mono && col_types_[c] == ValueType::kInt64;
    int64_t prev = 0;
    bool have_prev = false;
    uint64_t expected_id = 1;
    for (const LSTablePageBlock& blk : store->blocks) {
      const LSTableColumnBlock& col = blk.columns[c];
      if (col.has_null) {
        any_null = true;
        mono = false;
        strict_mono = false;
        identity_1_to_n = false;
      }
      if (col.has_min_max) {
        store->column_min_int[c] =
            std::min(store->column_min_int[c], col.min_int);
        store->column_max_int[c] =
            std::max(store->column_max_int[c], col.max_int);
      }
      if (mono) {
        if (have_prev && col.has_min_max && col.min_int < prev) {
          mono = false;
          strict_mono = false;
          identity_1_to_n = false;
        } else {
          for (uint32_t r = 0; r < blk.row_count; ++r) {
            const int64_t v = col.int_cells[r];
            if (have_prev) {
              if (v < prev) {
                mono = false;
                strict_mono = false;
                identity_1_to_n = false;
                break;
              }
              if (v == prev) {
                strict_mono = false;
                identity_1_to_n = false;
              }
            }
            if (identity_1_to_n && std::cmp_not_equal(v, expected_id)) {
              identity_1_to_n = false;
            }
            ++expected_id;
            prev = v;
            have_prev = true;
          }
        }
      }
    }
    if (!strict_mono && c == 0 && !any_null && store->total_rows > 0 &&
        col_types_[c] == ValueType::kInt64 && col_unsigned_[c] == 0 &&
        store->column_min_int[c] <= store->column_max_int[c]) {
      const uint64_t span = static_cast<uint64_t>(store->column_max_int[c]) -
                            static_cast<uint64_t>(store->column_min_int[c]);
      if (span <= 33'554'432ULL && span + 1 >= store->total_rows) {
        std::vector<uint64_t> seen_bits(static_cast<size_t>((span >> 6U) + 1U),
                                        0ULL);
        const auto base_u = static_cast<uint64_t>(store->column_min_int[c]);
        bool all_unique = true;
        for (const LSTablePageBlock& blk : store->blocks) {
          const int64_t* __restrict cells = blk.columns[c].int_cells.data();
          for (uint32_t r = 0; r < blk.row_count; ++r) {
            const uint64_t off = static_cast<uint64_t>(cells[r]) - base_u;
            if (off > span) {
              all_unique = false;
              break;
            }
            const uint64_t mask = uint64_t{1} << (off & 63U);
            uint64_t& word = seen_bits[static_cast<size_t>(off >> 6U)];
            if ((word & mask) != 0) {
              all_unique = false;
              break;
            }
            word |= mask;
          }
          if (!all_unique) {
            break;
          }
        }
        strict_mono = all_unique;
      }
    }
    store->column_has_null[c] = any_null ? 1 : 0;
    store->column_is_monotonic[c] = mono ? 1 : 0;
    store->column_is_unique_nonnull[c] = strict_mono ? 1 : 0;
    store->column_is_identity_1_to_n[c] = identity_1_to_n ? 1 : 0;
  }
  return store;
}

std::shared_ptr<const LSTableStore> TlcsFile::BuildProjectedResidentStore(
    const Schema& schema, const std::vector<slot_t>* projection,
    uint64_t effective_max_bytes, bool* capacity_rejected) const {
  if (capacity_rejected != nullptr) {
    *capacity_rejected = false;
  }
  if (projection == nullptr) {
    std::shared_ptr<const platform::MemoryMap> mmap_copy;
    {
      std::scoped_lock lk(resident_mu_);
      mmap_copy = resident_mmap_;
    }
    if (mmap_copy != nullptr && file_size_ > effective_max_bytes) {
      mmap_copy.reset();
    }
    return BuildResidentStoreInternal(schema, nullptr, effective_max_bytes,
                                      mmap_copy, capacity_rejected);
  }
  if (schema.ColumnCount() != column_count_ ||
      !LSTableStore::SupportsSchema(schema)) {
    return nullptr;
  }
  std::vector<slot_t> cols;
  cols.reserve(projection->size());
  for (const slot_t c : *projection) {
    if (c >= column_count_) {
      return nullptr;
    }
    cols.push_back(c);
  }
  std::ranges::sort(cols);
  const auto dup = std::ranges::unique(cols);
  cols.erase(dup.begin(), dup.end());
  auto layout = BuildBlockLayout(schema, layout_generation_, capacity_rejected);
  if (!layout) {
    return nullptr;
  }
  std::vector<slot_t> rejected_cols;
  auto entries = BuildColumnCacheEntries(schema, layout_generation_, cols,
                                         effective_max_bytes, &rejected_cols);
  if (!rejected_cols.empty()) {
    if (capacity_rejected != nullptr) {
      *capacity_rejected = true;
    }
    return nullptr;
  }
  std::vector<std::shared_ptr<const LSTableColumnCacheEntry>> assembled(
      column_count_);
  for (size_t i = 0; i < cols.size(); ++i) {
    if (!entries[i]) {
      return nullptr;
    }
    assembled[cols[i]] = std::move(entries[i]);
  }
  return AssembleLSTableStoreView(layout_generation_, column_count_, layout,
                                  assembled, projection);
}

StatusOr<std::shared_ptr<TlcsFile>> TlcsFile::Open(const std::string& path,
                                                   const Schema& schema) {
  return OpenInternal(path, &schema, nullptr);
}

StatusOr<std::shared_ptr<TlcsFile>> TlcsFile::OpenWithStoredSchema(
    const std::string& path, Schema* schema_out) {
  return OpenInternal(path, nullptr, schema_out);
}

StatusOr<std::shared_ptr<TlcsFile>> TlcsFile::OpenInternal(
    const std::string& path, const Schema* expected_schema,
    Schema* schema_out) {
  std::error_code ec;
  const uint64_t fsize = std::filesystem::file_size(path, ec);
  if (ec || fsize < kTlcsHeaderBytes) {
    return StatusError(StatusCode::kIOError,
                       "tlcs file missing or truncated: " + path);
  }
  const int fd = platform::OpenFile(
      path, platform::OpenFlags{.mode = platform::OpenMode::kReadOnly});
  if (fd < 0) {
    return StatusError(StatusCode::kIOError,
                       "failed to open tlcs file: " + path);
  }
  auto file = std::make_shared<TlcsFile>();
  file->path_ = path;
  file->fd_ = fd;
  file->file_size_ = fsize;

  std::array<uint8_t, kTlcsHeaderBytes> hdr{};
  RETURN_IF_FAIL(PreadAllFd(fd, hdr.data(), kTlcsHeaderBytes, 0));
  if (std::string_view(reinterpret_cast<const char*>(hdr.data()),
                       kTlcsMagic.size()) != kTlcsMagic) {
    return StatusError(StatusCode::kCorrupt, "bad tlcs magic: " + path);
  }
  size_t pos = kTlcsMagic.size();
  uint32_t version = 0;
  uint32_t flags = 0;
  uint32_t block_count = 0;
  uint32_t column_count = 0;
  uint64_t dir_offset = 0;
  uint64_t dir_length = 0;
  uint32_t dir_crc = 0;
  uint32_t hdr_crc = 0;
  if (!ReadU32(hdr, &pos, &version) || !ReadU32(hdr, &pos, &flags) ||
      !ReadU64(hdr, &pos, &file->layout_generation_) ||
      !ReadU64(hdr, &pos, &file->first_pid_) ||
      !ReadU64(hdr, &pos, &file->total_rows_) ||
      !ReadU32(hdr, &pos, &block_count) || !ReadU32(hdr, &pos, &column_count) ||
      !ReadU64(hdr, &pos, &dir_offset) || !ReadU64(hdr, &pos, &dir_length) ||
      !ReadU32(hdr, &pos, &dir_crc) || !ReadU32(hdr, &pos, &hdr_crc)) {
    return StatusError(StatusCode::kCorrupt, "truncated tlcs header");
  }
  const uint32_t computed_hdr_crc = Crc32C(hdr.data(), kTlcsHeaderBytes - 4);
  if (computed_hdr_crc != hdr_crc ||
      version != kFormatVersion ||
      (expected_schema != nullptr &&
       (!expected_schema->HasSupportedColumnCount() ||
        column_count != expected_schema->ColumnCount())) ||
      column_count == 0 || column_count > Schema::kMaxColumns ||
      dir_offset > fsize ||
      dir_length > fsize - dir_offset) {
    return StatusError(StatusCode::kCorrupt, "tlcs header validation failed");
  }
  file->format_version_ = version;

  // Every column needs two schema bytes and a nine-byte dictionary header.
  // Reject impossible counts before allocating any column-indexed vectors.
  constexpr uint64_t kMinDirectoryBytesPerColumn = 2 + 9;
  if (column_count > dir_length / kMinDirectoryBytesPerColumn) {
    return StatusError(StatusCode::kCorrupt,
                       "tlcs directory too short for column count");
  }

  file->column_count_ = static_cast<slot_t>(column_count);
  std::vector<uint8_t> dir_buf(dir_length);
  if (dir_length > 0) {
    RETURN_IF_FAIL(PreadAllFd(fd, dir_buf.data(), dir_length, dir_offset));
  }
  if (Crc32C(dir_buf.data(), dir_buf.size()) != dir_crc) {
    return StatusError(StatusCode::kCorrupt,
                       "tlcs directory checksum mismatch");
  }

  size_t dpos = 0;
  file->col_types_.resize(column_count);
  file->col_unsigned_.resize(column_count);
  std::vector<Column> stored_cols;
  if (expected_schema == nullptr) {
    stored_cols.reserve(column_count);
  }
  for (uint32_t c = 0; c < column_count; ++c) {
    uint8_t vt = 0;
    uint8_t u = 0;
    if (!ReadU8(dir_buf, &dpos, &vt) || !ReadU8(dir_buf, &dpos, &u)) {
      return StatusError(StatusCode::kCorrupt,
                         "tlcs directory schema truncated");
    }
    if (vt > static_cast<uint8_t>(ValueType::kArray) || u > 1) {
      return StatusError(StatusCode::kCorrupt, "tlcs invalid column type byte");
    }
    const auto ftype = static_cast<ValueType>(vt);
    if (expected_schema != nullptr &&
        (ftype != expected_schema->GetColumn(c).Type() ||
         (u != 0) != expected_schema->GetColumn(c).IsUnsigned())) {
      return StatusError(StatusCode::kCorrupt, "tlcs schema mismatch");
    }
    if (expected_schema == nullptr) {
      Column col("c" + std::to_string(c), ftype);
      col.SetUnsigned(u != 0);
      stored_cols.push_back(std::move(col));
    }
    file->col_types_[c] = ftype;
    file->col_unsigned_[c] = u;
  }
  if (expected_schema == nullptr && schema_out != nullptr) {
    *schema_out = Schema("tlcs", std::move(stored_cols));
  }

  file->dicts_.resize(column_count);
  file->dict_kinds_.resize(column_count, TlcsDictKind::kNone);
  for (uint32_t c = 0; c < column_count; ++c) {
    uint8_t kind = 0;
    uint8_t vt = 0;
    uint8_t bw = 0;
    uint8_t ord = 0;
    uint8_t sb = 0;
    uint32_t nent = 0;
    if (!ReadU8(dir_buf, &dpos, &kind) || !ReadU8(dir_buf, &dpos, &vt) ||
        !ReadU8(dir_buf, &dpos, &bw) || !ReadU8(dir_buf, &dpos, &ord) ||
        !ReadU8(dir_buf, &dpos, &sb) || !ReadU32(dir_buf, &dpos, &nent)) {
      return StatusError(StatusCode::kCorrupt, "tlcs dict truncated");
    }
    if (kind > static_cast<uint8_t>(TlcsDictKind::kPartialFrozen)) {
      return StatusError(StatusCode::kCorrupt, "tlcs unknown dict kind");
    }
    if (vt > static_cast<uint8_t>(ValueType::kArray) || bw > 1 || ord > 1 ||
        sb > 1) {
      return StatusError(StatusCode::kCorrupt, "tlcs invalid dictionary flags");
    }
    file->dict_kinds_[c] = static_cast<TlcsDictKind>(kind);
    LSTableColumnDictionary& d = file->dicts_[c];
    d.value_type = static_cast<ValueType>(vt);
    d.active = (file->dict_kinds_[c] == TlcsDictKind::kCompleteNarrow);
    d.bytewise_equality = (bw != 0);
    d.codes_follow_string_order = (ord != 0);
    d.all_single_byte = (sb != 0);
    if (file->dict_kinds_[c] == TlcsDictKind::kNone) {
      continue;
    }
    if (nent > LSTableColumnDictionary::kMaxEntries) {
      return StatusError(StatusCode::kCorrupt, "tlcs dict entries > 254");
    }
    if (d.value_type == ValueType::kInt64 || d.value_type == ValueType::kDate) {
      d.int_entries.resize(nent);
      for (uint32_t k = 0; k < nent; ++k) {
        if (!ReadI64(dir_buf, &dpos, &d.int_entries[k])) {
          return StatusError(StatusCode::kCorrupt, "tlcs int dict truncated");
        }
      }
    } else if (d.value_type == ValueType::kDouble) {
      d.double_entries.resize(nent);
      d.one_minus_double_entries.resize(nent);
      d.one_plus_double_entries.resize(nent);
      for (uint32_t k = 0; k < nent; ++k) {
        if (!ReadF64(dir_buf, &dpos, &d.double_entries[k])) {
          return StatusError(StatusCode::kCorrupt,
                             "tlcs double dict truncated");
        }
        d.one_minus_double_entries[k] = 1.0 - d.double_entries[k];
        d.one_plus_double_entries[k] = 1.0 + d.double_entries[k];
      }
    } else {
      d.entries.resize(nent);
      for (uint32_t k = 0; k < nent; ++k) {
        uint32_t slen = 0;
        if (!ReadU32(dir_buf, &dpos, &slen) || dpos + slen > dir_buf.size()) {
          return StatusError(StatusCode::kCorrupt, "tlcs str dict truncated");
        }
        d.entries[k].assign(
            reinterpret_cast<const char*>(dir_buf.data() + dpos), slen);
        if (slen == 1) {
          d.single_byte_chars[k] = static_cast<uint8_t>(d.entries[k][0]);
        }
        dpos += slen;
      }
    }
  }

  uint32_t dir_group_size = 0;
  uint32_t group_count = 0;
  if (!ReadU32(dir_buf, &dpos, &dir_group_size) || dir_group_size == 0 ||
      !ReadU32(dir_buf, &dpos, &group_count)) {
    return StatusError(StatusCode::kCorrupt, "tlcs group header truncated");
  }
  file->dir_groups_.resize(group_count);
  uint64_t sum_blocks = 0;
  bool mono = true;
  page_id_t prev_last_pid = 0;
  bool have_prev_group = false;
  page_id_t max_pid = 0;
  for (uint32_t g = 0; g < group_count; ++g) {
    DirGroupMeta& gm = file->dir_groups_[g];
    uint8_t mono_flag = 0;
    if (!ReadU64(dir_buf, &dpos, &gm.group_offset) ||
        !ReadU32(dir_buf, &dpos, &gm.block_count) ||
        !ReadU32(dir_buf, &dpos, &gm.pid_crc32c) ||
        !ReadU64(dir_buf, &dpos, &gm.body_size) ||
        !ReadU32(dir_buf, &dpos, &gm.body_crc32c) ||
        !ReadU64(dir_buf, &dpos, &gm.first_pid) ||
        !ReadU64(dir_buf, &dpos, &gm.last_pid) ||
        !ReadU64(dir_buf, &dpos, &gm.min_pid) ||
        !ReadU64(dir_buf, &dpos, &gm.max_pid) ||
        !ReadU8(dir_buf, &dpos, &mono_flag)) {
      return StatusError(StatusCode::kCorrupt, "tlcs group meta truncated");
    }
    gm.pids_monotonic = (mono_flag != 0);
    if (gm.block_count == 0 || gm.block_count > dir_group_size ||
        (g + 1 < group_count && gm.block_count != dir_group_size)) {
      return StatusError(StatusCode::kCorrupt,
                         "tlcs group block count invariant failed");
    }
    const uint64_t group_span =
        (static_cast<uint64_t>(gm.block_count) * 8ULL) + gm.body_size;
    if (gm.group_offset > dir_offset ||
        group_span > dir_offset - gm.group_offset) {
      return StatusError(StatusCode::kCorrupt, "tlcs dir group out of bounds");
    }
    sum_blocks += gm.block_count;
    max_pid = std::max(max_pid, gm.max_pid);
    if (!gm.pids_monotonic ||
        (have_prev_group && gm.first_pid <= prev_last_pid)) {
      mono = false;
    }
    prev_last_pid = gm.last_pid;
    have_prev_group = true;
  }
  if (sum_blocks != block_count) {
    return StatusError(StatusCode::kCorrupt, "tlcs group block sum mismatch");
  }
  file->block_count_ = block_count;
  file->dir_group_size_ = dir_group_size;
  file->groups_monotonic_pids_ = mono;

  uint32_t stats_size = 0;
  if (!ReadU32(dir_buf, &dpos, &stats_size) ||
      dpos + stats_size > dir_buf.size()) {
    return StatusError(StatusCode::kCorrupt, "tlcs stats trailer truncated");
  }
  file->stats_file_offset_ = dir_offset + dpos;
  file->stats_file_size_ = stats_size;
  file->serialized_column_stats_.assign(
      reinterpret_cast<const char*>(dir_buf.data() + dpos), stats_size);
  dpos += stats_size;
  if (version >= 3) {
    file->col_synopses_.resize(column_count);
    for (uint32_t c = 0; c < column_count; ++c) {
      uint8_t hn = 0;
      uint8_t syn_mono = 0;
      uint8_t uniq = 0;
      uint8_t id1n = 0;
      int64_t mn = 0;
      int64_t mx = 0;
      if (!ReadU8(dir_buf, &dpos, &hn) || !ReadU8(dir_buf, &dpos, &syn_mono) ||
          !ReadU8(dir_buf, &dpos, &uniq) || !ReadU8(dir_buf, &dpos, &id1n) ||
          !ReadI64(dir_buf, &dpos, &mn) || !ReadI64(dir_buf, &dpos, &mx) ||
          hn > 1 || syn_mono > 1 || uniq > 1 || id1n > 1) {
        return StatusError(StatusCode::kCorrupt,
                           "tlcs column synopsis trailer corrupt");
      }
      file->col_synopses_[c] = ColumnSynopsis{
          .has_null = hn != 0,
          .is_monotonic = syn_mono != 0,
          .is_unique_nonnull = uniq != 0,
          .is_identity_1_to_n = id1n != 0,
          .min_int = mn,
          .max_int = mx,
      };
    }
    file->has_col_synopses_ = true;
    if (dpos != dir_buf.size()) {
      return StatusError(StatusCode::kCorrupt,
                         "tlcs column synopsis trailer length mismatch");
    }
  }

  const size_t meta_bytes = dir_length + (group_count * sizeof(DirGroupMeta));
  if (meta_bytes > 0) {
    Status reserve_st;
    if (ProcessMemoryBudget::Global().TryReserveWithReclaim(
            MemoryPoolKind::kColumnar, meta_bytes, &reserve_st)) {
      file->meta_reserved_bytes_ = meta_bytes;
      file->meta_emergency_ = false;
    } else if (ProcessMemoryBudget::Global().ReserveEmergency(
                   MemoryPoolKind::kColumnar, meta_bytes)) {
      // T1835: grant via the centrally capped emergency ledger.
      file->meta_reserved_bytes_ = meta_bytes;
      file->meta_emergency_ = true;
    } else if (meta_bytes <= ProcessMemoryBudget::Global().EmergencyLimit()) {
      // Aggregate cap exhausted but the request itself is reasonable: keep
      // the open alive untracked (the ledger, not the allocation, is full).
      file->meta_reserved_bytes_ = 0;
      file->meta_emergency_ = false;
    } else {
      return reserve_st;
    }
  }
  file->cache_reclaim_bridge_ = std::make_shared<CacheReclaimBridge>();
  file->cache_reclaim_bridge_->self = file.get();
  std::weak_ptr<CacheReclaimBridge> weak = file->cache_reclaim_bridge_;
  file->cache_reclaim_hook_ = ScopedReclaimHook(
      ReclaimStage::kColumnar,
      [weak = std::move(weak)](size_t target) {
        if (auto b = weak.lock()) {
          std::unique_lock<std::mutex> lk(b->mu, std::try_to_lock);
          if (lk.owns_lock() && b->self != nullptr) {
            return b->self->EvictDecodedCacheForReclaim(target);
          }
        }
        return size_t{0};
      },
      std::make_shared<std::atomic<uint64_t>>(
          std::numeric_limits<uint64_t>::max() - 1));

  // Load full BlockDirEntry tables eagerly only when tiny (`<= min(4 MiB, 1/32
  // pool)` and `!AlwaysSpillEnabled()`); large tables keep `O(blocks / 128)`
  // DirGroupMeta headers in RAM and fault groups through `dir_cache_`.
  const auto& budget = ProcessMemoryBudget::Global();
  const size_t effective_col_max = EffectiveColumnarPoolMaxBytes();
  const size_t dir_eager_cap =
      std::min<size_t>(size_t{4} << 20, effective_col_max / 32);
  const size_t pid_map_bytes =
      (max_pid < kMaxFlatPageId && block_count > 0)
          ? (static_cast<size_t>(max_pid + 1) * sizeof(uint32_t))
          : (static_cast<size_t>(block_count) * 32ULL);
  const size_t est_eager_bytes =
      (static_cast<size_t>(block_count) *
       (sizeof(BlockDirEntry) + column_count * sizeof(ColumnMeta) +
        sizeof(page_id_t))) +
      pid_map_bytes;
  if (!budget.AlwaysSpillEnabled() && block_count > 0 &&
      est_eager_bytes <= dir_eager_cap &&
      ProcessMemoryBudget::Global().TryReserveWithReclaim(
          MemoryPoolKind::kColumnar, est_eager_bytes)) {
    // Record the charge immediately so any failure below can release it via
    // the existing accounting field instead of leaking the reservation.
    file->eager_dir_reserved_bytes_ = est_eager_bytes;
    std::vector<std::shared_ptr<const DirGroup>> loaded_groups;
    loaded_groups.reserve(group_count);
    uint64_t dir_rows = 0;
    for (uint32_t g = 0; g < group_count; ++g) {
      StatusOr<std::shared_ptr<const DirGroup>> grp_or =
          file->LoadDirGroupFromFd(g);
      if (!grp_or.HasValue()) {
        ProcessMemoryBudget::Global().Release(MemoryPoolKind::kColumnar,
                                              est_eager_bytes);
        file->eager_dir_reserved_bytes_ = 0;
        return grp_or.GetStatus();
      }
      std::shared_ptr<const DirGroup> grp = std::move(grp_or).MoveValue();
      for (const BlockDirEntry& b : grp->blocks) {
        dir_rows += b.row_count;
      }
      loaded_groups.push_back(std::move(grp));
    }
    // total_rows_ (header) and the sum of per-block row counts (directory
    // bodies) describe the same rows. A mismatch means a corrupt file; the
    // resident-store builder would otherwise size its buffers from the header
    // and write past them using the directory counts.
    if (dir_rows != file->total_rows_) {
      ProcessMemoryBudget::Global().Release(MemoryPoolKind::kColumnar,
                                            est_eager_bytes);
      file->eager_dir_reserved_bytes_ = 0;
      return StatusError(StatusCode::kCorrupt,
                         "tlcs header/directory row count mismatch");
    }
    std::scoped_lock dlk(file->dir_cache_mu_);
    file->eager_dir_reserved_bytes_ = est_eager_bytes;
    file->eager_dir_groups_ = std::move(loaded_groups);
    if (max_pid < kMaxFlatPageId) {
      file->flat_page_to_block_.assign(max_pid + 1,
                                       LSTableStore::kInvalidBlockIndex);
    }
    uint32_t bidx = 0;
    for (const auto& grp : file->eager_dir_groups_) {
      for (const page_id_t pid : grp->pids) {
        if (pid < file->flat_page_to_block_.size()) {
          auto& index = file->flat_page_to_block_[pid];
          if (index == LSTableStore::kInvalidBlockIndex) {
            index = bidx;
          }
        } else {
          file->page_to_block_.emplace(pid, bidx);
        }
        ++bidx;
      }
    }
  }

  // Decide whether to memory-map and expose a resident LSTableStore or stay
  // paged with bounded Pread I/O. Full resident LSTableStore materialization
  // stays bounded by `fsize <= mmap_cap`. Read-only `resident_mmap_` alone can
  // cover larger fixed-width tables up to `budget.Limit() / 2` (and is dropped
  // by `EvictDecodedCacheForReclaim` under memory pressure) so sparse gathers
  // read directly from the OS page cache without faulting multi-decagigabyte
  // files past `RssWarnLimit()`. Explicit `TINYLAMB_TLCS_MMAP_LIMIT_BYTES`
  // caps both.
  const uint64_t pool_half = effective_col_max / 2;
  const auto env_limit = ReadResidentMmapLimitBytes();
  const uint64_t mmap_cap = std::min(
      pool_half, env_limit.value_or(kDefaultResidentMmapLimitBytes));
  const bool env_override = env_limit.has_value();
  const uint64_t resident_cap =
      env_override ? mmap_cap
                   : std::min<uint64_t>(mmap_cap, kEagerResidentFileBytes);
  const uint64_t map_only_cap =
      env_override
          ? mmap_cap
          : (budget.Limit() == 0
                 ? kEagerResidentFileBytes
                 : std::min<uint64_t>(
                       kEagerResidentFileBytes,
                       std::max<uint64_t>(mmap_cap, budget.Limit() / 2)));
  if (expected_schema != nullptr && !budget.AlwaysSpillEnabled() &&
      mmap_cap > 0 && fsize > 0 && fsize <= map_only_cap) {
    auto mmap = std::make_shared<platform::MemoryMap>();
    if (mmap->Open(path) == 0 && mmap->IsValid()) {
      std::shared_ptr<LSTableStore> res_store;
      if (fsize <= resident_cap) {
        res_store = file->BuildResidentStoreInternal(*expected_schema, nullptr,
                                                     resident_cap, mmap);
      }
      std::scoped_lock rlk(file->resident_mu_);
      file->resident_mmap_ = std::move(mmap);
      if (res_store != nullptr) {
        file->resident_store_ = std::move(res_store);
      }
    }
  }
  return file;
}

PendingTlcsCommit::PendingTlcsCommit(PendingTlcsCommit&& other) noexcept
    : fd(std::exchange(other.fd, -1)),
      temp_path(std::move(other.temp_path)),
      schema(std::move(other.schema)),
      layout_generation(other.layout_generation),
      first_pid(other.first_pid),
      total_rows(other.total_rows),
      block_count(other.block_count),
      max_chain_lsn(other.max_chain_lsn),
      directory_offset(other.directory_offset),
      old_pids(std::move(other.old_pids)),
      block_row_counts(std::move(other.block_row_counts)),
      dir_groups(std::move(other.dir_groups)),
      dicts(std::move(other.dicts)),
      dict_kinds(std::move(other.dict_kinds)),
      serialized_column_stats(std::move(other.serialized_column_stats)),
      col_synopses(std::move(other.col_synopses)) {}

PendingTlcsCommit& PendingTlcsCommit::operator=(
    PendingTlcsCommit&& other) noexcept {
  if (this != &other) {
    Abort();
    fd = std::exchange(other.fd, -1);
    temp_path = std::move(other.temp_path);
    schema = std::move(other.schema);
    layout_generation = other.layout_generation;
    first_pid = other.first_pid;
    total_rows = other.total_rows;
    block_count = other.block_count;
    max_chain_lsn = other.max_chain_lsn;
    directory_offset = other.directory_offset;
    old_pids = std::move(other.old_pids);
    block_row_counts = std::move(other.block_row_counts);
    dir_groups = std::move(other.dir_groups);
    dicts = std::move(other.dicts);
    dict_kinds = std::move(other.dict_kinds);
    serialized_column_stats = std::move(other.serialized_column_stats);
    col_synopses = std::move(other.col_synopses);
  }
  return *this;
}

PendingTlcsCommit::~PendingTlcsCommit() { Abort(); }

void PendingTlcsCommit::Abort() noexcept {
  if (fd >= 0) {
    platform::CloseFile(fd);
    fd = -1;
  }
  if (!temp_path.empty()) {
    std::error_code ec;
    std::filesystem::remove(temp_path, ec);
    temp_path.clear();
  }
}

StatusOr<PendingTlcsCommit> WriteTlcsTempFile(Transaction& txn,
                                              const Table& table,
                                              const TlcsFile* existing_tlcs,
                                              const std::string& temp_path) {
  if (txn.IsFailed()) {
    return Status::kConflicts;
  }
  const Schema& schema = table.GetSchema();
  if (!schema.HasSupportedColumnCount()) {
    return StatusError(StatusCode::kTooBigData, "too many columns in TLCS file");
  }
  if (!LSTableStore::SupportsSchema(schema)) {
    return Status::kNotImplemented;
  }
  ASSIGN_OR_RETURN(TableLease, lease, table.AcquireLease(txn.ID()));
  (void)lease;
  txn.SweepCommittedVersionChains();

  const int fd = platform::OpenFile(temp_path,
                                    platform::OpenFlags{
                                        .mode = platform::OpenMode::kReadWrite,
                                        .create = true,
                                        .truncate = true,
                                    },
                                    0644);
  if (fd < 0) {
    return StatusError(StatusCode::kIOError,
                       "failed to create temp tlcs file: " + temp_path);
  }

  PendingTlcsCommit pending;
  pending.fd = fd;
  pending.temp_path = temp_path;
  pending.schema = schema;
  pending.layout_generation = table.LayoutGeneration();
  pending.first_pid = table.FirstPageId();

  // Write placeholder header; finalized after stub pids + directory are known.
  std::array<uint8_t, kTlcsHeaderBytes> zero_hdr{};
  if (const Status s = WriteAllFd(fd, zero_hdr.data(), zero_hdr.size());
      s != Status::kSuccess) {
    return s;
  }

  BufferedSeqWriter writer(fd, kTlcsHeaderBytes);
  std::vector<OnlineColumnDict> online_dicts(schema.ColumnCount());
  for (slot_t c = 0; c < schema.ColumnCount(); ++c) {
    online_dicts[c].Init(schema.GetColumn(c).Type(),
                         schema.GetColumn(c).IsUnsigned());
  }

  BlockStaging staging;
  staging.Init(schema);
  std::vector<StreamingColSynopsis> streaming_synopses(schema.ColumnCount());
  for (slot_t c = 0; c < schema.ColumnCount(); ++c) {
    streaming_synopses[c].Init(schema.GetColumn(c).Type(),
                               schema.GetColumn(c).IsUnsigned());
  }
  StreamingStatsCollector stats_collector(schema);
  std::vector<uint8_t> scratch_codes;
  std::vector<uint8_t> scratch_for;
  Row scratch_row;
  scratch_row.values_.resize(schema.ColumnCount());

  std::vector<TlcsFile::BlockDirEntry> dir_batch;
  dir_batch.reserve(TlcsFile::kDirGroupSize);
  std::string pid_placeholder_scratch;
  std::string dir_body_scratch;

  auto flush_dir_batch = [&]() -> Status {
    if (dir_batch.empty()) {
      return Status::kSuccess;
    }
    RETURN_IF_FAIL(writer.AlignTo8());
    TlcsFile::DirGroupMeta gm;
    gm.group_offset = writer.Offset();
    gm.block_count = static_cast<uint32_t>(dir_batch.size());
    const size_t pid_bytes_len = static_cast<size_t>(gm.block_count) * 8ULL;
    pid_placeholder_scratch.assign(pid_bytes_len, '\0');
    RETURN_IF_FAIL(
        writer.Append(pid_placeholder_scratch.data(), pid_bytes_len));
    SerializeDirGroupBody(dir_batch, schema.ColumnCount(), &dir_body_scratch);
    gm.body_size = dir_body_scratch.size();
    gm.body_crc32c = Crc32C(dir_body_scratch.data(), dir_body_scratch.size());
    RETURN_IF_FAIL(
        writer.Append(dir_body_scratch.data(), dir_body_scratch.size()));
    pending.dir_groups.push_back(gm);
    dir_batch.clear();
    return Status::kSuccess;
  };

  auto flush_block = [&]() -> Status {
    if (staging.row_count == 0) {
      return Status::kSuccess;
    }
    TlcsFile::BlockDirEntry dir;
    RETURN_IF_FAIL(FlushStagedBlock(staging, online_dicts, writer,
                                    scratch_codes, scratch_for, &dir));
    pending.block_row_counts.push_back(dir.row_count);
    ++pending.block_count;
    pending.total_rows += staging.row_count;
    dir_batch.push_back(std::move(dir));
    staging.ResetBlock();
    if (dir_batch.size() >= TlcsFile::kDirGroupSize) {
      RETURN_IF_FAIL(flush_dir_batch());
    }
    return Status::kSuccess;
  };

  std::unordered_set<page_id_t> visited;
  page_id_t pid = table.FirstPageId();

  while (pid != 0) {
    if (!visited.insert(pid).second) {
      return StatusError(StatusCode::kCorrupt,
                         "cycle detected while compacting table to tlcs");
    }
    ASSIGN_OR_RETURN(PageRef, page,
                     txn.GetPageManager()->GetPage(pid, /*shared=*/true));
    if (page.IsNull() || page->Type() != PageType::kRowPage) {
      return StatusError(StatusCode::kCorrupt,
                         "invalid page type during tlcs compaction");
    }
    pending.old_pids.push_back(pid);
    pending.max_chain_lsn = std::max(pending.max_chain_lsn, page->PageLSN());
    const RowPage& rp = page->body.row_page;
    const page_id_t next_pid = rp.next_page_id_;

    if (rp.IsColumnarStub()) {
      if (existing_tlcs == nullptr) {
        return StatusError(
            StatusCode::kNotExists,
            "cannot compact columnar stub page without backing tlcs");
      }
      page.PageUnlock();
      // The block's rows are needed only as transient inputs to the staging
      // columns — decode straight to Row instead of the encoded-image cache's
      // per-row Serialize + TryDeserialize round-trip.
      RETURN_IF_FAIL(existing_tlcs->ForEachBlockRowByPid(
          pid, schema, [&](slot_t /*slot*/, const Row& row) -> Status {
            stats_collector.AddRow(row);
            RETURN_IF_FAIL(staging.AppendRow(row, &streaming_synopses));
            if (staging.row_count == RowPage::kMaxColumnarRows) {
              RETURN_IF_FAIL(flush_block());
            }
            return Status::kSuccess;
          }));
    } else {
      const bool fast_direct =
          !txn.HasPageVersionChains(pid) && txn.WriteSetEmpty();
      const slot_t row_max = rp.RowMax();
      for (slot_t slot = 0; slot < row_max; ++slot) {
        if (!rp.IsSlotOccupied(slot)) {
          continue;
        }
        if (fast_direct) {
          const std::string_view raw = rp.GetRow(slot);
          if (raw.empty()) {
            continue;
          }
          RETURN_IF_FAIL(
              staging.AppendRawRow(raw, &stats_collector, &streaming_synopses));
        } else {
          ASSIGN_OR_RETURN(std::string_view, raw, page->Read(txn, slot));
          if (raw.empty()) {
            continue;
          }
          ASSIGN_OR_RETURN(
              size_t, consumed,
              scratch_row.TryDeserialize(
                  raw.data(),  // NOLINT(bugprone-suspicious-stringview-data-usage)
                  schema));
          if (consumed != raw.size()) {
            return StatusError(StatusCode::kCorrupt,
                               "tlcs row image has trailing bytes");
          }
          stats_collector.AddRow(scratch_row);
          RETURN_IF_FAIL(staging.AppendRow(scratch_row, &streaming_synopses));
        }
        if (staging.row_count == RowPage::kMaxColumnarRows) {
          RETURN_IF_FAIL(flush_block());
        }
      }
    }
    pid = next_pid;
  }

  RETURN_IF_FAIL(flush_block());
  RETURN_IF_FAIL(flush_dir_batch());
  RETURN_IF_FAIL(writer.AlignTo8());
  RETURN_IF_FAIL(writer.Flush());
  pending.directory_offset = writer.Offset();

  pending.dicts.resize(schema.ColumnCount());
  pending.dict_kinds.resize(schema.ColumnCount(), TlcsDictKind::kNone);
  for (slot_t c = 0; c < schema.ColumnCount(); ++c) {
    online_dicts[c].Finalize(&pending.dicts[c], &pending.dict_kinds[c]);
  }
  pending.serialized_column_stats = stats_collector.FinishEncoded();
  pending.col_synopses.reserve(schema.ColumnCount());
  for (const StreamingColSynopsis& ss : streaming_synopses) {
    pending.col_synopses.push_back(ss.Finish(pending.total_rows));
  }
  return pending;
}

Status FinalizeTlcsTempFile(PendingTlcsCommit& pending,
                            const std::vector<page_id_t>& stub_pids,
                            uint64_t final_layout_generation) {
  if (pending.fd < 0) {
    return Status::kInvalidArgument;
  }
  if (pending.block_count > 0 && stub_pids.size() != pending.block_count) {
    return StatusError(StatusCode::kInvalidArgument,
                       "stub pid count mismatch during tlcs finalize");
  }
  size_t pid_cursor = 0;
  std::string pid_buf;
  for (TlcsFile::DirGroupMeta& gm : pending.dir_groups) {
    if (pid_cursor + gm.block_count > stub_pids.size()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "stub pid count smaller than dir group blocks");
    }
    pid_buf.clear();
    pid_buf.reserve(static_cast<size_t>(gm.block_count) * sizeof(uint64_t));
    gm.first_pid = stub_pids[pid_cursor];
    gm.last_pid = stub_pids[pid_cursor + gm.block_count - 1];
    gm.min_pid = gm.first_pid;
    gm.max_pid = gm.first_pid;
    gm.pids_monotonic = true;
    for (uint32_t i = 0; i < gm.block_count; ++i) {
      const page_id_t pid = stub_pids[pid_cursor + i];
      AppendU64(pid_buf, pid);
      gm.min_pid = std::min(gm.min_pid, pid);
      gm.max_pid = std::max(gm.max_pid, pid);
      if (i > 0 && pid <= stub_pids[pid_cursor + i - 1]) {
        gm.pids_monotonic = false;
      }
    }
    gm.pid_crc32c = Crc32C(pid_buf.data(), pid_buf.size());
    RETURN_IF_FAIL(PwriteAllFd(pending.fd, pid_buf.data(), pid_buf.size(),
                               gm.group_offset));
    pid_cursor += gm.block_count;
  }

  uint64_t stats_rel = 0;
  const std::string dir_bytes = EncodeMetaDirectoryBytes(
      pending.schema, pending.dicts, pending.dict_kinds,
      TlcsFile::kDirGroupSize, pending.dir_groups,
      pending.serialized_column_stats, pending.col_synopses, &stats_rel);
  (void)stats_rel;
  RETURN_IF_FAIL(PwriteAllFd(pending.fd, dir_bytes.data(), dir_bytes.size(),
                             pending.directory_offset));

  const uint32_t dir_crc = Crc32C(dir_bytes.data(), dir_bytes.size());
  const std::string hdr_bytes = EncodeHeaderBytes(
      final_layout_generation, pending.first_pid, pending.total_rows,
      pending.block_count, pending.schema.ColumnCount(),
      pending.directory_offset, dir_bytes.size(), dir_crc);
  RETURN_IF_FAIL(
      PwriteAllFd(pending.fd, hdr_bytes.data(), hdr_bytes.size(), 0));

  if (platform::DataSync(pending.fd) != 0) {
    return StatusError(StatusCode::kIOError,
                       "tlcs fsync failed: " + pending.temp_path);
  }
  platform::CloseFile(pending.fd);
  pending.fd = -1;
  return Status::kSuccess;
}

Status RenameFinalizedTlcsTemp(PendingTlcsCommit& pending,
                               const std::string& final_path) {
  std::error_code ec;
  std::filesystem::rename(pending.temp_path, final_path, ec);
  if (ec) {
    return StatusError(StatusCode::kIOError,
                       "failed to rename tlcs file: " + ec.message());
  }
  // The file contents were fsynced before the rename, but the directory
  // entry publishing final_path is not durable until the parent directory
  // is fsynced too; without this a crash can lose the rename and leave the
  // stub pages pointing at a silently missing column file (.tmp orphan).
  const std::filesystem::path final(final_path);
  const std::filesystem::path dir = final.has_parent_path()
                                        ? final.parent_path()
                                        : std::filesystem::path(".");
  if (platform::SyncDirectory(dir) != 0) {
    const int saved = errno;
    return StatusError(StatusCode::kIOError,
                       "failed to fsync tlcs directory: " + dir.string() +
                           ": " + std::strerror(saved));
  }
  pending.temp_path.clear();
  return Status::kSuccess;
}

Status FinalizeTlcsFile(PendingTlcsCommit& pending,
                        const std::vector<page_id_t>& stub_pids,
                        uint64_t final_layout_generation,
                        const std::string& final_path) {
  RETURN_IF_FAIL(
      FinalizeTlcsTempFile(pending, stub_pids, final_layout_generation));
  return RenameFinalizedTlcsTemp(pending, final_path);
}

Status WriteTableToTlcsReadOnly(Transaction& txn, const Table& table,
                                const TlcsFile* existing_tlcs,
                                const std::string& final_path) {
  const std::string temp_path = final_path + ".tmp";
  ASSIGN_OR_RETURN(PendingTlcsCommit, pending,
                   WriteTlcsTempFile(txn, table, existing_tlcs, temp_path));
  // The block ids recorded in the directory must be unique and must not alias
  // a live page of an unrelated table: a duplicated id makes the pid -> block
  // index collapse two blocks onto one of them. This helper never touches
  // `PageManager`, so the only page ids it may reference are the table's own
  // chain pages. `WriteTlcsTempFile` emits at most one block per chain page,
  // hence `block_count <= old_pids.size()` holds; refuse to write a file
  // instead of minting `old_pids.back() + i` ids, whose space above
  // `old_pids.back()` is not guaranteed to be free.
  if (pending.block_count > pending.old_pids.size()) {
    return StatusError(StatusCode::kCorrupt,
                       "tlcs read-only write produced more blocks than chain "
                       "pages");
  }
  std::vector<page_id_t> pseudo_pids(
      pending.old_pids.begin(),
      pending.old_pids.begin() + static_cast<ptrdiff_t>(pending.block_count));
  return FinalizeTlcsFile(pending, pseudo_pids, table.LayoutGeneration(),
                          final_path);
}

}  // namespace tinylamb
