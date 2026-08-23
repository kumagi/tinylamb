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

#include "sorted_run.hpp"
#include <endian.h>

#ifdef TARGET_OS_LINUX
#include <bits/types/struct_iovec.h>
#elif defined(__APPLE__)
#endif
#include <fcntl.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <map>
#include <memory>
#include <ostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include "common/constants.hpp"
#include "common/debug.hpp"
#include "common/log_message.hpp"
#include "common/status_or.hpp"
#include "common/vm_cache.hpp"
#include "index/lsm_detail/blob_file.hpp"

namespace tinylamb {
namespace {
int OpenFile(const std::filesystem::path& path) {
  return ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0666);
}

// Returns -1 if lhs is definitely bigger than `rhs`.
// Returns 1 if rhs is definitely bigger than `lhs` whatever suffix follows.
// Returns 0 if lhs
int MemoryCompare(std::string_view lhs, std::string_view rhs) {
  if (lhs == rhs) {
    return 0;
  }
  if (lhs < rhs) {
    return 1;
  }
  return -1;
}
}  // namespace

/* A comparison between SortedRun::Entry vs general length binary.
 * The logical payload of SortedRun::Entry key looks like:
 * [ key_head_ | key_.inline_ ]
 * When the key length is longer, key is logically:
 * [ *blob[key_.reference_.offset_] ]
 * But physically
 * [ key_head_ | ... ]
 * We should use the key_head_ as much as possible for speed.
 *
 * `rhs`: comparison target.
 * `blob`: backend storage for longer key.
 */
int SortedRun::Entry::Compare(std::string_view rhs,
                              const BlobFile& blob) const {
  uint32_t head = 0;
  const size_t head_bytes = std::min(4UL, rhs.size());
  if (0 < head_bytes) {
    memcpy(&head, rhs.data(), head_bytes);
  }
  head = be32toh(head);
  if (head < key_head_) {
    return -1;
  }
  if (head > key_head_) {
    return 1;
  }
  if (rhs.size() <= 4 && length_ == rhs.size()) {
    return 0;
  }
  if (length_ != rhs.length() && (length_ <= 4 || rhs.length() <= 4)) {
    // The shorter of two keys sharing a prefix sorts first.
    return length_ < rhs.length() ? 1 : -1;
  }

  assert(key_head_ == head);
  assert(4 < rhs.length());
  assert(4 < length_);
  if (length_ <= kIndirectThreshold) {
    uint64_t inlined = 0;
    memcpy(&inlined, rhs.data() + 4, std::min(rhs.length() - 4, 8UL));
    inlined = be64toh(inlined);
    // Do not subtract: unsigned wrap flips the order for large suffixes.
    if (inlined > key_.inline_) {
      return 1;  // entry < rhs
    }
    if (inlined < key_.inline_) {
      return -1;  // entry > rhs
    }
    return static_cast<int>(rhs.length()) - static_cast<int>(length_);
  }
  // This is slow-path.
  std::string body = blob.ReadAt(key_.reference_, length_);
  return MemoryCompare(body, rhs);
}

// *this < rhs => positive value
// *this = rhs => 0
// *this > rhs => negative value
int SortedRun::Entry::Compare(const SortedRun::Entry& rhs,
                              const BlobFile& blob) const {
  if (rhs.key_head_ < key_head_) {
    return -1;
  }
  if (rhs.key_head_ > key_head_) {
    return 1;
  }
  if (length_ <= 4 || rhs.length_ <= 4) {
    // The shorter of two keys sharing a prefix sorts first; compare in
    // signed width so the difference cannot wrap around.
    if (length_ != rhs.length_) {
      return length_ < rhs.length_ ? 1 : -1;
    }
    return 0;
  }
  assert(key_head_ == rhs.key_head_);
  assert(4 < rhs.length_);
  assert(4 < length_);
  if (length_ <= kIndirectThreshold && rhs.length_ <= kIndirectThreshold) {
    if (rhs.key_.inline_ > key_.inline_) {
      return 1;  // *this < rhs
    }
    if (rhs.key_.inline_ < key_.inline_) {
      return -1;  // *this > rhs
    }
    return static_cast<int>(rhs.length_) - static_cast<int>(length_);
  }

  assert(kIndirectThreshold < length_ || kIndirectThreshold < rhs.length_);

  // TODO(kumagi): below code is slow, make it faster.
  std::string left = BuildKey(blob);
  std::string right = rhs.BuildKey(blob);
  if (left < right) {
    return 1;
  }
  if (right < left) {
    return -1;
  }
  return 0;
}

SortedRun::Entry::Entry(std::string_view key, const LSMValue& value,
                        BlobFile& blob_) {
  length_ = key.length();
  key_head_ = 0;
  const size_t head_bytes = std::min(key.size(), sizeof(uint32_t));
  if (0 < head_bytes) {
    memcpy(&key_head_, key.data(), head_bytes);
  }
  key_head_ = be32toh(key_head_);
  if (kIndirectThreshold < key.length()) {
    key_.reference_ = blob_.Append(key);
  } else if (sizeof(key_head_) < key.length()) {
    // Zero the inline tail so Compare() does not read uninitialized bytes
    // when the key is shorter than 12 bytes. For keys of at most 4 bytes the
    // pre-initialized key_{0} stays untouched; only key_head_ matters.
    key_.inline_ = 0;
    std::memcpy(&key_.inline_, key.data() + 4, key.length() - 4);
    key_.inline_ = be64toh(key_.inline_);
  }
  if (value.is_delete) {
    value_.offset_ = kDeletedValue;
    value_length_ = 0;
  } else {
    value_length_ = value.payload.length();
    if (value_length_ <= sizeof(uint64_t)) {
      ::memcpy(value_.inline_.data(), value.payload.data(), value_length_);
    } else {
      value_.offset_ = blob_.Append(value.payload);
    }
  }
}

std::string SortedRun::Entry::BuildKey(const BlobFile& blob) const {
  if (length_ <= kIndirectThreshold) {
    std::string ret(length_, '\0');
    uint32_t head = htobe32(key_head_);
    memcpy(ret.data(), &head, std::min(4U, length_));
    if (4 < length_) {
      uint64_t inlined = htobe64(key_.inline_);
      memcpy(ret.data() + 4, &inlined, std::min(8U, length_ - 4));
    }
    return ret;
  }
  return blob.ReadAt(key_.reference_, length_);
}

std::string SortedRun::Entry::BuildValue(const BlobFile& blob) const {
  assert(!IsDeleted());
  if (value_length_ <= sizeof(uint64_t)) {
    return {value_.inline_.data(), value_length_};
  }
  return blob.ReadAt(value_.offset_, value_length_);
}

SortedRun::SortedRun(const std::filesystem::path& file) {
  int fd = ::open(file.c_str(), O_RDONLY);
  if (fd < 0) {
    // A constructor cannot report Status; a broken run must not survive as a
    // half-initialized object whose later GetEntry() dereferences null.
    throw std::runtime_error("Failed to open file: " + file.string());
  }
  auto read_full = [&](void* dst, size_t bytes) -> bool {
    size_t done = 0;
    while (done < bytes) {
      const ssize_t got = ::read(fd, static_cast<char*>(dst) + done,
                                 bytes - done);
      if (got <= 0) {
        return false;
      }
      done += static_cast<size_t>(got);
    }
    return true;
  };
  size_t len = 0;
  size_t offset = 0;

  if (!read_full(&len, sizeof(len)) || len > (1LLU << 20)) {
    ::close(fd);
    throw std::runtime_error("Corrupted run header: " + file.string());
  }
  offset += sizeof(len);
  min_key_.resize(len);
  if (!read_full(min_key_.data(), len)) {
    ::close(fd);
    throw std::runtime_error("Short read: " + file.string());
  }
  offset += len;

  len = 0;
  if (!read_full(&len, sizeof(len)) || len > (1LLU << 20)) {
    ::close(fd);
    throw std::runtime_error("Corrupted run header: " + file.string());
  }
  offset += sizeof(len);
  max_key_.resize(len);
  if (!read_full(max_key_.data(), len)) {
    ::close(fd);
    throw std::runtime_error("Short read: " + file.string());
  }
  offset += len;

  if (!read_full(&length_, sizeof(length_)) ||
      !read_full(&generation_, sizeof(generation_))) {
    ::close(fd);
    throw std::runtime_error("Short read: " + file.string());
  }
  offset += sizeof(length_) + sizeof(generation_);
  index_ = std::make_unique<VMCache<Entry> >(fd, 4096 * 4096, offset);
}

Status SortedRun::Construct(const std::filesystem::path& file,
                            const std::map<std::string, LSMValue>& tree,
                            BlobFile& blob, size_t generation) {
  if (tree.empty()) {
    return FlushInternal(file, "", "", {}, generation);
  }
  std::string_view min_key(tree.begin()->first);
  std::string_view max_key(tree.rbegin()->first);

  std::vector<Entry> entries;
  entries.reserve(tree.size());
  for (const auto& t : tree) {
    entries.emplace_back(t.first, t.second, blob);
  }
  return FlushInternal(file, min_key, max_key, entries, generation);
}

Status SortedRun::FlushInternal(const std::filesystem::path& path,
                                std::string_view min, std::string_view max,
                                const std::vector<Entry>& index,
                                size_t generation) {
  const int fd = OpenFile(path);
  if (fd < 0) {
    LOG(ERROR) << "failed to open run file: " << ::strerror(errno) << " for "
               << path;
    return Status::kCorrupt;
  }
  auto fail = [&](const char* what) {
    LOG(ERROR) << "failed to flush run " << path << ": " << what << ": "
               << ::strerror(errno);
    ::close(fd);
    std::error_code ec;
    std::filesystem::remove(path, ec);
    return Status::kCorrupt;
  };
  size_t length = index.size();
  std::array<iovec, 7> vec{};

  size_t min_key_length = min.length();
  vec[0].iov_len = sizeof(min.size());
  vec[0].iov_base = &min_key_length;

  vec[1].iov_len = min_key_length;
  vec[1].iov_base = const_cast<char*>(min.data());

  size_t max_key_length = max.length();
  vec[2].iov_len = sizeof(max.size());
  vec[2].iov_base = &max_key_length;

  vec[3].iov_len = max_key_length;
  vec[3].iov_base = const_cast<char*>(max.data());

  vec[4].iov_len = sizeof(length_);
  vec[4].iov_base = &length;

  vec[5].iov_len = sizeof(generation);
  vec[5].iov_base = &generation;

  vec[6].iov_len = length * sizeof(Entry);
  vec[6].iov_base = const_cast<Entry*>(index.data());

  size_t total_length = 0;
  for (const auto& v : vec) {
    total_length += v.iov_len;
  }
  // writev() may accept fewer bytes than requested; keep writing from where
  // the previous call stopped or the run file ends up truncated.
  size_t written_total = 0;
  size_t vec_index = 0;
  while (vec_index < vec.size()) {
    const ssize_t written =
        ::writev(fd, vec.data() + vec_index, vec.size() - vec_index);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      return fail("writev");
    }
    auto done = static_cast<size_t>(written);
    written_total += done;
    while (vec_index < vec.size() && vec[vec_index].iov_len <= done) {
      done -= vec[vec_index].iov_len;
      ++vec_index;
    }
    if (done != 0 && vec_index < vec.size()) {
      vec[vec_index].iov_base =
          static_cast<char*>(vec[vec_index].iov_base) + done;
      vec[vec_index].iov_len -= done;
    }
  }
  if (written_total != total_length) {
    // Short write: the on-disk index would disagree with `length`; the file
    // is garbage, so fail() removes it.
    errno = ENOSPC;
    return fail("short write");
  }
  if (::fsync(fd) != 0) {
    return fail("fsync");
  }
  ::close(fd);
  return Status::kSuccess;
}

auto SortedRun::Find(std::string_view key, const BlobFile& blob) const
    -> StatusOr<std::string> {
  if (length_ == 0 || key < min_key_ || max_key_ < key) {
    return Status::kNotExists;
  }

  // int64 arithmetic: length_ is size_t and a plain int would overflow past
  // 2^31 entries.
  int64_t left = 0, right = static_cast<int64_t>(length_);
  while (1 < std::abs(right - left)) {
    const int64_t mid = left + ((right - left) / 2);
    const Entry mid_entry = GetEntry(static_cast<size_t>(mid));
    if (0 <= mid_entry.Compare(key, blob)) {
      left = mid;
    } else {
      right = mid;
    }
  }
  Entry left_entry = GetEntry(static_cast<size_t>(left));
  int result = left_entry.Compare(key, blob);
  if (result == 0) {
    if (left_entry.IsDeleted()) {
      return Status::kDeleted;
    }
    return left_entry.BuildValue(blob);
  }
  return Status::kNotExists;
}

bool SortedRun::Iterator::operator==(const SortedRun::Iterator& rhs) const {
  return offset_ == rhs.offset_ && parent_ == rhs.parent_;
}

bool SortedRun::Iterator::operator!=(const SortedRun::Iterator& rhs) const {
  return !operator==(rhs);
}

SortedRun::Entry SortedRun::GetEntry(size_t offset) const {
  SortedRun::Entry entry;
  index_->Read(&entry, offset, 1);
  return entry;
}

std::ostream& operator<<(std::ostream& o, const SortedRun::Iterator& it) {
  if (it.IsDeleted()) {
    o << it.Key() << "=>(deleted)";
  } else {
    o << it.Key() << "=>" << it.Value();
  }
  return o;
}

namespace {
std::string HeadString(uint32_t in) {
  std::string out(4, '\0');
  const uint32_t raw = be32toh(in);
  memcpy(out.data(), &raw, sizeof(raw));
  return out;
}
}  // namespace

std::ostream& operator<<(std::ostream& o, const SortedRun& s) {
  std::string min_key = OmittedString(s.min_key_, 20);
  std::string max_key = OmittedString(s.max_key_, 20);
  o << "{\n"
    << "  key range: [" << min_key << " ~ " << max_key << "]\n"
    << "  Entries: " << s.length_ << "\n  [";
  for (size_t i = 0; i < s.length_; ++i) {
    if (0 < i) {
      o << ", ";
    }
    SortedRun::Entry entry = s.GetEntry(i);
    if (entry.IsDeleted()) {
      o << "(" << HeadString(entry.key_head_) << ")";
    } else {
      o << HeadString(entry.key_head_);
    }
  }
  o << "]\n  Generation: " << s.generation_ << "\n"
    << "}\n";
  return o;
}
}  // namespace tinylamb
