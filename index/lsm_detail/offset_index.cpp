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

#include "offset_index.hpp"

#include <endian.h>
#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <ostream>
#include <utility>
#include <vector>

#include "common/log_message.hpp"

namespace tinylamb {

OffsetIndex::OffsetIndex(std::filesystem::path path) : path_(std::move(path)) {
  int fd = ::open(path_.c_str(), O_RDONLY, 0666);
  size_t remaining = std::filesystem::file_size(path_);
  offsets_.resize(remaining / sizeof(uint64_t));
  size_t completed = 0;
  while (0 < remaining) {
    ssize_t read_size = ::read(fd, offsets_.data() + completed, remaining);
    if (read_size <= 0) {
      LOG(FATAL) << strerror(errno);
    }
    remaining -= read_size;
    completed += read_size;
  }
  for (uint64_t& offset : offsets_) {
    offset = be64toh(offset);
  }
  ::close(fd);
}

size_t OffsetIndex::Size() const { return offsets_.size(); }

const uint64_t& OffsetIndex::operator[](size_t i) const { return offsets_[i]; }

OffsetIndex::Iterator::Iterator(const OffsetIndex* idx, size_t pos)
    : pos_(pos), offset_(idx) {}

uint64_t OffsetIndex::Iterator::operator*() const { return (*offset_)[pos_]; }

OffsetIndex::Iterator& OffsetIndex::Iterator::operator++() {
  if (offset_ == nullptr) {
    return *this;
  }
  if (offset_ == nullptr || ++pos_ == offset_->Size()) {
    offset_ = nullptr;
  }
  return *this;
}

bool OffsetIndex::Iterator::operator==(const OffsetIndex::Iterator& rhs) const {
  if (offset_ == nullptr && rhs.offset_ == nullptr) {
    return true;
  }
  return &offset_ == &rhs.offset_ && pos_ == rhs.pos_;
}

bool OffsetIndex::Iterator::operator!=(const OffsetIndex::Iterator& rhs) const {
  return !operator==(rhs);
}

std::ostream& operator<<(std::ostream& o, const OffsetIndex::Iterator& it) {
  if (it.IsInvalid()) {
    o << "(empty)";
  }
  o << it.pos_ << "/" << it.offset_->Size();
  return o;
}

}  // namespace tinylamb