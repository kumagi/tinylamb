/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/spill_file.hpp"

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <ios>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "common/memory_budget.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"

namespace tinylamb {
namespace {
std::string RandomSuffix() {
  static thread_local std::mt19937_64 rng{std::random_device{}()};
  std::uniform_int_distribution<uint64_t> dist;
  std::ostringstream oss;
  oss << std::hex << dist(rng);
  return oss.str();
}
}  // namespace

std::filesystem::path SpillFile::TempDirectory() {
  std::filesystem::path dir = ProcessMemoryBudget::ResolveTempDirectory();
  std::error_code ec;
  std::filesystem::create_directories(dir, ec);
  if (ec) {
    // The caller will fail OpenForWrite with an unclear path; surface the
    // real cause (permissions / not-a-directory) here instead.
    return {};
  }
  return dir;
}

SpillFile::SpillFile() {
  path_ = TempDirectory() / ("tinylamb-spill-" + RandomSuffix() + ".bin");
}

SpillFile::SpillFile(SpillFile&& other) noexcept
    : path_(std::move(other.path_)),
      stream_(std::move(other.stream_)),
      count_(other.count_),
      file_bytes_(other.file_bytes_),
      writing_(other.writing_),
      finished_(other.finished_),
      has_positions_(other.has_positions_) {
  // Clear the moved-from state so this object exclusively owns the file:
  // otherwise destroying `other` would delete the file still in use here.
  other.count_ = 0;
  other.file_bytes_ = 0;
  other.path_.clear();
  other.writing_ = false;
  other.finished_ = false;
  other.has_positions_ = false;
}

SpillFile& SpillFile::operator=(SpillFile&& other) noexcept {
  if (this != &other) {
    if (stream_.is_open()) {
      stream_.close();
    }
    // Same non-throwing cleanup as ~SpillFile: the error_code overload of
    // remove() treats "not found" as a normal result, so the throwing
    // exists() probe is unnecessary — and it could not throw inside this
    // noexcept function anyway without calling std::terminate.
    if (!path_.empty()) {
      std::error_code ec;
      std::filesystem::remove(path_, ec);
    }
    path_ = std::move(other.path_);
    stream_ = std::move(other.stream_);
    count_ = other.count_;
    file_bytes_ = other.file_bytes_;
    writing_ = other.writing_;
    finished_ = other.finished_;
    has_positions_ = other.has_positions_;
    other.count_ = 0;
    other.file_bytes_ = 0;
    other.path_.clear();
    other.writing_ = false;
    other.finished_ = false;
    other.has_positions_ = false;
  }
  return *this;
}

SpillFile::~SpillFile() {
  if (stream_.is_open()) {
    stream_.close();
  }
  if (!path_.empty()) {
    std::error_code ec;
    std::filesystem::remove(path_, ec);
  }
}

namespace {
// Issue 357/5033: a failed direct write()/open() on the spill stream leaves
// the true cause in errno, which used to be dropped -- every disk-full
// (ENOSPC) event surfaced as the bare "spill write failed: <path>" the TPC-DS
// benchmark logged.  Map quota/space errors to kNoSpace with an actionable
// hint and embed errno in the remaining IOError text (same classification as
// common/spill/spill_store.cpp's ErrnoToSpillStatus).
Status SpillWriteStatus(const char* context,
                        const std::filesystem::path& path) {
  const int err = errno;
  std::string msg(context);
  msg += ": ";
  msg += path.string();
  if (err == ENOSPC || err == EDQUOT) {
    msg +=
        " (no space left on the spill filesystem; free space or point "
        "TINYLAMB_TEMP at a filesystem with headroom)";
    return StatusError(StatusCode::kNoSpace, std::move(msg));
  }
  if (err != 0) {
    msg += " (errno=" + std::to_string(err) + ": " + std::strerror(err) + ")";
  }
  return StatusError(StatusCode::kIOError, std::move(msg));
}
}  // namespace

Status SpillFile::OpenForWrite() {
  if (writing_) {
    return Status::kSuccess;
  }
  stream_.open(path_, std::ios::binary | std::ios::out | std::ios::trunc);
  if (!stream_) {
    return SpillWriteStatus("failed to create spill file", path_);
  }
  // Placeholder for count; rewritten in FinishWriting.
  const uint64_t zero = 0;
  stream_.write(reinterpret_cast<const char*>(&zero), sizeof(zero));
  if (!stream_) {
    return SpillWriteStatus("spill write failed", path_);
  }
  writing_ = true;
  return Status::kSuccess;
}

Status SpillFile::Append(const Row& row) {
  if (finished_) {
    return StatusError(StatusCode::kInvalidArgument,
                       "Append after FinishWriting");
  }
  if (count_ > 0 && has_positions_) {
    return StatusError(StatusCode::kInvalidArgument,
                       "SpillFile position mode mismatch");
  }
  has_positions_ = false;
  RETURN_IF_FAIL(OpenForWrite());
  Encoder enc(stream_);
  enc << row;
  if (!stream_) {
    return SpillWriteStatus("spill write failed", path_);
  }
  ++count_;
  return Status::kSuccess;
}

Status SpillFile::Append(const Row& row, const RowPosition& position) {
  if (finished_) {
    return StatusError(StatusCode::kInvalidArgument,
                       "Append after FinishWriting");
  }
  if (count_ > 0 && !has_positions_) {
    return StatusError(StatusCode::kInvalidArgument,
                       "SpillFile position mode mismatch");
  }
  has_positions_ = true;
  RETURN_IF_FAIL(OpenForWrite());
  Encoder enc(stream_);
  enc << row << position;
  if (!stream_) {
    return SpillWriteStatus("spill write failed", path_);
  }
  ++count_;
  return Status::kSuccess;
}

Status SpillFile::FinishWriting() {
  if (!writing_ || finished_) {
    finished_ = true;
    return Status::kSuccess;
  }
  stream_.flush();
  if (!stream_) {
    // ostream failures set badbit/failbit; errno is not necessarily meaningful
    // here and may hold an unrelated stale value.
    return StatusError(StatusCode::kIOError,
                       "failed to flush spill file: " + path_.string());
  }
  stream_.seekp(0);
  if (!stream_) {
    return StatusError(StatusCode::kIOError,
                       "failed to seek spill file: " + path_.string());
  }
  stream_.write(reinterpret_cast<const char*>(&count_), sizeof(count_));
  stream_.flush();
  if (!stream_) {
    return StatusError(StatusCode::kIOError,
                       "failed to finalize spill file: " + path_.string());
  }
  stream_.close();
  if (!stream_) {
    return StatusError(StatusCode::kIOError,
                       "failed to close spill file: " + path_.string());
  }
  writing_ = false;
  finished_ = true;
  if (count_ > 0 && !path_.empty()) {
    std::error_code ec;
    const uint64_t sz = std::filesystem::file_size(path_, ec);
    if (ec) {
      return StatusError(StatusCode::kIOError,
                         "failed to size spill file: " + path_.string() +
                             " (" + ec.message() + ")");
    }
    file_bytes_ = sz;
  }
  return Status::kSuccess;
}

StatusOr<SpillFile::RowReader> SpillFile::OpenRowReader() {
  if (!finished_) {
    RETURN_IF_FAIL(FinishWriting());
  }
  if (has_positions_) {
    return StatusError(StatusCode::kInvalidArgument,
                       "OpenRowReader on positioned spill");
  }
  if (count_ == 0) {
    return RowReader{};
  }
  RowReader reader;
  reader.path_ = path_;
  const auto bytes = static_cast<size_t>(
      std::clamp<uint64_t>(file_bytes_, 4096, RowReader::kReadBufferBytes));
  reader.io_buffer_.assign(bytes, '\0');
  reader.stream_ = std::make_unique<std::ifstream>();
  reader.stream_->rdbuf()->pubsetbuf(
      reader.io_buffer_.data(),
      static_cast<std::streamsize>(bytes));
  reader.stream_->open(path_, std::ios::binary | std::ios::in);
  if (!*reader.stream_) {
    return StatusError(StatusCode::kIOError,
                       "failed to open spill file: " + path_.string());
  }
  uint64_t stored = 0;
  reader.stream_->read(reinterpret_cast<char*>(&stored), sizeof(stored));
  if (reader.stream_->gcount() !=
      static_cast<std::streamsize>(sizeof(stored))) {
    return StatusError(StatusCode::kCorrupt,
                       "truncated spill file header: " + path_.string());
  }
  if (stored != count_) {
    return StatusError(StatusCode::kCorrupt,
                       "spill file header mismatch: " + path_.string());
  }
  reader.remaining_ = stored;
  return reader;
}

Status SpillFile::RowReader::Next(Row* out, bool* has_row) {
  if (out == nullptr || has_row == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "spill row reader requires row and presence outputs");
  }
  if (!stream_ || remaining_ == 0) {
    *has_row = false;
    stream_.reset();
    io_buffer_.clear();
    io_buffer_.shrink_to_fit();
    return Status::kSuccess;
  }
  Decoder dec(*stream_);
  dec >> *out;
  if (!*stream_) {
    return StatusError(StatusCode::kCorrupt,
                       "truncated spill file: " + path_.string());
  }
  --remaining_;
  if (remaining_ == 0) {
    stream_.reset();
    io_buffer_.clear();
    io_buffer_.shrink_to_fit();
  }
  *has_row = true;
  return Status::kSuccess;
}

Status SpillFile::EnsureReader() {
  if (!finished_) {
    RETURN_IF_FAIL(FinishWriting());
  }
  if (stream_.is_open()) {
    stream_.close();
  }
  stream_.open(path_, std::ios::binary | std::ios::in);
  if (!stream_) {
    return SpillWriteStatus("failed to open spill file", path_);
  }
  return Status::kSuccess;
}

StatusOr<uint64_t> SpillFile::ReadStoredCount() {
  uint64_t stored = 0;
  stream_.read(reinterpret_cast<char*>(&stored), sizeof(stored));
  if (stream_.gcount() != static_cast<std::streamsize>(sizeof(stored))) {
    return StatusError(StatusCode::kCorrupt,
                       "truncated spill file header: " + path_.string());
  }
  // The header was rewritten by this process; a mismatch means the file is
  // corrupt. It also bounds the loop below by a value we trust.
  if (stored != count_) {
    return StatusError(StatusCode::kCorrupt,
                       "spill file header mismatch: " + path_.string());
  }
  return stored;
}

StatusOr<std::vector<Row>> SpillFile::ReadAllRows() {
  // An empty spill has no layout yet (nothing fixed has_positions_), so it
  // reads as empty under either reader, like ForEachRow/ForEachPositioned.
  if (count_ == 0) {
    return std::vector<Row>{};
  }
  if (has_positions_) {
    return StatusError(StatusCode::kInvalidArgument,
                       "ReadAllRows on positioned spill");
  }
  RETURN_IF_FAIL(EnsureReader());
  ASSIGN_OR_RETURN(uint64_t, stored, ReadStoredCount());
  Decoder dec(stream_);
  std::vector<Row> rows;
  rows.reserve(static_cast<size_t>(stored));
  for (uint64_t i = 0; i < stored; ++i) {
    Row row;
    dec >> row;
    if (!stream_) {
      return StatusError(StatusCode::kCorrupt,
                         "truncated spill file: " + path_.string());
    }
    rows.push_back(std::move(row));
  }
  return rows;
}

StatusOr<std::vector<std::pair<Row, RowPosition>>>
SpillFile::ReadAllPositioned() {
  if (count_ == 0) {
    return std::vector<std::pair<Row, RowPosition>>{};
  }
  if (!has_positions_) {
    return StatusError(StatusCode::kInvalidArgument,
                       "ReadAllPositioned on row-only spill");
  }
  RETURN_IF_FAIL(EnsureReader());
  ASSIGN_OR_RETURN(uint64_t, stored, ReadStoredCount());
  Decoder dec(stream_);
  std::vector<std::pair<Row, RowPosition>> rows;
  rows.reserve(static_cast<size_t>(stored));
  for (uint64_t i = 0; i < stored; ++i) {
    Row row;
    RowPosition position;
    dec >> row >> position;
    if (!stream_) {
      return StatusError(StatusCode::kCorrupt,
                         "truncated spill file: " + path_.string());
    }
    rows.emplace_back(std::move(row), position);
  }
  return rows;
}

}  // namespace tinylamb
