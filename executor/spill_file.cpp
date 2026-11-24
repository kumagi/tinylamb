/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/spill_file.hpp"

#include <algorithm>
#include <array>
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
#include "common/platform/process.hpp"
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
    // An empty directory signals that temporary storage is unavailable.
    return {};
  }
  return dir;
}

SpillFile::SpillFile() {
  const std::filesystem::path dir = TempDirectory();
  if (!dir.empty()) {
    path_ = dir / ("tinylamb-spill-" + RandomSuffix() + ".bin");
  }
}

SpillFile::SpillFile(SpillFile&& other) noexcept
    : path_(std::move(other.path_)),
      stream_(std::move(other.stream_)),
      count_(other.count_),
      file_bytes_(other.file_bytes_),
      finish_error_(std::move(other.finish_error_)),
      writing_(other.writing_),
      finished_(other.finished_),
      has_positions_(other.has_positions_) {
  // Clear the moved-from state so this object exclusively owns the file:
  // otherwise destroying `other` would delete the file still in use here.
  other.count_ = 0;
  other.file_bytes_ = 0;
  other.finish_error_ = Status::kSuccess;
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
    finish_error_ = other.finish_error_;
    writing_ = other.writing_;
    finished_ = other.finished_;
    has_positions_ = other.has_positions_;
    other.count_ = 0;
    other.file_bytes_ = 0;
    other.finish_error_ = Status::kSuccess;
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
    msg += " (errno=" + std::to_string(err) + ": " +
           ::tinylamb::platform::StrError(err) + ")";
  }
  return StatusError(StatusCode::kIOError, std::move(msg));
}
}  // namespace

Status SpillFile::OpenForWrite() {
  if (finish_error_ != Status::kSuccess) {
    return finish_error_;
  }
  if (writing_) {
    return Status::kSuccess;
  }
  if (path_.empty()) {
    return StatusError(StatusCode::kIOError,
                       "temporary spill directory is unavailable");
  }
  stream_.open(path_, std::ios::binary | std::ios::out | std::ios::trunc);
  if (!stream_) {
    return SpillWriteStatus("failed to create spill file", path_);
  }
  // Placeholder for count; rewritten in FinishWriting.
  // Copy into a char buffer rather than reinterpret_casting the integer:
  // byte-wise memcpy avoids aliasing/alignment assumptions about the stream.
  const uint64_t zero = 0;
  std::array<char, sizeof(zero)> zero_bytes{};
  std::memcpy(zero_bytes.data(), &zero, sizeof(zero));
  stream_.write(zero_bytes.data(),
                static_cast<std::streamsize>(zero_bytes.size()));
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
  // Validate before changing the file so a rejected row can be skipped and
  // the rows already in the file remain readable.
  RETURN_IF_FAIL(row.CheckSerializable());
  RETURN_IF_FAIL(OpenForWrite());
  Encoder enc(stream_);
  enc << row;
  RETURN_IF_FAIL(enc.GetStatus());
  if (!stream_) {
    return SpillWriteStatus("spill write failed", path_);
  }
  // Commit the layout only after the row actually reached the file. A failed
  // OpenForWrite/encode must leave has_positions_ alone, otherwise a later
  // append of the other kind could proceed with count_ == 0 while the stream
  // already carries the first kind's format (header/body mismatch).
  has_positions_ = false;
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
  RETURN_IF_FAIL(row.CheckSerializable());
  RETURN_IF_FAIL(OpenForWrite());
  Encoder enc(stream_);
  enc << row << position;
  Status encode_status = enc.GetStatus();
  if (encode_status != Status::kSuccess) {
    finished_ = true;
    return encode_status;
  }
  if (!stream_) {
    finished_ = true;
    return SpillWriteStatus("spill write failed", path_);
  }
  // Same as the row-only Append: publish the layout only on a successful
  // write so an earlier failed attempt cannot pin a format that was never
  // written.
  has_positions_ = true;
  ++count_;
  return Status::kSuccess;
}

Status SpillFile::FinishWriting() {
  if (finish_error_ != Status::kSuccess) {
    return finish_error_;
  }
  if (finished_) {
    return Status::kSuccess;
  }
  if (!writing_) {
    // No Append succeeded yet. Still write the count header (zero) so the
    // "FinishWriting always produces a header" contract holds: setting
    // finished_ on an unopened file left a headerless file that external
    // 0-row validation could not parse.
    RETURN_IF_FAIL(OpenForWrite());
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
  std::array<char, sizeof(count_)> count_bytes{};
  std::memcpy(count_bytes.data(), &count_, sizeof(count_));
  stream_.write(count_bytes.data(),
                static_cast<std::streamsize>(count_bytes.size()));
  stream_.flush();
  if (!stream_) {
    return StatusError(StatusCode::kIOError,
                       "failed to finalize spill file: " + path_.string());
  }
  stream_.close();
  if (!stream_) {
    finish_error_ = StatusError(
        StatusCode::kIOError, "failed to close spill file: " + path_.string());
    writing_ = false;
    return finish_error_;
  }
  writing_ = false;
  finished_ = true;
  // Record the real on-disk size for every finished file, including an empty
  // one (its 8-byte count header), so FileBytes() never disagrees with the
  // file that OpenRowReader will actually read.
  if (!path_.empty()) {
    std::error_code ec;
    const uint64_t sz = std::filesystem::file_size(path_, ec);
    if (ec) {
      // The file itself is complete and closed; only the size probe failed.
      // Returning an error here would be self-defeating: finished_ is already
      // set, so a caller's retry would short-circuit to kSuccess and keep a
      // stale size.  Clear file_bytes_ so OpenRowReader takes its existing
      // real-on-disk-size fallback instead.
      file_bytes_ = 0;
    } else {
      file_bytes_ = sz;
    }
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
  // Size the stream buffer to the actual file (capped at kReadBufferBytes).
  // A fixed 4096-byte minimum wasted ~4 KiB for every concurrently read small
  // spill. A stale/zero file_bytes_ falls back to the real on-disk size so a
  // reader never shrinks its buffer below the file it is about to read.
  uint64_t bytes_source = file_bytes_;
  if (bytes_source == 0) {
    std::error_code ec;
    bytes_source = std::filesystem::file_size(path_, ec);
    if (ec) {
      bytes_source = 1;  // pubsetbuf still requires a non-empty buffer
    }
  }
  const size_t bytes = static_cast<size_t>(
      std::clamp<uint64_t>(bytes_source, 1, RowReader::kReadBufferBytes));
  reader.io_buffer_.assign(bytes, '\0');
  reader.stream_ = std::make_unique<std::ifstream>();
  reader.stream_->rdbuf()->pubsetbuf(reader.io_buffer_.data(),
                                     static_cast<std::streamsize>(bytes));
  reader.stream_->open(path_, std::ios::binary | std::ios::in);
  if (!*reader.stream_) {
    return StatusError(StatusCode::kIOError,
                       "failed to open spill file: " + path_.string());
  }
  uint64_t stored = 0;
  std::array<char, sizeof(stored)> stored_bytes{};
  reader.stream_->read(stored_bytes.data(),
                       static_cast<std::streamsize>(stored_bytes.size()));
  if (reader.stream_->gcount() !=
      static_cast<std::streamsize>(stored_bytes.size())) {
    return StatusError(StatusCode::kCorrupt,
                       "truncated spill file header: " + path_.string());
  }
  std::memcpy(&stored, stored_bytes.data(), sizeof(stored));
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
  // Decode into a temporary first: decoding straight into *out would leave a
  // partially decoded (wrong) row behind on failure, which a caller reusing
  // that Row could then observe. Keep the count diagnostic so a truncated body
  // is distinguishable from a header/body count mismatch.
  Decoder dec(*stream_);
  Row decoded;
  dec >> decoded;
  if (!*stream_) {
    return StatusError(StatusCode::kCorrupt,
                       "truncated spill file: " + path_.string() + " (" +
                           std::to_string(remaining_) +
                           " row(s) still expected by the count header)");
  }
  *out = std::move(decoded);
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
  std::array<char, sizeof(stored)> stored_bytes{};
  stream_.read(stored_bytes.data(),
               static_cast<std::streamsize>(stored_bytes.size()));
  if (stream_.gcount() != static_cast<std::streamsize>(stored_bytes.size())) {
    return StatusError(StatusCode::kCorrupt,
                       "truncated spill file header: " + path_.string());
  }
  std::memcpy(&stored, stored_bytes.data(), sizeof(stored));
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
