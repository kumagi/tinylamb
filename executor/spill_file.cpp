/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/spill_file.hpp"

#include <cstdint>
#include <cstdlib>
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
  if (const char* env = std::getenv("TINYLAMB_TEMP");
      env != nullptr && env[0] != '\0') {
    std::filesystem::path dir(env);
    std::filesystem::create_directories(dir);
    return dir;
  }
  return std::filesystem::temp_directory_path();
}

SpillFile::SpillFile() {
  path_ = TempDirectory() / ("tinylamb-spill-" + RandomSuffix() + ".bin");
}

SpillFile::SpillFile(SpillFile&& other) noexcept
    : path_(std::move(other.path_)),
      stream_(std::move(other.stream_)),
      count_(other.count_),
      writing_(other.writing_),
      finished_(other.finished_),
      has_positions_(other.has_positions_) {
  // Clear the moved-from state so this object exclusively owns the file:
  // otherwise destroying `other` would delete the file still in use here.
  other.count_ = 0;
  other.path_.clear();
  other.writing_ = false;
  other.finished_ = false;
  other.has_positions_ = false;
}

SpillFile& SpillFile::operator=(SpillFile&& other) noexcept {
  if (this != &other) {
    if (!path_.empty() && std::filesystem::exists(path_)) {
      std::error_code ec;
      std::filesystem::remove(path_, ec);
    }
    path_ = std::move(other.path_);
    stream_ = std::move(other.stream_);
    count_ = other.count_;
    writing_ = other.writing_;
    finished_ = other.finished_;
    has_positions_ = other.has_positions_;
    other.count_ = 0;
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

void SpillFile::OpenForWrite() {
  if (writing_) {
    return;
  }
  stream_.open(path_, std::ios::binary | std::ios::out | std::ios::trunc);
  if (!stream_) {
    throw std::runtime_error("failed to create spill file: " + path_.string());
  }
  // Placeholder for count; rewritten in FinishWriting.
  const uint64_t zero = 0;
  stream_.write(reinterpret_cast<const char*>(&zero), sizeof(zero));
  if (!stream_) {
    throw std::runtime_error("spill write failed: " + path_.string());
  }
  writing_ = true;
}

void SpillFile::Append(const Row& row) {
  if (finished_) {
    throw std::runtime_error("Append after FinishWriting");
  }
  if (count_ > 0 && has_positions_) {
    throw std::runtime_error("SpillFile position mode mismatch");
  }
  has_positions_ = false;
  OpenForWrite();
  Encoder enc(stream_);
  enc << row;
  if (!stream_) {
    throw std::runtime_error("spill write failed: " + path_.string());
  }
  ++count_;
}

void SpillFile::Append(const Row& row, const RowPosition& position) {
  if (finished_) {
    throw std::runtime_error("Append after FinishWriting");
  }
  if (count_ > 0 && !has_positions_) {
    throw std::runtime_error("SpillFile position mode mismatch");
  }
  has_positions_ = true;
  OpenForWrite();
  Encoder enc(stream_);
  enc << row << position;
  if (!stream_) {
    throw std::runtime_error("spill write failed: " + path_.string());
  }
  ++count_;
}

void SpillFile::FinishWriting() {
  if (!writing_ || finished_) {
    finished_ = true;
    return;
  }
  stream_.seekp(0);
  stream_.write(reinterpret_cast<const char*>(&count_), sizeof(count_));
  stream_.flush();
  if (!stream_) {
    // Leave the stream open so the destructor still cleans up the file; the
    // failed query must not pretend the spill succeeded.
    throw std::runtime_error("failed to finalize spill file: " +
                             path_.string());
  }
  stream_.close();
  writing_ = false;
  finished_ = true;
}

void SpillFile::EnsureReader() {
  if (!finished_) {
    FinishWriting();
  }
  if (stream_.is_open()) {
    stream_.close();
  }
  stream_.open(path_, std::ios::binary | std::ios::in);
  if (!stream_) {
    throw std::runtime_error("failed to open spill file: " + path_.string());
  }
}

uint64_t SpillFile::ReadStoredCount() {
  uint64_t stored = 0;
  stream_.read(reinterpret_cast<char*>(&stored), sizeof(stored));
  if (stream_.gcount() != static_cast<std::streamsize>(sizeof(stored))) {
    throw std::runtime_error("truncated spill file header: " + path_.string());
  }
  // The header was rewritten by this process; a mismatch means the file is
  // corrupt. It also bounds the loop below by a value we trust.
  if (stored != count_) {
    throw std::runtime_error("spill file header mismatch: " + path_.string());
  }
  return stored;
}

std::vector<Row> SpillFile::ReadAllRows() {
  if (count_ == 0) {
    return {};
  }
  if (has_positions_) {
    throw std::runtime_error("ReadAllRows on positioned spill");
  }
  EnsureReader();
  const uint64_t stored = ReadStoredCount();
  Decoder dec(stream_);
  std::vector<Row> rows;
  rows.reserve(static_cast<size_t>(stored));
  for (uint64_t i = 0; i < stored; ++i) {
    Row row;
    dec >> row;
    if (!stream_) {
      throw std::runtime_error("truncated spill file: " + path_.string());
    }
    rows.push_back(std::move(row));
  }
  return rows;
}

std::vector<std::pair<Row, RowPosition>> SpillFile::ReadAllPositioned() {
  if (count_ == 0) {
    return {};
  }
  if (!has_positions_) {
    throw std::runtime_error("ReadAllPositioned on row-only spill");
  }
  EnsureReader();
  const uint64_t stored = ReadStoredCount();
  Decoder dec(stream_);
  std::vector<std::pair<Row, RowPosition>> rows;
  rows.reserve(static_cast<size_t>(stored));
  for (uint64_t i = 0; i < stored; ++i) {
    Row row;
    RowPosition position;
    dec >> row >> position;
    if (!stream_) {
      throw std::runtime_error("truncated spill file: " + path_.string());
    }
    rows.emplace_back(std::move(row), position);
  }
  return rows;
}

}  // namespace tinylamb
