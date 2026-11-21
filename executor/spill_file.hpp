/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_SPILL_FILE_HPP
#define TINYLAMB_SPILL_FILE_HPP

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/decoder.hpp"
#include "common/status_or.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"

namespace tinylamb {

// Temporary on-disk row store for operator spill. Files live under
// ProcessMemoryBudget::ResolveTempDirectory() (or TINYLAMB_TEMP) and are
// deleted on destruction.
class SpillFile {
 public:
  // Incremental streaming row cursor that owns an independent buffered reader
  // and releases its file handle + buffer once exhausted or destroyed.
  class RowReader {
   public:
    RowReader() = default;
    RowReader(const RowReader&) = delete;
    RowReader& operator=(const RowReader&) = delete;
    RowReader(RowReader&&) = default;
    RowReader& operator=(RowReader&&) = default;
    ~RowReader() = default;

    [[nodiscard]] bool IsOpen() const noexcept { return stream_ != nullptr; }
    [[nodiscard]] uint64_t Remaining() const noexcept { return remaining_; }
    [[nodiscard]] Status Next(Row* out, bool* has_row);

   private:
    friend class SpillFile;
    static constexpr size_t kReadBufferBytes = size_t{64} << 10;
    std::filesystem::path path_;
    std::vector<char> io_buffer_;
    std::unique_ptr<std::ifstream> stream_;
    uint64_t remaining_{0};
  };

  SpillFile();
  SpillFile(const SpillFile&) = delete;
  SpillFile& operator=(const SpillFile&) = delete;
  SpillFile(SpillFile&& other) noexcept;
  SpillFile& operator=(SpillFile&& other) noexcept;
  ~SpillFile();

  Status Append(const Row& row);
  Status Append(const Row& row, const RowPosition& position);
  Status FinishWriting();

  [[nodiscard]] uint64_t Count() const { return count_; }
  [[nodiscard]] bool Empty() const { return count_ == 0; }
  [[nodiscard]] uint64_t FileBytes() const noexcept { return file_bytes_; }
  [[nodiscard]] const std::filesystem::path& Path() const { return path_; }

  [[nodiscard]] StatusOr<RowReader> OpenRowReader();

  // Sequential read of all rows (positions ignored if written without them).
  StatusOr<std::vector<Row>> ReadAllRows();
  StatusOr<std::vector<std::pair<Row, RowPosition>>> ReadAllPositioned();

  // Stream rows without buffering the full file. A callback returning Status
  // can stop iteration by returning a failure.
  template <typename Fn>
  Status ForEachRow(Fn&& fn) {
    if (count_ == 0) {
      return Status::kSuccess;
    }
    if (has_positions_) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ForEachRow on positioned spill");
    }
    RETURN_IF_FAIL(EnsureReader());
    ASSIGN_OR_RETURN(uint64_t, stored, ReadStoredCount());
    Decoder dec(stream_);
    for (uint64_t i = 0; i < stored; ++i) {
      Row row;
      dec >> row;
      if (!stream_) {
        return StatusError(
            StatusCode::kCorrupt,
            "truncated spill file: " + path_.string());  // NOLINT(readability)
      }
      if constexpr (std::is_same_v<
                        std::remove_cvref_t<std::invoke_result_t<Fn&, Row&>>,
                        Status>) {
        RETURN_IF_FAIL(fn(row));
      } else {
        fn(row);
      }
    }
    return Status::kSuccess;
  }

  template <typename Fn>
  Status ForEachPositioned(Fn&& fn) {
    if (count_ == 0) {
      return Status::kSuccess;
    }
    if (!has_positions_) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ForEachPositioned on row-only spill");
    }
    RETURN_IF_FAIL(EnsureReader());
    ASSIGN_OR_RETURN(uint64_t, stored, ReadStoredCount());
    Decoder dec(stream_);
    for (uint64_t i = 0; i < stored; ++i) {
      Row row;
      RowPosition position;
      dec >> row >> position;
      if (!stream_) {
        return StatusError(
            StatusCode::kCorrupt,
            "truncated spill file: " + path_.string());  // NOLINT(readability)
      }
      fn(row, position);
    }
    return Status::kSuccess;
  }

  static std::filesystem::path TempDirectory();

 private:
  Status OpenForWrite();
  Status EnsureReader();
  // Reads and validates the row-count header at the start of the file.
  [[nodiscard]] StatusOr<uint64_t> ReadStoredCount();

  std::filesystem::path path_;
  std::fstream stream_;
  uint64_t count_{0};
  uint64_t file_bytes_{0};
  bool writing_{false};
  bool finished_{false};
  bool has_positions_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_SPILL_FILE_HPP
