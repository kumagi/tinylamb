/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_SPILL_FILE_HPP
#define TINYLAMB_SPILL_FILE_HPP

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "common/decoder.hpp"
#include "common/status_or.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"

namespace tinylamb {

// Temporary on-disk row store for operator spill. Files live under
// TINYLAMB_TEMP (or /tmp) and are deleted on destruction.
class SpillFile {
 public:
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
  [[nodiscard]] const std::filesystem::path& Path() const { return path_; }

  // Sequential read of all rows (positions ignored if written without them).
  StatusOr<std::vector<Row>> ReadAllRows();
  StatusOr<std::vector<std::pair<Row, RowPosition>>> ReadAllPositioned();

  // Stream rows without buffering the full file in memory.
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
      fn(row);
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
  bool writing_{false};
  bool finished_{false};
  bool has_positions_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_SPILL_FILE_HPP
