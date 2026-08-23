/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_SPILL_FILE_HPP
#define TINYLAMB_SPILL_FILE_HPP

#include <cstdint>
#include <fstream>
#include <filesystem>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "common/decoder.hpp"
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

  void Append(const Row& row);
  void Append(const Row& row, const RowPosition& position);
  void FinishWriting();

  [[nodiscard]] uint64_t Count() const { return count_; }
  [[nodiscard]] bool Empty() const { return count_ == 0; }
  [[nodiscard]] const std::filesystem::path& Path() const { return path_; }

  // Sequential read of all rows (positions ignored if written without them).
  std::vector<Row> ReadAllRows();
  std::vector<std::pair<Row, RowPosition>> ReadAllPositioned();

  // Stream rows without buffering the full file in memory.
  template <typename Fn>
  void ForEachRow(Fn&& fn) {
    if (count_ == 0) {
      return;
    }
    if (has_positions_) {
      throw std::runtime_error("ForEachRow on positioned spill");
    }
    EnsureReader();
    const uint64_t stored = ReadStoredCount();
    Decoder dec(stream_);
    for (uint64_t i = 0; i < stored; ++i) {
      Row row;
      dec >> row;
      if (!stream_) {
        throw std::runtime_error("truncated spill file: " + path_.string());
      }
      fn(row);
    }
  }

  static std::filesystem::path TempDirectory();

 private:
  void OpenForWrite();
  void EnsureReader();
  // Reads and validates the row-count header at the start of the file.
  [[nodiscard]] uint64_t ReadStoredCount();

  std::filesystem::path path_;
  std::fstream stream_;
  uint64_t count_{0};
  bool writing_{false};
  bool finished_{false};
  bool has_positions_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_SPILL_FILE_HPP
