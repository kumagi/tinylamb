/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_SORTED_DISTINCT_RUNS_HPP
#define TINYLAMB_SORTED_DISTINCT_RUNS_HPP

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "common/status_or.hpp"

namespace tinylamb {
// Exact external union of sorted uint64 runs. Binary carries keep at most
// log2(runs) anonymous temporary files open; every merge has fixed-size I/O
// buffers. Temporary files are closed/deleted on both success and failure.
class SortedDistinctRuns {
 public:
  Status Add(std::span<const uint64_t> sorted);
  Status Absorb(SortedDistinctRuns&& other);
  Status ForEach(const std::function<void(uint64_t)>& consume);
  [[nodiscard]] bool Empty() const { return levels_.empty(); }

  // Test-only fault injection: makes the next Merge fail before it reads or
  // creates any file, so the strong-guarantee failure path is reproducible.
  static void SetFailNextMergeForTest(bool fail) {
    fail_next_merge_for_test_ = fail;
  }

 private:
  struct CloseFile {
    void operator()(std::FILE* file) const {
      // unique_ptr transfers sole ownership to this deleter. Write errors
      // are checked by fwrite/fflush before the run is consumed.
      if (file == nullptr) {
        return;
      }
      // NOLINTNEXTLINE(cppcoreguidelines-owning-memory)
      static_cast<void>(std::fclose(file));
    }
  };
  using File = std::unique_ptr<std::FILE, CloseFile>;
  struct Run {
    File file;
    uint64_t count{0};
  };
  // Merges two runs without taking ownership: the inputs keep their files on
  // failure so callers retain the strong exception guarantee.
  static StatusOr<Run> Merge(const Run& left, const Run& right);
  Status Insert(Run run, size_t level);
  static inline bool fail_next_merge_for_test_{false};
  std::vector<std::optional<Run>> levels_;
};
}  // namespace tinylamb
#endif
