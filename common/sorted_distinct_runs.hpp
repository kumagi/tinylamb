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
// Exact external union of sorted uint64 runs. Binary carries keep one run per
// level (at most log2(runs) live files in total); a merge opens at most three
// at once -- two inputs plus one output -- with fixed-size I/O buffers.
// Temporary files are closed/deleted on both success and failure.
class SortedDistinctRuns {
 public:
  Status Add(std::span<const uint64_t> sorted);
  Status Absorb(SortedDistinctRuns&& other);
  // Visits distinct values in order and consumes the runs on success.
  // An I/O failure can leave a partially filled destination. Retrying
  // starts from the first value, so discard the earlier output first.
  Status ForEach(const std::function<void(uint64_t)>& consume);
  [[nodiscard]] bool Empty() const { return levels_.empty(); }

  // Test-only introspection: live runs (at most one per binary-carry level)
  // and the total values retained across them, so boundedness tests can pin
  // the logarithmic run count instead of only checking output correctness.
  [[nodiscard]] size_t RunCountForTest() const {
    size_t count = 0;
    for (const auto& run : levels_) {
      count += run.has_value() ? 1 : 0;
    }
    return count;
  }
  [[nodiscard]] uint64_t RetainedCountForTest() const {
    uint64_t count = 0;
    for (const auto& run : levels_) {
      count += run.has_value() ? run->count : 0;
    }
    return count;
  }

  // Test-only fault injection: makes the next Merge fail before it reads or
  // creates any file, so the strong-guarantee failure path is reproducible.
  void SetFailNextMergeForTest(bool fail) {
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
  StatusOr<Run> Merge(const Run& left, const Run& right);
  Status Insert(Run run, size_t level);
  // Instance-scoped so fault injection in one test cannot be consumed by an
  // unrelated SortedDistinctRuns object running concurrently.
  bool fail_next_merge_for_test_{false};
  std::vector<std::optional<Run>> levels_;
};
}  // namespace tinylamb
#endif
