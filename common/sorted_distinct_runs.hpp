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

 private:
  struct CloseFile {
    void operator()(std::FILE* file) const {
      // unique_ptr transfers sole ownership to this deleter. Write errors
      // are checked by fwrite/fflush before the run is consumed.
      // NOLINTNEXTLINE(cppcoreguidelines-owning-memory)
      static_cast<void>(std::fclose(file));
    }
  };
  using File = std::unique_ptr<std::FILE, CloseFile>;
  struct Run {
    File file;
    uint64_t count{0};
  };
  static StatusOr<Run> Merge(Run left, Run right);
  Status Insert(Run run, size_t level);
  std::vector<std::optional<Run>> levels_;
};
}  // namespace tinylamb
#endif
