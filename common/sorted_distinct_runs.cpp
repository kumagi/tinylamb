/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "common/sorted_distinct_runs.hpp"

#include <algorithm>
#include <array>
#include <exception>
#include <utility>

#include "common/memory_budget.hpp"
#include "common/platform/file_io.hpp"

namespace tinylamb {
namespace {
constexpr size_t kBufferValues = 4096;
struct Reader {
  std::FILE* file{nullptr};
  uint64_t remaining{0};
  std::array<uint64_t, kBufferValues> buffer{};
  size_t offset{0};
  size_t size{0};
  StatusOr<std::optional<uint64_t>> Next() {
    if (offset == size) {
      if (remaining == 0) {
        return std::optional<uint64_t>{};
      }
      size = static_cast<size_t>(std::min<uint64_t>(remaining, buffer.size()));
      if (std::fread(buffer.data(), sizeof(uint64_t), size, file) != size) {
        return StatusError(StatusCode::kIOError, "distinct run read failed");
      }
      remaining -= size;
      offset = 0;
    }
    return std::optional<uint64_t>(buffer[offset++]);
  }
};
Status Rewind(std::FILE* file) {
  if (file == nullptr) {
    return StatusError(StatusCode::kIOError, "distinct run rewind failed");
  }
  if (std::fflush(file) != 0 || std::fseek(file, 0, SEEK_SET) != 0) {
    return StatusError(StatusCode::kIOError, "distinct run rewind failed");
  }
  return Status::kSuccess;
}

// Spill placement follows the engine-wide knobs -- TINYLAMB_TEMP via
// ProcessMemoryBudget::ResolveTempDirectory(), the same directory SpillStore
// and the rest of the OOC machinery honor -- instead of std::tmpfile()'s
// fixed P_tmpdir.  The unlinked fd keeps tmpfile()'s auto-delete contract.
std::FILE* OpenRunFile() {
  const int fd = platform::OpenUnlinkedTempFile(
      ProcessMemoryBudget::ResolveTempDirectory());
  return platform::FdOpenStream(fd, "w+b");
}
}  // namespace

Status SortedDistinctRuns::Add(std::span<const uint64_t> sorted) try {
  if (sorted.empty()) {
    return Status::kSuccess;
  }
  // Merge relies on 2-pointer union + adjacent-dup elimination; an unsorted
  // input silently breaks COUNT(DISTINCT) correctness.
  for (size_t i = 1; i < sorted.size(); ++i) {
    if (sorted[i] < sorted[i - 1]) {
      return StatusError(StatusCode::kInvalidArgument,
                         "SortedDistinctRuns::Add requires a sorted span");
    }
  }
  Run run{.file = File(OpenRunFile()), .count = sorted.size()};
  if (!run.file || std::fwrite(sorted.data(), sizeof(uint64_t), sorted.size(),
                               run.file.get()) != sorted.size()) {
    return StatusError(StatusCode::kIOError, "distinct run write failed");
  }
  return Insert(std::move(run), 0);
} catch (const std::exception& error) {
  return StatusError(StatusCode::kRuntimeError, error.what());
} catch (...) {
  return StatusError(StatusCode::kRuntimeError, "distinct run failed");
}

StatusOr<SortedDistinctRuns::Run> SortedDistinctRuns::Merge(const Run& left,
                                                            const Run& right) {
  if (fail_next_merge_for_test_) {
    fail_next_merge_for_test_ = false;
    return StatusError(StatusCode::kIOError, "injected distinct merge failure");
  }
  RETURN_IF_FAIL(Rewind(left.file.get()));
  RETURN_IF_FAIL(Rewind(right.file.get()));
  Run output{.file = File(OpenRunFile())};
  if (!output.file) {
    return StatusError(StatusCode::kIOError, "distinct merge create failed");
  }
  Reader a{.file = left.file.get(), .remaining = left.count};
  Reader b{.file = right.file.get(), .remaining = right.count};
  ASSIGN_OR_RETURN(std::optional<uint64_t>, av, a.Next());
  ASSIGN_OR_RETURN(std::optional<uint64_t>, bv, b.Next());
  std::array<uint64_t, kBufferValues> buffer{};
  size_t used = 0;
  std::optional<uint64_t> previous;
  const auto flush = [&]() -> Status {
    if (used == 0) {
      return Status::kSuccess;  // no buffered values; skip the zero-byte write
    }
    if (std::fwrite(buffer.data(), sizeof(uint64_t), used, output.file.get()) !=
        used) {
      return StatusError(StatusCode::kIOError, "distinct merge write failed");
    }
    used = 0;
    return Status::kSuccess;
  };
  while (av || bv) {
    const bool take_left = av && (!bv || *av <= *bv);
    const uint64_t value = take_left ? *av : *bv;
    if (!previous || *previous != value) {
      buffer[used++] = value;
      ++output.count;
      previous = value;
      if (used == buffer.size()) {
        RETURN_IF_FAIL(flush());
      }
    }
    ASSIGN_OR_RETURN(std::optional<uint64_t>, next,
                     take_left ? a.Next() : b.Next());
    (take_left ? av : bv) = next;
  }
  RETURN_IF_FAIL(flush());
  return output;
}

Status SortedDistinctRuns::Insert(Run run, size_t level) {
  while (level < levels_.size()) {
    auto& existing = levels_[level];
    if (!existing) {
      break;
    }
    ASSIGN_OR_RETURN(Run, merged, Merge(run, *existing));
    existing.reset();
    run = std::move(merged);
    ++level;
  }
  if (level >= levels_.size()) {
    levels_.resize(level + 1);
  }
  levels_[level] = std::move(run);
  return Status::kSuccess;
}

Status SortedDistinctRuns::Absorb(SortedDistinctRuns&& other) try {
  if (&other == this) {
    return StatusError(StatusCode::kInvalidArgument, "cannot absorb self");
  }
  struct StagedRun {
    Run* borrowed{nullptr};
    std::optional<Run> owned;
    [[nodiscard]] const Run& Get() const { return owned ? *owned : *borrowed; }
    [[nodiscard]] bool HasValue() const { return borrowed != nullptr || owned.has_value(); }
  };
  // Each level holds either an original run or a newly merged temporary.
  // Original files are untouched until every merge has succeeded.
  const size_t max_levels = std::max(levels_.size(), other.levels_.size());
  std::vector<StagedRun> staged;
  if (max_levels >= staged.max_size()) {
    return StatusError(StatusCode::kTooBigData, "too many distinct run levels");
  }
  staged.resize(max_levels + 1);
  for (size_t i = 0; i < levels_.size(); ++i) {
    if (levels_[i]) {
      staged[i].borrowed = &*levels_[i];
    }
  }
  for (size_t i = 0; i < other.levels_.size(); ++i) {
    if (!other.levels_[i]) {
      continue;
    }
    Run* borrowed = &*other.levels_[i];
    std::optional<Run> carry;
    size_t level = i;
    while (staged[level].HasValue()) {
      const Run& incoming = carry ? *carry : *borrowed;
      ASSIGN_OR_RETURN(Run, merged, Merge(staged[level].Get(), incoming));
      staged[level].borrowed = nullptr;
      staged[level].owned.reset();
      carry = std::move(merged);
      borrowed = nullptr;
      ++level;
    }
    if (carry) {
      staged[level].owned = std::move(carry);
    } else {
      staged[level].borrowed = borrowed;
    }
  }
  std::vector<std::optional<Run>> result(staged.size());
  for (size_t i = 0; i < staged.size(); ++i) {
    if (staged[i].owned) {
      result[i] = std::move(staged[i].owned);
    } else if (staged[i].borrowed != nullptr) {
      result[i] = std::move(*staged[i].borrowed);
    }
  }
  while (!result.empty() && !result.back()) {
    result.pop_back();
  }
  levels_ = std::move(result);
  other.levels_.clear();
  return Status::kSuccess;
} catch (const std::exception& error) {
  return StatusError(StatusCode::kRuntimeError, error.what());
} catch (...) {
  return StatusError(StatusCode::kRuntimeError, "distinct absorb failed");
}

Status SortedDistinctRuns::ForEach(
    const std::function<void(uint64_t)>& consume) {
  // The merged run is the only copy once levels_ has been cleared. Keep it
  // in a scope that survives the catch blocks so every failure path -- I/O
  // error or an exception thrown by `consume` -- can hand it back. T4182
  // fixed the Rewind/Next paths; a throwing callback used to be caught after
  // `final` was destroyed, leaving an empty run set and losing the distinct
  // values permanently on retry.
  std::optional<Run> final;
  Status status = Status::kSuccess;
  try {
    // Keep every input owned by levels_ until all merges succeed. A failed
    // merge then leaves the original runs intact for a retry.
    std::optional<Run> merged_run;
    size_t first_level = levels_.size();
    for (size_t i = 0; i < levels_.size(); ++i) {
      if (!levels_[i]) {
        continue;
      }
      if (first_level == levels_.size()) {
        first_level = i;
        continue;
      }
      const Run& left = merged_run ? *merged_run : *levels_[first_level];
      ASSIGN_OR_RETURN(Run, merged, Merge(left, *levels_[i]));
      merged_run = std::move(merged);
    }
    if (merged_run) {
      final = std::move(merged_run);
    } else if (first_level < levels_.size()) {
      final = std::move(levels_[first_level]);
    }
    levels_.clear();
    if (!final) {
      return Status::kSuccess;
    }
    status = Rewind(final->file.get());
    if (status == Status::kSuccess) {
      Reader reader{.file = final->file.get(), .remaining = final->count};
      std::optional<uint64_t> previous;
      for (;;) {
        StatusOr<std::optional<uint64_t>> next = reader.Next();
        if (!next.HasValue()) {
          status = next.GetStatus();
          break;
        }
        std::optional<uint64_t> value = next.MoveValue();
        if (!value) {
          break;
        }
        if (!previous || *previous != *value) {
          consume(*value);
          previous = value;
        }
      }
    }
  } catch (const std::exception& error) {
    status = StatusError(StatusCode::kRuntimeError, error.what());
  } catch (...) {
    status =
        StatusError(StatusCode::kRuntimeError, "distinct iteration failed");
  }
  if (status != Status::kSuccess && final) {
    levels_.push_back(std::move(final));
  }
  return status;
}
}  // namespace tinylamb
