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
  for (size_t level = 0; level < other.levels_.size(); ++level) {
    auto& run = other.levels_[level];
    if (run) {
      RETURN_IF_FAIL(Insert(std::move(*run), level));
    }
  }
  other.levels_.clear();
  return Status::kSuccess;
} catch (const std::exception& error) {
  return StatusError(StatusCode::kRuntimeError, error.what());
} catch (...) {
  return StatusError(StatusCode::kRuntimeError, "distinct absorb failed");
}

Status SortedDistinctRuns::ForEach(
    const std::function<void(uint64_t)>& consume) try {
  std::optional<Run> final;
  for (auto& run : levels_) {
    if (!run) {
      continue;
    }
    if (!final) {
      final = std::move(run);
    } else {
      ASSIGN_OR_RETURN(Run, merged, Merge(*final, *run));
      final = std::move(merged);
    }
  }
  levels_.clear();
  if (!final) {
    return Status::kSuccess;
  }
  // T4182: a Rewind/Next failure used to return with levels_ already
  // cleared and the merged `final` run destroyed on the way out -- a retry
  // then saw an EMPTY run set and "succeeded" with zero values, so the
  // distinct value set was permanently lost. Hand the merged run back to
  // levels_ on every failure path so ForEach stays retryable.
  const Status rewind_st = Rewind(final->file.get());
  if (rewind_st != Status::kSuccess) {
    levels_.push_back(std::move(final));
    return rewind_st;  // NOLINT(performance-no-automatic-move)
  }
  Reader reader{.file = final->file.get(), .remaining = final->count};
  std::optional<uint64_t> previous;
  for (;;) {
    StatusOr<std::optional<uint64_t>> next = reader.Next();
    if (!next.HasValue()) {
      levels_.push_back(std::move(final));
      return next.GetStatus();
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
  return Status::kSuccess;
} catch (const std::exception& error) {
  return StatusError(StatusCode::kRuntimeError, error.what());
} catch (...) {
  return StatusError(StatusCode::kRuntimeError, "distinct iteration failed");
}
}  // namespace tinylamb
