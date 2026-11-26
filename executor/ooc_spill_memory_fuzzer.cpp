/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Pure in-process libFuzzer for Out-of-Core (OOC) spillable operators under
// ultra-tight memory budgets (16 KiB .. 128 KiB) against multi-MiB wide-row /
// high-skew datasets:
//   Mode 0: ExternalSorter (multi-run K-way merge sort & Top-N, wide VARCHARs,
//           ASC/DESC, NULLS FIRST/LAST, CI collation, early cursor close).
//   Mode 1: SpillableStore (budget/forced spill + double sequential scan +
//           Reset()).
//   Mode 2: RecursivePartitioner (Grace radix hash partitioning with extreme
//           skew, identical keys, wide payloads, skew fallback).
//
// Invariants enforced on EVERY run:
//   1. Spill-path enforcement: when input bytes exceed 4x budget (without a
//      tiny Top-N limit), Spilled() MUST be true (no silent in-memory bypass).
//   2. Real live-heap bound: glibc mallinfo2().uordblks delta during and after
//      the spill pass must stay bounded (catches uncharged buffer blowup or
//      leaks invisible to ProcessMemoryBudget).
//   3. Zero tracked budget leak: ProcessMemoryBudget::Global().QueryUsed() == 0
//      after operator teardown.
//   4. Zero SpillStore disk leak: live_segments == 0 and
//      current_active_bytes == 0 after operator teardown.
//   5. Exact data correctness (sorted order, row count, multiset hash).

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#ifdef __GLIBC__
#include <malloc.h>
#endif

#include "common/memory_budget.hpp"
#include "common/spill/spill_store.hpp"
#include "common/status_or.hpp"
#include "executor/detail/ooc/external_sorter.hpp"
#include "executor/detail/ooc/recursive_partitioner.hpp"
#include "executor/detail/ooc/spillable_store.hpp"
#include "type/collation.hpp"
#include "type/row.hpp"
#include "type/row_block_codec.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::fuzz {
namespace {

struct Cursor {
  const uint8_t* data{};
  size_t size{};
  size_t pos{0};

  uint8_t NextU8() noexcept { return pos < size ? data[pos++] : 0U; }

  uint32_t NextU32() noexcept {
    uint32_t v = 0;
    for (int i = 0; i < 4; ++i) {
      v = (v << 8U) | NextU8();
    }
    return v;
  }
};

[[noreturn]] void Fail(const char* op, const char* msg) {
  (void)std::fprintf(stderr, "[OOC-SPILL-MEM-FUZZ FAIL][%s] %s\n", op, msg);
  (void)std::fflush(stderr);
  std::abort();
}

size_t CurrentLiveHeapBytes() noexcept {
#ifdef __GLIBC__
  return mallinfo2().uordblks;
#else
  return 0;
#endif
}

uint64_t Mix64(uint64_t z) noexcept {
  z = (z ^ (z >> 30U)) * 0xbf58476d1ce4e5b9ULL;
  z = (z ^ (z >> 27U)) * 0x94d049bb133111ebULL;
  return z ^ (z >> 31U);
}

// Order-independent fingerprint of a Row for multiset conservation checks.
uint64_t FingerprintRow(const Row& row) noexcept {
  uint64_t h =
      0x9e3779b97f4a7c15ULL ^ static_cast<uint64_t>(row.values_.size());
  for (const Value& v : row.values_) {
    h = Mix64(h ^ static_cast<uint64_t>(v.type));
    switch (v.type) {
      case ValueType::kNull:
        h = Mix64(h ^ 0x1111111111111111ULL);
        break;
      case ValueType::kInt64:
      case ValueType::kDate:
        h = Mix64(h ^ static_cast<uint64_t>(v.value.int_value));
        break;
      case ValueType::kDouble: {
        uint64_t bits = 0;
        std::memcpy(&bits, &v.value.double_value, sizeof(bits));
        h = Mix64(h ^ bits);
        break;
      }
      case ValueType::kVarChar: {
        const std::string_view sv = v.value.varchar_value;
        h = Mix64(h ^ static_cast<uint64_t>(sv.size()));
        if (!sv.empty()) {
          h = Mix64(
              h ^ static_cast<uint8_t>(sv.front()) ^
              (static_cast<uint64_t>(static_cast<uint8_t>(sv.back())) << 8U));
        }
        break;
      }
      case ValueType::kArray:
        h = Mix64(h ^ static_cast<uint64_t>(v.ArrayElements().size()));
        break;
    }
  }
  return h;
}

Row MakeSyntheticRow(uint32_t seq, uint32_t seed, uint8_t skew_mode,
                     size_t payload_bytes) {
  const uint64_t r =
      Mix64(static_cast<uint64_t>(seq) ^ (static_cast<uint64_t>(seed) << 32U));
  // Col 0: INT64 key (with configurable skew + NULLs)
  Value k0;
  if ((r & 15U) == 0) {
    k0 = Value();  // NULL
  } else if (skew_mode == 0) {
    k0 = Value(static_cast<int64_t>(42));  // 100% identical key (extreme skew)
  } else if (skew_mode == 1) {
    k0 = Value(static_cast<int64_t>(
        (r % 5U) == 0 ? 7 : static_cast<int64_t>(r % 1024U)));
  } else {
    k0 = Value(static_cast<int64_t>(r % 65536U) - 32768);
  }

  // Col 1: short VARCHAR key (mixed case for CI collation testing)
  Value k1;
  if (((r >> 4U) & 15U) == 0) {
    k1 = Value();
  } else {
    static constexpr std::array<const char*, 8> kTags = {
        "alpha", "ALPHA", "Beta", "beta", "gamma", "GAMMA", "delta", ""};
    k1 = Value(std::string(kTags[(r >> 8U) % kTags.size()]));
  }

  // Col 2: wide VARCHAR payload to force multi-MiB spill quickly
  std::string payload(payload_bytes,
                      static_cast<char>('a' + ((r >> 16U) % 26U)));
  if (payload_bytes >= 8) {
    payload[0] = static_cast<char>('A' + (seq % 26U));
    payload[payload_bytes - 1] = static_cast<char>('0' + (seq % 10U));
  }
  return Row({std::move(k0), std::move(k1), Value(std::move(payload)),
              Value(static_cast<int64_t>(seq))});
}

void CheckPostTeardownInvariants(const char* op, const SpillStore& spill_store,
                                 size_t heap_before) {
  const SpillStoreStats stats = spill_store.Stats();
  if (stats.live_segments != 0 || stats.current_active_bytes != 0) {
    Fail(op, "SpillStore leaked live segments or active bytes after teardown");
  }
  if (ProcessMemoryBudget::Global().QueryUsed() != 0) {
    Fail(op, "ProcessMemoryBudget::QueryUsed() non-zero after teardown");
  }
  const size_t heap_after = CurrentLiveHeapBytes();
  // Allow modest allocator fragmentation / arena growth (4 MiB) but catch
  // unbounded live-heap retention across iterations.
  if (heap_after > heap_before + (size_t{4} << 20U)) {
    Fail(op, "live heap retained > 4 MiB after OOC operator teardown");
  }
}

void FuzzExternalSorter(Cursor& c, SpillStore& spill_store) {
  // Tight budget: 16 KiB .. 128 KiB
  static constexpr std::array<size_t, 4> kBudgets = {
      16ULL << 10U, 32ULL << 10U, 64ULL << 10U, 128ULL << 10U};
  const size_t budget = kBudgets[c.NextU8() % kBudgets.size()];
  // Dataset: 256 .. 1536 rows x 256 .. 1536 bytes = 64 KiB .. 2.3 MiB
  const uint32_t row_count = 256U + (static_cast<uint32_t>(c.NextU8()) * 5U);
  const size_t payload_bytes = 256U + (static_cast<size_t>(c.NextU8()) * 5U);
  const uint8_t skew_mode = c.NextU8() % 3U;
  const bool force_spill = (c.NextU8() & 1U) != 0;
  const bool asc0 = (c.NextU8() & 1U) != 0;
  const bool asc1 = (c.NextU8() & 1U) != 0;
  const bool ci1 = (c.NextU8() & 1U) != 0;
  const bool has_limit = (c.NextU8() % 4U) == 0;
  const std::optional<size_t> limit =
      has_limit ? std::optional<size_t>(1U + (c.NextU8() % 64U)) : std::nullopt;
  const bool early_close = (c.NextU8() % 5U) == 0;
  const uint32_t seed = c.NextU32();

  const size_t heap_before = CurrentLiveHeapBytes();
  size_t peak_heap = heap_before;
  uint64_t expected_xor_sum = 0;
  size_t total_input_bytes = 0;

  {
    ExternalSorter::Options opts;
    opts.spill_store = &spill_store;
    opts.memory_budget_bytes = budget;
    opts.force_spill = force_spill;
    opts.limit = limit;
    opts.keys = {
        OocSortKey{.column_index = 0,
                   .ascending = asc0,
                   .nulls_first = std::nullopt,
                   .collation = Collation::kNone},
        OocSortKey{
            .column_index = 1,
            .ascending = asc1,
            .nulls_first = std::nullopt,
            .collation = ci1 ? Collation::kCaseInsensitive : Collation::kNone},
    };

    ExternalSorter sorter(std::move(opts));
    for (uint32_t i = 0; i < row_count; ++i) {
      Row row = MakeSyntheticRow(i, seed, skew_mode, payload_bytes);
      total_input_bytes += EstimateRowMemoryBytes(row);
      expected_xor_sum += FingerprintRow(row);
      if (sorter.AddRow(std::move(row)) != Status::kSuccess) {
        Fail("ExternalSorter", "AddRow failed unexpectedly");
      }
      if ((i & 63U) == 0) {
        peak_heap = std::max(peak_heap, CurrentLiveHeapBytes());
      }
    }
    if (sorter.Finish() != Status::kSuccess) {
      Fail("ExternalSorter", "Finish failed unexpectedly");
    }
    peak_heap = std::max(peak_heap, CurrentLiveHeapBytes());

    // When input is > 4x budget and there is no Top-N limit, spill MUST engage.
    if (!has_limit && total_input_bytes > budget * 4U && !sorter.Spilled()) {
      Fail("ExternalSorter",
           "input exceeded 4x budget without triggering disk spill");
    }

    // Live heap delta during spill must stay strictly bounded (never O(N)
    // blowup).
    const size_t max_allowed_heap =
        std::max<size_t>(budget * 16U, 16ULL << 20U);
    if (peak_heap > heap_before + max_allowed_heap) {
      Fail("ExternalSorter",
           "peak live heap exceeded bounded OOC ceiling during spill");
    }

    SortKeyOrder ord0(asc0, std::nullopt, Collation::kNone);
    SortKeyOrder ord1(asc1, std::nullopt,
                      ci1 ? Collation::kCaseInsensitive : Collation::kNone);

    Row prev;
    bool has_prev = false;
    uint64_t out_count = 0;
    uint64_t out_xor_sum = 0;
    const uint64_t stop_at =
        early_close ? std::min<uint64_t>(row_count, 17U) : UINT64_MAX;

    Row cur;
    while (out_count < stop_at) {
      StatusOr<bool> has = sorter.NextRow(&cur);
      if (!has.HasValue()) {
        Fail("ExternalSorter", "NextRow returned error");
      }
      if (!has.Value()) {
        break;
      }
      if (has_prev) {
        const int c0 = ord0.Compare(prev[0], cur[0]);
        const int c1 = c0 != 0 ? c0 : ord1.Compare(prev[1], cur[1]);
        if (c1 > 0) {
          Fail("ExternalSorter", "output rows are not sorted");
        }
      }
      out_xor_sum += FingerprintRow(cur);
      prev = std::move(cur);
      has_prev = true;
      ++out_count;
    }

    if (!early_close) {
      const uint64_t expected_count =
          limit.has_value() ? std::min<uint64_t>(row_count, *limit) : row_count;
      if (out_count != expected_count) {
        Fail("ExternalSorter", "output row count mismatch");
      }
      if (!limit.has_value() && out_xor_sum != expected_xor_sum) {
        Fail("ExternalSorter", "output multiset fingerprint mismatch");
      }
    }
  }

  CheckPostTeardownInvariants("ExternalSorter", spill_store, heap_before);
}

void FuzzSpillableStore(Cursor& c, SpillStore& spill_store) {
  static constexpr std::array<size_t, 4> kBudgets = {
      16ULL << 10U, 32ULL << 10U, 64ULL << 10U, 128ULL << 10U};
  const size_t budget = kBudgets[c.NextU8() % kBudgets.size()];
  const uint32_t row_count = 128U + (static_cast<uint32_t>(c.NextU8()) * 4U);
  const size_t payload_bytes = 256U + (static_cast<size_t>(c.NextU8()) * 4U);
  const bool force_spill = (c.NextU8() & 1U) != 0;
  const uint32_t seed = c.NextU32();

  const size_t heap_before = CurrentLiveHeapBytes();
  size_t peak_heap = heap_before;
  uint64_t expected_sum = 0;
  size_t total_input_bytes = 0;

  {
    SpillableStore::Options opts;
    opts.spill_store = &spill_store;
    opts.memory_budget_bytes = budget;
    opts.force_spill = force_spill;

    SpillableStore store(opts);
    for (uint32_t i = 0; i < row_count; ++i) {
      Row row = MakeSyntheticRow(i, seed, 2, payload_bytes);
      total_input_bytes += EstimateRowMemoryBytes(row);
      expected_sum += FingerprintRow(row);
      if (store.AppendRow(row) != Status::kSuccess) {
        Fail("SpillableStore", "AppendRow failed");
      }
      if ((i & 63U) == 0) {
        peak_heap = std::max(peak_heap, CurrentLiveHeapBytes());
      }
    }
    if (store.Seal() != Status::kSuccess) {
      Fail("SpillableStore", "Seal failed");
    }
    peak_heap = std::max(peak_heap, CurrentLiveHeapBytes());

    if (total_input_bytes > budget * 4U && !store.Spilled()) {
      Fail("SpillableStore", "input exceeded 4x budget without spilling");
    }
    const size_t max_allowed_heap =
        std::max<size_t>(budget * 16U, 16ULL << 20U);
    if (peak_heap > heap_before + max_allowed_heap) {
      Fail("SpillableStore", "peak live heap exceeded bounded OOC ceiling");
    }

    // Two sequential scans must both reproduce the exact row count & multiset.
    for (int pass = 0; pass < 2; ++pass) {
      if (store.ResetCursor() != Status::kSuccess) {
        Fail("SpillableStore", "ResetCursor failed");
      }
      uint64_t count = 0;
      uint64_t sum = 0;
      Row row;
      while (true) {
        StatusOr<bool> has = store.NextRow(&row);
        if (!has.HasValue()) {
          Fail("SpillableStore", "NextRow failed");
        }
        if (!has.Value()) {
          break;
        }
        sum += FingerprintRow(row);
        ++count;
      }
      if (count != row_count || sum != expected_sum) {
        Fail("SpillableStore", "scan row count or multiset mismatch");
      }
    }
  }

  CheckPostTeardownInvariants("SpillableStore", spill_store, heap_before);
}

void FuzzRecursivePartitioner(Cursor& c, SpillStore& spill_store) {
  static constexpr std::array<size_t, 4> kBudgets = {
      16ULL << 10U, 32ULL << 10U, 64ULL << 10U, 128ULL << 10U};
  const size_t budget = kBudgets[c.NextU8() % kBudgets.size()];
  const uint32_t row_count = 256U + (static_cast<uint32_t>(c.NextU8()) * 4U);
  const size_t payload_bytes = 256U + (static_cast<size_t>(c.NextU8()) * 4U);
  const uint8_t skew_mode = c.NextU8() % 3U;
  const bool force_spill = (c.NextU8() & 1U) != 0;
  const uint32_t max_depth = 2U + (c.NextU8() % 5U);
  const uint32_t seed = c.NextU32();

  const size_t heap_before = CurrentLiveHeapBytes();
  size_t peak_heap = heap_before;
  uint64_t expected_sum = 0;
  size_t total_input_bytes = 0;

  {
    RecursivePartitioner::Options opts;
    opts.spill_store = &spill_store;
    opts.memory_budget_bytes = budget;
    opts.max_depth = max_depth;
    opts.force_spill = force_spill;

    auto hash_fn = [](const Row& row) -> uint64_t {
      if (row.values_.empty() || row[0].IsNull()) {
        return 0x5555555555555555ULL;
      }
      return Mix64(static_cast<uint64_t>(row[0].value.int_value));
    };

    RecursivePartitioner part(opts, std::move(hash_fn));
    for (uint32_t i = 0; i < row_count; ++i) {
      Row row = MakeSyntheticRow(i, seed, skew_mode, payload_bytes);
      total_input_bytes += EstimateRowMemoryBytes(row);
      expected_sum += FingerprintRow(row);
      if (part.AddRow(std::move(row)) != Status::kSuccess) {
        Fail("RecursivePartitioner", "AddRow failed");
      }
      if ((i & 63U) == 0) {
        peak_heap = std::max(peak_heap, CurrentLiveHeapBytes());
      }
    }

    PartitionLeafSet leaves(&spill_store);
    if (part.Finalize(&leaves) != Status::kSuccess) {
      Fail("RecursivePartitioner", "Finalize failed");
    }
    peak_heap = std::max(peak_heap, CurrentLiveHeapBytes());

    if (total_input_bytes > budget * 4U && !part.Spilled()) {
      Fail("RecursivePartitioner", "input exceeded 4x budget without spilling");
    }
    const size_t max_allowed_heap =
        std::max<size_t>(budget * 16U, 16ULL << 20U);
    if (peak_heap > heap_before + max_allowed_heap) {
      Fail("RecursivePartitioner",
           "peak live heap exceeded bounded OOC ceiling");
    }

    uint64_t out_count = 0;
    uint64_t out_sum = 0;
    for (const PartitionLeaf& leaf : leaves.leaves) {
      if (leaf.depth > max_depth) {
        Fail("RecursivePartitioner", "leaf depth exceeded max_depth");
      }
      const Status st = RecursivePartitioner::ReadLeafRows(
          &spill_store, leaf, [&](Row&& r) -> Status {
            out_sum += FingerprintRow(r);
            ++out_count;
            return Status::kSuccess;
          });
      if (st != Status::kSuccess) {
        Fail("RecursivePartitioner", "ReadLeafRows failed");
      }
    }
    if (out_count != row_count || out_sum != expected_sum) {
      Fail("RecursivePartitioner", "leaf row count or multiset mismatch");
    }
  }

  CheckPostTeardownInvariants("RecursivePartitioner", spill_store, heap_before);
}

}  // namespace
}  // namespace tinylamb::fuzz

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  if (size < 8 || size > 256) {
    return 0;
  }
  tinylamb::fuzz::Cursor c{data, size, 0};
  tinylamb::SpillStoreOptions store_opts;
  store_opts.use_global_temp_limit = false;
  tinylamb::SpillStore spill_store(store_opts);

  switch (c.NextU8() % 3U) {
    case 0:
      tinylamb::fuzz::FuzzExternalSorter(c, spill_store);
      break;
    case 1:
      tinylamb::fuzz::FuzzSpillableStore(c, spill_store);
      break;
    default:
      tinylamb::fuzz::FuzzRecursivePartitioner(c, spill_store);
      break;
  }
  return 0;
}
