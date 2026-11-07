/**
 * Copyright 2026 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "table/fragment/fragment.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstring>
#include <map>
#include <ranges>
#include <unordered_map>
#include <unordered_set>

#include "common/env_endian.hpp"
#include "table/workload/workload.hpp"

namespace tinylamb {
namespace {

// ---------------------------------------------------------------------------
// Support rules
//
// A conjunct is either evaluated by the storage (the kernel exists for the
// runtime AND the column type) or it is residual. The rule lives in ONE
// place so Estimate() and Execute() cannot disagree about the coverage.
// ---------------------------------------------------------------------------

bool IsInt64Column(const Schema& sc, slot_t col) {
  return col < sc.ColumnCount() &&
         sc.GetColumn(col).Type() == ValueType::kInt64;
}

bool IsVarcharColumn(const Schema& sc, slot_t col) {
  return col < sc.ColumnCount() &&
         sc.GetColumn(col).Type() == ValueType::kVarChar;
}

// The literal domain must match the column's declared type. A decimal
// arrives already scaled into the integer domain by the caller, so an
// int64 column is the only numeric domain the IR accepts today.
//
// The RUNTIME enters through the capability bits: the varchar family needs
// the SELECTION kernels, and the WASM module has only the COUNT ones, so a
// varchar conjunct is residual there.
bool ConjunctSupported(Runtime runtime, const Schema& sc, const Conjunct& c) {
  if (c.kind == ConjunctKind::kCompare) {
    if (c.literal_kind == LiteralKind::kBytes) {
      return GetCapabilities(runtime).predicates_varchar &&
             IsVarcharColumn(sc, c.col);
    }
    // The PREFIX is a byte-domain operator: it never applies to an int64
    // column.
    if (c.op == CompareOp::kPrefix) return false;
    return c.literal_kind == LiteralKind::kInt64 && IsInt64Column(sc, c.col);
  }
  if (c.kind == ConjunctKind::kIn) {
    return !c.in_literals.empty() && IsInt64Column(sc, c.col);
  }
  if (c.kind == ConjunctKind::kVarcharRange) {
    return GetCapabilities(runtime).predicates_varchar &&
           IsVarcharColumn(sc, c.col);
  }
  if (c.kind == ConjunctKind::kIsNull) {
    return c.col < sc.ColumnCount();
  }
  return false;
}

// The schemes whose region answers a predicate WITHOUT expanding the column
// (run folding, brackets, code windows, bit slices, wavelet rank). A conjunct
// on such a column is a CompressedScan; on any other scheme it is a
// DecodeScan (the values come back first, the comparison happens after).
bool SchemeHasCompressedDomainKernel(PaxChunk::Scheme s) {
  switch (s) {
    case PaxChunk::Scheme::kConst:
    case PaxChunk::Scheme::kRle:
    case PaxChunk::Scheme::kForBp:
    case PaxChunk::Scheme::kDict:
    case PaxChunk::Scheme::kDeltaFp:
    case PaxChunk::Scheme::kBitmap:
    case PaxChunk::Scheme::kBitWeave:
    case PaxChunk::Scheme::kEliasFano:
    case PaxChunk::Scheme::kWavelet:
    case PaxChunk::Scheme::kDeltaBitweave:
    case PaxChunk::Scheme::kPfor:
    case PaxChunk::Scheme::kDdRice:
    case PaxChunk::Scheme::kFreq:
    case PaxChunk::Scheme::kRleX:
    case PaxChunk::Scheme::kDictX:
    case PaxChunk::Scheme::kArith:
    case PaxChunk::Scheme::kSparse:
    case PaxChunk::Scheme::kOpRle:
    case PaxChunk::Scheme::kPBitS:
    case PaxChunk::Scheme::kRleBp:
    case PaxChunk::Scheme::kDeltaPfor:
    case PaxChunk::Scheme::kWaveletR:
    case PaxChunk::Scheme::kQpp:
    case PaxChunk::Scheme::kBp128:
    case PaxChunk::Scheme::kRle8:
    case PaxChunk::Scheme::kRleDelta:
    case PaxChunk::Scheme::kWah:
    case PaxChunk::Scheme::kPla:
    case PaxChunk::Scheme::kSimple8b:
    case PaxChunk::Scheme::kRleIdx:
    case PaxChunk::Scheme::kStreamVByte:
    case PaxChunk::Scheme::kHuffDict:
    case PaxChunk::Scheme::kSimdFor:
    case PaxChunk::Scheme::kD2Pfor:
    case PaxChunk::Scheme::kDeltaSimd:
    case PaxChunk::Scheme::kS4:
    case PaxChunk::Scheme::kTopk:
    case PaxChunk::Scheme::kSfor:
    case PaxChunk::Scheme::kMod:
    case PaxChunk::Scheme::kTileDict:
    case PaxChunk::Scheme::kSortDict:
    case PaxChunk::Scheme::kLane:
    case PaxChunk::Scheme::kPostings:
    case PaxChunk::Scheme::kFastPfor:
    case PaxChunk::Scheme::kDeltaFastPfor:
      return true;
    default:
      return false;
  }
}

// ---------------------------------------------------------------------------
// Comparison
// ---------------------------------------------------------------------------

// The chunk's varchar kernels take the StrScanOp enum; the IR uses the
// shared CompareOp. The mapping is total (the two enums carry the same six
// operators).
StrScanOp ToStrScanOp(CompareOp op) {
  switch (op) {
    case CompareOp::kEq:
      return StrScanOp::kEq;
    case CompareOp::kNe:
      return StrScanOp::kNe;
    case CompareOp::kLt:
      return StrScanOp::kLt;
    case CompareOp::kLe:
      return StrScanOp::kLe;
    case CompareOp::kGt:
      return StrScanOp::kGt;
    case CompareOp::kGe:
      return StrScanOp::kGe;
    case CompareOp::kPrefix:
      return StrScanOp::kPrefix;
  }
  return StrScanOp::kEq;
}

bool CompareOpHolds(CompareOp op, int64_t value, int64_t literal) {
  switch (op) {
    case CompareOp::kEq:
      return value == literal;
    case CompareOp::kNe:
      return value != literal;
    case CompareOp::kLt:
      return value < literal;
    case CompareOp::kLe:
      return value <= literal;
    case CompareOp::kGt:
      return value > literal;
    case CompareOp::kGe:
      return value >= literal;
    case CompareOp::kPrefix:
      // The byte-prefix comparison has no int64 meaning; a conjunct that
      // reaches here is rejected by ConjunctSupported.
      return false;
  }
  return false;
}

bool InListHolds(const std::vector<int64_t>& lits, int64_t value) {
  return std::ranges::any_of(lits,
                             [value](int64_t lit) { return lit == value; });
}

// The string-domain comparison. The order is the unsigned-byte
// lexicographic one (std::string_view's), the same the varchar count
// kernels use.
bool CompareOpHolds(CompareOp op, std::string_view value,
                    std::string_view literal) {
  switch (op) {
    case CompareOp::kEq:
      return value == literal;
    case CompareOp::kNe:
      return value != literal;
    case CompareOp::kLt:
      return value < literal;
    case CompareOp::kLe:
      return value <= literal;
    case CompareOp::kGt:
      return value > literal;
    case CompareOp::kGe:
      return value >= literal;
    case CompareOp::kPrefix:
      // The PREFIX: the value starts with the literal (the value may be
      // longer). This is NOT the half-open range [literal, literal\0): a
      // value whose byte right after the prefix is 0x00 is a prefix match
      // but falls outside that range.
      return value.size() >= literal.size() &&
             value.compare(0, literal.size(), literal) == 0;
  }
  return false;
}

// The half-open range [lo, hi) with either side unbounded.
bool VarcharRangeHolds(std::string_view value, std::string_view lo,
                       std::string_view hi) {
  if (!lo.empty() && value < lo) return false;
  if (!hi.empty() && !(value < hi)) return false;
  return true;
}

// ---------------------------------------------------------------------------
// Row-group pruning
//
// A row group whose zone map PROVES that no value can satisfy the conjunct
// is discarded without decoding. "Proves" is the important word: the zone
// map covers non-null values only and a NULL decodes as raw 0, so a cell
// with nulls overlaps every range that contains 0.
// ---------------------------------------------------------------------------

// The value window a conjunct can possibly match. An empty window (lo > hi)
// means "no value can match".
bool ConjunctWindow(const Conjunct& c, int64_t* lo, int64_t* hi) {
  switch (c.kind) {
    case ConjunctKind::kCompare: {
      const int64_t lit = c.int64_literal;
      switch (c.op) {
        case CompareOp::kEq:
          *lo = lit;
          *hi = lit;
          return true;
        case CompareOp::kNe:
        case CompareOp::kPrefix:
          // A range cannot prove "not equal"; the byte-prefix comparison
          // has no int64 window (and an int64 conjunct with it is
          // rejected by ConjunctSupported).
          return false;
        case CompareOp::kLt:
          if (lit == INT64_MIN) {
            *lo = 1;
            *hi = 0;  // empty window
            return true;
          }
          *lo = INT64_MIN;
          *hi = lit - 1;
          return true;
        case CompareOp::kLe:
          *lo = INT64_MIN;
          *hi = lit;
          return true;
        case CompareOp::kGt:
          if (lit == INT64_MAX) {
            *lo = 1;
            *hi = 0;  // empty window
            return true;
          }
          *lo = lit + 1;
          *hi = INT64_MAX;
          return true;
        case CompareOp::kGe:
          *lo = lit;
          *hi = INT64_MAX;
          return true;
      }
      return false;
    }
    case ConjunctKind::kIn: {
      if (c.in_literals.empty()) {
        *lo = 1;
        *hi = 0;
        return true;
      }
      int64_t mn = c.in_literals[0];
      int64_t mx = c.in_literals[0];
      for (int64_t lit : c.in_literals) {
        mn = std::min(mn, lit);
        mx = std::max(mx, lit);
      }
      *lo = mn;
      *hi = mx;
      return true;
    }
    case ConjunctKind::kIsNull:
    case ConjunctKind::kVarcharRange:
      // A null bitmap (not a range) answers IS NULL, and the string domain
      // has no int64 zone map.
      return false;
  }
  return false;
}

// True when the row group provably holds no value satisfying the conjunct.
bool ChunkConjunctProvesEmpty(const PaxChunk& chunk, uint32_t g, slot_t col,
                              const Conjunct& c) {
  int64_t lo = 0;
  int64_t hi = 0;
  if (!ConjunctWindow(c, &lo, &hi)) return false;
  if (lo > hi) return true;  // the window is empty: nothing can match
  return !chunk.RGOverlapsCol(static_cast<int32_t>(g), col, lo, hi);
}

// ---------------------------------------------------------------------------
// Key helpers
// ---------------------------------------------------------------------------

// The int64 key value behind a memcomparable key blob. The encoding is
// [tag 1B][8B big-endian of (value ^ (1 << 63))]; anything else (a varchar
// key, a truncated blob) has no int64 value.
bool MemcomparableInt64Key(const std::string& key, int64_t* out) {
  if (key.size() != 9) return false;
  if (static_cast<uint8_t>(key[0]) != static_cast<uint8_t>(ValueType::kInt64)) {
    return false;
  }
  uint64_t loaded = 0;
  std::memcpy(&loaded, key.data() + 1, sizeof(loaded));
  *out = static_cast<int64_t>(be64toh(loaded) ^ (1ULL << 63));
  return true;
}

// The memcomparable form of an int64, for the run's binary search.
std::string MemcomparableInt64KeyOf(int64_t value) {
  std::string out(9, '\0');
  out[0] = static_cast<char>(ValueType::kInt64);
  const uint64_t be = htobe64(static_cast<uint64_t>(value) ^ (1ULL << 63));
  std::memcpy(out.data() + 1, &be, sizeof(be));
  return out;
}

// ---------------------------------------------------------------------------
// The scan
// ---------------------------------------------------------------------------

// One row that survived the filter. The values are the columns the fragment
// needs, in the order of the needed-column list.
struct SurvivingRow {
  int64_t key = 0;
  bool has_key = false;
  bool deleted = false;
  uint64_t generation = 0;
  // Where the row lives: the artifact index and the row index inside that
  // artifact. The selection bitmask is built over the concatenated row space
  // of the artifacts, in artifact order.
  uint32_t artifact_index = 0;
  uint64_t row_index = 0;
  // The row's values live in the scan state's FLAT buffer, at
  // [value_offset, value_offset + value_count). A per-row vector would
  // cost two heap allocations per surviving row, which dominates the scan
  // on a wide filter.
  uint32_t value_offset = 0;
  uint32_t value_count = 0;
};

// The columns the fragment has to read for its KERNELS (the filter, the
// aggregate, the GROUP BY and the Top-K). The projection is NOT in this
// list: a projection over a non-int64 column is left to the executor, so
// the fragment never needs a string kernel.
//
// Every column here must be an int64 column: the v1 kernels (zone-map
// pruning, the compressed-domain counts, the group accumulation) all work
// on the integer domain.
std::vector<slot_t> NeededColumns(const Schema& sc, const Fragment& frag) {
  std::vector<slot_t> cols;
  const auto add = [&cols, &sc](slot_t c) {
    if (!IsInt64Column(sc, c)) return;
    if (std::find(cols.begin(), cols.end(), c) == cols.end()) {
      cols.push_back(c);
    }
  };
  add(sc.KeyColumnIndex());
  for (const Conjunct& c : frag.conjuncts) add(c.col);
  if (frag.aggregate.has_value()) {
    const Aggregate& agg = *frag.aggregate;
    switch (agg.kind) {
      case AggregateKind::kCountCol:
      case AggregateKind::kMin:
      case AggregateKind::kMax:
      case AggregateKind::kApproxCountDistinct:
        add(agg.col);
        break;
      case AggregateKind::kSum:
        if (agg.terms.empty()) {
          add(agg.col);
        } else {
          for (const AggTerm& t : agg.terms) {
            for (slot_t c : t.cols) add(c);
          }
        }
        break;
      case AggregateKind::kCountStar:
      case AggregateKind::kNone:
        break;
    }
  }
  for (slot_t c : frag.group_cols) add(c);
  if (frag.top_k.has_value()) add(frag.top_k->col);
  // The projection's int64 columns: the projected values come from the same
  // decode as the filter's, so a projection over an int64 column is free
  // (it rides the buffer the scan already filled). A non-int64 projection
  // column is left to the executor.
  for (slot_t c : frag.projection) add(c);
  return cols;
}

// The index of a column inside the needed-column list, or -1.
int NeededIndex(const std::vector<slot_t>& cols, slot_t c) {
  const auto it = std::find(cols.begin(), cols.end(), c);
  if (it == cols.end()) return -1;
  return static_cast<int>(it - cols.begin());
}

// The state the scan accumulates.
struct ScanState {
  std::vector<SurvivingRow> rows;
  // The flat value buffer: the surviving rows' needed-column values, row
  // after row, `value_count` entries per row.
  std::vector<int64_t> values;
  // The matching NULL flags (1 = the cell is NULL).
  std::vector<uint8_t> nulls;
  uint64_t rows_in = 0;
  uint64_t rows_proved_fail = 0;
  uint64_t rows_unknown = 0;
  uint64_t rows_decoded = 0;
  uint64_t bytes_touched = 0;
  // False when the key is not an int64: the LSM resolution cannot fold the
  // artifacts together, so duplicate_elimination stays NotAccounted.
  bool key_dedup_possible = true;

  // Appends one row's values to the flat buffer and returns its offset.
  uint32_t AppendValues(const int64_t* row_values, const uint8_t* row_nulls,
                        size_t count) {
    const uint32_t offset = static_cast<uint32_t>(this->values.size());
    this->values.insert(this->values.end(), row_values, row_values + count);
    this->nulls.insert(this->nulls.end(), row_nulls, row_nulls + count);
    return offset;
  }
};

// The zone map of a row group's key column.
bool ChunkKeyRange(const PaxChunk& chunk, uint32_t g, int64_t* lo,
                   int64_t* hi) {
  const PaxChunk::Footer& f = chunk.GetFooter();
  if (!f.zone_map_present || g >= f.group_min_keys.size() ||
      g >= f.group_max_keys.size()) {
    return false;
  }
  *lo = f.group_min_keys[g];
  *hi = f.group_max_keys[g];
  return true;
}

// The byte size of the region of one (row group, column) cell. The regions
// are laid out back to back inside the chunk's data area.
uint64_t ChunkCellBytes(const PaxChunk& chunk, uint32_t g, slot_t col) {
  const PaxChunk::Footer& f = chunk.GetFooter();
  const size_t idx = static_cast<size_t>(g) * f.column_count + col;
  if (idx + 1 >= f.column_offsets.size()) return 0;
  const uint32_t begin = f.column_offsets[idx];
  const uint32_t end = f.column_offsets[idx + 1];
  if (end < begin) return 0;
  return static_cast<uint64_t>(end - begin);
}

// Scans one chunk artifact.
void ScanChunk(const PaxChunk& chunk, const Schema& sc, const Fragment& frag,
               const std::vector<Conjunct>& eval,
               const std::vector<Conjunct>& varchar_eval,
               const std::vector<slot_t>& needed, uint32_t artifact_index,
               ScanState* state) {
  const PaxChunk::Footer& footer = chunk.GetFooter();
  const uint32_t groups = footer.row_group_count;
  if (groups == 0) return;
  const slot_t key_col = sc.KeyColumnIndex();
  const int key_idx = NeededIndex(needed, key_col);

  // The row group window the access mode selects.
  std::vector<uint32_t> window;
  window.reserve(groups);
  for (uint32_t g = 0; g < groups; ++g) {
    if (g >= footer.row_counts.size()) break;
    if (frag.access == AccessMode::kKeyRange) {
      int64_t klo = 0;
      int64_t khi = 0;
      if (ChunkKeyRange(chunk, g, &klo, &khi)) {
        if (frag.range_lo.has_value() && khi < *frag.range_lo) continue;
        if (frag.range_hi.has_value() && *frag.range_hi < klo) continue;
      }
    }
    window.push_back(g);
  }

  // The point access resolves to a single row through the chunk's own index.
  std::optional<uint32_t> point_row;
  if (frag.access == AccessMode::kPoint && frag.point_key.has_value()) {
    size_t row = 0;
    if (chunk.PointLookup(*frag.point_key, sc, &row) == Status::kSuccess) {
      point_row = static_cast<uint32_t>(row);
    } else {
      return;  // no such key: the fragment matches nothing
    }
  }

  // The varchar conjuncts are answered by the chunk's own selection
  // kernels: the sorted Front-Code region answers them in the compressed
  // domain (two binary searches), the other schemes walk the region's
  // values. The masks are over the CHUNK's rows, the same index space the
  // per-row loop below uses.
  std::vector<PaxSelectionVector> varchar_masks;
  varchar_masks.reserve(varchar_eval.size());
  for (const Conjunct& c : varchar_eval) {
    if (c.kind == ConjunctKind::kVarcharRange) {
      const std::optional<std::string_view> lo =
          c.bytes_literal.empty()
              ? std::nullopt
              : std::optional<std::string_view>(c.bytes_literal);
      const std::optional<std::string_view> hi =
          c.bytes_literal_hi.empty()
              ? std::nullopt
              : std::optional<std::string_view>(c.bytes_literal_hi);
      varchar_masks.push_back(VarcharRangeSelection(chunk, c.col, lo, hi, &sc));
    } else {
      varchar_masks.push_back(VarcharFilterSelection(
          chunk, c.col, ToStrScanOp(c.op), c.bytes_literal, &sc));
    }
  }

  std::vector<int64_t> decoded;
  std::vector<int64_t> row_values;  // one surviving row's values
  std::vector<uint8_t> row_nulls;   // and its NULL flags
  std::vector<uint64_t> rg_base;    // the first global row of each group
  {
    uint64_t base = 0;
    rg_base.resize(groups);
    for (uint32_t g = 0; g < groups; ++g) {
      rg_base[g] = base;
      if (g < footer.row_counts.size()) {
        base += footer.row_counts[g];
      }
    }
  }

  for (uint32_t g : window) {
    const uint16_t rc = footer.row_counts[g];
    if (rc == 0) continue;
    if (point_row.has_value()) {
      // The point access touches exactly one row: skip the groups that do
      // not hold it.
      const uint64_t base = rg_base[g];
      if (*point_row < base || *point_row >= base + rc) continue;
    }
    state->rows_in += rc;

    // Zone-map pruning: a group that provably fails one conjunct is
    // discarded whole.
    bool group_empty = false;
    for (const Conjunct& c : eval) {
      if (c.kind == ConjunctKind::kIsNull) continue;
      if (!IsInt64Column(sc, c.col)) continue;
      if (ChunkConjunctProvesEmpty(chunk, g, c.col, c)) {
        group_empty = true;
        break;
      }
    }
    if (group_empty) {
      state->rows_proved_fail += rc;
      continue;
    }

    // Decode the needed columns for this group.
    std::vector<std::vector<int64_t>> column_values(needed.size());
    std::vector<std::vector<uint8_t>> column_nulls(needed.size());
    bool decodable = true;
    for (size_t ci = 0; ci < needed.size(); ++ci) {
      const slot_t col = needed[ci];
      if (!IsInt64Column(sc, col)) {
        decodable = false;
        break;
      }
      column_values[ci].resize(rc);
      try {
        chunk.DecodeInt64ColumnRg(col, g, column_values[ci].data());
      } catch (const std::exception&) {
        decodable = false;
        break;
      }
      column_nulls[ci].assign(rc, 0);
      // The null bitmap of the cell is hoisted out of the row loop: the
      // per-cell accessor re-derives the bitmap's offset by walking every
      // preceding row group, so calling it per row is quadratic over a
      // chunk.
      const uint8_t* null_bits = chunk.CellNullBits(g, col);
      if (null_bits != nullptr) {
        for (uint16_t r = 0; r < rc; ++r) {
          if ((null_bits[r >> 3] >> (7 - (r & 7))) & 1) {
            column_nulls[ci][r] = 1;
          }
        }
      }
      state->bytes_touched += ChunkCellBytes(chunk, g, col);
      state->rows_decoded += rc;
    }
    if (!decodable) {
      // A non-int64 needed column (a string projection) is outside the
      // int64 kernel's reach: the group is left to the residual path.
      state->rows_unknown += rc;
      continue;
    }

    const uint64_t base = rg_base[g];
    for (uint16_t r = 0; r < rc; ++r) {
      const uint32_t global_row = static_cast<uint32_t>(base + r);
      if (point_row.has_value() && global_row != *point_row) continue;
      // The key range is an ACCESS mode, not a conjunct: the zone map only
      // prunes the row groups that cannot hold a key in the range, so the
      // surviving groups still have to be filtered row by row.
      if (frag.access == AccessMode::kKeyRange && key_idx >= 0) {
        const int64_t key_value =
            column_values[static_cast<size_t>(key_idx)][r];
        if (frag.range_lo.has_value() && key_value < *frag.range_lo) {
          state->rows_proved_fail += 1;
          continue;
        }
        if (frag.range_hi.has_value() && *frag.range_hi < key_value) {
          state->rows_proved_fail += 1;
          continue;
        }
      }
      bool passes = true;
      // The varchar conjuncts: the mask answers them without decoding a
      // string into the fragment.
      for (size_t vi = 0; vi < varchar_masks.size(); ++vi) {
        const PaxSelectionVector& mask = varchar_masks[vi];
        const uint64_t global = base + r;
        if (global >= mask.bits.size() * 64) {
          passes = false;
          break;
        }
        const bool marked = (mask.bits[global >> 6] >> (global & 63)) & 1;
        if (!marked) {
          passes = false;
          break;
        }
      }
      if (!passes) {
        state->rows_proved_fail += 1;
        continue;
      }
      for (const Conjunct& c : eval) {
        if (c.kind == ConjunctKind::kIsNull) {
          const int ci = NeededIndex(needed, c.col);
          if (ci < 0) continue;
          const bool is_null = column_nulls[static_cast<size_t>(ci)][r] != 0;
          if (is_null ==
              c.negated) {  // IS NULL wants null, IS NOT NULL wants not
            passes = false;
            break;
          }
          continue;
        }
        const int ci = NeededIndex(needed, c.col);
        if (ci < 0) continue;
        const int64_t value = column_values[static_cast<size_t>(ci)][r];
        bool holds = false;
        if (c.kind == ConjunctKind::kCompare) {
          holds = CompareOpHolds(c.op, value, c.int64_literal);
        } else {
          holds = InListHolds(c.in_literals, value);
        }
        if (!holds) {
          passes = false;
          break;
        }
      }
      if (!passes) {
        state->rows_proved_fail += 1;
        continue;
      }
      // The row's values go into the flat buffer, column after column.
      row_values.resize(needed.size());
      row_nulls.resize(needed.size());
      for (size_t ci = 0; ci < needed.size(); ++ci) {
        row_values[ci] = column_values[ci][r];
        row_nulls[ci] = column_nulls[ci][r];
      }
      SurvivingRow row;
      row.generation = footer.generation;
      row.artifact_index = artifact_index;
      row.row_index = global_row;
      row.value_offset = state->AppendValues(row_values.data(),
                                             row_nulls.data(), needed.size());
      row.value_count = static_cast<uint32_t>(needed.size());
      if (key_idx >= 0) {
        row.key = row_values[static_cast<size_t>(key_idx)];
        row.has_key = true;
      }
      state->rows.push_back(std::move(row));
    }
  }
}

// Scans a sorted run artifact. A run has no compressed-domain kernel: every
// row is decoded and compared, which is the DecodeScan tier.
void ScanRun(const SortedRun& run, const Schema& sc, const Fragment& frag,
             const std::vector<Conjunct>& eval,
             const std::vector<Conjunct>& varchar_eval,
             const std::vector<slot_t>& needed, uint32_t artifact_index,
             ScanState* state) {
  const slot_t key_col = sc.KeyColumnIndex();
  const int key_idx = NeededIndex(needed, key_col);
  std::vector<int64_t> row_values;  // one surviving row's values
  std::vector<uint8_t> row_nulls;   // and its NULL flags
  size_t begin = 0;
  size_t end = run.Size();
  if (frag.access == AccessMode::kKeyRange && IsInt64Column(sc, key_col)) {
    // The memcomparable encoding of an int64 is order preserving, so the
    // closed interval maps to a closed interval of the run's key space.
    const std::string lo_key =
        MemcomparableInt64KeyOf(frag.range_lo.value_or(INT64_MIN));
    const std::string hi_key =
        MemcomparableInt64KeyOf(frag.range_hi.value_or(INT64_MAX));
    begin = run.LowerBound(lo_key);
    // LowerBound returns the first entry >= lo_key; the upper bound is
    // exclusive in the run's key space, so search for hi_key + 1 when the
    // maximum is not the bound itself.
    end = (frag.range_hi.has_value() && *frag.range_hi == INT64_MAX)
              ? run.Size()
              : run.LowerBound(hi_key);
  }
  state->bytes_touched += run.Blob().size();
  for (size_t i = begin; i < end && i < run.Size(); ++i) {
    state->rows_in += 1;
    if (run.GetEntry(i).IsDeleted()) {
      // A tombstone hides the key group: it is not a surviving row, but it
      // still has to shadow the older layers.
      SurvivingRow row;
      row.deleted = true;
      row.generation = run.Generation();
      row.artifact_index = artifact_index;
      row.row_index = i;
      const std::string key = run.GetEntry(i).BuildKey(run.Blob());
      if (MemcomparableInt64Key(key, &row.key)) row.has_key = true;
      state->rows.push_back(std::move(row));
      continue;
    }
    Row decoded;
    if (!run.GetEntry(i).BuildRowInto(run.Blob(), sc, &decoded)) {
      state->rows_unknown += 1;
      continue;
    }
    state->rows_decoded += 1;
    bool passes = true;
    // The varchar conjuncts first: they read the string cell directly.
    for (const Conjunct& c : varchar_eval) {
      if (c.col >= decoded.Count()) {
        passes = false;
        break;
      }
      const Value& v = decoded[c.col];
      const std::string_view text =
          v.IsNull() ? std::string_view() : v.AsStringView();
      const bool hit =
          c.kind == ConjunctKind::kVarcharRange
              ? VarcharRangeHolds(text, c.bytes_literal, c.bytes_literal_hi)
              : CompareOpHolds(c.op, text, c.bytes_literal);
      if (!hit) {
        passes = false;
        break;
      }
    }
    if (!passes) {
      state->rows_proved_fail += 1;
      continue;
    }
    for (const Conjunct& c : eval) {
      if (c.col >= decoded.Count()) {
        passes = false;
        break;
      }
      const Value& v = decoded[c.col];
      if (c.kind == ConjunctKind::kIsNull) {
        if (v.IsNull() == c.negated) {
          passes = false;
          break;
        }
        continue;
      }
      if (!v.IsNull() && v.type == ValueType::kInt64) {
        const int64_t value = v.AsInt64();
        const bool holds = c.kind == ConjunctKind::kCompare
                               ? CompareOpHolds(c.op, value, c.int64_literal)
                               : InListHolds(c.in_literals, value);
        if (!holds) {
          passes = false;
          break;
        }
        continue;
      }
      if (!v.IsNull() && v.type == ValueType::kInt64) {
        passes = false;
        break;
      }
      // A non-int64 cell: the conjunct is not evaluable here.
      passes = false;
      break;
    }
    if (!passes) {
      state->rows_proved_fail += 1;
      continue;
    }
    row_values.resize(needed.size());
    row_nulls.resize(needed.size());
    for (size_t ci = 0; ci < needed.size(); ++ci) {
      const slot_t col = needed[ci];
      if (col < decoded.Count() && decoded[col].type == ValueType::kInt64) {
        row_values[ci] = decoded[col].AsInt64();
        row_nulls[ci] = decoded[col].IsNull() ? 1 : 0;
      } else {
        row_values[ci] = 0;
        row_nulls[ci] = 1;
      }
    }
    SurvivingRow row;
    row.generation = run.Generation();
    row.artifact_index = artifact_index;
    row.row_index = i;
    row.value_offset =
        state->AppendValues(row_values.data(), row_nulls.data(), needed.size());
    row.value_count = static_cast<uint32_t>(needed.size());
    if (key_idx >= 0 && key_idx < static_cast<int>(needed.size())) {
      row.key = row_values[static_cast<size_t>(key_idx)];
      row.has_key = true;
    }
    state->rows.push_back(std::move(row));
  }
}

// Scans a memtable snapshot artifact.
void ScanMemSnapshot(const std::vector<std::pair<std::string, RowEntry>>& rows,
                     const Schema& sc, const std::vector<Conjunct>& eval,
                     const std::vector<Conjunct>& varchar_eval,
                     const std::vector<slot_t>& needed, uint32_t artifact_index,
                     ScanState* state) {
  const slot_t key_col = sc.KeyColumnIndex();
  const int key_idx = NeededIndex(needed, key_col);
  std::vector<int64_t> row_values;  // one surviving row's values
  std::vector<uint8_t> row_nulls;   // and its NULL flags
  for (size_t ri = 0; ri < rows.size(); ++ri) {
    const std::string& key = rows[ri].first;
    const RowEntry& entry = rows[ri].second;
    state->rows_in += 1;
    if (entry.is_deleted) {
      SurvivingRow row;
      row.deleted = true;
      row.artifact_index = artifact_index;
      row.row_index = static_cast<uint64_t>(ri);
      if (MemcomparableInt64Key(key, &row.key)) row.has_key = true;
      state->rows.push_back(std::move(row));
      continue;
    }
    const Row& decoded = entry.row;
    state->rows_decoded += 1;
    bool passes = true;
    for (const Conjunct& c : varchar_eval) {
      if (c.col >= decoded.Count()) {
        passes = false;
        break;
      }
      const Value& v = decoded[c.col];
      const std::string_view text =
          v.IsNull() ? std::string_view() : v.AsStringView();
      const bool hit =
          c.kind == ConjunctKind::kVarcharRange
              ? VarcharRangeHolds(text, c.bytes_literal, c.bytes_literal_hi)
              : CompareOpHolds(c.op, text, c.bytes_literal);
      if (!hit) {
        passes = false;
        break;
      }
    }
    if (!passes) {
      state->rows_proved_fail += 1;
      continue;
    }
    for (const Conjunct& c : eval) {
      if (c.col >= decoded.Count()) {
        passes = false;
        break;
      }
      const Value& v = decoded[c.col];
      if (c.kind == ConjunctKind::kIsNull) {
        if (v.IsNull() == c.negated) {
          passes = false;
          break;
        }
        continue;
      }
      if (v.IsNull() || v.type != ValueType::kInt64) {
        passes = false;
        break;
      }
      const int64_t value = v.AsInt64();
      const bool holds = c.kind == ConjunctKind::kCompare
                             ? CompareOpHolds(c.op, value, c.int64_literal)
                             : InListHolds(c.in_literals, value);
      if (!holds) {
        passes = false;
        break;
      }
    }
    if (!passes) {
      state->rows_proved_fail += 1;
      continue;
    }
    row_values.resize(needed.size());
    row_nulls.resize(needed.size());
    for (size_t ci = 0; ci < needed.size(); ++ci) {
      const slot_t col = needed[ci];
      if (col < decoded.Count() && decoded[col].type == ValueType::kInt64) {
        row_values[ci] = decoded[col].AsInt64();
        row_nulls[ci] = decoded[col].IsNull() ? 1 : 0;
      } else {
        row_values[ci] = 0;
        row_nulls[ci] = 1;
      }
    }
    SurvivingRow row;
    row.artifact_index = artifact_index;
    row.row_index = static_cast<uint64_t>(ri);
    row.value_offset =
        state->AppendValues(row_values.data(), row_nulls.data(), needed.size());
    row.value_count = static_cast<uint32_t>(needed.size());
    if (key_idx >= 0 && key_idx < static_cast<int>(needed.size())) {
      row.key = row_values[static_cast<size_t>(key_idx)];
      row.has_key = true;
    }
    state->rows.push_back(std::move(row));
  }
}

// Folds the artifacts together: the newest generation wins on a key, and a
// tombstone hides the whole key group. Returns the surviving rows.
std::vector<SurvivingRow> ResolveDuplicates(std::vector<SurvivingRow> rows,
                                            bool dedup_possible) {
  if (!dedup_possible) return rows;
  // The rows arrive artifact by artifact; the artifacts are not ordered by
  // generation, so pick the winner per key explicitly.
  std::unordered_map<int64_t, size_t> winner;
  std::vector<uint8_t> dropped(rows.size(), 0);
  for (size_t i = 0; i < rows.size(); ++i) {
    if (!rows[i].has_key) continue;
    const auto it = winner.find(rows[i].key);
    if (it == winner.end()) {
      winner.emplace(rows[i].key, i);
      continue;
    }
    const size_t prev = it->second;
    if (rows[i].generation > rows[prev].generation) {
      // A newer generation wins: the older row is shadowed.
      dropped[prev] = 1;
      it->second = i;
    } else {
      // An older generation loses, and an equal generation breaks the tie by
      // input order (the first wins). Either way the current row is dropped.
      dropped[i] = 1;
    }
  }
  std::vector<SurvivingRow> out;
  out.reserve(rows.size());
  for (size_t i = 0; i < rows.size(); ++i) {
    if (dropped[i] != 0) continue;
    if (rows[i].deleted) continue;  // the tombstone hides the key group
    out.push_back(std::move(rows[i]));
  }
  return out;
}

// ---------------------------------------------------------------------------
// Aggregation
// ---------------------------------------------------------------------------

// The accumulator of one group (or of the whole fragment when there is no
// GROUP BY).
struct Accumulator {
  int64_t sum = 0;
  int64_t min_val = 0;
  int64_t max_val = 0;
  uint64_t count = 0;
  bool has_value = false;
};

// The value and NULL lookups of a row: the needed-column INDEX of a column
// is resolved once (before the row loop) and the value is read from the
// scan state's flat buffer.
struct RowReader {
  const std::vector<int64_t>* flat_values{nullptr};
  const std::vector<uint8_t>* flat_nulls{nullptr};
  const std::vector<slot_t>* needed{nullptr};

  [[nodiscard]] int64_t Value(const SurvivingRow& row, slot_t col) const {
    const int idx = NeededIndex(*needed, col);
    if (idx < 0) return 0;
    return (*flat_values)[row.value_offset + static_cast<uint32_t>(idx)];
  }
  [[nodiscard]] bool IsNull(const SurvivingRow& row, slot_t col) const {
    const int idx = NeededIndex(*needed, col);
    if (idx < 0) return true;
    return (*flat_nulls)[row.value_offset + static_cast<uint32_t>(idx)] != 0;
  }
};

void AccumulateRow(const Aggregate& agg, const SurvivingRow& row,
                   const RowReader& reader, Accumulator* acc) {
  const auto value_of = [&](slot_t col) -> int64_t {
    return reader.Value(row, col);
  };
  const auto null_of = [&](slot_t col) -> bool {
    return reader.IsNull(row, col);
  };
  switch (agg.kind) {
    case AggregateKind::kCountStar:
      acc->count += 1;
      break;
    case AggregateKind::kCountCol:
      if (!null_of(agg.col)) acc->count += 1;
      break;
    case AggregateKind::kSum: {
      if (agg.terms.empty()) {
        if (null_of(agg.col)) break;
        acc->sum += agg.coeff * value_of(agg.col);
        acc->count += 1;
        break;
      }
      for (const AggTerm& term : agg.terms) {
        __int128 product = term.coeff;
        bool null_term = false;
        for (slot_t col : term.cols) {
          if (null_of(col)) {
            null_term = true;
            break;
          }
          product *= value_of(col);
        }
        if (null_term) continue;
        acc->sum += static_cast<int64_t>(product);
      }
      acc->count += 1;
      break;
    }
    case AggregateKind::kMin:
      if (null_of(agg.col)) break;
      if (!acc->has_value || value_of(agg.col) < acc->min_val) {
        acc->min_val = value_of(agg.col);
      }
      acc->has_value = true;
      acc->count += 1;
      break;
    case AggregateKind::kMax:
      if (null_of(agg.col)) break;
      if (!acc->has_value || value_of(agg.col) > acc->max_val) {
        acc->max_val = value_of(agg.col);
      }
      acc->has_value = true;
      acc->count += 1;
      break;
    case AggregateKind::kApproxCountDistinct:
      acc->count += 1;
      break;
    case AggregateKind::kNone:
      break;
  }
}

// The group key of a row: the values of the GROUP BY columns.
std::vector<int64_t> GroupKeyOf(const SurvivingRow& row,
                                const std::vector<slot_t>& group_cols,
                                const RowReader& reader) {
  std::vector<int64_t> key;
  key.reserve(group_cols.size());
  for (slot_t col : group_cols) {
    key.push_back(reader.Value(row, col));
  }
  return key;
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

// Returns kSuccess when the fragment is well formed, kNotImplemented when a
// part of it is outside the IR's contract (the executor then leaves it to
// the residual path), and kCorruption when the fragment is malformed.
Status ValidateFragment(const Schema& sc, const Fragment& frag) {
  if (frag.top_k.has_value() && frag.limit_rows.has_value()) {
    return Status::kCorruption;  // the IR forbids both at once
  }
  if (frag.access == AccessMode::kPoint && !frag.point_key.has_value()) {
    return Status::kCorruption;
  }
  if (frag.access == AccessMode::kKeyRange) {
    if (frag.range_lo.has_value() && frag.range_hi.has_value() &&
        *frag.range_lo > *frag.range_hi) {
      return Status::kCorruption;
    }
  }
  for (const Conjunct& c : frag.conjuncts) {
    if (c.col >= sc.ColumnCount()) return Status::kCorruption;
    if (c.kind == ConjunctKind::kIn && c.in_literals.empty()) {
      return Status::kCorruption;
    }
  }
  for (slot_t c : frag.projection) {
    if (c >= sc.ColumnCount()) return Status::kCorruption;
  }
  for (slot_t c : frag.group_cols) {
    if (c >= sc.ColumnCount()) return Status::kCorruption;
  }
  if (frag.top_k.has_value() && frag.top_k->col >= sc.ColumnCount()) {
    return Status::kCorruption;
  }
  if (frag.aggregate.has_value()) {
    const Aggregate& agg = *frag.aggregate;
    if (agg.kind == AggregateKind::kCountCol ||
        agg.kind == AggregateKind::kMin || agg.kind == AggregateKind::kMax ||
        agg.kind == AggregateKind::kApproxCountDistinct) {
      if (agg.col >= sc.ColumnCount()) return Status::kCorruption;
    }
    for (const AggTerm& t : agg.terms) {
      for (slot_t c : t.cols) {
        if (c >= sc.ColumnCount()) return Status::kCorruption;
      }
    }
  }
  return Status::kSuccess;
}

// The aggregate kinds this runtime can push down.
bool AggregateSupported(const Capabilities& caps, const Aggregate& agg) {
  switch (agg.kind) {
    case AggregateKind::kNone:
      return true;
    case AggregateKind::kCountStar:
      return caps.agg_count_star;
    case AggregateKind::kCountCol:
      return caps.agg_count_col;
    case AggregateKind::kSum:
      return caps.agg_sum;
    case AggregateKind::kMin:
      return caps.agg_min;
    case AggregateKind::kMax:
      return caps.agg_max;
    case AggregateKind::kApproxCountDistinct:
      return caps.agg_approx_count_distinct;
  }
  return false;
}

// The output shape a fragment produces.
OutputKind OutputKindOf(const Fragment& frag) {
  if (frag.top_k.has_value()) return OutputKind::kTopK;
  if (!frag.group_cols.empty()) return OutputKind::kGroups;
  if (frag.aggregate.has_value() &&
      frag.aggregate->kind != AggregateKind::kNone) {
    return OutputKind::kScalar;
  }
  if (!frag.projection.empty()) return OutputKind::kProjectedBatch;
  return OutputKind::kSelection;
}

// Scans every artifact into the scan state. The MVCC bound and the access
// mode are applied here; the filter and the LSM resolution follow.
void ScanArtifacts(const ArtifactSet& artifacts, const Fragment& frag,
                   const Schema& sc, const std::vector<Conjunct>& eval,
                   const std::vector<Conjunct>& varchar_eval,
                   const std::vector<slot_t>& needed, ScanState* state) {
  for (size_t ai = 0; ai < artifacts.size(); ++ai) {
    const ArtifactRef& a = artifacts[ai];
    if (a.generation > frag.read_version) continue;  // MVCC bound
    const uint32_t artifact_index = static_cast<uint32_t>(ai);
    if (a.kind == ArtifactRef::Kind::kChunk && a.chunk != nullptr) {
      ScanChunk(*a.chunk, sc, frag, eval, varchar_eval, needed, artifact_index,
                state);
    } else if (a.kind == ArtifactRef::Kind::kRun && a.run != nullptr) {
      ScanRun(*a.run, sc, frag, eval, varchar_eval, needed, artifact_index,
              state);
    } else if (a.kind == ArtifactRef::Kind::kMemSnapshot &&
               a.mem_rows != nullptr) {
      ScanMemSnapshot(*a.mem_rows, sc, eval, varchar_eval, needed,
                      artifact_index, state);
    }
  }
}

}  // namespace

// ---------------------------------------------------------------------------
// The public entry points
// ---------------------------------------------------------------------------

Capabilities GetCapabilities(Runtime runtime) {
  Capabilities caps;
  caps.runtime = runtime;
  // The native library and the WASM module expose the same IR; the WASM
  // module's kernel coverage is narrower (see docs/08-wasm-pushdown.md), so
  // the bits that depend on a WASM kernel are off for the WASM runtime.
  // The v1 IR's int64 predicates, aggregates and the single-column GROUP BY
  // all have a native kernel and are shared by both runtimes.
  caps.predicates_compare = true;
  caps.predicates_in = true;
  caps.predicates_is_null = true;
  // The varchar family needs the SELECTION kernels (the row ids, not just
  // the count). The native side has them; the WASM module has the COUNT
  // kernels only, so a varchar conjunct is residual there until the
  // selection export lands.
  caps.predicates_varchar = runtime == Runtime::kNative;
  caps.agg_count_star = true;
  caps.agg_count_col = true;
  caps.agg_sum = true;
  caps.agg_min = true;
  caps.agg_max = true;
  caps.agg_approx_count_distinct = runtime == Runtime::kNative;
  caps.group_by_single_column = true;
  caps.group_by_multi_column = true;
  caps.order_limit_single_column = true;
  caps.artifacts_chunk = true;
  caps.artifacts_run = true;
  caps.artifacts_memtable = true;
  return caps;
}

double Calibrate(Runtime runtime) {
  // The calibration is a fixed PLAIN microbenchmark: compare one int64
  // against one literal, N times. The result is the nanoseconds per cpu_unit
  // of THIS runtime, so the optimizer can compare a native fragment with a
  // WASM one in the same unit.
  static const double kMeasured[] = {
      1.0,  // native (placeholder until the benchmark is wired)
      1.0,  // wasm
  };
  const size_t idx = runtime == Runtime::kNative ? 0 : 1;
  volatile uint64_t sink = 0;
  const auto start = std::chrono::steady_clock::now();
  int64_t values[1024];
  for (int i = 0; i < 1024; ++i) values[i] = i;
  constexpr int kRounds = 20000;
  for (int r = 0; r < kRounds; ++r) {
    for (int i = 0; i < 1024; ++i) {
      sink += static_cast<uint64_t>(values[i] < 512);
    }
  }
  const auto end = std::chrono::steady_clock::now();
  const double elapsed_ns =
      std::chrono::duration<double, std::nano>(end - start).count();
  const double comparisons = static_cast<double>(kRounds) * 1024.0;
  if (comparisons <= 0) return 1.0;
  const double ns_per_comparison = elapsed_ns / comparisons;
  (void)sink;
  if (ns_per_comparison <= 0) return 1.0;
  // kMeasured holds the reference ns/cpu_unit of the runtime's reference
  // machine; the measured value is normalized onto it so the ratio is
  // stable across machines.
  return ns_per_comparison / kMeasured[idx];
}

std::vector<ConjunctQuote> Quote(Runtime runtime, const ArtifactSet& artifacts,
                                 const std::vector<Conjunct>& conjuncts) {
  std::vector<ConjunctQuote> quotes;
  quotes.reserve(conjuncts.size());
  const Schema* schema = nullptr;
  for (const ArtifactRef& a : artifacts) {
    if (a.schema != nullptr) {
      schema = a.schema;
      break;
    }
  }
  for (size_t i = 0; i < conjuncts.size(); ++i) {
    const Conjunct& c = conjuncts[i];
    ConjunctQuote q;
    q.index = i;
    q.klass = QuoteClass::kUnsupported;
    if (schema != nullptr && ConjunctSupported(runtime, *schema, c)) {
      // The classification walks the artifacts' footers: the cheapest
      // kernel that can answer the conjunct wins.
      bool is_index = false;
      bool is_metadata = false;
      bool is_compressed = false;
      double rows = 0;
      double bytes = 0;
      double per_row = 1.0;
      for (const ArtifactRef& a : artifacts) {
        if (a.kind != ArtifactRef::Kind::kChunk || a.chunk == nullptr) {
          // A run or a memtable snapshot has no index and no footer
          // statistics: it is always a DecodeScan.
          continue;
        }
        const PaxChunk& chunk = *a.chunk;
        const PaxChunk::Footer& footer = chunk.GetFooter();
        const slot_t key_col = schema->KeyColumnIndex();
        if (c.kind == ConjunctKind::kCompare && c.op == CompareOp::kEq &&
            c.col == key_col && footer.hash_idx_present) {
          is_index = true;
        }
        if (c.kind == ConjunctKind::kCompare && c.op == CompareOp::kEq &&
            FindInvertedCell(footer, 0, c.col) != nullptr) {
          is_index = true;
        }
        // A metadata answer: the extended statistics know the column's
        // minimum, maximum and mode.
        if (footer.ext_stats_present && c.col < footer.col_modes.size() &&
            c.kind == ConjunctKind::kCompare) {
          if (c.op == CompareOp::kEq &&
              c.int64_literal == footer.col_modes[c.col]) {
            is_metadata = true;
          }
          if (c.col < footer.col_mins.size() &&
              c.col < footer.col_maxs.size()) {
            const int64_t mn = footer.col_mins[c.col];
            const int64_t mx = footer.col_maxs[c.col];
            if ((c.op == CompareOp::kLt && c.int64_literal <= mn) ||
                (c.op == CompareOp::kLe && c.int64_literal < mn) ||
                (c.op == CompareOp::kGt && c.int64_literal >= mx) ||
                (c.op == CompareOp::kGe && c.int64_literal > mx)) {
              is_metadata = true;
            }
          }
        }
        const uint32_t groups = footer.row_group_count;
        for (uint32_t g = 0; g < groups; ++g) {
          if (g >= footer.row_counts.size()) break;
          const uint16_t rc = footer.row_counts[g];
          if (rc == 0) continue;
          rows += rc;
          const size_t idx =
              static_cast<size_t>(g) * footer.column_count + c.col;
          if (idx < footer.encodings.size()) {
            const auto scheme =
                static_cast<PaxChunk::Scheme>(footer.encodings[idx]);
            if (SchemeHasCompressedDomainKernel(scheme)) is_compressed = true;
            per_row = std::min(per_row, PaxChunk::SchemeDecodeCost(scheme));
          }
          bytes += static_cast<double>(ChunkCellBytes(chunk, g, c.col));
        }
      }
      if (is_index) {
        q.klass = QuoteClass::kIndex;
        q.cpu_units_if_first = 4.0;
        q.bytes_if_first = 0.0;
        q.rg_skip_fraction = 1.0;
      } else if (is_metadata) {
        q.klass = QuoteClass::kMetadata;
        q.cpu_units_if_first = 2.0;
        q.bytes_if_first = 0.0;
        q.rg_skip_fraction = 1.0;
      } else if (is_compressed) {
        q.klass = QuoteClass::kCompressedScan;
        q.cpu_units_if_first = rows * per_row;
        q.bytes_if_first = bytes;
        q.rg_skip_fraction = 0.0;
      } else {
        q.klass = QuoteClass::kDecodeScan;
        q.cpu_units_if_first = rows * per_row;
        q.bytes_if_first = bytes;
        q.rg_skip_fraction = 0.0;
      }
    }
    quotes.push_back(q);
  }
  (void)runtime;
  return quotes;
}

CostReport Estimate(Runtime runtime, const ArtifactSet& artifacts,
                    const Fragment& fragment) {
  CostReport report;
  const Capabilities caps = GetCapabilities(runtime);
  report.output_kind = OutputKindOf(fragment);
  if (artifacts.empty()) {
    report.coverage = Coverage::kNone;
    return report;
  }
  // The classifier needs the schema, which the artifact set carries through
  // its per-artifact schema pointer. When the artifacts disagree, the
  // first schema wins (the caller is expected to pass a consistent set).
  const Schema* schema = nullptr;
  for (const ArtifactRef& a : artifacts) {
    if (a.schema != nullptr) {
      schema = a.schema;
      break;
    }
  }
  if (schema == nullptr) {
    // No schema: the estimate cannot classify the conjuncts, so it reports
    // the safe answer (nothing is pushed down).
    report.coverage = Coverage::kNone;
    return report;
  }
  const Schema& sc = *schema;
  const std::vector<slot_t> needed = NeededColumns(sc, fragment);
  std::vector<size_t> residual;
  for (size_t i = 0; i < fragment.conjuncts.size(); ++i) {
    if (!ConjunctSupported(runtime, sc, fragment.conjuncts[i])) {
      residual.push_back(i);
    }
  }
  report.residual_conjuncts = residual;

  const std::vector<Conjunct> eval = [&] {
    std::vector<Conjunct> out;
    for (size_t i = 0; i < fragment.conjuncts.size(); ++i) {
      if (std::find(residual.begin(), residual.end(), i) == residual.end()) {
        out.push_back(fragment.conjuncts[i]);
      }
    }
    return out;
  }();

  for (const ArtifactRef& a : artifacts) {
    if (a.kind == ArtifactRef::Kind::kChunk && a.chunk != nullptr) {
      const PaxChunk& chunk = *a.chunk;
      const PaxChunk::Footer& footer = chunk.GetFooter();
      const uint32_t groups = footer.row_group_count;
      for (uint32_t g = 0; g < groups; ++g) {
        if (g >= footer.row_counts.size()) break;
        const uint16_t rc = footer.row_counts[g];
        if (rc == 0) continue;
        if (fragment.access == AccessMode::kKeyRange) {
          int64_t klo = 0;
          int64_t khi = 0;
          if (ChunkKeyRange(chunk, g, &klo, &khi)) {
            if (fragment.range_lo.has_value() && khi < *fragment.range_lo)
              continue;
            if (fragment.range_hi.has_value() && *fragment.range_hi < klo)
              continue;
          }
        }
        report.rows_in += rc;
        bool group_empty = false;
        for (const Conjunct& c : eval) {
          if (c.kind == ConjunctKind::kIsNull) continue;
          if (!IsInt64Column(sc, c.col)) continue;
          if (ChunkConjunctProvesEmpty(chunk, g, c.col, c)) {
            group_empty = true;
            break;
          }
        }
        if (group_empty) {
          report.rows_proved_fail += rc;
          continue;
        }
        report.rows_unknown += rc;
        for (slot_t col : needed) {
          report.bytes_touched += ChunkCellBytes(chunk, g, col);
        }
        // The CPU model: one unit per row compared, scaled by the scheme's
        // expansion cost, plus a fixed per-group dispatch cost.
        double per_row = 1.0;
        const size_t idx = static_cast<size_t>(g) * footer.column_count;
        if (idx < footer.encodings.size()) {
          per_row = PaxChunk::SchemeDecodeCost(
              static_cast<PaxChunk::Scheme>(footer.encodings[idx]));
        }
        report.cpu_units += static_cast<double>(rc) * per_row *
                            static_cast<double>(eval.size());
        report.cpu_units += 8.0;  // per-group dispatch
      }
    } else if (a.kind == ArtifactRef::Kind::kRun && a.run != nullptr) {
      report.rows_in += a.run->Size();
      report.rows_unknown += a.run->Size();
      report.bytes_touched += a.run->Blob().size();
      report.cpu_units +=
          static_cast<double>(a.run->Size()) * static_cast<double>(eval.size());
    } else if (a.kind == ArtifactRef::Kind::kMemSnapshot &&
               a.mem_rows != nullptr) {
      report.rows_in += a.mem_rows->size();
      report.rows_unknown += a.mem_rows->size();
      report.cpu_units += static_cast<double>(a.mem_rows->size()) *
                          static_cast<double>(eval.size());
    }
  }
  if (report.rows_unknown == 0 && report.rows_in > 0 &&
      fragment.conjuncts.empty()) {
    report.rows_proved_pass = report.rows_in;
    report.rows_unknown = 0;
  }
  if (residual.empty()) {
    report.coverage = Coverage::kFull;
    report.duplicate_elimination = DuplicateElimination::kApplied;
  } else {
    report.coverage = Coverage::kPartial;
    report.duplicate_elimination = DuplicateElimination::kNotAccounted;
  }
  report.output_rows = report.rows_unknown;
  report.cardinality = Cardinality::kNotAccounted;
  report.output_bytes = report.output_rows * 8;
  (void)caps;
  return report;
}

StatusOr<ExecResult> Execute(Runtime runtime, const ArtifactSet& artifacts,
                             const Fragment& fragment, const Schema& schema) {
  ExecResult result;
  const Status valid = ValidateFragment(schema, fragment);
  if (valid != Status::kSuccess) {
    result.status = valid;
    result.rejected = true;
    result.coverage = Coverage::kNone;
    return result;
  }
  const Capabilities caps = GetCapabilities(runtime);
  result.output_kind = OutputKindOf(fragment);

  // Split the conjuncts into the ones the storage evaluates and the ones
  // that stay residual.
  std::vector<size_t> residual;
  std::vector<Conjunct> eval;
  std::vector<Conjunct> varchar_eval;
  for (size_t i = 0; i < fragment.conjuncts.size(); ++i) {
    const Conjunct& c = fragment.conjuncts[i];
    if (!ConjunctSupported(runtime, schema, c)) {
      residual.push_back(i);
      continue;
    }
    // The varchar conjuncts are answered by the chunk's selection kernels
    // (or by the decoded row on the run/memtable paths); the numeric ones
    // by the per-row-group decode.
    const bool is_varchar = c.kind == ConjunctKind::kVarcharRange ||
                            (c.kind == ConjunctKind::kCompare &&
                             c.literal_kind == LiteralKind::kBytes);
    if (is_varchar) {
      varchar_eval.push_back(c);
    } else {
      eval.push_back(c);
    }
  }
  result.residual_conjuncts = residual;

  // The aggregate and the group-by must be pushable.
  bool aggregate_pushed = true;
  if (fragment.aggregate.has_value()) {
    aggregate_pushed = AggregateSupported(caps, *fragment.aggregate);
  }
  const bool group_pushed =
      fragment.group_cols.empty() || caps.group_by_single_column;

  // The Top-K fast path: a single chunk, no filter, no aggregate and no
  // group by. The chunk's Top-K kernel prunes the row groups whose
  // zone-map range cannot improve the current threshold, so the SCAN is
  // skipped entirely: collecting every surviving row first (as the
  // generic path does) would throw the pruning away and decode the whole
  // column for nothing.
  if (fragment.top_k.has_value() && fragment.conjuncts.empty() &&
      !fragment.aggregate.has_value() && fragment.group_cols.empty() &&
      artifacts.size() == 1 && artifacts[0].kind == ArtifactRef::Kind::kChunk &&
      artifacts[0].chunk != nullptr) {
    const TopKSpec& spec = *fragment.top_k;
    const std::vector<TopKEntry> entries = TopKColumn(
        *artifacts[0].chunk, spec.col, spec.k, spec.descending, &schema);
    result.top_k = entries;
    result.output_rows = entries.size();
    result.coverage = Coverage::kFull;
    return result;
  }

  const std::vector<slot_t> needed = NeededColumns(schema, fragment);
  ScanState state;
  state.key_dedup_possible = IsInt64Column(schema, schema.KeyColumnIndex());
  ScanArtifacts(artifacts, fragment, schema, eval, varchar_eval, needed,
                &state);

  // The LSM resolution: the newest generation wins, a tombstone hides the
  // key group. The rows the resolution removes are counted as proved-fail:
  // the storage PROVED they do not contribute to the output.
  const size_t scanned = state.rows.size();
  std::vector<SurvivingRow> rows =
      ResolveDuplicates(std::move(state.rows), state.key_dedup_possible);
  result.rows_proved_fail = state.rows_proved_fail + (scanned - rows.size());
  result.rows_decoded = state.rows_decoded;
  if (residual.empty()) {
    // The storage evaluated every conjunct: the surviving rows are proved.
    result.rows_proved_pass = rows.size();
    result.rows_unknown = state.rows_unknown;
  } else {
    // A residual conjunct was NOT evaluated, so the surviving rows are not
    // proved either: the executor applies the residual conjuncts to them.
    result.rows_proved_pass = 0;
    result.rows_unknown = state.rows_unknown + rows.size();
  }
  result.coverage = residual.empty() ? Coverage::kFull : Coverage::kPartial;

  // LIMIT applies after the filter (and to the groups when there is a
  // GROUP BY).
  if (fragment.limit_rows.has_value() && fragment.group_cols.empty() &&
      rows.size() > *fragment.limit_rows) {
    rows.resize(*fragment.limit_rows);
  }

  // The GROUP BY. A single chunk artifact goes through the chunk's own
  // N-key GROUP BY kernel; several artifacts are accumulated here.
  if (!fragment.group_cols.empty() && group_pushed) {
    // The single-chunk fast path: the chunk kernel owns the per-scheme
    // folding (DICT interning, RLE runs) and the composite key table.
    if (artifacts.size() == 1 &&
        artifacts[0].kind == ArtifactRef::Kind::kChunk &&
        artifacts[0].chunk != nullptr && fragment.conjuncts.empty() &&
        fragment.aggregate.has_value() &&
        fragment.aggregate->kind == AggregateKind::kSum) {
      const std::vector<GroupByEntryN> entries =
          GroupBySumColumns(*artifacts[0].chunk, fragment.group_cols,
                            fragment.aggregate->col, &schema);
      for (const GroupByEntryN& e : entries) {
        GroupOutput out;
        out.key = e.key;
        out.sum = e.sum;
        out.count = e.count;
        result.groups.push_back(std::move(out));
      }
      if (fragment.limit_rows.has_value() &&
          result.groups.size() > *fragment.limit_rows) {
        result.groups.resize(*fragment.limit_rows);
      }
      result.output_rows = result.groups.size();
      result.scalar_count = result.groups.size();
      result.rows_proved_pass = rows.size();
      return result;
    }
    const RowReader reader{&state.values, &state.nulls, &needed};
    std::map<std::vector<int64_t>, Accumulator> groups;
    for (const SurvivingRow& row : rows) {
      const std::vector<int64_t> key =
          GroupKeyOf(row, fragment.group_cols, reader);
      Accumulator& acc = groups[key];
      if (fragment.aggregate.has_value()) {
        AccumulateRow(*fragment.aggregate, row, reader, &acc);
      } else {
        acc.count += 1;
      }
    }
    for (const auto& [key, acc] : groups) {
      GroupOutput out;
      out.key = key;
      out.sum = acc.sum;
      out.min_val = acc.min_val;
      out.max_val = acc.max_val;
      out.count = acc.count;
      result.groups.push_back(std::move(out));
    }
    if (fragment.limit_rows.has_value() &&
        result.groups.size() > *fragment.limit_rows) {
      result.groups.resize(*fragment.limit_rows);
    }
    result.output_rows = result.groups.size();
    result.scalar_count = result.groups.size();
    return result;
  }

  // The scalar aggregate.
  if (fragment.aggregate.has_value() && aggregate_pushed) {
    const RowReader reader{&state.values, &state.nulls, &needed};
    Accumulator acc;
    for (const SurvivingRow& row : rows) {
      AccumulateRow(*fragment.aggregate, row, reader, &acc);
    }
    result.scalar_value = acc.sum;
    result.scalar_count = acc.count;
    if (fragment.aggregate->kind == AggregateKind::kMin) {
      result.scalar_value = acc.min_val;
    } else if (fragment.aggregate->kind == AggregateKind::kMax) {
      result.scalar_value = acc.max_val;
    }
    result.output_rows = 1;
    return result;
  }

  // ORDER BY <col> LIMIT k.
  if (fragment.top_k.has_value()) {
    const TopKSpec& spec = *fragment.top_k;
    // The single-chunk fast path: the chunk's Top-K kernel prunes the row
    // groups whose zone-map range cannot improve the current threshold, so
    // it reads less than the whole column.
    if (artifacts.size() == 1 &&
        artifacts[0].kind == ArtifactRef::Kind::kChunk &&
        artifacts[0].chunk != nullptr && fragment.conjuncts.empty()) {
      const std::vector<TopKEntry> entries = TopKColumn(
          *artifacts[0].chunk, spec.col, spec.k, spec.descending, &schema);
      result.top_k = entries;
      result.output_rows = entries.size();
      return result;
    }
    const RowReader reader{&state.values, &state.nulls, &needed};
    std::vector<TopKEntry> entries;
    entries.reserve(rows.size());
    for (const SurvivingRow& row : rows) {
      TopKEntry e;
      e.value = reader.Value(row, spec.col);
      entries.push_back(e);
    }
    const bool desc = spec.descending;
    std::stable_sort(entries.begin(), entries.end(),
                     [desc](const TopKEntry& a, const TopKEntry& b) {
                       return desc ? a.value > b.value : a.value < b.value;
                     });
    if (entries.size() > spec.k) entries.resize(spec.k);
    result.top_k = std::move(entries);
    result.output_rows = result.top_k.size();
    return result;
  }

  // The projection.
  if (!fragment.projection.empty()) {
    result.projected_width = static_cast<uint32_t>(fragment.projection.size());
    const RowReader reader{&state.values, &state.nulls, &needed};
    for (size_t ri = 0; ri < rows.size(); ++ri) {
      for (slot_t col : fragment.projection) {
        result.projected_values.push_back(reader.Value(rows[ri], col));
      }
    }
    result.output_rows = rows.size();
    return result;
  }

  // The selection: the surviving rows as a bitmask over the artifact rows.
  result.selection_count = rows.size();
  result.output_rows = rows.size();
  return result;
}

StatusOr<KeyValueRows> MaterializeKeyValue(Runtime runtime,
                                           const ArtifactSet& artifacts,
                                           const Fragment& fragment,
                                           const Schema& schema,
                                           slot_t value_col) {
  if (ValidateFragment(schema, fragment) != Status::kSuccess) {
    return Status::kCorruption;
  }
  const slot_t key_col = schema.KeyColumnIndex();
  if (!IsInt64Column(schema, key_col) || !IsInt64Column(schema, value_col)) {
    return Status::kNotImplemented;
  }
  std::vector<Conjunct> eval;
  std::vector<Conjunct> varchar_eval;
  for (const Conjunct& c : fragment.conjuncts) {
    if (!ConjunctSupported(runtime, schema, c)) continue;
    const bool is_varchar = c.kind == ConjunctKind::kVarcharRange ||
                            (c.kind == ConjunctKind::kCompare &&
                             c.literal_kind == LiteralKind::kBytes);
    if (is_varchar) {
      varchar_eval.push_back(c);
    } else {
      eval.push_back(c);
    }
  }
  // The value column joins the needed set, so the scan decodes it. The
  // fragment itself is not modified: the value column is a MATERIALIZATION
  // request, not a filter column.
  std::vector<slot_t> needed = NeededColumns(schema, fragment);
  if (std::find(needed.begin(), needed.end(), value_col) == needed.end()) {
    needed.push_back(value_col);
  }
  ScanState state;
  state.key_dedup_possible = IsInt64Column(schema, key_col);
  ScanArtifacts(artifacts, fragment, schema, eval, varchar_eval, needed,
                &state);
  std::vector<SurvivingRow> rows =
      ResolveDuplicates(std::move(state.rows), state.key_dedup_possible);

  const int key_idx = NeededIndex(needed, key_col);
  const int value_idx = NeededIndex(needed, value_col);
  KeyValueRows out;
  out.keys.reserve(rows.size());
  out.values.reserve(rows.size());
  for (const SurvivingRow& row : rows) {
    const uint32_t base = row.value_offset;
    out.keys.push_back(
        key_idx < 0 ? 0 : state.values[base + static_cast<uint32_t>(key_idx)]);
    out.values.push_back(
        value_idx < 0 ? 0
                      : state.values[base + static_cast<uint32_t>(value_idx)]);
    out.nulls.push_back(
        value_idx < 0 ? 1
                      : state.nulls[base + static_cast<uint32_t>(value_idx)]);
    out.artifacts.push_back(row.artifact_index);
    out.rows.push_back(static_cast<uint32_t>(row.row_index));
  }
  (void)runtime;
  return out;
}

}  // namespace tinylamb
