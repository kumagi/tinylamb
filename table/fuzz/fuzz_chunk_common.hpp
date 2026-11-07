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

// Shared building blocks for the storage-facing fuzz harnesses.
//
// The existing harnesses (pax_mutate_fuzzer) gate every decode behind
// ValidateChunkBytes but always reuse the PRISTINE in-memory Footer, so the
// serialized footer itself is never hostile: `column_offsets`, `row_counts`,
// `encodings`, `params`, the null bitmaps, the hash index, the inverted index,
// the Xor8 tables and the PGM segments are all untested surfaces. These
// helpers let a harness rebuild a self-describing wire image, corrupt the
// footer, RE-SIGN the CRC32C words so the structural walk is actually
// reached, and then hammer every consumer of the reconstructed Footer.

#ifndef TINYLAMB_FUZZ_CHUNK_COMMON_HPP
#define TINYLAMB_FUZZ_CHUNK_COMMON_HPP

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/crc32c.hpp"
#include "common/platform/endian.hpp"
#include "common/status_or.hpp"
#include "table/pax_chunk/pax_chunk.hpp"
#include "table/sorted_run/sorted_run.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/constraint.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::fuzz {

// ---- lightweight PaxChunk scan iterator & multi-run merge ------------------

class PaxChunkScanIterator {
 public:
  PaxChunkScanIterator(const PaxChunk* chunk, const Schema* sc,
                       size_t start_index = 0)
      : chunk_(chunk),
        sc_(sc),
        idx_(start_index),
        total_(chunk != nullptr ? chunk->TotalRowCount() : 0) {}

  PaxChunkScanIterator(const PaxChunk* chunk, const Schema& sc,
                       size_t start_index = 0)
      : PaxChunkScanIterator(chunk, &sc, start_index) {}

  [[nodiscard]] bool IsValid() const {
    return chunk_ != nullptr && sc_ != nullptr && idx_ < total_;
  }

  PaxChunkScanIterator& operator++() {
    if (idx_ < total_) {
      ++idx_;
      decoded_ = false;
    }
    return *this;
  }

  void Project(std::span<const slot_t> cols) {
    cache_.SelectColumns(cols);
    decoded_ = false;
  }

  void Seek(size_t row_index) {
    idx_ = row_index;
    decoded_ = false;
  }

  [[nodiscard]] StatusOr<Row> TryValue() {
    if (!IsValid()) return Status::kNotExists;
    Status st = chunk_->ReadRowIntoCached(idx_, *sc_, &cache_, &cur_);
    if (st != Status::kSuccess) return st;
    decoded_ = true;
    return cur_;
  }

  [[nodiscard]] const Row& ValueRef() {
    if (!decoded_ && IsValid()) {
      Status st = chunk_->ReadRowIntoCached(idx_, *sc_, &cache_, &cur_);
      if (st != Status::kSuccess) cur_ = Row{};
      decoded_ = true;
    }
    return cur_;
  }

  [[nodiscard]] const Row& GetRow() { return ValueRef(); }

  [[nodiscard]] std::string GetKey() {
    return ValueRef().EncodeKeyMemcomparableFormat(*sc_);
  }

 private:
  const PaxChunk* chunk_ = nullptr;
  const Schema* sc_ = nullptr;
  size_t idx_ = 0;
  size_t total_ = 0;
  bool decoded_ = false;
  Row cur_;
  PaxChunk::VarcharGroupCache cache_;
};

// Multi-way merge of SortedRuns (latest generation wins on key tie; tombstones
// suppress older versions and are elided from the resulting PaxChunk).
inline StatusOr<PaxChunk> MergeAllSortedRuns(
    const std::vector<const SortedRun*>& runs, const Schema& sc) {
  struct Cursor {
    SortedRun::Iterator it;
    uint64_t generation = 0;
  };
  std::vector<Cursor> cursors;
  cursors.reserve(runs.size());
  uint64_t max_gen = 0;
  for (const SortedRun* run : runs) {
    if (run == nullptr) continue;
    max_gen = std::max<uint64_t>(max_gen, run->Generation());
    SortedRun::Iterator it = run->Begin(sc);
    if (it.IsValid()) {
      cursors.push_back(Cursor{std::move(it), run->Generation()});
    }
  }

  std::vector<char> merged_blob;
  std::vector<SortedRun::Entry> merged_index;

  while (!cursors.empty()) {
    size_t best_idx = 0;
    std::string min_key = cursors[0].it.Key();
    for (size_t i = 1; i < cursors.size(); ++i) {
      const std::string k = cursors[i].it.Key();
      if (k < min_key || (k == min_key && cursors[i].generation >
                                              cursors[best_idx].generation)) {
        min_key = k;
        best_idx = i;
      }
    }
    const std::string key_copy = min_key;
    const bool winner_deleted = cursors[best_idx].it.IsDeleted();
    Row winner_row;
    if (!winner_deleted) {
      try {
        winner_row = cursors[best_idx].it.Value();
      } catch (const std::exception&) {
        return Status::kCorruption;
      }
      if (winner_row.Count() == 0) {
        return Status::kCorruption;
      }
    }

    for (size_t i = 0; i < cursors.size();) {
      while (cursors[i].it.IsValid() && cursors[i].it.Key() == key_copy) {
        ++cursors[i].it;
      }
      if (!cursors[i].it.IsValid()) {
        cursors.erase(cursors.begin() + static_cast<ptrdiff_t>(i));
      } else {
        ++i;
      }
    }

    if (!winner_deleted) {
      const auto key_off = static_cast<uint32_t>(merged_blob.size());
      const uint32_t be_len = htobe32(static_cast<uint32_t>(key_copy.size()));
      const char* p = reinterpret_cast<const char*>(&be_len);
      merged_blob.insert(merged_blob.end(), p, p + sizeof(uint32_t));
      merged_blob.insert(merged_blob.end(), key_copy.begin(), key_copy.end());
      const auto row_off = static_cast<uint32_t>(merged_blob.size());
      std::vector<char> rbuf(winner_row.Size());
      winner_row.Serialize(rbuf.data());
      merged_blob.insert(merged_blob.end(), rbuf.begin(), rbuf.end());
      merged_index.emplace_back(key_off, row_off, false);
    }
  }

  SortedRun merged_run(std::move(merged_blob), std::move(merged_index),
                       max_gen);
  return PaxChunk::BuildFromSortedRun(merged_run, sc);
}

// ---- byte helpers ---------------------------------------------------------

inline uint32_t LoadBE32(const std::vector<char>& b, size_t off) {
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i)
    v = (v << 8) | static_cast<unsigned char>(b[off + static_cast<size_t>(i)]);
  return v;
}

inline void StoreBE32(std::vector<char>& b, size_t off, uint32_t v) {
  for (int i = 0; i < 4; ++i)
    b[off + static_cast<size_t>(i)] =
        static_cast<char>((v >> (8 * (3 - i))) & 0xFF);
}

inline uint64_t LoadBE64(const std::vector<char>& b, size_t off) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i)
    v = (v << 8) | static_cast<unsigned char>(b[off + static_cast<size_t>(i)]);
  return v;
}

inline uint16_t LoadBE16(const std::vector<char>& b, size_t off) {
  return static_cast<uint16_t>((static_cast<unsigned char>(b[off]) << 8) |
                               static_cast<unsigned char>(b[off + 1]));
}

// ---- wire-image geometry --------------------------------------------------

// A serialized chunk is [data region][footer][crc_data u32][crc_footer u32]
// [footer_size u32]. Returns false for images too short to hold a trailer.
inline bool WireLayout(const std::vector<char>& b, size_t* footer_off,
                       size_t* footer_size) {
  if (b.size() < 4) return false;
  const size_t fsz = LoadBE32(b, b.size() - 4);
  if (fsz < 34 || fsz > b.size() || b.size() - fsz < 4) return false;
  *footer_size = fsz;
  *footer_off = b.size() - fsz - 4;
  return true;
}

// Rewrite crc_data / crc_footer so a mutated image still passes the integrity
// gate. Without this every footer mutation dies at the CRC check and the
// structural walk (and Deserialize, which only runs after it) is never
// reached -- the exact hole this helper exists to plug. No-op on format v1.
inline void ResignChunkCrc(std::vector<char>& b) {
  size_t fo = 0;
  size_t fs = 0;
  if (!WireLayout(b, &fo, &fs)) return;
  const uint32_t fmtver = LoadBE32(b, fo + 8);
  if (fmtver < 2 || fs < 8) return;
  StoreBE32(b, fo + fs - 8, Crc32cOf(b.data(), fo));
  StoreBE32(b, fo + fs - 4, Crc32cOf(b.data() + fo, fs - 4));
}

// ---- schema / chunk construction -----------------------------------------

// Five columns chosen so every optional footer section is materialized:
// zone maps (int64 key), all-column zone maps, HLL + bloom + rg sums,
// the inverted secondary index (low-cardinality flag), the Xor8 pre-filter
// (high-cardinality qty), the per-row hash index, the null bitmaps (nullable
// qty) and the v12 extended statistics.
inline Schema MakeStorageSchema() {
  std::vector<Column> cols;
  cols.emplace_back(ColumnName{"", "id"}, ValueType::kInt64,
                    Constraint{Constraint::kPrimaryKey});
  cols.emplace_back(ColumnName{"", "qty"}, ValueType::kInt64);
  cols.emplace_back(ColumnName{"", "name"}, ValueType::kVarChar,
                    Constraint{Constraint::kNotNull});
  cols.emplace_back(ColumnName{"", "score"}, ValueType::kDouble);
  cols.emplace_back(ColumnName{"", "flag"}, ValueType::kInt64);
  return Schema{"fz", std::move(cols), 0, 1};
}

inline SortedRun MakeRun(const std::vector<Row>& rows, const Schema& sc,
                         uint64_t generation = 1) {
  std::vector<char> blob;
  std::vector<SortedRun::Entry> index;
  for (const Row& r : rows) {
    const std::string key = r.EncodeKeyMemcomparableFormat(sc);
    const auto key_off = static_cast<uint32_t>(blob.size());
    const uint32_t be_len = htobe32(static_cast<uint32_t>(key.size()));
    const char* p = reinterpret_cast<const char*>(&be_len);
    blob.insert(blob.end(), p, p + sizeof(uint32_t));
    blob.insert(blob.end(), key.begin(), key.end());
    const auto row_off = static_cast<uint32_t>(blob.size());
    std::vector<char> buf(r.Size());
    r.Serialize(buf.data());
    blob.insert(blob.end(), buf.begin(), buf.end());
    index.emplace_back(key_off, row_off, false);
  }
  return SortedRun(std::move(blob), std::move(index), generation);
}

// 1200 rows => 2 row groups (1024 + 176), so every per-row-group
// section (all-zone maps, inverted postings, Xor8 cells, PGM brackets) has
// more than one instance, the last group is a PARTIAL one (the 128-row
// checkpoint / 1024-row group arithmetic all have an off-by-one tail), and
// the build stays cheap enough to keep the exec rate in the tens per second
// under ASan+UBSan -- which is what decides how much of the space a campaign
// actually reaches.
constexpr int64_t kStorageRows = 1200;

// `shape` steers the adaptive encoder so different scheme families become
// feasible:
//   bit0 arithmetic ramp on qty/score (kArith / kPla / kPfor territory)
//   bit1 monotone low-entropy tail on flag (kOpRle / kRleIdx / kInterp)
//   bit2 two-valued flag (kBitmap / kWah -- they gate on exactly 2 distinct)
//   bit3 keep the nulls even in ramp mode (kSparse needs a nullable column)
//   bit4 constant flag (kConst needs exactly 1 distinct value, which no other
//      bit produces: every other flag shape varies by construction)
//   bit7 bare round-robin qty (kMod needs a wrapped affine sequence whose
//      on-model values are the bare residues)
inline std::vector<Row> MakeStorageRows(uint8_t shape) {
  const bool ramp = (shape & 1) != 0;
  const bool monotone = (shape & 2) != 0;
  const bool two_valued = (shape & 4) != 0;
  const bool keep_nulls = (shape & 8) != 0;
  const bool const_flag = (shape & 16) != 0;
  std::vector<Row> rows;
  rows.reserve(static_cast<size_t>(kStorageRows));
  for (int64_t i = 0; i < kStorageRows; ++i) {
    std::vector<Value> vals;
    vals.reserve(5);
    vals.emplace_back(i);  // id (primary key)
    if ((shape & 32) != 0) {
      constexpr int64_t kSkewD = 64;
      static const std::vector<double> weights = [] {
        std::vector<double> v(static_cast<size_t>(kSkewD), 0.0);
        double t = 0.0;
        for (int64_t k = 1; k <= kSkewD; ++k) {
          v[static_cast<size_t>(k - 1)] =
              1.0 / std::pow(static_cast<double>(k), 1.5);
          t += v[static_cast<size_t>(k - 1)];
        }
        for (double& x : v) x /= t;
        return v;
      }();
      double acc = 0.0;
      const double target =
          static_cast<double>((static_cast<uint64_t>(i) * 2654435761ULL) %
                              10000ULL) /
          10000.0;
      for (int64_t k = 0; k < kSkewD; ++k) {
        acc += weights[static_cast<size_t>(k)];
        if (acc >= target) {
          vals.emplace_back(INT64_C(1000000000000) + k * 7919);
          break;
        }
      }
    } else if ((shape & 64) != 0) {
      constexpr uint32_t kLocalD = 8;
      const uint64_t tile = static_cast<uint64_t>(i) / 64U;
      const uint64_t slot =
          (static_cast<uint64_t>(i) * 2654435761ULL + tile * 40503ULL) %
          kLocalD;
      vals.emplace_back(INT64_C(1000000000000) +
                        static_cast<int64_t>(tile) * INT64_C(1048576) +
                        static_cast<int64_t>(slot) * 7919);
    } else if ((shape & 128) != 0) {
      vals.emplace_back(i % 97);
    } else if (ramp) {
      vals.emplace_back(i * 2);
    } else if ((i % 7) == 3) {
      vals.emplace_back();  // explicit NULL: the footer null bitmap
    } else {
      vals.emplace_back(i * 3 + 1);
    }
    vals.emplace_back("name-" + std::to_string(i % 61));
    if (ramp) {
      vals.emplace_back(3.0 + static_cast<double>(i) * 0.25);
    } else {
      vals.emplace_back(12.5 + static_cast<double>(i % 97) * 0.5);
    }
    if (const_flag) {
      vals.emplace_back(int64_t{7});
    } else if (monotone) {
      vals.emplace_back(i / 40);
    } else if (two_valued) {
      vals.emplace_back(i & 1);
    } else {
      vals.emplace_back(i % 5);
    }
    rows.emplace_back(std::move(vals));
  }
  if (keep_nulls) {
    for (int64_t i = 0; i < kStorageRows; i += 7) {
      rows[static_cast<size_t>(i)][1] = Value();
    }
  }
  return rows;
}

inline PaxChunk BuildStorageChunk(uint8_t shape = 0) {
  const Schema sc = MakeStorageSchema();
  return PaxChunk::BuildFromSortedRun(MakeRun(MakeStorageRows(shape), sc), sc);
}

// Deserialize(chunk.Data()) must reproduce the in-memory chunk exactly --
// same footer fields, same rows.
inline bool DeserializeRoundTrips(const PaxChunk& src, const Schema& sc) {
  auto rt = PaxChunk::Deserialize(src.Data());
  if (!rt.HasValue()) return false;
  const PaxChunk& d = rt.Value();
  const PaxChunk::Footer& a = src.GetFooter();
  const PaxChunk::Footer& b = d.GetFooter();
  if (src.TotalRowCount() != d.TotalRowCount()) return false;
  if (a.column_offsets != b.column_offsets || a.row_counts != b.row_counts ||
      a.encodings != b.encodings || a.params != b.params ||
      a.null_present != b.null_present ||
      a.zone_map_present != b.zone_map_present ||
      a.group_min_keys != b.group_min_keys ||
      a.group_max_keys != b.group_max_keys ||
      a.all_zone_mins != b.all_zone_mins ||
      a.all_zone_maxs != b.all_zone_maxs || a.hash_keys != b.hash_keys ||
      a.hash_rows != b.hash_rows || a.rg_sums != b.rg_sums ||
      a.col_sums != b.col_sums || a.hll_sketches != b.hll_sketches ||
      a.rg_blooms != b.rg_blooms || a.col_mins != b.col_mins ||
      a.col_maxs != b.col_maxs || a.col_nulls != b.col_nulls ||
      a.col_modes != b.col_modes || a.col_hists != b.col_hists ||
      a.inv_index.size() != b.inv_index.size() ||
      a.xor_cells.size() != b.xor_cells.size() ||
      a.pgm_segments.size() != b.pgm_segments.size() ||
      a.format_version != b.format_version || a.generation != b.generation ||
      a.schema_version != b.schema_version ||
      a.key_column_index != b.key_column_index ||
      a.key_column_type != b.key_column_type ||
      a.hash_idx_log2 != b.hash_idx_log2 || a.pgm_eps != b.pgm_eps) {
    return false;
  }
  for (size_t i = 0; i < src.TotalRowCount(); ++i) {
    auto ra = src.ReadRow(i, sc);
    auto rb = d.ReadRow(i, sc);
    if (ra.HasValue() != rb.HasValue()) return false;
    if (ra.HasValue() && ra.Value() != rb.Value()) return false;
  }
  return true;
}

// ---- mutation primitives ---------------------------------------------------

enum class MutOp : uint8_t { kXorByte, kOnesWord, kZeroWord, kNarrowByte };

inline void ApplyMutation(std::vector<char>& bytes, size_t site, size_t limit,
                          uint8_t op_mask, uint8_t index, uint32_t roll,
                          uint8_t narrow) {
  if (site >= limit) return;
  switch (static_cast<MutOp>((op_mask >> (index & 3)) & 0x3)) {
    case MutOp::kXorByte:
      bytes[site] = static_cast<char>(
          static_cast<uint32_t>(static_cast<unsigned char>(bytes[site])) ^
          (roll >> 24));
      return;
    case MutOp::kOnesWord:
      if (site + 4 <= limit)
        StoreBE32(bytes, site & ~static_cast<size_t>(3), 0xFFFFFFFFU);
      return;
    case MutOp::kZeroWord:
      if (site + 4 <= limit)
        StoreBE32(bytes, site & ~static_cast<size_t>(3), 0x00000000U);
      return;
    case MutOp::kNarrowByte:
      bytes[site] = static_cast<char>(narrow);
      return;
  }
}

// ---- consumer hammer ------------------------------------------------------

inline void HammerChunk(const PaxChunk& chunk, const Schema& sc,
                        int64_t key_probe) {
  const size_t total = chunk.TotalRowCount();
  const slot_t ncols = sc.ColumnCount();

  const size_t stride = total > 64 ? total / 64 : 1;
  for (size_t i = 0; i < total; i += stride) {
    auto r = chunk.ReadRow(i, sc);
    if (r.HasValue() && r.Value().Count() != ncols) std::abort();
    for (slot_t c = 0; c < ncols; ++c) {
      (void)chunk.ReadColumnValue(i, c, sc);
    }
  }
  for (size_t over : {total, total + 1, total + 1000, SIZE_MAX}) {
    auto r = chunk.ReadRow(over, sc);
    if (r.HasValue()) std::abort();
    (void)chunk.ReadColumnValue(over, 0, sc);
  }

  size_t idx = 0;
  (void)chunk.PointLookup(key_probe, sc, &idx);
  (void)chunk.PointLookup(INT64_MIN, sc, &idx);
  (void)chunk.PointLookup(INT64_MAX, sc, &idx);
  (void)chunk.LowerBoundIndex(key_probe, sc, &idx);
  (void)chunk.LowerBoundIndex(INT64_MIN, sc, &idx);
  (void)chunk.HashLookup(key_probe, &idx);

  for (slot_t c = 0; c < ncols; ++c) {
    PaxChunk::Footer::ColumnStats st;
    (void)chunk.ColumnStats(c, sc, &st);
    uint64_t u = 0;
    bool ex = false;
    (void)chunk.CountNonNull(c, sc, &u);
    (void)chunk.CountEqualFromStats(c, sc, key_probe, &u, &ex);
    (void)chunk.CountRangeFromStats(c, sc, INT64_MIN, INT64_MAX, &u);
    int64_t mn = 0;
    int64_t mx = 0;
    (void)chunk.ColumnMinMax(c, &mn, &mx);
    int64_t sum = 0;
    uint64_t cnt = 0;
    (void)FastColumnSum(chunk, c, &sum, &cnt);
    (void)ApproxCountDistinct(chunk, c);
    (void)XorRgMask(chunk, c, key_probe);
  }
  (void)PgmPredict(chunk.GetFooter(), key_probe, static_cast<uint64_t>(total));

  static const ScanOp kOps[] = {ScanOp::kEq, ScanOp::kLe, ScanOp::kLt,
                                ScanOp::kGe, ScanOp::kGt};
  const int64_t lits[] = {0, 1, key_probe, INT64_MIN, INT64_MAX, -1};
  for (slot_t c = 0; c < ncols; ++c) {
    for (ScanOp op : kOps) {
      (void)VectorizedScanColumn(chunk, c, op, key_probe, false, 0, 1, &sc);
    }
    for (int64_t lit : lits) {
      (void)VectorizedScanColumn(chunk, c, ScanOp::kEq, lit, false, 0, 1, &sc);
    }
    const std::vector<int64_t> list{key_probe, 0, 1, key_probe};
    (void)VectorizedScanColumnInList(chunk, c, list, false, 0, &sc);
    if (c + 1 < ncols) {
      (void)GroupBySumColumn(chunk, c, c + 1, &sc);
    }
    (void)TopKColumn(chunk, c, 8, false, &sc);
    (void)TopKColumn(chunk, c, 8, true, &sc);
  }

  for (ScanOp op : kOps) {
    AggPlan plan;
    plan.count_only = true;
    plan.preds.push_back(Predicate{0, op, key_probe});
    if (ncols > 1) {
      AggTerm t;
      t.coeff = 3;
      t.cols = {1, 2};
      plan.terms.push_back(t);
    }
    (void)PaxChunk::ScanAggregate(chunk, plan, false, 0, 1, &sc);

    for (AggOp aop : {AggOp::kSum, AggOp::kMin, AggOp::kMax, AggOp::kCount,
                      AggOp::kApproxDistinct}) {
      ReducePlan rp;
      rp.op = aop;
      rp.col = static_cast<slot_t>(ncols > 1 ? 1 : 0);
      rp.preds.push_back(Predicate{0, op, key_probe});
      (void)ReduceColumn(chunk, rp, false, 0, 1, &sc);
    }
  }

  for (slot_t c = 0; c < ncols; ++c) {
    uint64_t cnt = 0;
    (void)InvertedEqCount(chunk, c, key_probe, &cnt);
    const int64_t vals[] = {key_probe, 0, 1};
    (void)InvertedInCount(chunk, c, vals, 3, &cnt);
    if (c + 1 < ncols)
      (void)InvertedAndCount(chunk, c, key_probe, c + 1, 0, &cnt);
  }

  static const StrScanOp kStrOps[] = {StrScanOp::kEq, StrScanOp::kNe,
                                      StrScanOp::kLt, StrScanOp::kLe,
                                      StrScanOp::kGt, StrScanOp::kGe};
  for (slot_t c = 0; c < ncols; ++c) {
    for (StrScanOp op : kStrOps) {
      (void)VarcharFilterCount(chunk, c, op, "name-7", &sc);
    }
    (void)VarcharRangeCount(chunk, c, std::nullopt, "name-7", &sc);
    (void)VarcharRangeCount(chunk, c, "name-1", std::nullopt, &sc);
    (void)VarcharRangeCount(chunk, c, "name-1", "name-30", &sc);
  }

  const PaxChunk::Footer& f = chunk.GetFooter();
  size_t throws = 0;
  for (uint32_t g = 0; g < f.row_group_count; ++g) {
    for (slot_t c = 0; c < ncols; ++c) {
      const size_t rc =
          g < f.row_counts.size() ? f.row_counts[g] : PaxChunk::kRowGroupSize;
      std::vector<int64_t> buf(rc > 0 ? rc : 1, 0);
      try {
        chunk.DecodeInt64ColumnRg(c, g, buf.data());
      } catch (const std::out_of_range&) {
        ++throws;
      } catch (const std::exception&) {
        ++throws;
      }
      int64_t sum = 0;
      (void)ByteSliceColumnSum(chunk, c, g, &sum);
      (void)PBitSColumnSum(chunk, c, g, &sum);
      std::vector<Predicate> preds{Predicate{0, ScanOp::kLe, key_probe}};
      (void)BuildSelectionVector(chunk, preds, g);
    }
    uint64_t cnt = 0;
    double dsum = 0;
    (void)Chimp128ColumnCountSum(chunk, 0, PaxChunk::CompareOp::kLe, 1.0, &cnt,
                                 &dsum, &sc);
    (void)AlpRdTansColumnCountSum(chunk, 0, PaxChunk::CompareOp::kLe, 1.0, &cnt,
                                  &dsum, &sc);
    (void)PatasColumnCountSum(chunk, 0, PaxChunk::CompareOp::kLe, 1.0, &cnt,
                              &dsum, &sc);
  }
  static_cast<void>(throws);
}

}  // namespace tinylamb::fuzz

#endif  // TINYLAMB_FUZZ_CHUNK_COMMON_HPP
