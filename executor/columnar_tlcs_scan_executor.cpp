/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/columnar_tlcs_scan_executor.hpp"

#include <algorithm>
#include <atomic>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/memory_budget.hpp"
#include "common/parallel_work.hpp"
#include "common/query_parallelism.hpp"
#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/data_chunk.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/columnar_tlcs_fast_path.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/pipeline_lowering.hpp"
#include "executor/query_memory.hpp"
#include "expression/column_value.hpp"
#include "plan/full_scan_plan.hpp"
#include "plan/limit_plan.hpp"
#include "plan/projection_plan.hpp"
#include "plan/relation_rename_plan.hpp"
#include "plan/selection_plan.hpp"
#include "plan/sort_plan.hpp"
#include "plan/topn_plan.hpp"
#include "table/table.hpp"
#include "table/tlcs_file.hpp"
#include "type/collation.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

namespace {

Schema EnsureQualified(const Schema& schema, const std::string& qualifier) {
  if (qualifier.empty()) {
    return schema;
  }
  bool any_empty = false;
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    if (schema.GetColumn(i).Name().schema.empty()) {
      any_empty = true;
      break;
    }
  }
  return any_empty ? relational_detail::QualifySchema(schema, qualifier)
                   : schema;
}

struct SortKeyColumnSpec {
  slot_t full_col{0};
  ValueType type{ValueType::kNull};
  SortKeyOrder order{true, false, Collation::kNone};
};

struct HeapKeyCell {
  bool is_null{false};
  int64_t int_val{0};
  double double_val{0.0};
  std::string str_val;
};

struct TopKWinner {
  std::vector<HeapKeyCell> keys;
  uint32_t block_idx{0};
  uint32_t row_idx{0};
};

int CompareHeapKeyCell(const HeapKeyCell& a, const HeapKeyCell& b,
                       const SortKeyColumnSpec& spec) noexcept {
  if (a.is_null || b.is_null) {
    if (a.is_null && b.is_null) {
      return 0;
    }
    const int null_side = a.is_null ? -1 : 1;
    return spec.order.NullsFirst() ? null_side : -null_side;
  }
  int cmp = 0;
  switch (spec.type) {
    case ValueType::kInt64:
    case ValueType::kDate:
      cmp = a.int_val < b.int_val ? -1 : (b.int_val < a.int_val ? 1 : 0);
      break;
    case ValueType::kDouble: {
      const bool na = std::isnan(a.double_val);
      const bool nb = std::isnan(b.double_val);
      if (na || nb) {
        cmp = (na && nb) ? 0 : (na ? -1 : 1);
      } else {
        cmp = a.double_val < b.double_val
                  ? -1
                  : (b.double_val < a.double_val ? 1 : 0);
      }
      break;
    }
    case ValueType::kVarChar:
      cmp = CompareVarCharForOrderBy(
          a.str_val, b.str_val,
          spec.order.KeyCollation() == Collation::kCaseInsensitive);
      break;
    default:
      cmp = 0;
      break;
  }
  return spec.order.Ascending() ? cmp : -cmp;
}

int CompareChunkRowAgainstHeapKey(const TlcsFile::DecodedColumnChunk& chunk,
                                  uint32_t row, const HeapKeyCell& rhs,
                                  const SortKeyColumnSpec& spec) noexcept {
  const bool a_null = chunk.IsNull(row);
  if (a_null || rhs.is_null) {
    if (a_null && rhs.is_null) {
      return 0;
    }
    const int null_side = a_null ? -1 : 1;
    return spec.order.NullsFirst() ? null_side : -null_side;
  }
  int cmp = 0;
  switch (spec.type) {
    case ValueType::kInt64:
    case ValueType::kDate: {
      const int64_t lv = chunk.int_cells[row];
      cmp = lv < rhs.int_val ? -1 : (rhs.int_val < lv ? 1 : 0);
      break;
    }
    case ValueType::kDouble: {
      const auto lv = std::bit_cast<double>(chunk.int_cells[row]);
      const bool na = std::isnan(lv);
      const bool nb = std::isnan(rhs.double_val);
      if (na || nb) {
        cmp = (na && nb) ? 0 : (na ? -1 : 1);
      } else {
        cmp = lv < rhs.double_val ? -1 : (rhs.double_val < lv ? 1 : 0);
      }
      break;
    }
    case ValueType::kVarChar:
      cmp = CompareVarCharForOrderBy(
          chunk.VarcharAt(row), rhs.str_val,
          spec.order.KeyCollation() == Collation::kCaseInsensitive);
      break;
    default:
      cmp = 0;
      break;
  }
  return spec.order.Ascending() ? cmp : -cmp;
}

// Strict weak ordering: returns true iff `a` precedes `b` in the requested
// ORDER BY sequence. Note that `std::push_heap` with `TopKWinnerComesFirst(a,
// b, specs)` places the *last* (worst) element at `heap.front()` for O(1) Top-K
// eviction, and `std::partial_sort` with the same comparator orders winners
// best-first.
bool TopKWinnerComesFirst(
    const TopKWinner& a, const TopKWinner& b,
    const std::vector<SortKeyColumnSpec>& specs) noexcept {
  for (size_t k = 0; k < specs.size(); ++k) {
    const int cmp = CompareHeapKeyCell(a.keys[k], b.keys[k], specs[k]);
    if (cmp != 0) {
      return cmp < 0;
    }
  }
  if (a.block_idx != b.block_idx) {
    return a.block_idx < b.block_idx;
  }
  return a.row_idx < b.row_idx;
}

void AppendDecodedColumnRows(const TlcsFile::DecodedColumnChunk& chunk,
                             std::span<const uint32_t> row_indices,
                             ColumnVector* dst,
                             std::vector<int64_t>* int_scratch,
                             std::vector<double>* dbl_scratch,
                             std::vector<std::string_view>* sv_scratch,
                             std::vector<uint8_t>* null_scratch) {
  const size_t count = row_indices.size();
  if (count == 0) {
    return;
  }
  null_scratch->resize(count);
  for (size_t i = 0; i < count; ++i) {
    (*null_scratch)[i] = chunk.IsNull(row_indices[i]) ? 1 : 0;
  }
  switch (chunk.type) {
    case ValueType::kInt64:
    case ValueType::kDate: {
      int_scratch->resize(count);
      for (size_t i = 0; i < count; ++i) {
        (*int_scratch)[i] = chunk.int_cells[row_indices[i]];
      }
      dst->AppendIntBatch(chunk.type, chunk.is_unsigned, int_scratch->data(),
                          null_scratch->data(), count);
      break;
    }
    case ValueType::kDouble: {
      dbl_scratch->resize(count);
      for (size_t i = 0; i < count; ++i) {
        (*dbl_scratch)[i] =
            std::bit_cast<double>(chunk.int_cells[row_indices[i]]);
      }
      dst->AppendDoubleBatch(dbl_scratch->data(), null_scratch->data(), count);
      break;
    }
    case ValueType::kVarChar: {
      sv_scratch->resize(count);
      for (size_t i = 0; i < count; ++i) {
        (*sv_scratch)[i] = (*null_scratch)[i] != 0
                               ? std::string_view{}
                               : chunk.VarcharAt(row_indices[i]);
      }
      dst->AppendStringViewBatch(sv_scratch->data(), null_scratch->data(),
                                 count);
      break;
    }
    default:
      for (size_t i = 0; i < count; ++i) {
        dst->Append(
            relational_detail::ValueFromChunkRow(chunk, row_indices[i]));
      }
      break;
  }
}

struct DirectColumnarScanSpec {
  const Table* table{nullptr};
  std::shared_ptr<const TlcsFile> tlcs;
  Schema table_schema;
  Schema output_schema;
  std::vector<slot_t> visible_to_full;
  relational_detail::ColumnarPredicateSet preds;
  bool has_sort{false};
  std::vector<SortKeyColumnSpec> sort_specs;
  bool has_limit{false};
  size_t limit{std::numeric_limits<size_t>::max()};
  size_t offset{0};
};

class ColumnarTlcsScanExecutor final : public ExecutorBase {
 public:
  ColumnarTlcsScanExecutor(DirectColumnarScanSpec spec, Plan fallback_plan,
                           PipelineLoweringOptions fallback_options,
                           TransactionContext& ctx)
      : spec_(std::move(spec)),
        fallback_plan_(std::move(fallback_plan)),
        fallback_options_(std::move(fallback_options)),
        ctx_(ctx) {}

  size_t NextBatch(DataChunk* destination, size_t max_rows) override {
    if (destination == nullptr || max_rows == 0 || finished_) {
      return 0;
    }
    if (fallback_executor_ != nullptr) {
      const size_t n = fallback_executor_->NextBatch(destination, max_rows);
      if (FailWithChildOf(*fallback_executor_)) {
        return 0;
      }
      if (n == 0) {
        finished_ = true;
      }
      return n;
    }
    if (!initialized_) {
      initialized_ = true;
      const auto current_tlcs =
          relational_detail::ResolveCleanBackingTlcs(*spec_.table, ctx_);
      if (current_tlcs == nullptr || current_tlcs.get() != spec_.tlcs.get()) {
        StatusOr<Executor> fb = PipelineLowering::LowerToExecutor(
            fallback_plan_, ctx_, fallback_options_);
        if (!fb.HasValue()) {
          FailWith(fb.GetStatus());
          finished_ = true;
          return 0;
        }
        fallback_executor_ = fb.MoveValue();
        const size_t n = fallback_executor_->NextBatch(destination, max_rows);
        if (FailWithChildOf(*fallback_executor_)) {
          return 0;
        }
        if (n == 0) {
          finished_ = true;
        }
        return n;
      }
      Status init_status = InitializeExecution();
      if (init_status != Status::kSuccess) {
        FailWith(std::move(init_status));
        finished_ = true;
        return 0;
      }
    }

    destination->Reset(spec_.output_schema,
                       std::min(max_rows, kDefaultVectorSize));
    if (spec_.has_sort) {
      return DrainBufferedChunks(destination, max_rows);
    }
    return NextFilteredScanBatch(destination, max_rows);
  }

  void Dump(std::ostream& o, int indent) const override {
    o << std::string(static_cast<size_t>(std::max(0, indent)), ' ')
      << (spec_.has_sort ? "ColumnarTlcsTopN" : "ColumnarTlcsScan")
      << "(limit=" << (spec_.has_limit ? std::to_string(spec_.limit) : "none")
      << ", offset=" << spec_.offset << ")\n";
  }

 private:
  Status InitializeExecution() {
    StatusOr<std::vector<uint32_t>> cand_or =
        relational_detail::SelectCandidateBlocks(*spec_.tlcs, spec_.preds);
    if (!cand_or.HasValue()) {
      return cand_or.GetStatus();
    }
    candidate_blocks_ = cand_or.MoveValue();
    if (candidate_blocks_.empty()) {
      finished_ = true;
      return Status::kSuccess;
    }
    if (!spec_.has_sort) {
      PrepareFilteredScanStage();
      return Status::kSuccess;
    }
    return RunTopKExecution();
  }

  void PrepareFilteredScanStage() {
    stage1_cols_.clear();
    stage1_lengths_only_.clear();
    const size_t ncols = spec_.table_schema.ColumnCount();
    std::vector<uint8_t> needed(ncols, 0);
    std::vector<uint8_t> needs_bytes(ncols, 0);
    for (size_t c = 0; c < ncols; ++c) {
      if (spec_.preds.col_referenced[c] != 0U) {
        needed[c] = 1;
        needs_bytes[c] = spec_.preds.col_needs_varchar_bytes[c];
      }
    }
    for (const slot_t c : spec_.visible_to_full) {
      if (c < ncols) {
        needed[c] = 1;
        if (spec_.table_schema.GetColumn(c).Type() == ValueType::kVarChar) {
          needs_bytes[c] = 1;
        }
      }
    }
    for (size_t c = 0; c < ncols; ++c) {
      if (needed[c] != 0U) {
        stage1_cols_.push_back(static_cast<slot_t>(c));
        stage1_lengths_only_.push_back(needs_bytes[c] != 0U ? 0 : 1);
      }
    }
  }

  size_t NextFilteredScanBatch(DataChunk* destination, size_t max_rows) {
    if (chunk_cursor_ < queued_chunks_.size()) {
      return DrainBufferedChunks(destination, max_rows);
    }
    queued_chunks_.clear();
    chunk_cursor_ = 0;

    const size_t limit_end =
        spec_.has_limit
            ? (spec_.offset <= std::numeric_limits<size_t>::max() - spec_.limit
                   ? spec_.offset + spec_.limit
                   : std::numeric_limits<size_t>::max())
            : std::numeric_limits<size_t>::max();

    const size_t workers = std::max<size_t>(
        1, std::min<size_t>(
               {QueryParallelWorkerLimit(std::thread::hardware_concurrency()),
                candidate_blocks_.size(), size_t{16}}));
    const size_t window_size = spec_.has_limit
                                   ? std::max<size_t>(32, workers * 8)
                                   : std::max<size_t>(512, workers * 64);
    const size_t ncols = spec_.table_schema.ColumnCount();

    while (block_cursor_ < candidate_blocks_.size() &&
           emitted_rows_seen_ < limit_end) {
      const size_t win_begin = block_cursor_;
      const size_t win_end =
          std::min(candidate_blocks_.size(), win_begin + window_size);
      block_cursor_ = win_end;
      const size_t win_blocks = win_end - win_begin;
      const size_t active_workers = std::min(workers, win_blocks);

      std::vector<std::vector<DataChunk>> worker_chunks(active_workers);
      std::vector<Status> worker_status(active_workers, Status::kSuccess);

      auto worker_fn = [&](size_t tid) {
        TlcsFile::DecodedBlock blk;
        std::vector<uint8_t> io_scratch;
        std::vector<const TlcsFile::DecodedColumnChunk*> col_ptrs(ncols,
                                                                  nullptr);
        std::vector<int32_t> col_to_chunk(ncols, -1);
        for (size_t i = 0; i < stage1_cols_.size(); ++i) {
          col_to_chunk[stage1_cols_[i]] = static_cast<int32_t>(i);
        }
        std::vector<uint32_t> matching_rows;
        std::vector<int64_t> int_buf;
        std::vector<double> dbl_buf;
        std::vector<std::string_view> sv_buf;
        std::vector<uint8_t> null_buf;

        for (size_t bi = tid; bi < win_blocks; bi += active_workers) {
          const uint32_t bidx = candidate_blocks_[win_begin + bi];
          Status st = spec_.tlcs->DecodeBlockColumnsWithOptions(
              bidx, stage1_cols_, spec_.table_schema, &blk, true, &io_scratch,
              stage1_lengths_only_);
          if (st != Status::kSuccess) {
            worker_status[tid] = std::move(st);
            return;
          }
          for (size_t i = 0; i < stage1_cols_.size(); ++i) {
            col_ptrs[stage1_cols_[i]] = &blk.columns[i];
          }
          matching_rows.clear();
          if (!relational_detail::TryJitContainsRows(spec_.preds, col_ptrs,
                                                     blk.row_count, false,
                                                     &matching_rows)) {
            bool precheck_ok = true;
            for (const auto& pc : spec_.preds.contains_prechecks) {
              const auto* ch = col_ptrs[pc.full_col];
              if (ch != nullptr &&
                  !relational_detail::BlockPassesContainsPrecheck(*ch,
                                                                  pc.needle)) {
                precheck_ok = false;
                break;
              }
            }
            if (!precheck_ok) {
              continue;
            }

            if (spec_.preds.Empty()) {
              matching_rows.resize(blk.row_count);
              for (uint32_t r = 0; r < blk.row_count; ++r) {
                matching_rows[r] = r;
              }
            } else if (spec_.preds.nulls.empty() &&
                       spec_.preds.inlists.empty() &&
                       spec_.preds.compares.size() == 1 &&
                       spec_.preds.compares[0].constant_payload &&
                       !spec_.preds.compares[0].is_double &&
                       !spec_.preds.compares[0].string_payload &&
                       spec_.preds.compares[0].op == BinaryOperation::kEquals &&
                       col_ptrs[spec_.preds.compares[0].left] != nullptr &&
                       !col_ptrs[spec_.preds.compares[0].left]->has_null) {
              const int64_t target = spec_.preds.compares[0].int_constant;
              const int64_t* cells =
                  col_ptrs[spec_.preds.compares[0].left]->int_cells.data();
              const uint32_t rc = blk.row_count;
              for (uint32_t r = 0; r < rc; ++r) {
                if (cells[r] == target) {
                  matching_rows.push_back(r);
                }
              }
            } else {
              for (uint32_t r = 0; r < blk.row_count; ++r) {
                if (relational_detail::RowMatchesColPtrs(spec_.preds, col_ptrs,
                                                         r)) {
                  matching_rows.push_back(r);
                }
              }
            }
          }
          if (matching_rows.empty()) {
            continue;
          }

          size_t off = 0;
          while (off < matching_rows.size()) {
            if (worker_chunks[tid].empty() ||
                worker_chunks[tid].back().Size() >= kDefaultVectorSize) {
              worker_chunks[tid].emplace_back(spec_.output_schema,
                                              kDefaultVectorSize);
            }
            DataChunk& dst = worker_chunks[tid].back();
            const size_t take = std::min(matching_rows.size() - off,
                                         kDefaultVectorSize - dst.Size());
            const std::span<const uint32_t> slice(matching_rows.data() + off,
                                                  take);
            for (size_t oc = 0; oc < spec_.visible_to_full.size(); ++oc) {
              const int32_t ci = col_to_chunk[spec_.visible_to_full[oc]];
              AppendDecodedColumnRows(blk.columns[static_cast<size_t>(ci)],
                                      slice, &dst.ColumnAt(oc), &int_buf,
                                      &dbl_buf, &sv_buf, &null_buf);
            }
            dst.CommitBatchAppend(take);
            off += take;
          }
        }
      };

      if (active_workers == 1) {
        worker_fn(0);
      } else {
        RunParallelTasks(active_workers, worker_fn);
      }
      for (const Status& st : worker_status) {
        if (st != Status::kSuccess) {
          FailWith(st);
          finished_ = true;
          return 0;
        }
      }

      for (auto& wv : worker_chunks) {
        for (DataChunk& chunk : wv) {
          if (chunk.Empty()) {
            continue;
          }
          if (emitted_rows_seen_ >= limit_end) {
            break;
          }
          const size_t csz = chunk.Size();
          if (emitted_rows_seen_ + csz <= spec_.offset) {
            emitted_rows_seen_ += csz;
            continue;
          }
          if (emitted_rows_seen_ < spec_.offset ||
              emitted_rows_seen_ + csz > limit_end) {
            DataChunk sliced(spec_.output_schema, csz);
            const size_t start = emitted_rows_seen_ < spec_.offset
                                     ? (spec_.offset - emitted_rows_seen_)
                                     : 0;
            const size_t stop = std::min(csz, limit_end - emitted_rows_seen_);
            for (size_t r = start; r < stop; ++r) {
              sliced.AppendFrom(chunk, r);
            }
            emitted_rows_seen_ += csz;
            if (!sliced.Empty()) {
              queued_chunks_.push_back(std::move(sliced));
            }
          } else {
            emitted_rows_seen_ += csz;
            queued_chunks_.push_back(std::move(chunk));
          }
        }
      }
      if (!queued_chunks_.empty()) {
        return DrainBufferedChunks(destination, max_rows);
      }
    }

    finished_ = true;
    return 0;
  }

  Status RunTopKExecution() {
    const size_t keep =
        spec_.offset <= std::numeric_limits<size_t>::max() - spec_.limit
            ? spec_.offset + spec_.limit
            : std::numeric_limits<size_t>::max();
    if (keep == 0 || candidate_blocks_.empty()) {
      finished_ = true;
      return Status::kSuccess;
    }

    const slot_t key0_col = spec_.sort_specs[0].full_col;
    const ValueType key0_type = spec_.sort_specs[0].type;
    const bool key0_asc = spec_.sort_specs[0].order.Ascending();
    const bool key0_nf = spec_.sort_specs[0].order.NullsFirst();
    const bool use_int_cutoff =
        (key0_type == ValueType::kInt64 || key0_type == ValueType::kDate);
    bool use_varchar_cutoff = (key0_type == ValueType::kVarChar &&
                               spec_.sort_specs[0].order.KeyCollation() !=
                                   Collation::kCaseInsensitive &&
                               candidate_blocks_.size() >= 2);

    struct BlockKeyMeta {
      uint32_t bidx{0};
      uint8_t tier{0};  // 0 = unknown/null-when-nulls-first, 1 = bounded, 2 =
                        // all-null-last
      bool is_const_null{false};
      bool has_null{false};
      bool has_min_max{false};
      int64_t min_int{0};
      int64_t max_int{0};
      uint64_t sort_score{0};
      TlcsFile::VarcharBlockBound vbound{};
    };
    std::vector<BlockKeyMeta> ordered_blocks;
    ordered_blocks.reserve(candidate_blocks_.size());
    std::shared_ptr<const std::vector<TlcsFile::VarcharBlockBound>> vbounds;
    if (use_varchar_cutoff) {
      auto vb_or = spec_.tlcs->GetOrComputeVarcharBlockBounds(
          key0_col, spec_.table_schema);
      RETURN_IF_FAIL(vb_or.GetStatus());
      vbounds = vb_or.MoveValue();
      if (vbounds == nullptr || vbounds->size() < spec_.tlcs->BlockCount()) {
        use_varchar_cutoff = false;
      }
    }
    // Lean proof: lean/TinylambOpt/ClickBenchRound2.lean
    //   (bottom_exclusion_tightens_min_bound_sound,
    //    interior_exclusion_min_bound_unsound_bug)
    bool key0_excludes_empty_and_null = false;
    if (use_varchar_cutoff) {
      for (const auto& fc : spec_.preds.compares) {
        if (fc.left == key0_col && fc.constant_payload && fc.string_payload &&
            fc.string_match == relational_detail::TypedFilterCompare::
                                   StringMatchKind::kExact &&
            fc.string_constant.empty() &&
            (fc.op == BinaryOperation::kNotEquals ||
             fc.op == BinaryOperation::kGreaterThan)) {
          key0_excludes_empty_and_null = true;
          break;
        }
      }
    }
    if (use_int_cutoff) {
      const uint32_t dg_count = spec_.tlcs->DirGroupCount();
      const uint32_t dg_size = spec_.tlcs->DirGroupSize();
      std::vector<std::shared_ptr<const TlcsFile::DirGroup>> dir_groups(
          dg_count);
      for (uint32_t g = 0; g < dg_count; ++g) {
        ASSIGN_OR_RETURN(std::shared_ptr<const TlcsFile::DirGroup>, grp,
                         spec_.tlcs->GetDirGroup(g));
        dir_groups[g] = std::move(grp);
      }
      for (const uint32_t bidx : candidate_blocks_) {
        const uint32_t g = bidx / dg_size;
        const uint32_t local = bidx % dg_size;
        const auto& meta = dir_groups[g]->blocks[local].columns[key0_col];
        BlockKeyMeta bm{.bidx = bidx};
        bm.is_const_null = (meta.encoding == TlcsColumnEncoding::kConstNull);
        bm.has_null = meta.has_null || bm.is_const_null;
        bm.has_min_max = meta.has_min_max;
        bm.min_int = meta.min_int;
        bm.max_int = meta.max_int;
        if (bm.is_const_null) {
          bm.tier = key0_nf ? 0 : 2;
          bm.sort_score = 0;
        } else if (key0_nf && bm.has_null) {
          bm.tier = 0;
          bm.sort_score = 1;
        } else if (bm.has_min_max) {
          bm.tier = 1;
          const int64_t best = key0_asc ? bm.min_int : bm.max_int;
          const uint64_t mapped =
              static_cast<uint64_t>(best) ^ (uint64_t{1} << 63);
          bm.sort_score = key0_asc ? mapped : ~mapped;
        } else {
          bm.tier = 0;
          bm.sort_score = 2;
        }
        ordered_blocks.push_back(bm);
      }
      std::sort(ordered_blocks.begin(), ordered_blocks.end(),
                [](const BlockKeyMeta& a, const BlockKeyMeta& b) {
                  if (a.tier != b.tier) {
                    return a.tier < b.tier;
                  }
                  if (a.sort_score != b.sort_score) {
                    return a.sort_score < b.sort_score;
                  }
                  return a.bidx < b.bidx;
                });
    } else if (use_varchar_cutoff) {
      for (const uint32_t bidx : candidate_blocks_) {
        const auto& vb = (*vbounds)[bidx];
        if (key0_excludes_empty_and_null &&
            (vb.is_const_null || !vb.has_nonempty_min)) {
          continue;
        }
        BlockKeyMeta bm{.bidx = bidx, .vbound = vb};
        if (vb.is_const_null) {
          bm.tier = key0_nf ? 0 : 2;
        } else if (vb.has_min_max && (key0_excludes_empty_and_null ||
                                      !(key0_nf && vb.has_null))) {
          bm.tier = 1;
        } else {
          bm.tier = 0;
        }
        ordered_blocks.push_back(bm);
      }
      if (ordered_blocks.empty()) {
        finished_ = true;
        return Status::kSuccess;
      }
      std::sort(
          ordered_blocks.begin(), ordered_blocks.end(),
          [&](const BlockKeyMeta& a, const BlockKeyMeta& b) {
            if (a.tier != b.tier) {
              return a.tier < b.tier;
            }
            if (a.tier == 1) {
              const std::string_view sa =
                  key0_asc ? (key0_excludes_empty_and_null
                                  ? std::string_view(
                                        a.vbound.nonempty_min_prefix.data(),
                                        a.vbound.nonempty_min_len)
                                  : std::string_view(a.vbound.min_prefix.data(),
                                                     a.vbound.min_len))
                           : std::string_view(a.vbound.max_prefix.data(),
                                              a.vbound.max_len);
              const std::string_view sb =
                  key0_asc ? (key0_excludes_empty_and_null
                                  ? std::string_view(
                                        b.vbound.nonempty_min_prefix.data(),
                                        b.vbound.nonempty_min_len)
                                  : std::string_view(b.vbound.min_prefix.data(),
                                                     b.vbound.min_len))
                           : std::string_view(b.vbound.max_prefix.data(),
                                              b.vbound.max_len);
              if (sa != sb) {
                return key0_asc ? (sa < sb) : (sa > sb);
              }
            }
            return a.bidx < b.bidx;
          });
    } else {
      for (const uint32_t bidx : candidate_blocks_) {
        ordered_blocks.push_back({.bidx = bidx});
      }
    }

    const size_t ncols = spec_.table_schema.ColumnCount();
    std::vector<uint8_t> in_stage1a(ncols, 0);
    std::vector<slot_t> stage1a_cols;
    for (const auto& pc : spec_.preds.contains_prechecks) {
      if (in_stage1a[pc.full_col] == 0U) {
        in_stage1a[pc.full_col] = 1;
        stage1a_cols.push_back(pc.full_col);
      }
    }

    std::vector<uint8_t> in_stage1b(ncols, 0);
    std::vector<uint8_t> stage1b_needs_bytes(ncols, 0);
    for (size_t c = 0; c < ncols; ++c) {
      if (spec_.preds.col_referenced[c] != 0U && in_stage1a[c] == 0U) {
        in_stage1b[c] = 1;
        if (spec_.preds.col_needs_varchar_bytes[c] != 0U) {
          stage1b_needs_bytes[c] = 1;
        }
      }
    }
    if (in_stage1a[key0_col] == 0U) {
      in_stage1b[key0_col] = 1;
      if (spec_.table_schema.GetColumn(key0_col).Type() ==
          ValueType::kVarChar) {
        stage1b_needs_bytes[key0_col] = 1;
      }
    }
    std::vector<slot_t> deferred_sort_cols;
    std::vector<uint8_t> is_deferred_sort_col(ncols, 0);
    for (size_t k = 1; k < spec_.sort_specs.size(); ++k) {
      const slot_t kc = spec_.sort_specs[k].full_col;
      if (in_stage1a[kc] != 0U || stage1b_needs_bytes[kc] != 0U) {
        continue;
      }
      if (spec_.table_schema.GetColumn(kc).Type() == ValueType::kVarChar) {
        if (is_deferred_sort_col[kc] == 0U) {
          is_deferred_sort_col[kc] = 1;
          deferred_sort_cols.push_back(kc);
        }
      } else {
        in_stage1b[kc] = 1;
      }
    }

    std::vector<slot_t> stage1b_cols;
    std::vector<uint8_t> stage1b_len_only;
    for (size_t c = 0; c < ncols; ++c) {
      if (in_stage1b[c] != 0U) {
        stage1b_cols.push_back(static_cast<slot_t>(c));
        stage1b_len_only.push_back(stage1b_needs_bytes[c] != 0U ? 0 : 1);
      }
    }

    const size_t workers = std::max<size_t>(
        1, std::min<size_t>(
               {QueryParallelWorkerLimit(std::thread::hardware_concurrency()),
                candidate_blocks_.size(), size_t{16}}));
    std::vector<std::vector<TopKWinner>> worker_heaps(workers);
    std::vector<Status> worker_status(workers, Status::kSuccess);
    std::atomic<size_t> next_idx{0};
    std::mutex cutoff_mu;
    std::atomic<bool> cutoff_ready{false};
    std::atomic<int64_t> cutoff_val{0};
    std::mutex v_cutoff_mu;
    std::atomic<uint64_t> v_cutoff_seq{0};
    std::string v_cutoff_str;

    auto publish_cutoff = [&](int64_t worst_val) {
      if (!use_int_cutoff) {
        return;
      }
      if (cutoff_ready.load(std::memory_order_acquire)) {
        const int64_t cur = cutoff_val.load(std::memory_order_relaxed);
        if (key0_asc ? (worst_val >= cur) : (worst_val <= cur)) {
          return;
        }
      }
      const std::scoped_lock<std::mutex> lock(cutoff_mu);
      if (!cutoff_ready.load(std::memory_order_relaxed)) {
        cutoff_val.store(worst_val, std::memory_order_relaxed);
        cutoff_ready.store(true, std::memory_order_release);
      } else {
        const int64_t cur = cutoff_val.load(std::memory_order_relaxed);
        if (key0_asc ? (worst_val < cur) : (worst_val > cur)) {
          cutoff_val.store(worst_val, std::memory_order_release);
        }
      }
    };

    // CompareVarCharForOrderBy re-keys ENUM spellings ("TESTENUM"+rank byte)
    // below every printable byte, but the block bounds and the cutoff below
    // use raw bytes. If either side is in that band the two orders disagree
    // and pruning would silently drop correct rows -- disable the cutoff
    // (the sibling relational_sort_limit.cpp bails out on the same band).
    const auto in_enum_band = [](std::string_view sv) noexcept {
      return sv.size() >= 9 && sv.size() <= 18 && sv.starts_with("TESTENUM");
    };
    auto publish_v_cutoff = [&](const std::string& worst_str) {
      if (!use_varchar_cutoff) {
        return;
      }
      const std::scoped_lock<std::mutex> lock(v_cutoff_mu);
      if (v_cutoff_seq.load(std::memory_order_relaxed) == 0 ||
          (key0_asc ? (worst_str < v_cutoff_str)
                    : (worst_str > v_cutoff_str))) {
        v_cutoff_str = worst_str;
        v_cutoff_seq.fetch_add(1, std::memory_order_release);
      }
    };

    auto worker_fn = [&](size_t tid) {
      std::vector<TopKWinner>& heap = worker_heaps[tid];
      heap.reserve(keep + 1);
      uint64_t local_v_seq = 0;
      std::string local_v_cut;
      TlcsFile::DecodedBlock blk1a;
      TlcsFile::DecodedBlock blk1b;
      TlcsFile::DecodedBlock blk_def;
      std::vector<uint8_t> io1a;
      std::vector<uint8_t> io1b;
      std::vector<uint8_t> iodef;
      std::vector<const TlcsFile::DecodedColumnChunk*> col_ptrs(ncols, nullptr);

      auto heap_cmp = [&](const TopKWinner& a, const TopKWinner& b) {
        return TopKWinnerComesFirst(a, b, spec_.sort_specs);
      };

      std::vector<uint32_t> cand_rows;
      while (true) {
        const size_t bi = next_idx.fetch_add(1, std::memory_order_relaxed);
        if (bi >= ordered_blocks.size()) {
          break;
        }
        const BlockKeyMeta& bm = ordered_blocks[bi];
        if (use_int_cutoff && cutoff_ready.load(std::memory_order_acquire)) {
          const int64_t cut = cutoff_val.load(std::memory_order_relaxed);
          if (bm.is_const_null) {
            if (!key0_nf) {
              continue;
            }
          } else if (bm.has_min_max && (!bm.has_null || !key0_nf)) {
            if (key0_asc ? (bm.min_int > cut) : (bm.max_int < cut)) {
              continue;
            }
          }
        } else if (use_varchar_cutoff) {
          const uint64_t cur_seq = v_cutoff_seq.load(std::memory_order_acquire);
          if (cur_seq != 0) {
            if (cur_seq != local_v_seq) {
              const std::scoped_lock<std::mutex> lock(v_cutoff_mu);
              local_v_cut = v_cutoff_str;
              local_v_seq = v_cutoff_seq.load(std::memory_order_relaxed);
            }
            // The cutoff is compared with raw bytes below; an ENUM-rekeyed
            // spelling would order differently from the winner heap and must
            // never prune blocks.
            if (in_enum_band(local_v_cut)) {
              // fall through to full decode for this block
            } else if (bm.vbound.is_const_null) {
              if (!key0_nf) {
                continue;
              }
            } else if (bm.vbound.has_min_max &&
                       (key0_excludes_empty_and_null || !bm.vbound.has_null ||
                        !key0_nf)) {
              if (key0_asc) {
                const std::string_view mp =
                    key0_excludes_empty_and_null
                        ? std::string_view(bm.vbound.nonempty_min_prefix.data(),
                                           bm.vbound.nonempty_min_len)
                        : std::string_view(bm.vbound.min_prefix.data(),
                                           bm.vbound.min_len);
                if (!mp.starts_with("TESTENUM") &&
                    mp > std::string_view(local_v_cut)) {
                  continue;
                }
              } else {
                const std::string_view mp(bm.vbound.max_prefix.data(),
                                          bm.vbound.max_len);
                if (!bm.vbound.max_truncated) {
                  if (mp < std::string_view(local_v_cut)) {
                    continue;
                  }
                } else {
                  const std::string_view c24(
                      local_v_cut.data(),
                      std::min<size_t>(24, local_v_cut.size()));
                  if (mp < c24) {
                    continue;
                  }
                }
              }
            }
          }
        }
        const uint32_t bidx = bm.bidx;
        for (const slot_t c : stage1a_cols) {
          col_ptrs[c] = nullptr;
        }
        for (const slot_t c : stage1b_cols) {
          col_ptrs[c] = nullptr;
        }
        for (const slot_t c : deferred_sort_cols) {
          col_ptrs[c] = nullptr;
        }

        cand_rows.clear();
        if (!stage1a_cols.empty()) {
          Status st = spec_.tlcs->DecodeBlockColumnsWithOptions(
              bidx, stage1a_cols, spec_.table_schema, &blk1a, true, &io1a, {});
          if (st != Status::kSuccess) {
            worker_status[tid] = std::move(st);
            return;
          }
          for (size_t i = 0; i < stage1a_cols.size(); ++i) {
            col_ptrs[stage1a_cols[i]] = &blk1a.columns[i];
          }
          if (!relational_detail::TryJitContainsRows(
                  spec_.preds, col_ptrs, blk1a.row_count, false, &cand_rows)) {
            bool precheck_ok = true;
            for (const auto& pc : spec_.preds.contains_prechecks) {
              const auto* ch = col_ptrs[pc.full_col];
              if (ch != nullptr &&
                  !relational_detail::BlockPassesContainsPrecheck(*ch,
                                                                  pc.needle)) {
                precheck_ok = false;
                break;
              }
            }
            if (!precheck_ok) {
              continue;
            }
            for (uint32_t r = 0; r < blk1a.row_count; ++r) {
              if (relational_detail::RowMatchesColPtrs(spec_.preds, col_ptrs,
                                                       r)) {
                cand_rows.push_back(r);
              }
            }
          }
          if (cand_rows.empty()) {
            continue;
          }
        }

        if (!stage1b_cols.empty()) {
          Status st = spec_.tlcs->DecodeBlockColumnsWithOptions(
              bidx, stage1b_cols, spec_.table_schema, &blk1b, true, &io1b,
              stage1b_len_only);
          if (st != Status::kSuccess) {
            worker_status[tid] = std::move(st);
            return;
          }
          for (size_t i = 0; i < stage1b_cols.size(); ++i) {
            col_ptrs[stage1b_cols[i]] = &blk1b.columns[i];
          }
        }
        const uint32_t block_rows =
            !stage1b_cols.empty() ? blk1b.row_count : blk1a.row_count;
        bool deferred_loaded = deferred_sort_cols.empty();

        auto process_row = [&](uint32_t r) -> bool {
          if (!stage1b_cols.empty() && !spec_.preds.Empty() &&
              !relational_detail::RowMatchesColPtrs(spec_.preds, col_ptrs, r)) {
            return true;
          }

          if (heap.size() == keep) {
            const auto* c0 = col_ptrs[key0_col];
            const int c = CompareChunkRowAgainstHeapKey(
                *c0, r, heap.front().keys[0], spec_.sort_specs[0]);
            if (c > 0) {
              return true;
            }
            if (c == 0 && spec_.sort_specs.size() == 1 &&
                (bidx > heap.front().block_idx ||
                 (bidx == heap.front().block_idx &&
                  r >= heap.front().row_idx))) {
              return true;
            }
          }

          if (!deferred_loaded) {
            Status st = spec_.tlcs->DecodeBlockColumnsWithOptions(
                bidx, deferred_sort_cols, spec_.table_schema, &blk_def, true,
                &iodef, {});
            if (st != Status::kSuccess) {
              worker_status[tid] = std::move(st);
              return false;
            }
            for (size_t i = 0; i < deferred_sort_cols.size(); ++i) {
              col_ptrs[deferred_sort_cols[i]] = &blk_def.columns[i];
            }
            deferred_loaded = true;
          }

          if (heap.size() == keep) {
            bool better = false;
            bool tied = true;
            for (size_t k = 0; k < spec_.sort_specs.size(); ++k) {
              const auto* ck = col_ptrs[spec_.sort_specs[k].full_col];
              const int c = CompareChunkRowAgainstHeapKey(
                  *ck, r, heap.front().keys[k], spec_.sort_specs[k]);
              if (c != 0) {
                better = (c < 0);
                tied = false;
                break;
              }
            }
            if (!better && !tied) {
              return true;
            }
            if (tied && (bidx > heap.front().block_idx ||
                         (bidx == heap.front().block_idx &&
                          r >= heap.front().row_idx))) {
              return true;
            }
          }

          TopKWinner cand;
          cand.block_idx = bidx;
          cand.row_idx = r;
          cand.keys.resize(spec_.sort_specs.size());
          for (size_t k = 0; k < spec_.sort_specs.size(); ++k) {
            const auto& sk = spec_.sort_specs[k];
            const auto* ck = col_ptrs[sk.full_col];
            HeapKeyCell& cell = cand.keys[k];
            cell.is_null = ck->IsNull(r);
            if (!cell.is_null) {
              switch (sk.type) {
                case ValueType::kInt64:
                case ValueType::kDate:
                  cell.int_val = ck->int_cells[r];
                  break;
                case ValueType::kDouble:
                  cell.double_val = std::bit_cast<double>(ck->int_cells[r]);
                  break;
                case ValueType::kVarChar:
                  cell.str_val.assign(ck->VarcharAt(r));
                  break;
                default:
                  break;
              }
            }
          }
          if (heap.size() < keep) {
            heap.push_back(std::move(cand));
            std::push_heap(heap.begin(), heap.end(), heap_cmp);
            if (heap.size() == keep && !heap.front().keys[0].is_null) {
              if (use_int_cutoff) {
                publish_cutoff(heap.front().keys[0].int_val);
              } else if (use_varchar_cutoff) {
                publish_v_cutoff(heap.front().keys[0].str_val);
              }
            }
          } else {
            std::pop_heap(heap.begin(), heap.end(), heap_cmp);
            heap.back() = std::move(cand);
            std::push_heap(heap.begin(), heap.end(), heap_cmp);
            if (!heap.front().keys[0].is_null) {
              if (use_int_cutoff) {
                publish_cutoff(heap.front().keys[0].int_val);
              } else if (use_varchar_cutoff) {
                publish_v_cutoff(heap.front().keys[0].str_val);
              }
            }
          }
          return true;
        };

        if (stage1a_cols.empty()) {
          for (uint32_t r = 0; r < block_rows; ++r) {
            if (!process_row(r)) {
              return;
            }
          }
        } else {
          for (const uint32_t r : cand_rows) {
            if (!process_row(r)) {
              return;
            }
          }
        }
      }
    };

    if (workers == 1) {
      worker_fn(0);
    } else {
      RunParallelTasks(workers, worker_fn);
    }
    for (const Status& st : worker_status) {
      if (st != Status::kSuccess) {
        return st;
      }
    }

    std::vector<TopKWinner> merged;
    size_t total_candidates = 0;
    for (const auto& h : worker_heaps) {
      total_candidates += h.size();
    }
    merged.reserve(total_candidates);
    for (auto& h : worker_heaps) {
      for (auto& item : h) {
        merged.push_back(std::move(item));
      }
    }
    if (merged.size() <= spec_.offset) {
      finished_ = true;
      return Status::kSuccess;
    }
    const size_t final_end = std::min(merged.size(), keep);
    std::partial_sort(
        merged.begin(), merged.begin() + static_cast<std::ptrdiff_t>(final_end),
        merged.end(), [&](const TopKWinner& a, const TopKWinner& b) {
          return TopKWinnerComesFirst(a, b, spec_.sort_specs);
        });

    std::unordered_map<uint32_t, std::unique_ptr<TlcsFile::DecodedBlock>>
        decoded_cache;
    for (size_t i = spec_.offset; i < final_end; ++i) {
      const uint32_t bidx = merged[i].block_idx;
      if (decoded_cache.contains(bidx)) {
        continue;
      }
      auto blk = std::make_unique<TlcsFile::DecodedBlock>();
      RETURN_IF_FAIL(spec_.tlcs->DecodeBlockColumns(
          bidx, spec_.visible_to_full, spec_.table_schema, blk.get(), true));
      decoded_cache.emplace(bidx, std::move(blk));
    }

    size_t off = spec_.offset;
    while (off < final_end) {
      const size_t batch =
          std::min<size_t>(final_end - off, kDefaultVectorSize);
      DataChunk chunk(spec_.output_schema, batch);
      for (size_t r = 0; r < batch; ++r) {
        const TopKWinner& w = merged[off + r];
        const auto& blk = *decoded_cache.at(w.block_idx);
        for (size_t c = 0; c < spec_.visible_to_full.size(); ++c) {
          chunk.ColumnAt(c).Append(
              relational_detail::ValueFromChunkRow(blk.columns[c], w.row_idx));
        }
        chunk.CommitBatchAppend(1);
      }
      queued_chunks_.push_back(std::move(chunk));
      off += batch;
    }
    return Status::kSuccess;
  }

  size_t DrainBufferedChunks(DataChunk* destination, size_t max_rows) {
    while (chunk_cursor_ < queued_chunks_.size() &&
           (queued_chunks_[chunk_cursor_].Empty() ||
            queued_row_offset_ >= queued_chunks_[chunk_cursor_].Size())) {
      ++chunk_cursor_;
      queued_row_offset_ = 0;
    }
    if (chunk_cursor_ >= queued_chunks_.size()) {
      queued_chunks_.clear();
      chunk_cursor_ = 0;
      if (spec_.has_sort) {
        finished_ = true;
      }
      return 0;
    }
    DataChunk& src = queued_chunks_[chunk_cursor_];
    if (queued_row_offset_ == 0 && src.Size() <= max_rows) {
      *destination = std::move(src);
      ++chunk_cursor_;
      if (spec_.has_sort && chunk_cursor_ >= queued_chunks_.size()) {
        finished_ = true;
      }
      return destination->Size();
    }
    const size_t take = std::min(max_rows, src.Size() - queued_row_offset_);
    for (size_t r = 0; r < take; ++r) {
      destination->AppendFrom(src, queued_row_offset_ + r);
    }
    queued_row_offset_ += take;
    if (queued_row_offset_ >= src.Size()) {
      ++chunk_cursor_;
      queued_row_offset_ = 0;
    }
    if (spec_.has_sort && chunk_cursor_ >= queued_chunks_.size()) {
      finished_ = true;
    }
    return take;
  }

  DirectColumnarScanSpec spec_;
  Plan fallback_plan_;
  PipelineLoweringOptions fallback_options_;
  TransactionContext& ctx_;

  Executor fallback_executor_;
  bool initialized_{false};
  bool finished_{false};

  std::vector<uint32_t> candidate_blocks_;
  std::vector<slot_t> stage1_cols_;
  std::vector<uint8_t> stage1_lengths_only_;
  size_t block_cursor_{0};
  size_t emitted_rows_seen_{0};

  std::vector<DataChunk> queued_chunks_;
  size_t chunk_cursor_{0};
  size_t queued_row_offset_{0};
};

bool TryBuildDirectColumnarScanSpec(
    const Plan& plan, TransactionContext& ctx,
    const std::vector<Expression>& order_expressions,
    const std::vector<bool>& order_ascending,
    const std::vector<std::optional<bool>>& order_nulls_first,
    const std::vector<OocSortSink::KeySpec>& sort_keys, bool has_limit,
    size_t limit, size_t offset, size_t visible_columns,
    DirectColumnarScanSpec* out) {
  static const bool dbg = std::getenv("TINYLAMB_DEBUG_TLCS") != nullptr;
  auto dbg_bail = [&](const char* why) -> bool {
    if (dbg) {
      (void)std::fprintf(stderr, "[TLCS-SCAN-BAIL] %s\n", why);
    }
    return false;
  };
  if (!plan || plan->ScanSource() == nullptr) {
    return dbg_bail("null plan or no ScanSource");
  }
  const bool need_limit =
      (has_limit || offset != 0) && !plan->EnforcesLimit(limit, offset);
  const bool need_sort =
      !order_expressions.empty() &&
      !plan->IsOrderedBy(order_expressions, order_ascending, order_nulls_first);

  constexpr size_t kMaxInMemoryTopK = 65536;
  if (has_limit && (offset > kMaxInMemoryTopK || limit > kMaxInMemoryTopK ||
                    offset + limit > kMaxInMemoryTopK)) {
    return dbg_bail("limit+offset > 65536");
  }
  if (!has_limit && offset > 0) {
    return dbg_bail("offset without limit");
  }

  struct Frame {
    enum class Kind : uint8_t {
      kLimit,
      kProjection,
      kSelection,
      kSort,
      kTopN,
      kRename,
    };
    Kind kind;
    const PlanBase* node{nullptr};
  };
  std::vector<Frame> chain;
  const PlanBase* cur = plan.get();
  const FullScanPlan* full_scan = nullptr;
  bool seen_limit = false;
  bool seen_sort = false;
  bool seen_selection = false;

  while (cur != nullptr) {
    if (const auto* fs = dynamic_cast<const FullScanPlan*>(cur)) {
      full_scan = fs;
      break;
    }
    if (const auto* lim = dynamic_cast<const LimitPlan*>(cur)) {
      if (seen_limit || seen_sort || seen_selection || need_limit) {
        return dbg_bail("limit position or need_limit");
      }
      seen_limit = true;
      chain.push_back({Frame::Kind::kLimit, lim});
      cur = lim->Src().get();
      continue;
    }
    if (const auto* topn = dynamic_cast<const TopNPlan*>(cur)) {
      if (seen_limit || seen_sort || seen_selection || need_sort ||
          topn->WithTies() || !has_limit || topn->Limit() != limit ||
          topn->Offset() != offset) {
        return dbg_bail("topn position/params mismatch");
      }
      seen_limit = true;
      seen_sort = true;
      chain.push_back({Frame::Kind::kTopN, topn});
      cur = topn->Child().get();
      continue;
    }
    if (const auto* proj = dynamic_cast<const ProjectionPlan*>(cur)) {
      chain.push_back({Frame::Kind::kProjection, proj});
      cur = proj->GetSource().get();
      continue;
    }
    if (const auto* ren = dynamic_cast<const RelationRenamePlan*>(cur)) {
      chain.push_back({Frame::Kind::kRename, ren});
      cur = ren->GetSource().get();
      continue;
    }
    if (const auto* sel = dynamic_cast<const SelectionPlan*>(cur)) {
      seen_selection = true;
      chain.push_back({Frame::Kind::kSelection, sel});
      cur = sel->Src().get();
      continue;
    }
    if (const auto* sort = dynamic_cast<const SortPlan*>(cur)) {
      if (seen_sort || need_sort || !has_limit) {
        return dbg_bail("sort position or no limit");
      }
      seen_sort = true;
      chain.push_back({Frame::Kind::kSort, sort});
      cur = sort->Child().get();
      continue;
    }
    if (dbg) {
      (void)std::fprintf(stderr, "[TLCS-SCAN-BAIL] unsupported plan node: %s\n",
                         cur->ToString().c_str());
    }
    return false;
  }

  if (full_scan == nullptr || !full_scan->EmitsChainOrderedScan() ||
      full_scan->MaxRows() != std::numeric_limits<size_t>::max()) {
    return dbg_bail("full_scan missing/lock_rows/max_rows");
  }
  const Table* table = full_scan->ScanSource();
  if (table == nullptr) {
    return dbg_bail("null table");
  }
  std::shared_ptr<const TlcsFile> tlcs =
      relational_detail::ResolveCleanBackingTlcs(*table, ctx);
  if (tlcs == nullptr || tlcs->BlockCount() == 0) {
    return dbg_bail("no clean backing tlcs");
  }

  const Schema& table_schema = table->GetSchema();
  std::vector<slot_t> cur_to_full;
  if (full_scan->Projection().has_value()) {
    cur_to_full = *full_scan->Projection();
  } else {
    cur_to_full.resize(table_schema.ColumnCount());
    for (slot_t c = 0; c < table_schema.ColumnCount(); ++c) {
      cur_to_full[c] = c;
    }
  }

  relational_detail::ColumnarPredicateSet preds;
  preds.compares.reserve(8);
  const TopNPlan* embedded_topn = nullptr;
  std::vector<slot_t> topn_to_full;
  Schema topn_schema("", std::vector<Column>{});
  const SortPlan* embedded_sort = nullptr;
  std::vector<slot_t> sort_to_full;
  Schema sort_schema("", std::vector<Column>{});

  std::string cur_qualifier = std::string(table_schema.Name());
  for (size_t idx = chain.size(); idx > 0; --idx) {
    if (chain[idx - 1].kind == Frame::Kind::kRename) {
      cur_qualifier =
          static_cast<const RelationRenamePlan*>(chain[idx - 1].node)  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)
              ->RelationName();
      break;
    }
  }
  if (cur_qualifier.empty()) {
    auto probe_expr = [&](const Expression& e) {
      if (!e || !cur_qualifier.empty()) {
        return;
      }
      for (const ColumnName& cn : e->TouchedColumns()) {
        if (!cn.schema.empty()) {
          cur_qualifier = cn.schema;
          return;
        }
      }
    };
    for (const Frame& f : chain) {
      if (f.kind == Frame::Kind::kSelection) {
        probe_expr(static_cast<const SelectionPlan*>(f.node)->Predicate());  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)
      } else if (f.kind == Frame::Kind::kProjection) {
        for (const auto& item :
             static_cast<const ProjectionPlan*>(f.node)->Columns()) {  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)
          probe_expr(item.expression);
        }
      } else if (f.kind == Frame::Kind::kTopN) {
        for (const auto& k : static_cast<const TopNPlan*>(f.node)->Keys()) {  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)
          probe_expr(k.expression);
        }
      } else if (f.kind == Frame::Kind::kSort) {
        for (const auto& k : static_cast<const SortPlan*>(f.node)->Keys()) {  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)
          probe_expr(k.expression);
        }
      }
    }
    for (const auto& e : order_expressions) {
      probe_expr(e);
    }
  }

  for (size_t idx = chain.size(); idx > 0; --idx) {
    const Frame& frame = chain[idx - 1];
    if (frame.kind == Frame::Kind::kRename) {
      cur_qualifier =
          static_cast<const RelationRenamePlan*>(frame.node)->RelationName();  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)
    } else if (frame.kind == Frame::Kind::kSelection) {
      const auto* sel = static_cast<const SelectionPlan*>(frame.node);  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)
      std::vector<Expression> conjuncts = SplitConjuncts(sel->Predicate());
      relational_detail::ColumnarPredicateSet sub;
      const Schema sel_schema =
          EnsureQualified(sel->GetSchema(), cur_qualifier);
      if (!relational_detail::CompilePredicatesFromExpressions(
              conjuncts, {}, *tlcs, sel_schema, cur_to_full, table_schema, &sub,
              /*allow_parameter_slots=*/true)) {
        if (dbg) {
          const std::string c0 = sel_schema.ColumnCount() > 0
                                     ? sel_schema.GetColumn(0).Name().schema +
                                           "." +
                                           sel_schema.GetColumn(0).Name().name
                                     : "<empty>";
          (void)std::fprintf(
              stderr,
              "[TLCS-SCAN-BAIL] selection predicate unsupported: %s "
              "(qual=%s col0=%s)\n",
              sel->Predicate() ? sel->Predicate()->ToString().c_str()
                               : "<null>",
              cur_qualifier.c_str(), c0.c_str());
        }
        return false;
      }
      preds.compares.insert(preds.compares.end(), sub.compares.begin(),
                            sub.compares.end());
      preds.inlists.insert(preds.inlists.end(), sub.inlists.begin(),
                           sub.inlists.end());
      preds.nulls.insert(preds.nulls.end(), sub.nulls.begin(), sub.nulls.end());
    } else if (frame.kind == Frame::Kind::kProjection) {
      const auto* proj = static_cast<const ProjectionPlan*>(frame.node);  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)
      const Schema child_schema =
          EnsureQualified(proj->GetSource()->GetSchema(), cur_qualifier);
      std::vector<slot_t> next_to_full;
      next_to_full.reserve(proj->Columns().size());
      for (const NamedExpression& item : proj->Columns()) {
        if (!item.expression ||
            item.expression->Type() != TypeTag::kColumnValue) {
          return dbg_bail("projection contains non-column expression");
        }
        const auto slot = relational_detail::LocalColumnOffset(
            child_schema, item.expression->AsColumnValue().GetColumnName());
        if (!slot || *slot >= cur_to_full.size()) {
          return dbg_bail("projection column not resolved");
        }
        next_to_full.push_back(cur_to_full[*slot]);
      }
      cur_to_full = std::move(next_to_full);
    } else if (frame.kind == Frame::Kind::kTopN) {
      embedded_topn = static_cast<const TopNPlan*>(frame.node);  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)
      topn_to_full = cur_to_full;
      topn_schema =
          EnsureQualified(embedded_topn->Child()->GetSchema(), cur_qualifier);
    } else if (frame.kind == Frame::Kind::kSort) {
      embedded_sort = static_cast<const SortPlan*>(frame.node);  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)
      sort_to_full = cur_to_full;
      sort_schema =
          EnsureQualified(embedded_sort->Child()->GetSchema(), cur_qualifier);
    }
  }

  if (!full_scan->PeekCompares().empty()) {
    relational_detail::ColumnarPredicateSet peek_set;
    if (!relational_detail::CompilePredicatesFromExpressions(
            {}, full_scan->PeekCompares(), *tlcs, table_schema, {},
            table_schema, &peek_set)) {
      return dbg_bail("peek_compares unsupported");
    }
    preds.compares.insert(preds.compares.end(), peek_set.compares.begin(),
                          peek_set.compares.end());
  }
  relational_detail::FinalizePredicateMetadata(*tlcs, table_schema, &preds);

  const Schema& top_schema = plan->GetSchema();
  if (cur_to_full.size() != top_schema.ColumnCount() ||
      visible_columns > cur_to_full.size() || visible_columns == 0) {
    return dbg_bail("top schema slot count mismatch");
  }

  const bool effective_has_sort = !order_expressions.empty();
  if (!effective_has_sort && preds.Empty() && !has_limit) {
    return dbg_bail("plain unfiltered scan");
  }
  if (effective_has_sort && !has_limit) {
    return dbg_bail("unbounded sort");
  }

  std::vector<SortKeyColumnSpec> sort_specs;
  const Schema qualified_top_schema =
      EnsureQualified(top_schema, cur_qualifier);
  if (effective_has_sort) {
    const Schema& key_schema = embedded_topn != nullptr ? topn_schema
                               : embedded_sort != nullptr
                                   ? sort_schema
                                   : qualified_top_schema;
    const std::vector<slot_t>& key_map = embedded_topn != nullptr ? topn_to_full
                                         : embedded_sort != nullptr
                                             ? sort_to_full
                                             : cur_to_full;
    const size_t nkeys = embedded_topn != nullptr ? embedded_topn->Keys().size()
                         : embedded_sort != nullptr
                             ? embedded_sort->Keys().size()
                             : order_expressions.size();
    if (nkeys != order_expressions.size()) {
      return dbg_bail("sort key count mismatch");
    }
    sort_specs.reserve(nkeys);
    for (size_t i = 0; i < nkeys; ++i) {
      const Expression& expr =
          embedded_topn != nullptr   ? embedded_topn->Keys()[i].expression
          : embedded_sort != nullptr ? embedded_sort->Keys()[i].expression
                                     : order_expressions[i];
      if (!expr || expr->Type() != TypeTag::kColumnValue) {
        return dbg_bail("sort key not bare column");
      }
      const auto slot = relational_detail::LocalColumnOffset(
          key_schema, expr->AsColumnValue().GetColumnName());
      if (!slot || *slot >= key_map.size()) {
        return dbg_bail("sort key column not resolved");
      }
      const slot_t full_col = key_map[*slot];
      const Column& col = table_schema.GetColumn(full_col);
      if (col.IsUnsigned() || col.IsInterval() ||
          (col.Type() != ValueType::kInt64 && col.Type() != ValueType::kDate &&
           col.Type() != ValueType::kDouble &&
           col.Type() != ValueType::kVarChar)) {
        return dbg_bail("sort key type unsupported");
      }
      OocSortSink::KeySpec ks;
      ks.expression = expr;
      if (embedded_topn != nullptr) {
        ks.ascending = embedded_topn->Keys()[i].ascending;
        ks.nulls_first = embedded_topn->Keys()[i].nulls_first;
        ks.collation = embedded_topn->Keys()[i].collation;
      } else if (embedded_sort != nullptr) {
        ks.ascending = embedded_sort->Keys()[i].ascending;
        ks.nulls_first = embedded_sort->Keys()[i].nulls_first;
        ks.collation = embedded_sort->Keys()[i].collation;
      } else {
        ks.ascending = i < order_ascending.size() ? order_ascending[i] : true;
        ks.nulls_first =
            i < order_nulls_first.size() ? order_nulls_first[i] : std::nullopt;
        ks.collation =
            i < sort_keys.size() ? sort_keys[i].collation : Collation::kNone;
      }
      sort_specs.push_back({.full_col = full_col,
                            .type = col.Type(),
                            .order = MakeSortKeyOrder(ks)});
    }
  }

  std::vector<Column> out_cols;
  out_cols.reserve(visible_columns);
  std::vector<slot_t> visible_to_full(visible_columns);
  for (size_t i = 0; i < visible_columns; ++i) {
    visible_to_full[i] = cur_to_full[i];
    out_cols.push_back(top_schema.GetColumn(static_cast<slot_t>(i)));
  }

  out->table = table;
  out->tlcs = std::move(tlcs);
  out->table_schema = table_schema;
  out->output_schema = Schema("", std::move(out_cols));
  out->visible_to_full = std::move(visible_to_full);
  out->preds = std::move(preds);
  out->has_sort = effective_has_sort;
  out->sort_specs = std::move(sort_specs);
  out->has_limit = has_limit;
  out->limit = limit;
  out->offset = offset;
  return true;
}

}  // namespace

StatusOr<Executor> TryCreateDirectColumnarScanOrTopNExecutor(
    const Plan& plan, TransactionContext& ctx, bool distinct,
    const std::vector<Expression>& order_expressions,
    const std::vector<bool>& order_ascending,
    const std::vector<std::optional<bool>>& order_nulls_first,
    const std::vector<OocSortSink::KeySpec>& sort_keys, bool has_limit,
    size_t limit, size_t offset, bool with_ties, size_t visible_columns,
    size_t final_select_size, bool keep_plan_explain_text) {
  if (distinct || with_ties || keep_plan_explain_text ||
      ProcessMemoryBudget::Global().AlwaysSpillEnabled()) {
    return Executor{};
  }
  DirectColumnarScanSpec spec;
  if (!TryBuildDirectColumnarScanSpec(
          plan, ctx, order_expressions, order_ascending, order_nulls_first,
          sort_keys, has_limit, limit, offset, visible_columns, &spec)) {
    return Executor{};
  }

  PipelineLoweringOptions fallback_opts;
  fallback_opts.keep_plan_explain_text = keep_plan_explain_text;
  fallback_opts.apply_post_distinct = false;
  const bool need_sort =
      !order_expressions.empty() &&
      !plan->IsOrderedBy(order_expressions, order_ascending, order_nulls_first);
  const bool need_limit =
      (has_limit || offset != 0) && !plan->EnforcesLimit(limit, offset);
  const bool need_proj = visible_columns != final_select_size;
  if (need_sort) {
    fallback_opts.post_sort_keys = sort_keys;
  }
  if (need_limit) {
    if (has_limit) {
      fallback_opts.post_limit = limit;
    }
    fallback_opts.post_offset = offset;
  }
  if (need_proj) {
    const Schema& schema = plan->GetSchema();
    std::vector<Column> proj_cols;
    proj_cols.reserve(visible_columns);
    fallback_opts.post_projection.reserve(visible_columns);
    for (size_t i = 0; i < visible_columns && i < schema.ColumnCount(); ++i) {
      fallback_opts.post_projection.emplace_back(schema.GetColumn(i).Name());
      proj_cols.push_back(schema.GetColumn(i));
    }
    fallback_opts.post_projection_schema = Schema("", std::move(proj_cols));
  }

  return std::make_shared<ColumnarTlcsScanExecutor>(
      std::move(spec), plan, std::move(fallback_opts), ctx);
}

}  // namespace tinylamb
