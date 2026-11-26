/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/columnar_tlcs_fast_path.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/memory_budget.hpp"
#include "common/parallel_work.hpp"
#include "common/platform/process.hpp"
#include "common/query_parallelism.hpp"
#include "common/regex_guard.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/analytics_factorized_join.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/columnar_tlcs_internal.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/timestamp_trunc_kernel.hpp"
#include "executor/detail/typed_stream_aggregate_internal.hpp"
#include "executor/detail/typed_stream_aggregate_partitioned.hpp"
#include "executor/query_memory.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/array_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/unary_expression.hpp"
#include "table/table_statistics.hpp"
#include "transaction/transaction.hpp"
#include "type/collation.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {
namespace {

// Specialized single-int64/date worker group table using FlatIntKeyMap so
// integer-key grouped scans (Q28/Q43) pay a single 16-byte probe instead of
// multi-key slice hashing + indirect key array chase.
class SingleIntWorkerTable {
 public:
  explicit SingleIntWorkerTable(size_t agg_count, QueryMemoryCharge* charge,
                                bool count_star_only = false)
      : agg_count_(agg_count),
        count_star_only_(count_star_only),
        charge_(charge),
        arena_(charge) {}

  StatusOr<uint32_t> FindOrInsertNonNull(int64_t key) {
    bool inserted = false;
    uint32_t* slot = map_.TryEmplace(key, &inserted);
    if (!inserted) {
      return *slot;
    }
    const size_t gbytes =
        (count_star_only_ ? sizeof(int64_t)
                          : agg_count_ * sizeof(RuntimeAggState)) +
        sizeof(int64_t) + sizeof(uint64_t) + 32;
    if (keys_.size() >= reserved_cap_) {
      const size_t next_cap =
          std::max<size_t>(256, reserved_cap_ == 0 ? 256 : reserved_cap_ * 2);
      if (charge_ != nullptr &&
          !charge_->TryAdd((next_cap - reserved_cap_) * gbytes)) {
        return StatusError(StatusCode::kMemoryLimitExceeded,
                           "columnar single-int hash table exceeded budget");
      }
      reserved_cap_ = next_cap;
      keys_.reserve(next_cap);
      hashes_.reserve(next_cap);
      if (count_star_only_) {
        counts_only_.reserve(next_cap);
      } else {
        aggs_.reserve(next_cap * agg_count_);
      }
    }
    const auto gid = static_cast<uint32_t>(keys_.size());
    *slot = gid;
    keys_.push_back(key);
    uint64_t x = static_cast<uint64_t>(key) + 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    hashes_.push_back(x ^ (x >> 31));
    if (count_star_only_) {
      counts_only_.push_back(0);
    } else {
      aggs_.resize(aggs_.size() + agg_count_);
    }
    return gid;
  }

  StatusOr<RuntimeAggState*> FindOrInsertNullAggs() {
    if (!has_null_) {
      if (charge_ != nullptr &&
          !charge_->TryAdd((agg_count_ * sizeof(RuntimeAggState)) + 16)) {
        return StatusError(StatusCode::kMemoryLimitExceeded,
                           "columnar single-int null group exceeded budget");
      }
      has_null_ = true;
      null_aggs_.resize(agg_count_);
    }
    return null_aggs_.data();
  }

  void IncrementNullCount(int64_t delta = 1) {
    if (!has_null_) {
      has_null_ = true;
      null_count_only_ = 0;
    }
    null_count_only_ += delta;
  }

  void Reserve(size_t n) {
    if (n <= reserved_cap_) {
      return;
    }
    const size_t gbytes =
        (count_star_only_ ? sizeof(int64_t)
                          : agg_count_ * sizeof(RuntimeAggState)) +
        sizeof(int64_t) + sizeof(uint64_t) + 32;
    if (charge_ != nullptr && !charge_->TryAdd((n - reserved_cap_) * gbytes)) {
      return;
    }
    reserved_cap_ = n;
    keys_.reserve(n);
    hashes_.reserve(n);
    if (count_star_only_) {
      counts_only_.reserve(n);
    } else {
      aggs_.reserve(n * agg_count_);
    }
  }

  [[nodiscard]] size_t TotalGroups() const noexcept {
    return keys_.size() + (has_null_ ? 1 : 0);
  }
  [[nodiscard]] size_t NonNullCount() const noexcept { return keys_.size(); }
  [[nodiscard]] bool HasNullGroup() const noexcept { return has_null_; }
  [[nodiscard]] int64_t KeyAt(size_t gid) const noexcept { return keys_[gid]; }
  [[nodiscard]] uint64_t HashAt(size_t gid) const noexcept {
    return hashes_[gid];
  }
  [[nodiscard]] RuntimeAggState* AggsAt(size_t gid) noexcept {
    return &aggs_[gid * agg_count_];
  }
  [[nodiscard]] RuntimeAggState* NullAggs() noexcept {
    return null_aggs_.data();
  }
  [[nodiscard]] int64_t& CountAt(size_t gid) noexcept {
    return counts_only_[gid];
  }
  [[nodiscard]] int64_t CountAt(size_t gid) const noexcept {
    return counts_only_[gid];
  }
  [[nodiscard]] int64_t NullCount() const noexcept {
    return count_star_only_ ? null_count_only_ : null_aggs_[0].count;
  }
  [[nodiscard]] bool IsCountStarOnly() const noexcept {
    return count_star_only_;
  }
  static constexpr uint64_t kNullHash = 0x243f6a8885a308d3ULL;
  [[nodiscard]] PagedStringArena& Arena() noexcept { return arena_; }

 private:
  size_t agg_count_{0};
  bool count_star_only_{false};
  size_t reserved_cap_{0};
  QueryMemoryCharge* charge_{nullptr};
  PagedStringArena arena_;
  FlatIntKeyMap<uint32_t> map_;
  std::vector<int64_t> keys_;
  std::vector<uint64_t> hashes_;
  std::vector<RuntimeAggState> aggs_;
  std::vector<int64_t> counts_only_;
  bool has_null_{false};
  std::vector<RuntimeAggState> null_aggs_;
  int64_t null_count_only_{0};
};

// Lean proof: lean/TinylambOpt/VectorizedJoinAndPruning.lean
//   (dense_group_block_accum_monoid_eq, finite_sum_implies_no_inf,
//    nonnull_avg_count_eq_count_star)
struct CompiledFastAgg {
  enum class Kind : uint8_t {
    kUnsupported = 0,
    kCountStar,
    kCountCol,
    kSumDoubleCol,
    kAvgDoubleCol,
    kSumDoubleMul2,               // c0 * c1
    kSumDoubleMulOneMinus,        // c0 * (k1 - c1)
    kSumDoubleMulOneMinusOnePlus  // c0 * (k1 - c1) * (k2 + c2)
  };
  Kind kind{Kind::kUnsupported};
  slot_t c0{0};
  slot_t c1{0};
  slot_t c2{0};
  double k1{0.0};
  double k2{0.0};
  bool is_avg{false};
  int8_t share_dp_with{
      -1};  // index of kSumDoubleMulOneMinus with same (c0,c1,k1)
};

struct CompiledFastAggPlan {
  static constexpr size_t kMaxAggs = 16;
  bool eligible{false};
  size_t count{0};
  std::array<CompiledFastAgg, kMaxAggs> ops{};
  std::vector<slot_t> checked_cols;
};

CompiledFastAggPlan CompileFastAggPlan(const std::vector<ColAggSpec>& specs) {
  CompiledFastAggPlan plan;
  if (specs.empty() || specs.size() > CompiledFastAggPlan::kMaxAggs) {
    return plan;
  }
  auto add_col = [&](slot_t c) {
    if (std::ranges::find(plan.checked_cols, c) == plan.checked_cols.end()) {
      plan.checked_cols.push_back(c);
    }
  };
  for (size_t a = 0; a < specs.size(); ++a) {
    const ColAggSpec& s = specs[a];
    CompiledFastAgg op;
    if (s.kind == ColAggKind::kCountStar) {
      op.kind = CompiledFastAgg::Kind::kCountStar;
    } else if (s.double_arith != nullptr) {
      if (s.kind != ColAggKind::kSumDouble &&
          s.kind != ColAggKind::kAvgDouble) {
        return plan;
      }
      op.is_avg = (s.kind == ColAggKind::kAvgDouble);
      const auto& rpn = s.double_arith->rpn;
      using Op = ColDoubleArithExpr::Op;
      if (rpn.size() == 3 && rpn[0].op == Op::kColDouble &&
          rpn[1].op == Op::kColDouble && rpn[2].op == Op::kMul) {
        op.kind = CompiledFastAgg::Kind::kSumDoubleMul2;
        op.c0 = rpn[0].full_col;
        op.c1 = rpn[1].full_col;
        add_col(op.c0);
        add_col(op.c1);
      } else if (rpn.size() == 5 && rpn[0].op == Op::kColDouble &&
                 rpn[1].op == Op::kConst && rpn[2].op == Op::kColDouble &&
                 rpn[3].op == Op::kSub && rpn[4].op == Op::kMul) {
        op.kind = CompiledFastAgg::Kind::kSumDoubleMulOneMinus;
        op.c0 = rpn[0].full_col;
        op.c1 = rpn[2].full_col;
        op.k1 = rpn[1].constant;
        add_col(op.c0);
        add_col(op.c1);
      } else if (rpn.size() == 9 && rpn[0].op == Op::kColDouble &&
                 rpn[1].op == Op::kConst && rpn[2].op == Op::kColDouble &&
                 rpn[3].op == Op::kSub && rpn[4].op == Op::kMul &&
                 rpn[5].op == Op::kConst && rpn[6].op == Op::kColDouble &&
                 rpn[7].op == Op::kAdd && rpn[8].op == Op::kMul) {
        op.kind = CompiledFastAgg::Kind::kSumDoubleMulOneMinusOnePlus;
        op.c0 = rpn[0].full_col;
        op.c1 = rpn[2].full_col;
        op.c2 = rpn[6].full_col;
        op.k1 = rpn[1].constant;
        op.k2 = rpn[5].constant;
        add_col(op.c0);
        add_col(op.c1);
        add_col(op.c2);
      } else {
        return plan;
      }
    } else if (s.kind == ColAggKind::kCountCol) {
      op.kind = CompiledFastAgg::Kind::kCountCol;
      op.c0 = s.full_col;
      add_col(op.c0);
    } else if (s.kind == ColAggKind::kSumDouble) {
      op.kind = CompiledFastAgg::Kind::kSumDoubleCol;
      op.c0 = s.full_col;
      add_col(op.c0);
    } else if (s.kind == ColAggKind::kAvgDouble) {
      op.kind = CompiledFastAgg::Kind::kAvgDoubleCol;
      op.c0 = s.full_col;
      op.is_avg = true;
      add_col(op.c0);
    } else {
      return plan;
    }
    plan.ops[a] = op;
  }
  // Detect CSE between kSumDoubleMulOneMinus and kSumDoubleMulOneMinusOnePlus.
  for (size_t a = 0; a < specs.size(); ++a) {
    if (plan.ops[a].kind ==
        CompiledFastAgg::Kind::kSumDoubleMulOneMinusOnePlus) {
      for (size_t b = 0; b < a; ++b) {
        if (plan.ops[b].kind == CompiledFastAgg::Kind::kSumDoubleMulOneMinus &&
            plan.ops[b].c0 == plan.ops[a].c0 &&
            plan.ops[b].c1 == plan.ops[a].c1 &&
            plan.ops[b].k1 == plan.ops[a].k1) {
          plan.ops[a].share_dp_with = static_cast<int8_t>(b);
          break;
        }
      }
    }
  }
  plan.count = specs.size();
  plan.eligible = true;
  return plan;
}

bool FastAggBlockColumnsReady(
    const CompiledFastAggPlan& plan,
    const std::vector<const TlcsFile::DecodedColumnChunk*>& col_ptrs) noexcept {
  if (!plan.eligible) {
    return false;
  }
  for (size_t a = 0; a < plan.count; ++a) {
    const CompiledFastAgg& op = plan.ops[a];
    if (op.kind == CompiledFastAgg::Kind::kCountStar) {
      continue;
    }
    if (op.kind == CompiledFastAgg::Kind::kCountCol) {
      const auto* ch = col_ptrs[op.c0];
      if (ch == nullptr || ch->has_null) {
        return false;
      }
      continue;
    }
    const auto* ch0 = col_ptrs[op.c0];
    if (ch0 == nullptr || ch0->has_null || ch0->type != ValueType::kDouble) {
      return false;
    }
    if (op.kind == CompiledFastAgg::Kind::kSumDoubleMul2 ||
        op.kind == CompiledFastAgg::Kind::kSumDoubleMulOneMinus ||
        op.kind == CompiledFastAgg::Kind::kSumDoubleMulOneMinusOnePlus) {
      const auto* ch1 = col_ptrs[op.c1];
      if (ch1 == nullptr || ch1->has_null || ch1->type != ValueType::kDouble) {
        return false;
      }
    }
    if (op.kind == CompiledFastAgg::Kind::kSumDoubleMulOneMinusOnePlus) {
      const auto* ch2 = col_ptrs[op.c2];
      if (ch2 == nullptr || ch2->has_null || ch2->type != ValueType::kDouble) {
        return false;
      }
    }
  }
  return true;
}

bool TryFoldBlockScalarFastAggs(
    const CompiledFastAggPlan& plan,
    const std::vector<const TlcsFile::DecodedColumnChunk*>& col_ptrs,
    const std::vector<uint32_t>& cand_rows,
    std::vector<RuntimeAggState>& sa) noexcept {
  if (!FastAggBlockColumnsReady(plan, col_ptrs)) {
    return false;
  }
  const size_t n = cand_rows.size();
  if (n == 0) {
    return true;
  }
  const uint32_t* rows = cand_rows.data();
  for (size_t a = 0; a < plan.count; ++a) {
    const CompiledFastAgg& op = plan.ops[a];
    RuntimeAggState& st = sa[a];
    if (op.kind == CompiledFastAgg::Kind::kCountStar ||
        op.kind == CompiledFastAgg::Kind::kCountCol) {
      st.count += static_cast<int64_t>(n);
      st.has_value = true;
      continue;
    }
    double local_sum = 0.0;
    const int64_t* c0 = col_ptrs[op.c0]->int_cells.data();
    switch (op.kind) {
      case CompiledFastAgg::Kind::kSumDoubleCol:
      case CompiledFastAgg::Kind::kAvgDoubleCol:
        for (size_t i = 0; i < n; ++i) {
          local_sum += std::bit_cast<double>(c0[rows[i]]);
        }
        break;
      case CompiledFastAgg::Kind::kSumDoubleMul2: {
        const int64_t* c1 = col_ptrs[op.c1]->int_cells.data();
        for (size_t i = 0; i < n; ++i) {
          const uint32_t r = rows[i];
          local_sum +=
              std::bit_cast<double>(c0[r]) * std::bit_cast<double>(c1[r]);
        }
        break;
      }
      case CompiledFastAgg::Kind::kSumDoubleMulOneMinus: {
        const int64_t* c1 = col_ptrs[op.c1]->int_cells.data();
        const double k1 = op.k1;
        for (size_t i = 0; i < n; ++i) {
          const uint32_t r = rows[i];
          local_sum += std::bit_cast<double>(c0[r]) *
                       (k1 - std::bit_cast<double>(c1[r]));
        }
        break;
      }
      case CompiledFastAgg::Kind::kSumDoubleMulOneMinusOnePlus: {
        const int64_t* c1 = col_ptrs[op.c1]->int_cells.data();
        const int64_t* c2 = col_ptrs[op.c2]->int_cells.data();
        const double k1 = op.k1;
        const double k2 = op.k2;
        for (size_t i = 0; i < n; ++i) {
          const uint32_t r = rows[i];
          local_sum += std::bit_cast<double>(c0[r]) *
                       (k1 - std::bit_cast<double>(c1[r])) *
                       (k2 + std::bit_cast<double>(c2[r]));
        }
        break;
      }
      default:
        break;
    }
    // Lean proof: finite_sum_implies_no_inf.
    if (!op.is_avg && !std::isfinite(local_sum)) {
      for (size_t i = 0; i < n; ++i) {
        const uint32_t r = rows[i];
        double v = 0.0;
        if (op.kind == CompiledFastAgg::Kind::kSumDoubleCol) {
          v = std::bit_cast<double>(c0[r]);
        } else if (op.kind == CompiledFastAgg::Kind::kSumDoubleMul2) {
          v = std::bit_cast<double>(c0[r]) *
              std::bit_cast<double>(col_ptrs[op.c1]->int_cells[r]);
        } else if (op.kind == CompiledFastAgg::Kind::kSumDoubleMulOneMinus) {
          v = std::bit_cast<double>(c0[r]) *
              (op.k1 - std::bit_cast<double>(col_ptrs[op.c1]->int_cells[r]));
        } else {
          v = std::bit_cast<double>(c0[r]) *
              (op.k1 - std::bit_cast<double>(col_ptrs[op.c1]->int_cells[r])) *
              (op.k2 + std::bit_cast<double>(col_ptrs[op.c2]->int_cells[r]));
        }
        if (std::isinf(v)) {
          st.saw_inf = true;
          break;
        }
      }
    }
    st.double_val += local_sum;
    if (op.is_avg) {
      st.count += static_cast<int64_t>(n);
    }
    st.has_value = true;
  }
  return true;
}

template <typename GroupTableT>
bool TryFoldBlockDenseGroupsFastAggs(
    const CompiledFastAggPlan& plan,
    const std::vector<const TlcsFile::DecodedColumnChunk*>& col_ptrs,
    const std::vector<uint32_t>& cand_rows,
    const std::vector<uint32_t>& dk_row_gid, size_t num_groups,
    GroupTableT& wtable) noexcept {
  constexpr size_t kMaxDenseGroups = 64;
  if (num_groups == 0 || num_groups > kMaxDenseGroups ||
      !FastAggBlockColumnsReady(plan, col_ptrs)) {
    return false;
  }
  const size_t n = cand_rows.size();
  if (n == 0) {
    return true;
  }
  // Check if cand_rows is dense [0, n) to avoid indirect row loads.
  const bool dense_rows = (cand_rows.front() == 0 && cand_rows.back() == n - 1);

  std::array<uint64_t, kMaxDenseGroups> g_cnt{};
  std::array<std::array<double, kMaxDenseGroups>, CompiledFastAggPlan::kMaxAggs>
      g_sum{};

  const uint32_t* gids = dk_row_gid.data();
  const uint32_t* rows = cand_rows.data();

  // Single fused pass over the block so every column is read from L1/L2 once!
  std::array<const int64_t*, CompiledFastAggPlan::kMaxAggs> p0{};
  std::array<const int64_t*, CompiledFastAggPlan::kMaxAggs> p1{};
  std::array<const int64_t*, CompiledFastAggPlan::kMaxAggs> p2{};
  for (size_t a = 0; a < plan.count; ++a) {
    const CompiledFastAgg& op = plan.ops[a];
    if (op.kind != CompiledFastAgg::Kind::kCountStar) {
      p0[a] = col_ptrs[op.c0]->int_cells.data();
    }
    if (op.kind == CompiledFastAgg::Kind::kSumDoubleMul2 ||
        op.kind == CompiledFastAgg::Kind::kSumDoubleMulOneMinus ||
        op.kind == CompiledFastAgg::Kind::kSumDoubleMulOneMinusOnePlus) {
      p1[a] = col_ptrs[op.c1]->int_cells.data();
    }
    if (op.kind == CompiledFastAgg::Kind::kSumDoubleMulOneMinusOnePlus) {
      p2[a] = col_ptrs[op.c2]->int_cells.data();
    }
  }

  // Hoist switch outside the row loop so each aggregate executes a tight,
  // branch-free inner loop. Fuse (c0*(k1-c1), c0*(k1-c1)*(k2+c2)) pairs so
  // the common subexpression dp is computed once.
  if (dense_rows) {
    for (uint32_t i = 0; i < static_cast<uint32_t>(n); ++i) {
      ++g_cnt[gids[i]];
    }
  } else {
    for (size_t i = 0; i < n; ++i) {
      ++g_cnt[gids[i]];
    }
  }

  std::array<bool, CompiledFastAggPlan::kMaxAggs> done{};
  for (size_t a = 0; a < plan.count; ++a) {
    if (done[a]) {
      continue;
    }
    const CompiledFastAgg& op = plan.ops[a];
    if (op.kind == CompiledFastAgg::Kind::kCountStar ||
        op.kind == CompiledFastAgg::Kind::kCountCol) {
      done[a] = true;
      continue;
    }
    // Check if another aggregate shares dp = c0*(k1-c1) with `a`.
    int8_t paired = -1;
    if (op.kind == CompiledFastAgg::Kind::kSumDoubleMulOneMinus) {
      for (size_t b = a + 1; b < plan.count; ++b) {
        if (!done[b] && plan.ops[b].share_dp_with == static_cast<int8_t>(a)) {
          paired = static_cast<int8_t>(b);
          break;
        }
      }
    }
    double* dst = g_sum[a].data();
    const int64_t* c0 = p0[a];
    if (paired >= 0) {
      const auto b = static_cast<size_t>(static_cast<unsigned char>(paired));
      double* dst_b = g_sum[b].data();
      const int64_t* c1 = p1[a];
      const int64_t* c2 = p2[b];
      const double k1 = op.k1;
      const double k2 = plan.ops[b].k2;
      if (dense_rows) {
        for (uint32_t r = 0; r < static_cast<uint32_t>(n); ++r) {
          const uint32_t g = gids[r];
          const double dp = std::bit_cast<double>(c0[r]) *
                            (k1 - std::bit_cast<double>(c1[r]));
          dst[g] += dp;
          dst_b[g] += dp * (k2 + std::bit_cast<double>(c2[r]));
        }
      } else {
        for (size_t i = 0; i < n; ++i) {
          const uint32_t r = rows[i];
          const uint32_t g = gids[i];
          const double dp = std::bit_cast<double>(c0[r]) *
                            (k1 - std::bit_cast<double>(c1[r]));
          dst[g] += dp;
          dst_b[g] += dp * (k2 + std::bit_cast<double>(c2[r]));
        }
      }
      done[a] = true;
      done[b] = true;
      continue;
    }
    // Check if another aggregate sums the exact same single column (e.g.
    // SUM(x) and AVG(x) on the same non-null column).
    int8_t same_col_dup = -1;
    if (op.kind == CompiledFastAgg::Kind::kSumDoubleCol ||
        op.kind == CompiledFastAgg::Kind::kAvgDoubleCol) {
      for (size_t b = a + 1; b < plan.count; ++b) {
        if (!done[b] &&
            (plan.ops[b].kind == CompiledFastAgg::Kind::kSumDoubleCol ||
             plan.ops[b].kind == CompiledFastAgg::Kind::kAvgDoubleCol) &&
            plan.ops[b].c0 == op.c0) {
          same_col_dup = static_cast<int8_t>(b);
          break;
        }
      }
    }
    switch (op.kind) {
      case CompiledFastAgg::Kind::kSumDoubleCol:
      case CompiledFastAgg::Kind::kAvgDoubleCol:
        if (dense_rows) {
          for (uint32_t r = 0; r < static_cast<uint32_t>(n); ++r) {
            dst[gids[r]] += std::bit_cast<double>(c0[r]);
          }
        } else {
          for (size_t i = 0; i < n; ++i) {
            dst[gids[i]] += std::bit_cast<double>(c0[rows[i]]);
          }
        }
        if (same_col_dup >= 0) {
          const auto b =
              static_cast<size_t>(static_cast<unsigned char>(same_col_dup));
          std::copy(dst, dst + num_groups, g_sum[b].data());
          done[b] = true;
        }
        break;
      case CompiledFastAgg::Kind::kSumDoubleMul2: {
        const int64_t* c1 = p1[a];
        if (dense_rows) {
          for (uint32_t r = 0; r < static_cast<uint32_t>(n); ++r) {
            dst[gids[r]] +=
                std::bit_cast<double>(c0[r]) * std::bit_cast<double>(c1[r]);
          }
        } else {
          for (size_t i = 0; i < n; ++i) {
            const uint32_t r = rows[i];
            dst[gids[i]] +=
                std::bit_cast<double>(c0[r]) * std::bit_cast<double>(c1[r]);
          }
        }
        break;
      }
      case CompiledFastAgg::Kind::kSumDoubleMulOneMinus: {
        const int64_t* c1 = p1[a];
        const double k1 = op.k1;
        if (dense_rows) {
          for (uint32_t r = 0; r < static_cast<uint32_t>(n); ++r) {
            dst[gids[r]] += std::bit_cast<double>(c0[r]) *
                            (k1 - std::bit_cast<double>(c1[r]));
          }
        } else {
          for (size_t i = 0; i < n; ++i) {
            const uint32_t r = rows[i];
            dst[gids[i]] += std::bit_cast<double>(c0[r]) *
                            (k1 - std::bit_cast<double>(c1[r]));
          }
        }
        break;
      }
      case CompiledFastAgg::Kind::kSumDoubleMulOneMinusOnePlus: {
        const int64_t* c1 = p1[a];
        const int64_t* c2 = p2[a];
        const double k1 = op.k1;
        const double k2 = op.k2;
        if (dense_rows) {
          for (uint32_t r = 0; r < static_cast<uint32_t>(n); ++r) {
            dst[gids[r]] += std::bit_cast<double>(c0[r]) *
                            (k1 - std::bit_cast<double>(c1[r])) *
                            (k2 + std::bit_cast<double>(c2[r]));
          }
        } else {
          for (size_t i = 0; i < n; ++i) {
            const uint32_t r = rows[i];
            dst[gids[i]] += std::bit_cast<double>(c0[r]) *
                            (k1 - std::bit_cast<double>(c1[r])) *
                            (k2 + std::bit_cast<double>(c2[r]));
          }
        }
        break;
      }
      default:
        break;
    }
    done[a] = true;
  }

  // Merge block-local dense accumulators into global worker table once per
  // group (Lean proof: dense_group_block_accum_monoid_eq,
  // finite_sum_implies_no_inf, nonnull_avg_count_eq_count_star).
  for (size_t g = 0; g < num_groups; ++g) {
    if (g_cnt[g] == 0) {
      continue;
    }
    RuntimeAggState* st_arr = wtable.AggsAt(static_cast<uint32_t>(g));
    for (size_t a = 0; a < plan.count; ++a) {
      const CompiledFastAgg& op = plan.ops[a];
      RuntimeAggState& st = st_arr[a];
      if (op.kind == CompiledFastAgg::Kind::kCountStar ||
          op.kind == CompiledFastAgg::Kind::kCountCol) {
        st.count += static_cast<int64_t>(g_cnt[g]);
        st.has_value = true;
      } else {
        const double lsum = g_sum[a][g];
        if (!op.is_avg && !std::isfinite(lsum)) {
          for (size_t i = 0; i < n; ++i) {
            if (gids[i] != g) {
              continue;
            }
            const uint32_t r = dense_rows ? static_cast<uint32_t>(i) : rows[i];
            double v = 0.0;
            if (op.kind == CompiledFastAgg::Kind::kSumDoubleCol) {
              v = std::bit_cast<double>(p0[a][r]);
            } else if (op.kind == CompiledFastAgg::Kind::kSumDoubleMul2) {
              v = std::bit_cast<double>(p0[a][r]) *
                  std::bit_cast<double>(p1[a][r]);
            } else if (op.kind ==
                       CompiledFastAgg::Kind::kSumDoubleMulOneMinus) {
              v = std::bit_cast<double>(p0[a][r]) *
                  (op.k1 - std::bit_cast<double>(p1[a][r]));
            } else {
              v = std::bit_cast<double>(p0[a][r]) *
                  (op.k1 - std::bit_cast<double>(p1[a][r])) *
                  (op.k2 + std::bit_cast<double>(p2[a][r]));
            }
            if (std::isinf(v)) {
              st.saw_inf = true;
              break;
            }
          }
        }
        st.double_val += lsum;
        if (op.is_avg) {
          st.count += static_cast<int64_t>(g_cnt[g]);
        }
        st.has_value = true;
      }
    }
  }
  return true;
}

}  // namespace

StatusOr<std::optional<Relation>> TryRunColumnarTlcsAggregate(
    TransactionContext& context, const SelectStatement& stmt,
    const Expression& having_expr, const Table& table,
    const std::vector<slot_t>& projection, const Schema& qualified_schema,
    const Schema& table_schema, const TypedAggregationPlan* typed_plan,
    const std::vector<const AggregateExpression*>& aggregate_expressions,
    const std::vector<Expression>& scan_predicates,
    const LSTableStore* resident_store_hint, ExecutionRuntime* runtime,
    bool publish_lstable_marker, const std::string& fold_cache_key,
    std::chrono::steady_clock::time_point scan_begin) {
  static const bool dbg =
      ::tinylamb::platform::GetEnv("TINYLAMB_DEBUG_TLCS") != nullptr;
  auto dbg_bail = [&](const char* why) -> std::optional<Relation> {
    if (dbg) {
      (void)std::fprintf(stderr, "[TLCS-AGG-BAIL] %s (typed=%d, ngroups=%zu)\n",
                         why, typed_plan != nullptr ? 1 : 0,
                         stmt.GroupBy().size());
    }
    return std::optional<Relation>{};
  };
  if (::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_COLUMNAR_TLCS_AGG") !=
      nullptr) {
    return dbg_bail("disabled by env");
  }
  if (stmt.Distinct() || stmt.HasDistinctOn() || stmt.Qualify() ||
      stmt.WithTies() || stmt.ForUpdate() || stmt.SelectList().empty() ||
      (stmt.HasLimit() &&
       stmt.Offset() > std::numeric_limits<size_t>::max() - stmt.Limit()) ||
      ProcessMemoryBudget::Global().AlwaysSpillEnabled()) {
    return dbg_bail("stmt flags/spill gate");
  }
  if (having_expr &&
      (ContainsQuery(having_expr) || ContainsParameterSlot(having_expr) ||
       ContainsVolatileCall(having_expr))) {
    return dbg_bail("having query/param/volatile");
  }

  std::shared_ptr<const TlcsFile> tlcs =
      ResolveCleanBackingTlcs(table, context);
  if (tlcs == nullptr || tlcs->BlockCount() == 0) {
    return dbg_bail("no clean tlcs");
  }

  // Compile predicates into full table slots.
  ColumnarPredicateSet preds;
  if (typed_plan != nullptr) {
    if (!CompilePredicatesFromTypedPlan(*typed_plan, *tlcs, projection,
                                        table_schema, &preds)) {
      return dbg_bail("typed_plan predicates unsupported");
    }
    if (resident_store_hint != nullptr && !preds.contains_prechecks.empty() &&
        preds.compares.size() >= 3) {
      return dbg_bail("resident uncompressed store faster on multi-LIKE");
    }
  } else {
    if (!CompilePredicatesFromExpressions(scan_predicates, {}, *tlcs,
                                          qualified_schema, projection,
                                          table_schema, &preds)) {
      return dbg_bail("scan_predicates unsupported");
    }
  }

  // Classify aggregates.
  std::vector<ColAggSpec> agg_specs;
  agg_specs.reserve(aggregate_expressions.size());
  bool any_scalar_distinct = false;
  for (const AggregateExpression* agg : aggregate_expressions) {
    ColAggSpec spec;
    if (!TryClassifyAggregate(agg, qualified_schema, projection, table_schema,
                              &spec)) {
      if (dbg && agg != nullptr) {
        (void)std::fprintf(stderr, "[TLCS-AGG-BAIL] agg=%s\n",
                           agg->ToString().c_str());
      }
      return std::optional<Relation>{};
    }
    if (spec.kind == ColAggKind::kCountDistinctInt64 ||
        spec.kind == ColAggKind::kCountDistinctDate ||
        spec.kind == ColAggKind::kCountDistinctDouble ||
        spec.kind == ColAggKind::kCountDistinctVarChar) {
      any_scalar_distinct = true;
    }
    agg_specs.push_back(spec);
  }

  // Classify group keys.
  const bool is_group_all = stmt.IsGroupAll() || stmt.GroupBy().empty();
  std::vector<ColGroupKeySpec> key_specs;
  bool any_regex_key = false;
  bool any_computed_key = false;
  if (!is_group_all) {
    key_specs.reserve(stmt.GroupBy().size());
    for (const Expression& k : stmt.GroupBy()) {
      ColGroupKeySpec ks;
      if (!TryClassifyGroupKey(k, qualified_schema, projection, table_schema,
                               &ks)) {
        if (dbg && k) {
          (void)std::fprintf(stderr, "[TLCS-AGG-BAIL] group_key=%s\n",
                             k->ToString().c_str());
        }
        return std::optional<Relation>{};
      }
      if (ks.kind != ColKeyKind::kColInt64 && ks.kind != ColKeyKind::kColDate &&
          ks.kind != ColKeyKind::kColDouble &&
          ks.kind != ColKeyKind::kColVarChar) {
        any_computed_key = true;
        if (ks.kind == ColKeyKind::kRegexReplaceVarChar) {
          any_regex_key = true;
          if (tlcs->BlockCount() >= 128) {
            auto capture = DescribeDelimitedRegexCapture(ks.regex_pattern,
                                                         ks.regex_replacement);
            if (capture.HasValue() && capture.Value()) {
              ks.regex_capture_jit =
                  JitDelimitedCapture::Compile(*capture.Value());
            }
          }
        }
      }
      key_specs.push_back(std::move(ks));
    }
    if (key_specs.size() > 8) {
      return dbg_bail(">8 group keys");
    }
  }

  // Scalar COUNT(DISTINCT) over large tables is already handled optimally by
  // TryRunParallelScalarCountDistinct / sharded path.
  if (is_group_all && any_scalar_distinct) {
    return dbg_bail("scalar count(distinct)");
  }

  // Compile PositionalHaving.
  std::vector<PositionalHaving> positional_having;
  if (having_expr != nullptr) {
    std::vector<TypedAggregateInput> agg_inputs(agg_specs.size());
    for (size_t i = 0; i < agg_specs.size(); ++i) {
      agg_inputs[i].aggregate = agg_specs[i].expr;
    }
    if (!TryCompilePositionalHavingConjuncts(having_expr, agg_inputs,
                                             &positional_having)) {
      return dbg_bail("having unsupported");
    }
  }

  // Bind output items and ORDER BY items.
  std::vector<OutputSlotBinding> emit_bindings;
  // Bind output items directly against `key_specs` (`stmt.GroupBy()`) and
  // `agg_specs`, falling back to `typed_plan->emit_items[i].post_agg` only when
  // `typed_plan` key indices align 1-to-1 (no dropped constants/dependent
  // keys).
  const bool typed_keys_aligned =
      typed_plan != nullptr && typed_plan->dependent_key_programs.empty() &&
      typed_plan->key_offsets.size() == key_specs.size() &&
      typed_plan->emit_items.size() == stmt.SelectList().size();
  emit_bindings.resize(stmt.SelectList().size());
  for (size_t i = 0; i < stmt.SelectList().size(); ++i) {
    if (BindExpressionToOutputSlot(stmt.SelectList()[i].expression, key_specs,
                                   agg_specs, nullptr, &emit_bindings[i])) {
      continue;
    }
    if (typed_keys_aligned && typed_plan->emit_items[i].post_agg != nullptr) {
      emit_bindings[i].kind = OutputSlotBinding::Kind::kPostAgg;
      emit_bindings[i].post_agg = typed_plan->emit_items[i].post_agg;
      continue;
    }
    if (dbg && stmt.SelectList()[i].expression) {
      (void)std::fprintf(stderr, "[TLCS-AGG-BAIL] select_bind=%s\n",
                         stmt.SelectList()[i].expression->ToString().c_str());
    }
    return std::optional<Relation>{};
  }

  std::vector<ColumnarOrderBinding> order_bindings;
  if (!stmt.OrderBy().empty()) {
    const bool typed_order_aligned =
        typed_keys_aligned &&
        typed_plan->order_items.size() == stmt.OrderBy().size();
    order_bindings.reserve(stmt.OrderBy().size());
    for (size_t i = 0; i < stmt.OrderBy().size(); ++i) {
      const auto& term = stmt.OrderBy()[i];
      OutputSlotBinding ob;
      struct LocalSortKeySpec {
        bool ascending{true};
        std::optional<bool> nulls_first{std::nullopt};
        Collation collation{Collation::kNone};
      } ks{.ascending = term.ascending,
           .nulls_first = term.nulls_first,
           .collation = CollationOfExpression(term.expression)};
      if (!BindExpressionToOutputSlot(term.expression, key_specs, agg_specs,
                                      &stmt.SelectList(), &ob)) {
        if (typed_order_aligned &&
            typed_plan->order_items[i].post_agg != nullptr) {
          ob.kind = OutputSlotBinding::Kind::kPostAgg;
          ob.post_agg = typed_plan->order_items[i].post_agg;
        } else {
          if (dbg && term.expression) {
            (void)std::fprintf(stderr, "[TLCS-AGG-BAIL] order_bind=%s\n",
                               term.expression->ToString().c_str());
          }
          return std::optional<Relation>{};
        }
      }
      order_bindings.push_back(
          {.binding = std::move(ob), .order = MakeSortKeyOrder(ks)});
    }
  }

  if (!having_expr) {
    ASSIGN_OR_RETURN(std::optional<Relation>, metadata,
                     TryFoldTlcsBoundaryAggregate(
                         *tlcs, stmt, table_schema, qualified_schema, preds,
                         key_specs, agg_specs, emit_bindings, order_bindings,
                         stmt.SelectList(), runtime));
    if (metadata) {
      if (publish_lstable_marker && runtime != nullptr) {
        auto marker = std::make_shared<Relation>(runtime);
        marker->schema = qualified_schema;
        runtime->base_relations.emplace(fold_cache_key, std::move(marker));
      }
      if (dbg) {
        (void)std::fprintf(stderr, "[TLCS-AGG-OK] exact-boundary-statistics\n");
      }
      return metadata;
    }
  }

  // Prune directory blocks.
  const auto dir_begin = std::chrono::steady_clock::now();
  ASSIGN_OR_RETURN(std::vector<uint32_t>, candidate_blocks,
                   SelectCandidateBlocks(*tlcs, preds));
  const double dir_ms = ColumnarMsSince(dir_begin);

  // Optional Stage 1 -> Stage 2 block survivor memo for CB-3: avoids
  // re-decoding non-matching blocks and re-evaluating expensive `LIKE` / regex
  // predicates in Stage 2 when Stage 1 already filtered the exact same blocks.
  struct Stage1BlockFilterMemo {
    std::vector<uint8_t> block_has_match;
    std::vector<std::vector<uint16_t>> block_rows;
    std::atomic<size_t> cached_rows_total{0};
    bool complete{false};
  };
  struct HavingBucketCertificate {
    std::vector<uint8_t> tracked;
    uint32_t minimum_count;
    bool observe_only;
    std::optional<std::vector<uint32_t>>* observed_buckets;

    [[nodiscard]] size_t Bucket(uint64_t hash) const noexcept {
      return (hash ^ (hash >> 32U)) & (tracked.size() - 1);
    }
  };

  auto sum_int64_provably_no_overflow = [&](slot_t full_col) -> StatusOr<bool> {
    const uint32_t dg_count = tlcs->DirGroupCount();
    const uint32_t dg_size = tlcs->DirGroupSize();
    std::vector<std::shared_ptr<const TlcsFile::DirGroup>> dir_groups(dg_count);
    for (uint32_t g = 0; g < dg_count; ++g) {
      ASSIGN_OR_RETURN(std::shared_ptr<const TlcsFile::DirGroup>, grp,
                       tlcs->GetDirGroup(g));
      dir_groups[g] = std::move(grp);
    }
    __int128 max_pos_sum = 0;
    __int128 min_neg_sum = 0;
    for (const uint32_t bidx : candidate_blocks) {
      const auto& blk = dir_groups[bidx / dg_size]->blocks[bidx % dg_size];
      const auto& meta = blk.columns[full_col];
      if (meta.encoding == TlcsColumnEncoding::kConstNull ||
          blk.row_count == 0) {
        continue;
      }
      if (!meta.has_min_max) {
        return false;
      }
      if (meta.max_int > 0) {
        max_pos_sum += static_cast<__int128>(meta.max_int) *
                       static_cast<__int128>(blk.row_count);
        if (max_pos_sum >
            static_cast<__int128>(std::numeric_limits<int64_t>::max())) {
          return false;
        }
      }
      if (meta.min_int < 0) {
        min_neg_sum += static_cast<__int128>(meta.min_int) *
                       static_cast<__int128>(blk.row_count);
        if (min_neg_sum <
            static_cast<__int128>(std::numeric_limits<int64_t>::min())) {
          return false;
        }
      }
    }
    return true;
  };

  // Helper that executes a classified `(key_specs, agg_specs, having, order,
  // emit)` plan over `candidate_blocks` using either the partitioned sharded
  // Wave-B2 engine or the resident worker engine.
  auto run_classified_plan =
      [&](const SelectStatement& plan_stmt,
          const std::vector<ColAggSpec>& plan_agg_specs,
          const std::vector<PositionalHaving>& plan_having,
          const std::vector<OutputSlotBinding>& plan_emit,
          const std::vector<ColumnarOrderBinding>& plan_order,
          const std::vector<NamedExpression>& plan_select_names,
          ExecutionRuntime* plan_runtime, bool plan_publish_marker,
          Stage1BlockFilterMemo* filter_memo_out = nullptr,
          const HavingBucketCertificate* certificate =
              nullptr) -> StatusOr<std::optional<Relation>> {
    const SelectStatement& stmt = plan_stmt;
    bool plan_any_byte_len = false;
    bool plan_any_distinct = false;
    bool plan_any_varchar_distinct = false;
    for (const ColAggSpec& spec : plan_agg_specs) {
      if (spec.kind == ColAggKind::kCountByteLen ||
          spec.kind == ColAggKind::kSumByteLen ||
          spec.kind == ColAggKind::kAvgByteLen ||
          spec.kind == ColAggKind::kMinByteLen ||
          spec.kind == ColAggKind::kMaxByteLen) {
        plan_any_byte_len = true;
      }
      if (spec.kind == ColAggKind::kCountDistinctInt64 ||
          spec.kind == ColAggKind::kCountDistinctDate ||
          spec.kind == ColAggKind::kCountDistinctDouble ||
          spec.kind == ColAggKind::kCountDistinctVarChar) {
        plan_any_distinct = true;
      }
      if (spec.kind == ColAggKind::kCountDistinctVarChar) {
        plan_any_varchar_distinct = true;
      }
    }

    // Empty pruned candidate set -> quick clean result!
    if (candidate_blocks.empty()) {
      Relation empty_out(plan_runtime);
      std::vector<Column> out_cols;
      out_cols.reserve(plan_emit.size());
      for (size_t i = 0; i < plan_emit.size(); ++i) {
        ValueType vt = ValueType::kInt64;
        std::string cname = "col_" + std::to_string(i);
        if (i < plan_select_names.size()) {
          cname = ProjectionName(plan_select_names[i], i);
          if (plan_select_names[i].expression) {
            auto rt =
                plan_select_names[i].expression->ResultType(qualified_schema);
            vt = rt.GetType() == TypeTag::kDouble    ? ValueType::kDouble
                 : rt.GetType() == TypeTag::kDate    ? ValueType::kDate
                 : rt.GetType() == TypeTag::kVarChar ? ValueType::kVarChar
                                                     : ValueType::kInt64;
          }
        }
        out_cols.emplace_back(std::move(cname), vt);
      }
      if (is_group_all) {
        std::vector<Value> finished_aggs(plan_agg_specs.size());
        for (size_t a = 0; a < plan_agg_specs.size(); ++a) {
          RuntimeAggState empty_st;
          ASSIGN_OR_RETURN(Value, finished_value,
                           (FinishAggState(plan_agg_specs[a], empty_st)));
          finished_aggs[a] = std::move(finished_value);
        }
        bool passes_having = true;
        for (const PositionalHaving& clause : plan_having) {
          const Value* rhs = clause.rhs_is_agg
                                 ? &finished_aggs[clause.rhs_agg_index]
                                 : nullptr;
          Status err = Status::kSuccess;
          const bool passes = EvalPositionalHavingValues(
              clause, finished_aggs[clause.agg_index], rhs, &err);
          RETURN_IF_FAIL(err);
          if (!passes) {
            passes_having = false;
            break;
          }
        }
        if (passes_having) {
          std::vector<Value> row_vals;
          row_vals.reserve(plan_emit.size());
          std::vector<Value> empty_keys;
          for (const auto& eb : plan_emit) {
            ASSIGN_OR_RETURN(Value, v,
                             EvaluateBindingValue(eb, key_specs, nullptr,
                                                  finished_aggs, &empty_keys));
            row_vals.push_back(std::move(v));
          }
          for (size_t i = 0; i < row_vals.size(); ++i) {
            if (!row_vals[i].IsNull()) {
              out_cols[i] =
                  Column(out_cols[i].Name(), ValueTypeOf(row_vals[i]));
              out_cols[i].SetUnsigned(row_vals[i].IsUnsigned());
            }
          }
          empty_out.AddRow(Row(std::move(row_vals)));
        }
      }
      empty_out.schema = Schema("", std::move(out_cols));
      return std::optional<Relation>(std::move(empty_out));
    }

    // Lean proof: lean/TinylambOpt/GroupKeyAndDictionaryRewrites.lean
    // (timestampTrunc_monotone, contiguous_prefix_preserves_complete_buckets,
    // periodic_extract_breaks_monotone_runs_bug)
    // CB-6: Monotone `TIMESTAMP_TRUNC` sorted-block early cutoff (ClickBench
    // Q43). When grouping by a single `TIMESTAMP_TRUNC(col, unit)` key,
    // ordering by that same key (`Collation::kNone`), and every candidate
    // block's `VarcharBlockBound` certifies canonical space-separated UTC
    // strings (`19` or `22` bytes) without NULLs, `TIMESTAMP_TRUNC` is strictly
    // monotone non-decreasing with respect to byte order. Sorting blocks by
    // `blk_min_trunc` (`ASC`) or `blk_max_trunc` (`DESC`) lets us stop reading
    // remaining blocks the instant `Offset + Limit` distinct buckets are
    // sealed!
    if (!is_group_all && key_specs.size() == 1 &&
        key_specs[0].kind == ColKeyKind::kUtcTimestampTrunc &&
        plan_having.empty() && !plan_any_distinct &&
        plan_agg_specs.size() == 1 &&
        plan_agg_specs[0].kind == ColAggKind::kCountStar && stmt.HasLimit() &&
        plan_order.size() == 1 &&
        plan_order[0].binding.kind == OutputSlotBinding::Kind::kKey &&
        plan_order[0].binding.index == 0 &&
        plan_order[0].order.KeyCollation() != Collation::kCaseInsensitive &&
        candidate_blocks.size() >= 4 &&
        std::all_of(plan_emit.begin(), plan_emit.end(),
                    [](const OutputSlotBinding& b) {
                      return b.kind == OutputSlotBinding::Kind::kKey ||
                             b.kind == OutputSlotBinding::Kind::kAgg ||
                             b.kind == OutputSlotBinding::Kind::kConstValue;
                    })) {
      const size_t cap =
          stmt.Offset() <= std::numeric_limits<size_t>::max() - stmt.Limit()
              ? stmt.Offset() + stmt.Limit()
              : std::numeric_limits<size_t>::max();
      // Serial sorted-block cutoff wins when `cap` buckets seal within the
      // first few blocks (`cap <= candidate_blocks * 16` — e.g. ~32 distinct
      // minutes per 32K-row block); when `cap` spans a large fraction of the
      // candidate blocks (`OFFSET 1000` over only 14 blocks), parallel multi-
      // worker scanning finishes faster.
      const bool cutoff_worthwhile =
          cap <= 64 || cap <= candidate_blocks.size() * 16;
      const slot_t ts_col = key_specs[0].full_col;
      std::shared_ptr<const std::vector<TlcsFile::VarcharBlockBound>> vbounds =
          cutoff_worthwhile ? tlcs->GetCachedVarcharBlockBounds(ts_col)
                            : nullptr;
      if (cutoff_worthwhile && vbounds == nullptr &&
          (tlcs->DictionaryKind(ts_col) == TlcsDictKind::kCompleteNarrow ||
           candidate_blocks.size() > 32 ||
           (preds.Empty() && candidate_blocks.size() >= 4 &&
            cap < candidate_blocks.size() * 64))) {
        auto vb_or = tlcs->GetOrComputeVarcharBlockBounds(ts_col, table_schema);
        if (vb_or.HasValue()) {
          vbounds = vb_or.MoveValue();
        }
      }
      if (vbounds != nullptr && cap > 0 && cap <= 32768) {
        struct BoundedBlock {
          uint32_t bidx{0};
          int64_t min_t{0};
          int64_t max_t{0};
        };
        std::vector<BoundedBlock> bblocks;
        bblocks.reserve(candidate_blocks.size());
        bool bounds_ok = true;
        for (const uint32_t bidx : candidate_blocks) {
          if (bidx >= vbounds->size()) {
            bounds_ok = false;
            break;
          }
          const auto& vb = (*vbounds)[bidx];
          if (vb.is_const_null) {
            if (vb.has_null) {
              bounds_ok = false;
              break;
            }
            continue;
          }
          if (vb.has_null || !vb.has_min_max || !vb.all_canonical_utc_ts ||
              vb.min_truncated || vb.max_truncated ||
              (vb.min_len != 19 && vb.min_len != 22) ||
              (vb.max_len != 19 && vb.max_len != 22)) {
            bounds_ok = false;
            break;
          }
          const auto min_k = TryUtcTimestampTruncKey(
              std::string_view(vb.min_prefix.data(), vb.min_len),
              key_specs[0].trunc_unit);
          const auto max_k = TryUtcTimestampTruncKey(
              std::string_view(vb.max_prefix.data(), vb.max_len),
              key_specs[0].trunc_unit);
          if (!min_k.has_value() || !max_k.has_value() || *min_k > *max_k) {
            bounds_ok = false;
            break;
          }
          bblocks.push_back({.bidx = bidx, .min_t = *min_k, .max_t = *max_k});
        }
        if (bounds_ok && !bblocks.empty()) {
          const bool asc = plan_order[0].order.Ascending();
          std::stable_sort(
              bblocks.begin(), bblocks.end(),
              [asc](const BoundedBlock& a, const BoundedBlock& b) noexcept {
                return asc ? (a.min_t < b.min_t) : (a.max_t > b.max_t);
              });
          const ColumnarStageSchedule cut_sched = BuildColumnarStageSchedule(
              table_schema, preds, key_specs, plan_agg_specs);
          QueryMemoryCharge cut_charge;
          SingleIntWorkerTable itable(1, &cut_charge,
                                      /*count_star_only=*/true);
          BlockDecodeScratch cut_scratch;
          TimestampPrefixCache ts_cache;
          std::vector<const TlcsFile::DecodedColumnChunk*> col_ptrs(
              table_schema.ColumnCount(), nullptr);
          std::vector<uint32_t> cand_rows;
          uint64_t scanned = 0;
          uint64_t matching = 0;

          auto heap_cmp = [asc](int64_t a, int64_t b) noexcept {
            return asc ? (a < b) : (a > b);
          };
          std::vector<int64_t> best_keys;
          best_keys.reserve(cap + 1);
          bool cut_ok = true;
          size_t blocks_visited = 0;

          for (const BoundedBlock& bb : bblocks) {
            if (best_keys.size() == cap) {
              const int64_t cutoff = best_keys.front();
              if (asc ? (bb.min_t > cutoff) : (bb.max_t < cutoff)) {
                break;
              }
            }
            ++blocks_visited;
            auto dec_or = DecodeAndFilterBlock(*tlcs, bb.bidx, table_schema,
                                               preds, cut_sched, &cut_scratch,
                                               &col_ptrs, &cand_rows, &scanned);
            if (!dec_or.HasValue()) {
              cut_ok = false;
              break;
            }
            if (!dec_or.Value()) {
              continue;
            }
            matching += cand_rows.size();
            const auto* kch = col_ptrs[ts_col];
            bool has_last = false;
            int64_t last_k = 0;
            uint32_t last_gid = 0;
            for (const uint32_t r : cand_rows) {
              const auto packed =
                  ts_cache.Trunc(kch->VarcharAt(r), key_specs[0].trunc_unit);
              if (!packed.has_value()) {
                cut_ok = false;
                break;
              }
              const int64_t kval = *packed;
              if (best_keys.size() == cap &&
                  (asc ? (kval > best_keys.front())
                       : (kval < best_keys.front()))) {
                continue;
              }
              uint32_t gid = 0;
              if (has_last && kval == last_k) {
                gid = last_gid;
              } else {
                const uint32_t prev_nn = itable.NonNullCount();
                auto gid_or = itable.FindOrInsertNonNull(kval);
                if (!gid_or.HasValue()) {
                  cut_ok = false;
                  break;
                }
                gid = gid_or.Value();
                if (itable.NonNullCount() > prev_nn) {
                  if (best_keys.size() < cap) {
                    best_keys.push_back(kval);
                    std::push_heap(best_keys.begin(), best_keys.end(),
                                   heap_cmp);
                  } else if (heap_cmp(kval, best_keys.front())) {
                    std::pop_heap(best_keys.begin(), best_keys.end(), heap_cmp);
                    best_keys.back() = kval;
                    std::push_heap(best_keys.begin(), best_keys.end(),
                                   heap_cmp);
                  }
                }
                last_k = kval;
                last_gid = gid;
                has_last = true;
              }
              ++itable.CountAt(gid);
            }
            if (!cut_ok) {
              break;
            }
          }

          if (cut_ok) {
            std::sort(best_keys.begin(), best_keys.end(), heap_cmp);
            if (plan_publish_marker && plan_runtime != nullptr) {
              auto marker = std::make_shared<Relation>(plan_runtime);
              marker->schema = qualified_schema;
              plan_runtime->base_relations.emplace(fold_cache_key,
                                                   std::move(marker));
            }
            Relation output(plan_runtime);
            output.rows.reserve(best_keys.size());
            std::vector<Column> out_cols;
            out_cols.reserve(plan_emit.size());
            for (size_t i = 0; i < plan_emit.size(); ++i) {
              std::string cname = i < plan_select_names.size()
                                      ? ProjectionName(plan_select_names[i], i)
                                      : ("col_" + std::to_string(i));
              out_cols.emplace_back(std::move(cname), ValueType::kInt64);
            }
            std::vector<uint8_t> type_refined(out_cols.size(), 0);
            for (const int64_t kv : best_keys) {
              ASSIGN_OR_RETURN(uint32_t, gid, itable.FindOrInsertNonNull(kv));
              const int64_t cnt = itable.CountAt(gid);
              std::vector<Value> row_vals;
              row_vals.reserve(plan_emit.size());
              for (size_t i = 0; i < plan_emit.size(); ++i) {
                Value v;
                if (plan_emit[i].kind == OutputSlotBinding::Kind::kConstValue) {
                  v = plan_emit[i].constant;
                } else if (plan_emit[i].kind == OutputSlotBinding::Kind::kAgg) {
                  v = Value(cnt);
                } else {
                  ASSIGN_OR_RETURN(Value, ts_v, FormatUtcTimestampTruncKey(kv));
                  v = std::move(ts_v);
                }
                if (type_refined[i] == 0U && !v.IsNull()) {
                  out_cols[i] = Column(out_cols[i].Name(), ValueTypeOf(v));
                  out_cols[i].SetUnsigned(v.IsUnsigned());
                  type_refined[i] = 1;
                }
                row_vals.push_back(std::move(v));
              }
              output.AddRow(Row(std::move(row_vals)));
            }
            output.schema = Schema("", std::move(out_cols));
            if (dbg) {
              (void)std::fprintf(stderr,
                                 "[TLCS-AGG-OK] mono-trunc-cutoff "
                                 "visited=%zu/%zu rows=%zu/%zu\n",
                                 blocks_visited, bblocks.size(), scanned,
                                 matching);
            }
            if (plan_runtime != nullptr) {
              const double scan_ms = ColumnarMsSince(scan_begin);
              plan_runtime->scan_ms += scan_ms;
              plan_runtime->filter_ms += scan_ms;
              plan_runtime->scan_rows += scanned;
              plan_runtime->scan_output_rows += matching;
              plan_runtime->typed_parallel_aggregations += 1;
              plan_runtime->aggregate_groups += itable.TotalGroups();
              plan_runtime->aggregate_input_rows += matching;
            }
            return std::optional<Relation>(std::move(output));
          }
        }
      }
    }

    // Block-monotonic streaming run aggregation: when grouping by a single
    // non-null integer/date column whose candidate TLCS block intervals are
    // monotonically non-decreasing (`block_max[m-1] <= block_min[m]`), any key
    // strictly inside `(shared_lo ? block_min[m] : -inf, shared_hi ?
    // block_max[m] : +inf)` is globally complete within block `m`. Evaluating
    // `HAVING` (or bounded Top-K) per block in O(1) streaming registers avoids
    // materializing multi-million-entry hash tables (e.g. TPC-H Q18's 60M-row,
    // 15M-group `lineitem` scan).
    const size_t mono_topk_cap =
        stmt.HasLimit() ? (stmt.Offset() <= std::numeric_limits<size_t>::max() -
                                                stmt.Limit()
                               ? stmt.Offset() + stmt.Limit()
                               : std::numeric_limits<size_t>::max())
                        : 0;
    const bool mono_bounded_topk = stmt.HasLimit() && !plan_order.empty() &&
                                   mono_topk_cap > 0 && mono_topk_cap <= 4096;
    // COUNT(DISTINCT int/date/double) folds per run like any other
    // aggregate (TPC-H Q21's `GROUP BY l_orderkey HAVING COUNT(DISTINCT
    // l_suppkey)` subqueries); the VARCHAR variant interns into the block
    // arena and stays on the partitioned path.
    if (filter_memo_out == nullptr && !is_group_all && key_specs.size() == 1 &&
        !plan_any_varchar_distinct && candidate_blocks.size() >= 4 &&
        (key_specs[0].kind == ColKeyKind::kColInt64 ||
         key_specs[0].kind == ColKeyKind::kColDate) &&
        (!plan_having.empty() || mono_bounded_topk)) {
      const slot_t kcol = key_specs[0].full_col;
      const uint32_t dg_count = tlcs->DirGroupCount();
      const uint32_t dg_size = tlcs->DirGroupSize();
      std::vector<std::shared_ptr<const TlcsFile::DirGroup>> dir_groups(
          dg_count);
      bool dir_ok = true;
      for (uint32_t g = 0; g < dg_count; ++g) {
        auto grp_or = tlcs->GetDirGroup(g);
        if (!grp_or.HasValue()) {
          dir_ok = false;
          break;
        }
        dir_groups[g] = std::move(grp_or.Value());
      }
      struct MonoBlockMeta {
        uint32_t bidx{0};
        int64_t min_k{0};
        int64_t max_k{0};
        bool shared_lo{false};
        bool shared_hi{false};
      };
      std::vector<MonoBlockMeta> mblocks;
      bool blocks_mono = dir_ok;
      bool any_int_sum_risk = false;
      __int128 pos_int_sum_bound = 0;
      __int128 neg_int_sum_bound = 0;
      if (blocks_mono) {
        mblocks.reserve(candidate_blocks.size());
        for (const uint32_t bidx : candidate_blocks) {
          const auto& blk = dir_groups[bidx / dg_size]->blocks[bidx % dg_size];
          if (blk.row_count == 0) {
            continue;
          }
          const auto& meta = blk.columns[kcol];
          if (meta.has_null || !meta.has_min_max || meta.is_unsigned ||
              meta.encoding == TlcsColumnEncoding::kConstNull) {
            blocks_mono = false;
            break;
          }
          if (!mblocks.empty() && mblocks.back().max_k > meta.min_int) {
            blocks_mono = false;
            break;
          }
          for (const ColAggSpec& as : plan_agg_specs) {
            if (as.kind == ColAggKind::kSumInt64 && !any_int_sum_risk) {
              const auto& am = blk.columns[as.full_col];
              if (am.encoding == TlcsColumnEncoding::kConstNull) {
                continue;
              }
              if (!am.has_min_max) {
                any_int_sum_risk = true;
              } else {
                if (am.max_int > 0) {
                  pos_int_sum_bound += static_cast<__int128>(am.max_int) *
                                       static_cast<__int128>(blk.row_count);
                }
                if (am.min_int < 0) {
                  neg_int_sum_bound += static_cast<__int128>(am.min_int) *
                                       static_cast<__int128>(blk.row_count);
                }
                if (pos_int_sum_bound >
                        static_cast<__int128>(
                            std::numeric_limits<int64_t>::max()) ||
                    neg_int_sum_bound <
                        static_cast<__int128>(
                            std::numeric_limits<int64_t>::min())) {
                  any_int_sum_risk = true;
                }
              }
            }
          }
          mblocks.push_back(
              {.bidx = bidx, .min_k = meta.min_int, .max_k = meta.max_int});
        }
      }
      if (blocks_mono && !mblocks.empty()) {
        for (size_t m = 0; m < mblocks.size(); ++m) {
          mblocks[m].shared_lo =
              (m > 0 && mblocks[m - 1].max_k == mblocks[m].min_k);
          mblocks[m].shared_hi = (m + 1 < mblocks.size() &&
                                  mblocks[m].max_k == mblocks[m + 1].min_k);
        }
        const ColumnarStageSchedule msched = BuildColumnarStageSchedule(
            table_schema, preds, key_specs, plan_agg_specs);
        const size_t mworkers =
            any_int_sum_risk
                ? size_t{1}
                : std::max<size_t>(
                      1, std::min<size_t>(
                             {QueryParallelWorkerLimit(
                                  std::thread::hardware_concurrency()),
                              mblocks.size(), size_t{16}}));
        // Lean proof: lean/TinylambOpt/VectorizedJoinAndPruning.lean
        //   (count_distinct_gt_one_iff_min_lt_max,
        //    count_distinct_eq_one_iff_min_eq_max)
        // Ultra-fast register-only streaming path for:
        //   SELECT k FROM T [WHERE ...] GROUP BY k HAVING MIN(v) < MAX(v) | =
        // Emits surviving keys directly into a 1-column ColumnarTable with
        // zero per-group Row/Value/MonoSurvivor allocations.
        const bool mono_key_only_minmax_fast =
            plan_emit.size() == 1 &&
            plan_emit[0].kind == OutputSlotBinding::Kind::kKey &&
            plan_order.empty() && !stmt.HasLimit() && stmt.Offset() == 0 &&
            plan_having.size() == 1 && plan_having[0].rhs_is_agg &&
            (plan_having[0].op == BinaryOperation::kLessThan ||
             plan_having[0].op == BinaryOperation::kEquals) &&
            plan_agg_specs.size() == 2 &&
            ((plan_agg_specs[0].kind == ColAggKind::kMinInt64 &&
              plan_agg_specs[1].kind == ColAggKind::kMaxInt64 &&
              plan_having[0].agg_index == 0 &&
              plan_having[0].rhs_agg_index == 1) ||
             (plan_agg_specs[0].kind == ColAggKind::kMaxInt64 &&
              plan_agg_specs[1].kind == ColAggKind::kMinInt64 &&
              plan_having[0].agg_index == 1 &&
              plan_having[0].rhs_agg_index == 0)) &&
            plan_agg_specs[0].full_col == plan_agg_specs[1].full_col &&
            !table_schema.GetColumn(plan_agg_specs[0].full_col).IsUnsigned();
        if (mono_key_only_minmax_fast) {
          const slot_t vcol = plan_agg_specs[0].full_col;
          const bool want_lt =
              (plan_having[0].op == BinaryOperation::kLessThan);
          struct MinMaxBoundary {
            int64_t key{0};
            int64_t rmin{0};
            int64_t rmax{0};
            bool has_val{false};
          };
          std::vector<std::vector<int64_t>> w_keys(mworkers);
          std::vector<std::vector<MinMaxBoundary>> w_bmm(mworkers);
          std::vector<Status> w_st(mworkers, Status::kSuccess);
          std::atomic<bool> fallback_needed{false};
          std::atomic<size_t> next_mb{0};
          std::atomic<uint64_t> mm_scanned{0};
          std::atomic<uint64_t> mm_matching{0};
          std::atomic<uint64_t> mm_groups{0};

          auto mm_worker = [&](size_t tid) {
            BlockDecodeScratch scratch;
            std::vector<const TlcsFile::DecodedColumnChunk*> col_ptrs(
                table_schema.ColumnCount(), nullptr);
            std::vector<uint32_t> cand_rows;
            uint64_t local_scanned = 0;
            uint64_t local_matching = 0;
            uint64_t local_groups = 0;
            auto& out_keys = w_keys[tid];
            auto& out_b = w_bmm[tid];
            out_keys.reserve(65536);

            while (!fallback_needed.load(std::memory_order_relaxed)) {
              const size_t m = next_mb.fetch_add(1, std::memory_order_relaxed);
              if (m >= mblocks.size()) {
                break;
              }
              const MonoBlockMeta& mb = mblocks[m];
              auto dec_or = DecodeAndFilterBlock(
                  *tlcs, mb.bidx, table_schema, preds, msched, &scratch,
                  &col_ptrs, &cand_rows, &local_scanned);
              if (!dec_or.HasValue()) {
                w_st[tid] = dec_or.GetStatus();
                fallback_needed.store(true, std::memory_order_relaxed);
                return;
              }
              if (!dec_or.Value() || cand_rows.empty()) {
                continue;
              }
              local_matching += cand_rows.size();
              const auto* kch = col_ptrs[kcol];
              const auto* vch = col_ptrs[vcol];
              if (kch == nullptr || vch == nullptr || kch->has_null ||
                  vch->has_null) {
                fallback_needed.store(true, std::memory_order_relaxed);
                return;
              }
              const int64_t* kcells = kch->int_cells.data();
              const int64_t* vcells = vch->int_cells.data();
              const uint32_t* rows = cand_rows.data();
              const size_t n = cand_rows.size();
              const bool dense = (rows[0] == 0 && rows[n - 1] == n - 1);
              // Verify monotonic key order within the block while streaming.
              int64_t cur_k = kcells[rows[0]];
              int64_t rmin = vcells[rows[0]];
              int64_t rmax = rmin;
              auto flush_mm = [&](int64_t fk, int64_t lo, int64_t hi) {
                if ((mb.shared_lo && fk == mb.min_k) ||
                    (mb.shared_hi && fk == mb.max_k)) {
                  out_b.push_back({fk, lo, hi, true});
                } else {
                  ++local_groups;
                  if (want_lt ? (lo < hi) : (lo == hi)) {
                    out_keys.push_back(fk);
                  }
                }
              };
              if (dense) {
                for (uint32_t r = 1; r < static_cast<uint32_t>(n); ++r) {
                  const int64_t kv = kcells[r];
                  const int64_t vv = vcells[r];
                  if (kv == cur_k) {
                    rmin = std::min(vv, rmin);
                    rmax = std::max(vv, rmax);
                  } else if (kv > cur_k) {
                    flush_mm(cur_k, rmin, rmax);
                    cur_k = kv;
                    rmin = vv;
                    rmax = vv;
                  } else {
                    fallback_needed.store(true, std::memory_order_relaxed);
                    return;
                  }
                }
              } else {
                for (size_t i = 1; i < n; ++i) {
                  const uint32_t r = rows[i];
                  const int64_t kv = kcells[r];
                  const int64_t vv = vcells[r];
                  if (kv == cur_k) {
                    rmin = std::min(vv, rmin);
                    rmax = std::max(vv, rmax);
                  } else if (kv > cur_k) {
                    flush_mm(cur_k, rmin, rmax);
                    cur_k = kv;
                    rmin = vv;
                    rmax = vv;
                  } else {
                    fallback_needed.store(true, std::memory_order_relaxed);
                    return;
                  }
                }
              }
              flush_mm(cur_k, rmin, rmax);
            }
            mm_scanned.fetch_add(local_scanned, std::memory_order_relaxed);
            mm_matching.fetch_add(local_matching, std::memory_order_relaxed);
            mm_groups.fetch_add(local_groups, std::memory_order_relaxed);
          };

          if (mworkers == 1) {
            mm_worker(0);
          } else {
            RETURN_IF_FAIL(RunParallelTasks(mworkers, mm_worker));
          }
          for (const Status& st : w_st) {
            RETURN_IF_FAIL(st);
          }
          if (!fallback_needed.load(std::memory_order_relaxed)) {
            const auto merge_begin = std::chrono::steady_clock::now();
            std::vector<MinMaxBoundary> all_b;
            for (auto& wb : w_bmm) {
              all_b.insert(all_b.end(), wb.begin(), wb.end());
            }
            std::ranges::sort(
                all_b, [](const MinMaxBoundary& a, const MinMaxBoundary& b) {
                  return a.key < b.key;
                });
            std::vector<int64_t> b_survivors;
            size_t b_groups = 0;
            for (size_t i = 0; i < all_b.size();) {
              int64_t lo = all_b[i].rmin;
              int64_t hi = all_b[i].rmax;
              size_t j = i + 1;
              while (j < all_b.size() && all_b[j].key == all_b[i].key) {
                lo = std::min(all_b[j].rmin, lo);
                hi = std::max(all_b[j].rmax, hi);
                ++j;
              }
              ++b_groups;
              if (want_lt ? (lo < hi) : (lo == hi)) {
                b_survivors.push_back(all_b[i].key);
              }
              i = j;
            }
            auto out_table = std::make_shared<ColumnarTable>();
            const ValueType ktype = (key_specs[0].kind == ColKeyKind::kColDate)
                                        ? ValueType::kDate
                                        : ValueType::kInt64;
            auto append_vec = [&](std::vector<int64_t>&& vec) {
              if (vec.empty()) {
                return;
              }
              ColumnarColumn col;
              col.type = ktype;
              col.is_uniform = true;
              col.has_null = false;
              col.encoding = ColumnarColumn::Encoding::kFlat;
              col.ints = std::move(vec);
              ColumnarChunk chunk;
              chunk.rows = col.ints.size();
              chunk.columns.push_back(std::move(col));
              out_table->AppendChunk(std::move(chunk));
            };
            for (auto& wk : w_keys) {
              append_vec(std::move(wk));
            }
            append_vec(std::move(b_survivors));

            Relation output(plan_runtime);
            std::string cname = plan_select_names[0].name;
            if (cname.empty()) {
              cname = "col_0";
            }
            output.schema = Schema("", {Column(std::move(cname), ktype)});
            output.SetColumnarTable(std::move(out_table), true);
            if (plan_runtime != nullptr) {
              const double scan_ms = ColumnarMsSince(scan_begin);
              plan_runtime->scan_ms += scan_ms;
              plan_runtime->filter_ms += scan_ms;
              plan_runtime->scan_rows += mm_scanned.load();
              plan_runtime->scan_output_rows += mm_matching.load();
              plan_runtime->aggregate_groups += mm_groups.load() + b_groups;
              plan_runtime->aggregate_input_rows += mm_matching.load();
              plan_runtime->aggregate_ms += ColumnarMsSince(merge_begin);
            }
            return std::optional<Relation>(std::move(output));
          }
        }

        struct MonoSurvivor {
          int64_t key{0};
          std::vector<Value> order_vals;
          std::vector<Value> emit_vals;
        };
        struct BoundaryGroup {
          int64_t key{0};
          std::vector<RuntimeAggState> aggs;
        };
        auto cmp_survivors = [&](const MonoSurvivor& a,
                                 const MonoSurvivor& b) noexcept {
          for (size_t k = 0; k < plan_order.size(); ++k) {
            const int c =
                plan_order[k].order.Compare(a.order_vals[k], b.order_vals[k]);
            if (c != 0) {
              return c < 0;
            }
          }
          return a.key < b.key;
        };
        auto emit_interior_or_boundary =
            [&](int64_t key, RuntimeAggState* ast, ChunkedCharge& charge,
                std::vector<MonoSurvivor>& surv_vec,
                std::vector<Value>& finished_scratch,
                std::vector<Value>& cached_keys) -> Status {
          ASSIGN_OR_RETURN(
              bool, pass,
              EvalPositionalHavingOnStates(plan_having, plan_agg_specs, ast,
                                           &finished_scratch));
          if (!pass) {
            return Status::kSuccess;
          }
          for (size_t a = 0; a < plan_agg_specs.size(); ++a) {
            ASSIGN_OR_RETURN(Value, finished_value,
                             (FinishAggState(plan_agg_specs[a], ast[a])));
            finished_scratch[a] = std::move(finished_value);
          }
          RuntimeKeyCell kc{.int_val = key};
          cached_keys.assign(1, Value());
          MonoSurvivor item;
          item.key = key;
          if (!plan_order.empty()) {
            item.order_vals.resize(plan_order.size());
            for (size_t k = 0; k < plan_order.size(); ++k) {
              ASSIGN_OR_RETURN(
                  Value, ov,
                  EvaluateBindingValue(plan_order[k].binding, key_specs, &kc,
                                       finished_scratch, &cached_keys));
              item.order_vals[k] = std::move(ov);
            }
            if (mono_bounded_topk && surv_vec.size() >= mono_topk_cap &&
                !cmp_survivors(item, surv_vec.front())) {
              return Status::kSuccess;
            }
          }
          item.emit_vals.resize(plan_emit.size());
          size_t approx_bytes =
              sizeof(MonoSurvivor) +
              ((item.order_vals.size() + plan_emit.size()) * sizeof(Value));
          for (size_t i = 0; i < plan_emit.size(); ++i) {
            ASSIGN_OR_RETURN(
                Value, ev,
                EvaluateBindingValue(plan_emit[i], key_specs, &kc,
                                     finished_scratch, &cached_keys));
            if (ev.type == ValueType::kVarChar) {
              approx_bytes += ev.AsStringView().size();
            }
            item.emit_vals[i] = std::move(ev);
          }
          if (mono_bounded_topk) {
            if (surv_vec.size() < mono_topk_cap) {
              if (!charge.TryAdd(approx_bytes)) {
                return StatusError(StatusCode::kMemoryLimitExceeded,
                                   "monotonic survivor heap exceeded budget");
              }
              surv_vec.push_back(std::move(item));
              std::push_heap(surv_vec.begin(), surv_vec.end(), cmp_survivors);
            } else {
              std::pop_heap(surv_vec.begin(), surv_vec.end(), cmp_survivors);
              surv_vec.back() = std::move(item);
              std::push_heap(surv_vec.begin(), surv_vec.end(), cmp_survivors);
            }
          } else {
            if (!charge.TryAdd(approx_bytes)) {
              return StatusError(StatusCode::kMemoryLimitExceeded,
                                 "monotonic survivors exceeded budget");
            }
            surv_vec.push_back(std::move(item));
          }
          return Status::kSuccess;
        };

        std::vector<QueryMemoryCharge> w_charges(mworkers);
        std::vector<std::unique_ptr<PagedStringArena>> b_arenas(mworkers);
        for (size_t w = 0; w < mworkers; ++w) {
          b_arenas[w] = std::make_unique<PagedStringArena>(&w_charges[w]);
        }
        std::vector<std::vector<MonoSurvivor>> w_survivors(mworkers);
        std::vector<std::vector<BoundaryGroup>> w_boundaries(mworkers);
        std::vector<Status> w_status(mworkers, Status::kSuccess);
        std::atomic<size_t> next_mb{0};
        std::atomic<uint64_t> mono_scanned{0};
        std::atomic<uint64_t> mono_matching{0};
        std::atomic<uint64_t> mono_interior_groups{0};

        auto copy_to_boundary =
            [&](int64_t key, RuntimeAggState* src_aggs, ChunkedCharge& charge,
                PagedStringArena& barena,
                std::vector<BoundaryGroup>& out_b) -> Status {
          const size_t bbytes =
              sizeof(BoundaryGroup) +
              (plan_agg_specs.size() * sizeof(RuntimeAggState));
          if (!charge.TryAdd(bbytes)) {
            return StatusError(StatusCode::kMemoryLimitExceeded,
                               "monotonic boundary groups exceeded budget");
          }
          BoundaryGroup bg{
              .key = key,
              .aggs = std::vector<RuntimeAggState>(plan_agg_specs.size())};
          for (size_t a = 0; a < plan_agg_specs.size(); ++a) {
            RETURN_IF_FAIL(MergeAggState(plan_agg_specs[a], bg.aggs[a],
                                         src_aggs[a], barena));
          }
          out_b.push_back(std::move(bg));
          return Status::kSuccess;
        };

        auto mono_worker_fn = [&](size_t tid) {
          // Survivors and boundary groups are charged per group; batch the
          // reservations (millions of HAVING survivors in TPC-H Q21).
          ChunkedCharge w_batch(&w_charges[tid]);
          BlockDecodeScratch scratch;
          std::vector<const TlcsFile::DecodedColumnChunk*> col_ptrs(
              table_schema.ColumnCount(), nullptr);
          std::vector<uint32_t> cand_rows;
          std::vector<RuntimeAggState> run_aggs(plan_agg_specs.size());
          std::vector<Value> finished_scratch(plan_agg_specs.size());
          std::vector<Value> cached_keys(1);
          uint64_t local_scanned = 0;
          uint64_t local_matching = 0;
          uint64_t local_groups = 0;

          while (true) {
            const size_t m = next_mb.fetch_add(1, std::memory_order_relaxed);
            if (m >= mblocks.size()) {
              break;
            }
            const MonoBlockMeta& mb = mblocks[m];
            auto dec_or = DecodeAndFilterBlock(
                *tlcs, mb.bidx, table_schema, preds, msched, &scratch,
                &col_ptrs, &cand_rows, &local_scanned);
            if (!dec_or.HasValue()) {
              w_status[tid] = dec_or.GetStatus();
              return;
            }
            if (!dec_or.Value() || cand_rows.empty()) {
              continue;
            }
            local_matching += cand_rows.size();
            const auto* kch = col_ptrs[kcol];
            const int64_t* kcells = kch->int_cells.data();
            bool intra_sorted = true;
            for (size_t i = 1; i < cand_rows.size(); ++i) {
              if (kcells[cand_rows[i]] < kcells[cand_rows[i - 1]]) {
                intra_sorted = false;
                break;
              }
            }
            if (intra_sorted) {
              QueryMemoryCharge blk_str_charge;
              PagedStringArena blk_arena(&blk_str_charge);
              int64_t cur_k = kcells[cand_rows[0]];
              run_aggs.clear();
              run_aggs.resize(plan_agg_specs.size());
              auto flush_run = [&](int64_t fk) -> Status {
                const bool is_boundary = (mb.shared_lo && fk == mb.min_k) ||
                                         (mb.shared_hi && fk == mb.max_k);
                if (is_boundary) {
                  return copy_to_boundary(fk, run_aggs.data(), w_batch,
                                          *b_arenas[tid], w_boundaries[tid]);
                }
                ++local_groups;
                return emit_interior_or_boundary(fk, run_aggs.data(), w_batch,
                                                 w_survivors[tid],
                                                 finished_scratch, cached_keys);
              };
              for (const uint32_t r : cand_rows) {
                const int64_t kv = kcells[r];
                if (kv != cur_k) {
                  Status fst = flush_run(cur_k);
                  if (fst != Status::kSuccess) {
                    w_status[tid] = std::move(fst);
                    return;
                  }
                  cur_k = kv;
                  for (auto& st : run_aggs) {
                    // Keep a small COUNT(DISTINCT) set allocated across runs:
                    // a fresh one per group is one allocation per key (15M
                    // for Q21).  A set that grew large is dropped instead,
                    // since Clear() costs its capacity.
                    std::unique_ptr<DistinctIntegerValues> keep =
                        std::move(st.distinct_ints);
                    st = RuntimeAggState{};
                    if (keep != nullptr && keep->values.Size() <= 64) {
                      keep->values.Clear();
                      st.distinct_ints = std::move(keep);
                    }
                  }
                }
                for (size_t a = 0; a < plan_agg_specs.size(); ++a) {
                  Status st = FoldRowIntoAggState(plan_agg_specs[a], col_ptrs,
                                                  r, run_aggs[a], blk_arena);
                  if (st != Status::kSuccess) {
                    w_status[tid] = std::move(st);
                    return;
                  }
                }
              }
              Status last_st = flush_run(cur_k);
              if (last_st != Status::kSuccess) {
                w_status[tid] = std::move(last_st);
                return;
              }
            } else {
              QueryMemoryCharge blk_charge;
              SingleIntWorkerTable btable(plan_agg_specs.size(), &blk_charge,
                                          false);
              for (const uint32_t r : cand_rows) {
                const int64_t kv = kcells[r];
                auto gid_or = btable.FindOrInsertNonNull(kv);
                if (!gid_or.HasValue()) {
                  w_status[tid] = gid_or.GetStatus();
                  return;
                }
                RuntimeAggState* st_arr = btable.AggsAt(gid_or.Value());
                for (size_t a = 0; a < plan_agg_specs.size(); ++a) {
                  Status st = FoldRowIntoAggState(plan_agg_specs[a], col_ptrs,
                                                  r, st_arr[a], btable.Arena());
                  if (st != Status::kSuccess) {
                    w_status[tid] = std::move(st);
                    return;
                  }
                }
              }
              for (uint32_t g = 0; g < btable.NonNullCount(); ++g) {
                const int64_t fk = btable.KeyAt(g);
                const bool is_boundary = (mb.shared_lo && fk == mb.min_k) ||
                                         (mb.shared_hi && fk == mb.max_k);
                Status fst =
                    is_boundary
                        ? copy_to_boundary(fk, btable.AggsAt(g), w_batch,
                                           *b_arenas[tid], w_boundaries[tid])
                        : (++local_groups,
                           emit_interior_or_boundary(
                               fk, btable.AggsAt(g), w_batch, w_survivors[tid],
                               finished_scratch, cached_keys));
                if (fst != Status::kSuccess) {
                  w_status[tid] = std::move(fst);
                  return;
                }
              }
            }
          }
          mono_scanned.fetch_add(local_scanned, std::memory_order_relaxed);
          mono_matching.fetch_add(local_matching, std::memory_order_relaxed);
          mono_interior_groups.fetch_add(local_groups,
                                         std::memory_order_relaxed);
        };

        if (mworkers == 1) {
          mono_worker_fn(0);
        } else {
          RETURN_IF_FAIL(RunParallelTasks(mworkers, mono_worker_fn));
        }
        bool mono_mem_exceeded = false;
        for (const Status& st : w_status) {
          if (st == StatusCode::kMemoryLimitExceeded ||
              st == StatusCode::kInvalidArgument) {
            mono_mem_exceeded = true;
            break;
          }
          RETURN_IF_FAIL(st);
        }
        if (!mono_mem_exceeded) {
          const auto merge_begin = std::chrono::steady_clock::now();
          size_t total_b = 0;
          for (const auto& wb : w_boundaries) {
            total_b += wb.size();
          }
          std::vector<BoundaryGroup> all_b;
          all_b.reserve(total_b);
          for (auto& wb : w_boundaries) {
            for (auto& bg : wb) {
              all_b.push_back(std::move(bg));
            }
          }
          std::ranges::sort(all_b, [](const BoundaryGroup& a,
                                      const BoundaryGroup& b) noexcept {
            return a.key < b.key;
          });

          QueryMemoryCharge final_charge;
          PagedStringArena final_arena(&final_charge);
          std::vector<MonoSurvivor> final_survivors;
          if (mono_bounded_topk) {
            final_survivors.reserve(mono_topk_cap + 1);
          }
          for (auto& ws : w_survivors) {
            for (auto& item : ws) {
              if (mono_bounded_topk) {
                if (final_survivors.size() < mono_topk_cap) {
                  final_survivors.push_back(std::move(item));
                  std::push_heap(final_survivors.begin(), final_survivors.end(),
                                 cmp_survivors);
                } else if (cmp_survivors(item, final_survivors.front())) {
                  std::pop_heap(final_survivors.begin(), final_survivors.end(),
                                cmp_survivors);
                  final_survivors.back() = std::move(item);
                  std::push_heap(final_survivors.begin(), final_survivors.end(),
                                 cmp_survivors);
                }
              } else {
                final_survivors.push_back(std::move(item));
              }
            }
          }

          size_t boundary_distinct_groups = 0;
          ChunkedCharge final_batch(&final_charge);
          std::vector<Value> f_scratch(plan_agg_specs.size());
          std::vector<Value> c_keys(1);
          bool merge_ok = true;
          for (size_t i = 0; i < all_b.size();) {
            size_t j = i + 1;
            while (j < all_b.size() && all_b[j].key == all_b[i].key) {
              for (size_t a = 0; a < plan_agg_specs.size(); ++a) {
                Status mst = MergeAggState(plan_agg_specs[a], all_b[i].aggs[a],
                                           all_b[j].aggs[a], final_arena);
                if (mst == StatusCode::kMemoryLimitExceeded) {
                  merge_ok = false;
                  break;
                }
                RETURN_IF_FAIL(mst);
              }
              if (!merge_ok) {
                break;
              }
              ++j;
            }
            if (!merge_ok) {
              break;
            }
            ++boundary_distinct_groups;
            Status est = emit_interior_or_boundary(
                all_b[i].key, all_b[i].aggs.data(), final_batch,
                final_survivors, f_scratch, c_keys);
            if (est == StatusCode::kMemoryLimitExceeded) {
              merge_ok = false;
              break;
            }
            RETURN_IF_FAIL(est);
            i = j;
          }

          if (merge_ok) {
            if (!plan_order.empty() && !final_survivors.empty()) {
              if (mono_bounded_topk) {
                std::sort_heap(final_survivors.begin(), final_survivors.end(),
                               cmp_survivors);
              } else {
                const size_t ecap =
                    stmt.HasLimit()
                        ? std::min(final_survivors.size(), mono_topk_cap)
                        : final_survivors.size();
                if (ecap < final_survivors.size() &&
                    ecap * 2 < final_survivors.size()) {
                  std::partial_sort(final_survivors.begin(),
                                    final_survivors.begin() +
                                        static_cast<std::ptrdiff_t>(ecap),
                                    final_survivors.end(), cmp_survivors);
                } else {
                  std::sort(final_survivors.begin(), final_survivors.end(),
                            cmp_survivors);
                }
              }
            }
            size_t emit_count = final_survivors.size();
            if (stmt.HasLimit()) {
              emit_count = std::min(emit_count, mono_topk_cap);
            }
            if (plan_publish_marker && plan_runtime != nullptr) {
              auto marker = std::make_shared<Relation>(plan_runtime);
              marker->schema = qualified_schema;
              plan_runtime->base_relations.emplace(fold_cache_key,
                                                   std::move(marker));
            }
            Relation output(plan_runtime);
            output.rows.reserve(emit_count);
            std::vector<Column> out_cols;
            out_cols.reserve(plan_emit.size());
            for (size_t i = 0; i < plan_emit.size(); ++i) {
              ValueType vt = ValueType::kInt64;
              std::string cname = "col_" + std::to_string(i);
              if (i < plan_select_names.size()) {
                cname = ProjectionName(plan_select_names[i], i);
                if (plan_select_names[i].expression) {
                  auto rt = plan_select_names[i].expression->ResultType(
                      qualified_schema);
                  vt = rt.GetType() == TypeTag::kDouble    ? ValueType::kDouble
                       : rt.GetType() == TypeTag::kDate    ? ValueType::kDate
                       : rt.GetType() == TypeTag::kVarChar ? ValueType::kVarChar
                                                           : ValueType::kInt64;
                }
              }
              out_cols.emplace_back(std::move(cname), vt);
            }
            std::vector<uint8_t> type_refined(out_cols.size(), 0);
            for (size_t r = 0; r < emit_count; ++r) {
              auto& vals = final_survivors[r].emit_vals;
              for (size_t i = 0; i < vals.size(); ++i) {
                if (type_refined[i] == 0U && !vals[i].IsNull()) {
                  out_cols[i] =
                      Column(out_cols[i].Name(), ValueTypeOf(vals[i]));
                  out_cols[i].SetUnsigned(vals[i].IsUnsigned());
                  type_refined[i] = 1;
                }
              }
              output.AddRow(Row(std::move(vals)));
            }
            output.schema = Schema("", std::move(out_cols));
            if (dbg) {
              (void)std::fprintf(
                  stderr,
                  "[TLCS-AGG-OK] mono-block-run scan=%.2fms merge=%.2fms "
                  "blocks=%zu surv=%zu\n",
                  ColumnarMsSince(scan_begin), ColumnarMsSince(merge_begin), mblocks.size(),
                  emit_count);
            }
            if (plan_runtime != nullptr) {
              const double scan_ms = ColumnarMsSince(scan_begin);
              plan_runtime->scan_ms += scan_ms;
              plan_runtime->filter_ms += scan_ms;
              plan_runtime->scan_rows += mono_scanned.load();
              plan_runtime->scan_output_rows += mono_matching.load();
              plan_runtime->typed_parallel_aggregations += 1;
              plan_runtime->aggregate_groups +=
                  mono_interior_groups.load() + boundary_distinct_groups;
              plan_runtime->aggregate_input_rows += mono_matching.load();
              plan_runtime->aggregate_ms += ColumnarMsSince(merge_begin);
            }
            return std::optional<Relation>(std::move(output));
          }
        }
      }
    }

    const bool plan_any_double_arith = std::ranges::any_of(
        plan_agg_specs,
        [](const ColAggSpec& s) { return s.double_arith != nullptr; });
    const double survive_ratio =
        static_cast<double>(candidate_blocks.size()) /
        static_cast<double>(std::max<uint32_t>(1, tlcs->BlockCount()));
    const bool grouped_admitted =
        is_group_all || survive_ratio < 0.60 ||
        !preds.contains_prechecks.empty() || plan_any_byte_len ||
        plan_any_double_arith || any_regex_key ||
        (resident_store_hint == nullptr && candidate_blocks.size() <= 512);
    const bool computed_unadmitted = !is_group_all && any_computed_key &&
                                     !any_regex_key && !plan_any_double_arith &&
                                     survive_ratio >= 0.60 &&
                                     candidate_blocks.size() > 512;
    const bool high_key_multi = !is_group_all && key_specs.size() >= 4 &&
                                !any_regex_key && !plan_any_distinct &&
                                !plan_any_double_arith &&
                                candidate_blocks.size() >= 8;
    const bool force_partitioned = (!is_group_all && plan_any_distinct) ||
                                   !grouped_admitted || computed_unadmitted ||
                                   high_key_multi;
    const PartitionedColumnarAggRequest part_req{
        .stmt = stmt,
        .tlcs = tlcs,
        .qualified_schema = qualified_schema,
        .table_schema = table_schema,
        .preds = preds,
        .candidate_blocks = candidate_blocks,
        .key_specs = key_specs,
        .agg_specs = plan_agg_specs,
        .positional_having = plan_having,
        .emit_bindings = plan_emit,
        .order_bindings = plan_order,
        .runtime = plan_runtime,
        .publish_lstable_marker = plan_publish_marker,
        .fold_cache_key = fold_cache_key,
        .scan_begin = scan_begin,
        .dir_ms = dir_ms,
        .any_scalar_distinct = plan_any_distinct,
    };
    if (force_partitioned) {
      return TryRunPartitionedColumnarTlcsAggregate(part_req);
    }
    if (!is_group_all && !plan_any_distinct) {
      if (key_specs.size() == 2) {
        ASSIGN_OR_RETURN(std::optional<Relation>, two_key_res,
                         TryRunDirectTwoKeyBoundedIntAggregate(part_req));
        if (two_key_res) {
          return two_key_res;
        }
      }
      if (key_specs.size() == 1 &&
          key_specs[0].kind == ColKeyKind::kColVarChar) {
        ASSIGN_OR_RETURN(std::optional<Relation>, local_dict_res,
                         TryRunSingleVarCharLocalDictAggregate(part_req));
        if (local_dict_res) {
          return local_dict_res;
        }
      }
    }

    const size_t ncols = table_schema.ColumnCount();
    const ColumnarStageSchedule sched = BuildColumnarStageSchedule(
        table_schema, preds, key_specs, plan_agg_specs);

    // Lean proof: lean/TinylambOpt/CountBoundsAndBlockAggregates.lean
    //   (bounded_int64_sum_parallel_sound, const_block_sum_fold_sound,
    //    unbounded_int64_sum_reordering_overflow_bug)
    bool has_unbounded_checked_int_sum = false;
    for (const ColAggSpec& spec : plan_agg_specs) {
      if (spec.kind == ColAggKind::kSumInt64) {
        ASSIGN_OR_RETURN(bool, safe_sum,
                         sum_int64_provably_no_overflow(spec.full_col));
        if (!safe_sum) {
          has_unbounded_checked_int_sum = true;
          break;
        }
      }
    }
    const size_t workers =
        has_unbounded_checked_int_sum
            ? size_t{1}
            : std::max<size_t>(
                  1, std::min<size_t>({QueryParallelWorkerLimit(
                                           std::thread::hardware_concurrency()),
                                       candidate_blocks.size(), size_t{16}}));

    const bool can_fold_const_blocks_scalar =
        is_group_all && preds.Empty() && !plan_any_distinct &&
        !plan_any_byte_len && !plan_any_double_arith &&
        !has_unbounded_checked_int_sum;
    std::vector<std::shared_ptr<const TlcsFile::DirGroup>> scalar_dir_groups;
    const uint32_t scalar_dg_size = tlcs->DirGroupSize();
    if (can_fold_const_blocks_scalar) {
      const uint32_t dg_count = tlcs->DirGroupCount();
      scalar_dir_groups.resize(dg_count);
      for (uint32_t g = 0; g < dg_count; ++g) {
        ASSIGN_OR_RETURN(std::shared_ptr<const TlcsFile::DirGroup>, grp,
                         tlcs->GetDirGroup(g));
        scalar_dir_groups[g] = std::move(grp);
      }
    }

    bool packed_int32_pair = false;
    if (!is_group_all && key_specs.size() == 2 && !plan_any_distinct &&
        (key_specs[0].kind == ColKeyKind::kColInt64 ||
         key_specs[0].kind == ColKeyKind::kColDate) &&
        (key_specs[1].kind == ColKeyKind::kColInt64 ||
         key_specs[1].kind == ColKeyKind::kColDate)) {
      const uint32_t dg_count = tlcs->DirGroupCount();
      const uint32_t dg_size = tlcs->DirGroupSize();
      std::vector<std::shared_ptr<const TlcsFile::DirGroup>> dir_groups(
          dg_count);
      for (uint32_t g = 0; g < dg_count; ++g) {
        ASSIGN_OR_RETURN(std::shared_ptr<const TlcsFile::DirGroup>, grp,
                         tlcs->GetDirGroup(g));
        dir_groups[g] = std::move(grp);
      }
      bool pair_ok = true;
      const slot_t c0 = key_specs[0].full_col;
      const slot_t c1 = key_specs[1].full_col;
      for (const uint32_t bidx : candidate_blocks) {
        const auto& blk = dir_groups[bidx / dg_size]->blocks[bidx % dg_size];
        const auto& m0 = blk.columns[c0];
        const auto& m1 = blk.columns[c1];
        if (m0.has_null || m1.has_null || !m0.has_min_max || !m1.has_min_max ||
            m0.min_int < std::numeric_limits<int32_t>::min() ||
            m0.max_int > std::numeric_limits<int32_t>::max() ||
            m1.min_int < std::numeric_limits<int32_t>::min() ||
            m1.max_int > std::numeric_limits<int32_t>::max()) {
          pair_ok = false;
          break;
        }
      }
      packed_int32_pair = pair_ok;
    }

    const bool single_int_key =
        packed_int32_pair ||
        (!is_group_all && key_specs.size() == 1 && !plan_any_distinct &&
         (key_specs[0].kind == ColKeyKind::kColInt64 ||
          key_specs[0].kind == ColKeyKind::kColDate ||
          key_specs[0].kind == ColKeyKind::kUtcTimestampTrunc ||
          key_specs[0].kind == ColKeyKind::kUtcTimestampExtract));

    std::vector<QueryMemoryCharge> worker_charges(workers);
    if (filter_memo_out != nullptr && !preds.Empty()) {
      filter_memo_out->block_has_match.assign(tlcs->BlockCount(), 0);
      filter_memo_out->block_rows.resize(tlcs->BlockCount());
      filter_memo_out->cached_rows_total.store(0, std::memory_order_relaxed);
      filter_memo_out->complete = false;
    }
    std::vector<std::unique_ptr<PagedStringArena>> scalar_arenas;
    std::vector<std::unique_ptr<WorkerGroupTable>> worker_tables;
    std::vector<std::unique_ptr<SingleIntWorkerTable>> single_int_tables;
    const CompiledFastAggPlan fast_agg_plan =
        CompileFastAggPlan(plan_agg_specs);

    // Lean proof: lean/TinylambOpt/VectorizedJoinAndPruning.lean
    //   (direct_int_key_injective, direct_int_key_in_bounds,
    //    parallel_direct_array_merge_eq, finite_sum_implies_no_inf)
    // Direct-indexed dense array for single bounded INT64/DATE group key with
    // a single compiled double-sum or count aggregate.
    bool use_direct_int_array = false;
    int64_t direct_lo = 0;
    size_t direct_span = 0;
    bool direct_is_count = false;
    QueryMemoryCharge direct_array_charge;
    std::vector<std::vector<double>> direct_sums;
    std::vector<std::vector<uint64_t>> direct_counts;
    std::vector<std::vector<uint8_t>> direct_seen;
    if (single_int_key && !packed_int32_pair && certificate == nullptr &&
        !plan_any_distinct && plan_having.empty() && plan_order.empty() &&
        !stmt.HasLimit() && stmt.Offset() == 0 &&
        (key_specs[0].kind == ColKeyKind::kColInt64 ||
         key_specs[0].kind == ColKeyKind::kColDate) &&
        !table_schema.GetColumn(key_specs[0].full_col).IsUnsigned() &&
        plan_agg_specs.size() == 1 && fast_agg_plan.eligible &&
        (fast_agg_plan.ops[0].kind == CompiledFastAgg::Kind::kCountStar ||
         fast_agg_plan.ops[0].kind == CompiledFastAgg::Kind::kCountCol ||
         fast_agg_plan.ops[0].kind == CompiledFastAgg::Kind::kSumDoubleCol ||
         fast_agg_plan.ops[0].kind == CompiledFastAgg::Kind::kSumDoubleMul2 ||
         fast_agg_plan.ops[0].kind ==
             CompiledFastAgg::Kind::kSumDoubleMulOneMinus ||
         fast_agg_plan.ops[0].kind ==
             CompiledFastAgg::Kind::kSumDoubleMulOneMinusOnePlus) &&
        std::ranges::all_of(plan_emit, [](const OutputSlotBinding& b) {
          return b.kind == OutputSlotBinding::Kind::kKey ||
                 b.kind == OutputSlotBinding::Kind::kAgg ||
                 b.kind == OutputSlotBinding::Kind::kConstValue;
        })) {
      const slot_t kcol = key_specs[0].full_col;
      const uint32_t dg_sz = tlcs->DirGroupSize();
      int64_t lo = std::numeric_limits<int64_t>::max();
      int64_t hi = std::numeric_limits<int64_t>::min();
      bool bounds_ok = !candidate_blocks.empty();
      // Check that all candidate blocks have non-null int key bounds and
      // non-null double aggregate columns.
      for (const uint32_t bidx : candidate_blocks) {
        auto dir_or = tlcs->GetDirGroup(bidx / dg_sz);
        if (!dir_or.HasValue()) {
          bounds_ok = false;
          break;
        }
        const auto& bmeta = dir_or.Value()->blocks[bidx % dg_sz];
        const auto& kcm = bmeta.columns[kcol];
        if (kcm.has_null || !kcm.has_min_max) {
          bounds_ok = false;
          break;
        }
        lo = std::min(lo, kcm.min_int);
        hi = std::max(hi, kcm.max_int);
        const bool agg_is_count =
            (fast_agg_plan.ops[0].kind == CompiledFastAgg::Kind::kCountStar ||
             fast_agg_plan.ops[0].kind == CompiledFastAgg::Kind::kCountCol);
        for (const slot_t ac : fast_agg_plan.checked_cols) {
          if (bmeta.columns[ac].has_null ||
              (!agg_is_count &&
               table_schema.GetColumn(ac).Type() != ValueType::kDouble)) {
            bounds_ok = false;
            break;
          }
        }
        if (!bounds_ok) {
          break;
        }
      }
      if (bounds_ok && hi >= lo) {
        const uint64_t diff =
            static_cast<uint64_t>(hi) - static_cast<uint64_t>(lo);
        if (diff < 2000000ULL) {
          direct_span = static_cast<size_t>(diff) + 1;
          direct_lo = lo;
          direct_is_count =
              (fast_agg_plan.ops[0].kind == CompiledFastAgg::Kind::kCountStar ||
               fast_agg_plan.ops[0].kind == CompiledFastAgg::Kind::kCountCol);
          const size_t bytes_per_worker =
              direct_is_count
                  ? direct_span * sizeof(uint64_t)
                  : direct_span * (sizeof(double) + sizeof(uint8_t));
          if (direct_array_charge.TryAdd(workers * bytes_per_worker)) {
            use_direct_int_array = true;
            if (direct_is_count) {
              direct_counts.assign(workers,
                                   std::vector<uint64_t>(direct_span, 0));
            } else {
              direct_sums.assign(workers,
                                 std::vector<double>(direct_span, 0.0));
              direct_seen.assign(workers, std::vector<uint8_t>(direct_span, 0));
            }
          }
        }
      }
    }
    std::vector<std::vector<RuntimeAggState>> scalar_worker_aggs;
    const bool single_int_count_star =
        single_int_key && plan_agg_specs.size() == 1 &&
        plan_agg_specs[0].kind == ColAggKind::kCountStar;
    if (is_group_all) {
      scalar_worker_aggs.resize(workers);
      scalar_arenas.reserve(workers);
      for (size_t w = 0; w < workers; ++w) {
        scalar_worker_aggs[w].resize(plan_agg_specs.size());
        scalar_arenas.push_back(
            std::make_unique<PagedStringArena>(&worker_charges[w]));
      }
    } else if (use_direct_int_array) {
      // Workers accumulate directly into direct_sums / direct_counts.
    } else if (single_int_key) {
      single_int_tables.reserve(workers);
      for (size_t w = 0; w < workers; ++w) {
        single_int_tables.push_back(std::make_unique<SingleIntWorkerTable>(
            plan_agg_specs.size(), &worker_charges[w], single_int_count_star));
      }
    } else {
      worker_tables.reserve(workers);
      for (size_t w = 0; w < workers; ++w) {
        worker_tables.push_back(std::make_unique<WorkerGroupTable>(
            &key_specs, &plan_agg_specs, &worker_charges[w]));
      }
    }
    std::vector<Status> worker_status(workers, Status::kSuccess);
    QueryMemoryCharge certificate_charge;
    std::vector<std::vector<uint32_t>> bucket_counts;
    if (certificate != nullptr) {
      if (!certificate_charge.TryAdd(
              (workers + 1) * certificate->tracked.size() * sizeof(uint32_t))) {
        return std::nullopt;
      }
      bucket_counts.assign(
          workers, std::vector<uint32_t>(certificate->tracked.size(), 0));
    }
    std::atomic<size_t> next_idx{0};
    std::atomic<uint64_t> total_scanned{0};
    std::atomic<uint64_t> total_matching{0};

    auto worker_fn = [&](size_t tid) {
      BlockDecodeScratch decode_scratch;
      TimestampPrefixCache ts_cache;
      RegexReplaceCache regex_cache;

      std::vector<uint32_t> cand_rows;
      std::vector<RuntimeKeyCell> probe_keys(key_specs.size());
      std::string regex_scratch;
      uint64_t local_scanned = 0;
      uint64_t local_matching = 0;
      std::vector<const TlcsFile::DecodedColumnChunk*> col_ptrs(ncols, nullptr);

      std::vector<slot_t> dyn_scalar_cols;
      dyn_scalar_cols.reserve(plan_agg_specs.size());
      // Dictionary-coded VARCHAR keys (TPC-H Q1's l_returnflag/l_linestatus):
      // TLCS dictionaries are file-wide, so the tuple of codes names the
      // group.  Each tuple is resolved through the ordinary FindOrInsert once
      // per worker (same groups and key bytes as the generic loop) and then
      // read from a flat array; the aggregates are folded column-wise.
      std::vector<const LSTableColumnDictionary*> dk_dicts;
      std::vector<int64_t> dk_gid;  // -1 = tuple not seen yet
      std::vector<uint32_t> dk_stride;
      std::vector<uint32_t> dk_row_gid;
      while (true) {
        const size_t bi = next_idx.fetch_add(1, std::memory_order_relaxed);
        if (bi >= candidate_blocks.size()) {
          break;
        }
        const uint32_t bidx = candidate_blocks[bi];
        if (can_fold_const_blocks_scalar) {
          const auto& blk_meta = scalar_dir_groups[bidx / scalar_dg_size]
                                     ->blocks[bidx % scalar_dg_size];
          const uint32_t b_rows = blk_meta.row_count;
          local_scanned += b_rows;
          local_matching += b_rows;
          if (b_rows == 0) {
            continue;
          }
          dyn_scalar_cols.clear();
          std::fill(col_ptrs.begin(), col_ptrs.end(), nullptr);
          for (const ColAggSpec& as : plan_agg_specs) {
            if (as.kind == ColAggKind::kCountStar) {
              continue;
            }
            const auto& cm = blk_meta.columns[as.full_col];
            const bool col_signed_int_or_date =
                table_schema.GetColumn(as.full_col).Type() !=
                    ValueType::kDouble &&
                table_schema.GetColumn(as.full_col).Type() !=
                    ValueType::kVarChar &&
                !table_schema.GetColumn(as.full_col).IsUnsigned();
            const bool is_const_int_or_date =
                (as.kind == ColAggKind::kCountCol ||
                 as.kind == ColAggKind::kSumInt64 ||
                 as.kind == ColAggKind::kAvgDouble ||
                 as.kind == ColAggKind::kMinInt64 ||
                 as.kind == ColAggKind::kMaxInt64 ||
                 as.kind == ColAggKind::kMinDate ||
                 as.kind == ColAggKind::kMaxDate) &&
                table_schema.GetColumn(as.full_col).Type() !=
                    ValueType::kDouble &&
                table_schema.GetColumn(as.full_col).Type() !=
                    ValueType::kVarChar &&
                (cm.encoding == TlcsColumnEncoding::kConstNull ||
                 (!cm.has_null && cm.has_min_max && cm.min_int == cm.max_int));
            // Lean proof:
            // lean/TinylambOpt/ResearchProofs/AnalyticalOperators/DictionarySlicesAndExtrema.lean
            //   (sound_DominatedColumn).
            // On an all-pass block, signed INT64/DATE MIN/MAX can be answered
            // directly from block metadata (cm.min_int / cm.max_int) without
            // decoding the column even when the block is non-constant or has
            // NULLs (has_min_max summarizes non-NULL values).
            const bool is_meta_extrema_int_or_date =
                col_signed_int_or_date && cm.has_min_max &&
                (as.kind == ColAggKind::kMinInt64 ||
                 as.kind == ColAggKind::kMaxInt64 ||
                 as.kind == ColAggKind::kMinDate ||
                 as.kind == ColAggKind::kMaxDate);
            if (!is_const_int_or_date && !is_meta_extrema_int_or_date &&
                std::ranges::find(dyn_scalar_cols, as.full_col) ==
                    dyn_scalar_cols.end()) {
              dyn_scalar_cols.push_back(as.full_col);
            }
          }
          if (!dyn_scalar_cols.empty()) {
            Status dst = tlcs->DecodeBlockColumnsWithOptions(
                bidx, dyn_scalar_cols, table_schema, &decode_scratch.blk1b,
                true, &decode_scratch.io1b, {});
            if (dst != Status::kSuccess) {
              worker_status[tid] = std::move(dst);
              return;
            }
            for (size_t i = 0; i < dyn_scalar_cols.size(); ++i) {
              col_ptrs[dyn_scalar_cols[i]] = &decode_scratch.blk1b.columns[i];
            }
          }
          auto& sa = scalar_worker_aggs[tid];
          for (size_t a = 0; a < plan_agg_specs.size(); ++a) {
            const ColAggSpec& as = plan_agg_specs[a];
            if (as.kind == ColAggKind::kCountStar) {
              sa[a].count += static_cast<int64_t>(b_rows);
              sa[a].has_value = true;
              continue;
            }
            if (col_ptrs[as.full_col] == nullptr) {
              const auto& cm = blk_meta.columns[as.full_col];
              if (cm.encoding == TlcsColumnEncoding::kConstNull) {
                continue;
              }
              const int64_t cv = cm.min_int;
              switch (as.kind) {
                case ColAggKind::kCountCol:
                  sa[a].count += static_cast<int64_t>(b_rows);
                  sa[a].has_value = true;
                  break;
                case ColAggKind::kSumInt64:
                  sa[a].int_val += static_cast<int64_t>(b_rows) * cv;
                  sa[a].has_value = true;
                  break;
                case ColAggKind::kAvgDouble:
                  sa[a].double_val +=
                      static_cast<double>(b_rows) * static_cast<double>(cv);
                  sa[a].count += static_cast<int64_t>(b_rows);
                  sa[a].has_value = true;
                  break;
                case ColAggKind::kMinInt64:
                case ColAggKind::kMinDate:
                  if (!sa[a].has_value || cv < sa[a].int_val) {
                    sa[a].int_val = cv;
                    sa[a].has_value = true;
                  }
                  break;
                case ColAggKind::kMaxInt64:
                case ColAggKind::kMaxDate:
                  if (!sa[a].has_value || cm.max_int > sa[a].int_val) {
                    sa[a].int_val = cm.max_int;
                    sa[a].has_value = true;
                  }
                  break;
                default:
                  break;
              }
              continue;
            }
            const auto* dch = col_ptrs[as.full_col];
            if (!dch->has_null &&
                table_schema.GetColumn(as.full_col).Type() ==
                    ValueType::kInt64 &&
                (as.kind == ColAggKind::kSumInt64 ||
                 as.kind == ColAggKind::kAvgDouble ||
                 as.kind == ColAggKind::kCountCol)) {
              if (as.kind == ColAggKind::kCountCol) {
                sa[a].count += static_cast<int64_t>(b_rows);
                sa[a].has_value = true;
                continue;
              }
              const int64_t* __restrict cells = dch->int_cells.data();
              if (as.kind == ColAggKind::kSumInt64) {
                int64_t bsum = 0;
                for (uint32_t r = 0; r < b_rows; ++r) {
                  bsum += cells[r];
                }
                sa[a].int_val += bsum;
                sa[a].has_value = true;
                continue;
              }
              // kAvgDouble over non-null INT64: if block values stay safely
              // within exact 53-bit double integer sum, accumulate int64 first!
              const auto& cm = blk_meta.columns[as.full_col];
              const __int128 max_abs =
                  cm.has_min_max
                      ? std::max<__int128>(
                            cm.min_int < 0 ? -static_cast<__int128>(cm.min_int)
                                           : static_cast<__int128>(cm.min_int),
                            cm.max_int < 0 ? -static_cast<__int128>(cm.max_int)
                                           : static_cast<__int128>(cm.max_int))
                      : (static_cast<__int128>(1) << 60);
              if (max_abs * b_rows < (static_cast<__int128>(1) << 52)) {
                int64_t bsum = 0;
                for (uint32_t r = 0; r < b_rows; ++r) {
                  bsum += cells[r];
                }
                sa[a].double_val += static_cast<double>(bsum);
                sa[a].count += static_cast<int64_t>(b_rows);
                sa[a].has_value = true;
                continue;
              }
              // Preserve the checked accumulator's conversion and addition
              // order. Signed integer inputs cannot supply NaN or infinity.
              if (!dch->is_unsigned) {
                double total = sa[a].double_val;
                for (uint32_t r = 0; r < b_rows; ++r) {
                  total += static_cast<double>(cells[r]);
                }
                sa[a].double_val = total;
                sa[a].count += static_cast<int64_t>(b_rows);
                sa[a].has_value = true;
                continue;
              }
            }
            for (uint32_t r = 0; r < b_rows; ++r) {
              Status fold_st = FoldRowIntoAggState(as, col_ptrs, r, sa[a],
                                                   *scalar_arenas[tid]);
              if (fold_st != Status::kSuccess) {
                worker_status[tid] = std::move(fold_st);
                return;
              }
            }
          }
          continue;
        }
        auto dec_or = DecodeAndFilterBlock(*tlcs, bidx, table_schema, preds,
                                           sched, &decode_scratch, &col_ptrs,
                                           &cand_rows, &local_scanned);
        if (!dec_or.HasValue()) {
          worker_status[tid] = dec_or.GetStatus();
          return;
        }
        if (!dec_or.Value()) {
          continue;
        }

        if (filter_memo_out != nullptr &&
            bidx < filter_memo_out->block_has_match.size()) {
          filter_memo_out->block_has_match[bidx] = 1;
          if (!cand_rows.empty() && cand_rows.size() <= 4096 &&
              cand_rows.back() <= std::numeric_limits<uint16_t>::max()) {
            const size_t prev = filter_memo_out->cached_rows_total.fetch_add(
                cand_rows.size(), std::memory_order_relaxed);
            if (prev + cand_rows.size() <= 262144) {
              auto& saved = filter_memo_out->block_rows[bidx];
              saved.resize(cand_rows.size());
              for (size_t i = 0; i < cand_rows.size(); ++i) {
                saved[i] = static_cast<uint16_t>(cand_rows[i]);
              }
            }
          }
        }

        local_matching += cand_rows.size();

        if (is_group_all) {
          auto& sa = scalar_worker_aggs[tid];
          if (plan_agg_specs.size() == 1 &&
              plan_agg_specs[0].kind == ColAggKind::kCountStar) {
            sa[0].count += static_cast<int64_t>(cand_rows.size());
            sa[0].has_value = true;
            continue;
          }
          if (TryFoldBlockScalarFastAggs(fast_agg_plan, col_ptrs, cand_rows,
                                         sa)) {
            continue;
          }
          for (const uint32_t r : cand_rows) {
            for (size_t a = 0; a < plan_agg_specs.size(); ++a) {
              Status fold_st = FoldRowIntoAggState(
                  plan_agg_specs[a], col_ptrs, r, sa[a], *scalar_arenas[tid]);
              if (fold_st != Status::kSuccess) {
                worker_status[tid] = std::move(fold_st);
                return;
              }
            }
          }
          continue;
        }

        if (packed_int32_pair) {
          SingleIntWorkerTable& itable = *single_int_tables[tid];
          const auto* kch0 = col_ptrs[key_specs[0].full_col];
          const auto* kch1 = col_ptrs[key_specs[1].full_col];
          if (kch0->has_null || kch1->has_null) {
            worker_status[tid] = StatusError(StatusCode::kInvalidArgument,
                                             "unexpected null in packed int32");
            return;
          }
          bool has_last = false;
          int64_t last_key = 0;
          uint32_t last_gid = 0;
          for (const uint32_t r : cand_rows) {
            const int64_t pkey =
                (static_cast<int64_t>(static_cast<int32_t>(kch0->int_cells[r]))
                 << 32) |
                static_cast<int64_t>(static_cast<uint32_t>(
                    static_cast<int32_t>(kch1->int_cells[r])));
            uint32_t gid = 0;
            if (has_last && pkey == last_key) {
              gid = last_gid;
            } else {
              auto gid_or = itable.FindOrInsertNonNull(pkey);
              if (!gid_or.HasValue()) {
                worker_status[tid] = gid_or.GetStatus();
                return;
              }
              gid = gid_or.Value();
              last_key = pkey;
              last_gid = gid;
              has_last = true;
            }
            if (single_int_count_star) {
              ++itable.CountAt(gid);
            } else {
              RuntimeAggState* st_arr = itable.AggsAt(gid);
              for (size_t a = 0; a < plan_agg_specs.size(); ++a) {
                Status fold_st = FoldRowIntoAggState(
                    plan_agg_specs[a], col_ptrs, r, st_arr[a], itable.Arena());
                if (fold_st != Status::kSuccess) {
                  worker_status[tid] = std::move(fold_st);
                  return;
                }
              }
            }
          }
          continue;
        }

        if (use_direct_int_array) {
          const int64_t* kcells =
              col_ptrs[key_specs[0].full_col]->int_cells.data();
          const uint32_t* rows = cand_rows.data();
          const size_t n = cand_rows.size();
          const int64_t lo = direct_lo;
          if (direct_is_count) {
            uint64_t* cnts = direct_counts[tid].data();
            for (size_t i = 0; i < n; ++i) {
              ++cnts[static_cast<uint64_t>(kcells[rows[i]] - lo)];
            }
          } else {
            double* sums = direct_sums[tid].data();
            uint8_t* seen = direct_seen[tid].data();
            const CompiledFastAgg& op = fast_agg_plan.ops[0];
            const int64_t* c0 = col_ptrs[op.c0]->int_cells.data();
            switch (op.kind) {
              case CompiledFastAgg::Kind::kSumDoubleCol:
                for (size_t i = 0; i < n; ++i) {
                  const uint32_t r = rows[i];
                  const auto idx = static_cast<uint64_t>(kcells[r] - lo);
                  sums[idx] += std::bit_cast<double>(c0[r]);
                  seen[idx] = 1;
                }
                break;
              case CompiledFastAgg::Kind::kSumDoubleMul2: {
                const int64_t* c1 = col_ptrs[op.c1]->int_cells.data();
                for (size_t i = 0; i < n; ++i) {
                  const uint32_t r = rows[i];
                  const auto idx = static_cast<uint64_t>(kcells[r] - lo);
                  sums[idx] += std::bit_cast<double>(c0[r]) *
                               std::bit_cast<double>(c1[r]);
                  seen[idx] = 1;
                }
                break;
              }
              case CompiledFastAgg::Kind::kSumDoubleMulOneMinus: {
                const int64_t* c1 = col_ptrs[op.c1]->int_cells.data();
                const double k1 = op.k1;
                for (size_t i = 0; i < n; ++i) {
                  const uint32_t r = rows[i];
                  const auto idx = static_cast<uint64_t>(kcells[r] - lo);
                  sums[idx] += std::bit_cast<double>(c0[r]) *
                               (k1 - std::bit_cast<double>(c1[r]));
                  seen[idx] = 1;
                }
                break;
              }
              case CompiledFastAgg::Kind::kSumDoubleMulOneMinusOnePlus: {
                const int64_t* c1 = col_ptrs[op.c1]->int_cells.data();
                const int64_t* c2 = col_ptrs[op.c2]->int_cells.data();
                const double k1 = op.k1;
                const double k2 = op.k2;
                for (size_t i = 0; i < n; ++i) {
                  const uint32_t r = rows[i];
                  const auto idx = static_cast<uint64_t>(kcells[r] - lo);
                  sums[idx] += std::bit_cast<double>(c0[r]) *
                               (k1 - std::bit_cast<double>(c1[r])) *
                               (k2 + std::bit_cast<double>(c2[r]));
                  seen[idx] = 1;
                }
                break;
              }
              default:
                break;
            }
          }
          continue;
        }

        if (single_int_key) {
          SingleIntWorkerTable& itable = *single_int_tables[tid];
          const ColGroupKeySpec& ks0 = key_specs[0];
          const auto* kch = col_ptrs[ks0.full_col];
          const bool k_no_null = !kch->has_null;
          const bool direct_int_col = (ks0.kind == ColKeyKind::kColInt64 ||
                                       ks0.kind == ColKeyKind::kColDate);
          const bool q28_fast =
              direct_int_col && plan_agg_specs.size() == 2 &&
              plan_agg_specs[0].kind == ColAggKind::kAvgByteLen &&
              plan_agg_specs[1].kind == ColAggKind::kCountStar;
          const auto* b0 =
              q28_fast ? col_ptrs[plan_agg_specs[0].full_col] : nullptr;
          const bool b0_no_null = q28_fast && !b0->has_null;
          std::array<int64_t, 256> dict_ts_vals{};
          size_t dict_ts_nd = 0;
          bool dict_ts_fast = false;
          if (!direct_int_col && !cand_rows.empty() && kch->dict_encoded &&
              kch->dict != nullptr && kch->dict->StringEntryCount() <= 256 &&
              kch->dict_codes.size() >= cand_rows.back() + 1) {
            dict_ts_fast = true;
            const size_t nd = kch->dict->StringEntryCount();
            for (size_t d = 0; d < nd; ++d) {
              const auto sv = kch->dict->StringEntryAt(d);
              const auto packed = (ks0.kind == ColKeyKind::kUtcTimestampTrunc)
                                      ? ts_cache.Trunc(sv, ks0.trunc_unit)
                                      : ts_cache.Extract(sv, ks0.trunc_unit);
              if (!packed.has_value()) {
                dict_ts_fast = false;
                break;
              }
              dict_ts_vals[d] = *packed;
            }
            dict_ts_nd = nd;
          }
          bool has_last = false;
          int64_t last_key = 0;
          uint32_t last_gid = 0;

          for (const uint32_t r : cand_rows) {
            if (!k_no_null && kch->IsNull(r)) {
              if (single_int_count_star) {
                itable.IncrementNullCount();
                continue;
              }
              auto n_or = itable.FindOrInsertNullAggs();
              if (!n_or.HasValue()) {
                worker_status[tid] = n_or.GetStatus();
                return;
              }
              RuntimeAggState* st_arr = n_or.Value();
              for (size_t a = 0; a < plan_agg_specs.size(); ++a) {
                Status fold_st = FoldRowIntoAggState(
                    plan_agg_specs[a], col_ptrs, r, st_arr[a], itable.Arena());
                if (fold_st != Status::kSuccess) {
                  worker_status[tid] = std::move(fold_st);
                  return;
                }
              }
              continue;
            }
            int64_t kval = 0;
            if (direct_int_col) {
              kval = kch->int_cells[r];
            } else if (dict_ts_fast && kch->dict_codes[r] < dict_ts_nd) {
              kval = dict_ts_vals[kch->dict_codes[r]];
            } else if (ks0.kind == ColKeyKind::kUtcTimestampTrunc) {
              const auto packed =
                  ts_cache.Trunc(kch->VarcharAt(r), ks0.trunc_unit);
              if (!packed.has_value()) {
                worker_status[tid] =
                    StatusError(StatusCode::kInvalidArgument,
                                "timestamp key fell back to generic path");
                return;
              }
              kval = *packed;
            } else {
              const auto packed =
                  ts_cache.Extract(kch->VarcharAt(r), ks0.trunc_unit);
              if (!packed.has_value()) {
                worker_status[tid] =
                    StatusError(StatusCode::kInvalidArgument,
                                "timestamp key fell back to generic path");
                return;
              }
              kval = *packed;
            }
            uint32_t gid = 0;
            if (has_last && kval == last_key) {
              gid = last_gid;
            } else {
              auto gid_or = itable.FindOrInsertNonNull(kval);
              if (!gid_or.HasValue()) {
                worker_status[tid] = gid_or.GetStatus();
                return;
              }
              gid = gid_or.Value();
              last_key = kval;
              last_gid = gid;
              has_last = true;
            }
            if (single_int_count_star) {
              ++itable.CountAt(gid);
            } else if (q28_fast) {
              RuntimeAggState* st_arr = itable.AggsAt(gid);
              ++st_arr[1].count;
              st_arr[1].has_value = true;
              if (b0_no_null || !b0->IsNull(r)) {
                st_arr[0].double_val +=
                    static_cast<double>(b0->VarcharLength(r));
                ++st_arr[0].count;
                st_arr[0].has_value = true;
              }
            } else {
              RuntimeAggState* st_arr = itable.AggsAt(gid);
              for (size_t a = 0; a < plan_agg_specs.size(); ++a) {
                Status fold_st = FoldRowIntoAggState(
                    plan_agg_specs[a], col_ptrs, r, st_arr[a], itable.Arena());
                if (fold_st != Status::kSuccess) {
                  worker_status[tid] = std::move(fold_st);
                  return;
                }
              }
            }
          }
          continue;
        }

        WorkerGroupTable& wtable = *worker_tables[tid];
        if (certificate == nullptr && !key_specs.empty() &&
            !cand_rows.empty()) {
          bool dict_keys = true;
          size_t tuples = 1;
          for (const ColGroupKeySpec& ks : key_specs) {
            const auto* kch = col_ptrs[ks.full_col];
            if (ks.kind != ColKeyKind::kColVarChar || kch == nullptr ||
                !kch->dict_encoded || kch->dict == nullptr ||
                kch->dict->StringEntryCount() > 255 ||
                kch->dict_codes.size() <= cand_rows.back()) {
              dict_keys = false;
              break;
            }
            tuples *= kch->dict->StringEntryCount() + 1;  // + NULL
            if (tuples > 65536) {
              dict_keys = false;
              break;
            }
          }
          if (dict_keys) {
            bool same_dicts = dk_dicts.size() == key_specs.size();
            for (size_t k = 0; same_dicts && k < key_specs.size(); ++k) {
              same_dicts = dk_dicts[k] == col_ptrs[key_specs[k].full_col]->dict;
            }
            if (!same_dicts) {
              dk_dicts.clear();
              dk_stride.clear();
              uint32_t stride = 1;
              for (const ColGroupKeySpec& ks : key_specs) {
                const auto* kch = col_ptrs[ks.full_col];
                dk_dicts.push_back(kch->dict);
                dk_stride.push_back(stride);
                stride *=
                    static_cast<uint32_t>(kch->dict->StringEntryCount() + 1);
              }
              dk_gid.assign(tuples, -1);
            }
            dk_row_gid.resize(cand_rows.size());
            bool resolved_all = true;
            for (size_t i = 0; i < cand_rows.size(); ++i) {
              const uint32_t r = cand_rows[i];
              uint32_t tuple = 0;
              for (size_t k = 0; k < key_specs.size(); ++k) {
                const auto* kch = col_ptrs[key_specs[k].full_col];
                const size_t nd = kch->dict->StringEntryCount();
                size_t code = kch->IsNull(r) ? nd : kch->dict_codes[r];
                if (code > nd) {
                  resolved_all = false;  // out-of-dictionary code
                  break;
                }
                tuple += static_cast<uint32_t>(code) * dk_stride[k];
              }
              if (!resolved_all) {
                break;
              }
              int64_t& slot = dk_gid[tuple];
              if (slot < 0) {
                Status key_st = Status::kSuccess;
                for (size_t k = 0; k < key_specs.size(); ++k) {
                  if (!ExtractSingleKeyCell(key_specs[k], col_ptrs, r,
                                            &probe_keys[k], &regex_scratch,
                                            &ts_cache, &key_st, &regex_cache)) {
                    worker_status[tid] = std::move(key_st);
                    return;
                  }
                }
                const uint64_t khash = GroupKeySliceHasher::HashTyped(
                    probe_keys.data(), key_specs);
                auto gid_or = wtable.FindOrInsert(probe_keys.data(), khash);
                if (!gid_or.HasValue()) {
                  worker_status[tid] = gid_or.GetStatus();
                  return;
                }
                slot = gid_or.Value();
              }
              dk_row_gid[i] = static_cast<uint32_t>(slot);
            }
            if (resolved_all) {
              if (TryFoldBlockDenseGroupsFastAggs(fast_agg_plan, col_ptrs,
                                                  cand_rows, dk_row_gid,
                                                  wtable.NumGroups(), wtable)) {
                continue;
              }
              for (size_t a = 0; a < plan_agg_specs.size(); ++a) {
                const ColAggSpec& as = plan_agg_specs[a];
                for (size_t i = 0; i < cand_rows.size(); ++i) {
                  RuntimeAggState& st = wtable.AggsAt(dk_row_gid[i])[a];
                  if (as.kind == ColAggKind::kCountStar) {
                    ++st.count;
                    st.has_value = true;
                    continue;
                  }
                  Status fold_st = FoldRowIntoAggState(
                      as, col_ptrs, cand_rows[i], st, wtable.Arena());
                  if (fold_st != Status::kSuccess) {
                    worker_status[tid] = std::move(fold_st);
                    return;
                  }
                }
              }
              continue;
            }
            // An out-of-dictionary code: this block takes the generic loop
            // from scratch (nothing has been folded for it yet).
          }
        }
        for (const uint32_t r : cand_rows) {
          Status key_st = Status::kSuccess;
          for (size_t k = 0; k < key_specs.size(); ++k) {
            if (!ExtractSingleKeyCell(key_specs[k], col_ptrs, r, &probe_keys[k],
                                      &regex_scratch, &ts_cache, &key_st,
                                      &regex_cache)) {
              worker_status[tid] = std::move(key_st);
              return;
            }
          }
          const uint64_t khash =
              GroupKeySliceHasher::HashTyped(probe_keys.data(), key_specs);
          if (certificate != nullptr) {
            const size_t bucket = certificate->Bucket(khash);
            ++bucket_counts[tid][bucket];
            if (!certificate->observe_only &&
                certificate->tracked[bucket] == 0) {
              continue;
            }
          }
          auto gid_or = wtable.FindOrInsert(probe_keys.data(), khash);
          if (!gid_or.HasValue()) {
            worker_status[tid] = gid_or.GetStatus();
            return;
          }
          RuntimeAggState* st_arr = wtable.AggsAt(gid_or.Value());
          for (size_t a = 0; a < plan_agg_specs.size(); ++a) {
            Status fold_st = FoldRowIntoAggState(plan_agg_specs[a], col_ptrs, r,
                                                 st_arr[a], wtable.Arena());
            if (fold_st != Status::kSuccess) {
              worker_status[tid] = std::move(fold_st);
              return;
            }
          }
        }
      }
      total_scanned.fetch_add(local_scanned, std::memory_order_relaxed);
      total_matching.fetch_add(local_matching, std::memory_order_relaxed);
    };

    if (workers == 1) {
      worker_fn(0);
    } else {
      RETURN_IF_FAIL(RunParallelTasks(workers, worker_fn));
    }
    for (const Status& st : worker_status) {
      if (st == StatusCode::kInvalidArgument ||
          st == StatusCode::kMemoryLimitExceeded) {
        return std::optional<Relation>{};
      }
      RETURN_IF_FAIL(st);
    }
    if (certificate != nullptr) {
      // Lean: QuotientFusion.untracked_group_fails_count_having.
      // An earlier execution proposes buckets; these full-scan bounds decide.
      auto& observed = certificate->observed_buckets->emplace();
      bool complete = true;
      for (size_t bucket = 0; bucket < certificate->tracked.size(); ++bucket) {
        uint64_t total = 0;
        for (const auto& worker : bucket_counts) {
          total += worker[bucket];
        }
        if (total >= certificate->minimum_count) {
          observed.push_back(static_cast<uint32_t>(bucket));
          complete &= certificate->tracked[bucket] != 0;
        }
      }
      if (!certificate->observe_only && !complete) {
        return std::nullopt;
      }
    }
    if (filter_memo_out != nullptr &&
        !filter_memo_out->block_has_match.empty()) {
      filter_memo_out->complete = true;
    }

    const auto merge_begin = std::chrono::steady_clock::now();

    if (is_group_all) {
      QueryMemoryCharge scalar_merge_charge;
      PagedStringArena scalar_merge_arena(&scalar_merge_charge);
      std::vector<RuntimeAggState> merged(plan_agg_specs.size());
      for (size_t w = 0; w < workers; ++w) {
        for (size_t a = 0; a < plan_agg_specs.size(); ++a) {
          Status mst =
              MergeAggState(plan_agg_specs[a], merged[a],
                            scalar_worker_aggs[w][a], scalar_merge_arena);
          if (mst == StatusCode::kMemoryLimitExceeded) {
            return std::optional<Relation>{};
          }
          RETURN_IF_FAIL(mst);
        }
      }
      std::vector<Value> finished_aggs(plan_agg_specs.size());
      for (size_t a = 0; a < plan_agg_specs.size(); ++a) {
        ASSIGN_OR_RETURN(Value, finished_value,
                         (FinishAggState(plan_agg_specs[a], merged[a])));
        finished_aggs[a] = std::move(finished_value);
      }
      bool passes = true;
      for (const PositionalHaving& clause : plan_having) {
        const Value* rhs =
            clause.rhs_is_agg ? &finished_aggs[clause.rhs_agg_index] : nullptr;
        Status err = Status::kSuccess;
        const bool matches = EvalPositionalHavingValues(
            clause, finished_aggs[clause.agg_index], rhs, &err);
        RETURN_IF_FAIL(err);
        if (!matches) {
          passes = false;
          break;
        }
      }
      if (plan_publish_marker && plan_runtime != nullptr) {
        auto marker = std::make_shared<Relation>(plan_runtime);
        marker->schema = qualified_schema;
        plan_runtime->base_relations.emplace(fold_cache_key, std::move(marker));
      }
      Relation out(plan_runtime);
      std::vector<Column> out_cols;
      out_cols.reserve(plan_emit.size());
      if (passes) {
        std::vector<Value> row_vals;
        row_vals.reserve(plan_emit.size());
        std::vector<Value> empty_keys;
        for (size_t i = 0; i < plan_emit.size(); ++i) {
          ASSIGN_OR_RETURN(
              Value, v,
              EvaluateBindingValue(plan_emit[i], key_specs, nullptr,
                                   finished_aggs, &empty_keys));
          std::string cname = i < plan_select_names.size()
                                  ? ProjectionName(plan_select_names[i], i)
                                  : ("col_" + IntegerToString(i));
          Column c(std::move(cname), ValueTypeOf(v));
          c.SetUnsigned(v.IsUnsigned());
          out_cols.push_back(std::move(c));
          row_vals.push_back(std::move(v));
        }
        out.AddRow(Row(std::move(row_vals)));
      } else {
        for (size_t i = 0; i < plan_emit.size(); ++i) {
          std::string cname = i < plan_select_names.size()
                                  ? ProjectionName(plan_select_names[i], i)
                                  : ("col_" + IntegerToString(i));
          out_cols.emplace_back(std::move(cname), ValueType::kInt64);
        }
      }
      out.schema = Schema("", std::move(out_cols));
      if (plan_runtime != nullptr) {
        const double scan_ms = ColumnarMsSince(scan_begin);
        plan_runtime->scan_ms += scan_ms;
        plan_runtime->filter_ms += scan_ms;
        plan_runtime->scan_rows += total_scanned.load();
        plan_runtime->scan_output_rows += total_matching.load();
        plan_runtime->aggregate_groups += passes ? 1 : 0;
        plan_runtime->aggregate_input_rows += total_matching.load();
        plan_runtime->aggregate_ms += ColumnarMsSince(merge_begin);
      }
      return std::optional<Relation>(std::move(out));
    }

    if (use_direct_int_array) {
      Relation out(plan_runtime);
      const bool key_is_date = (key_specs[0].kind == ColKeyKind::kColDate);
      size_t group_count = 0;
      if (direct_is_count) {
        auto& cnt0 = direct_counts[0];
        for (size_t w = 1; w < workers; ++w) {
          const auto& cw = direct_counts[w];
          for (size_t i = 0; i < direct_span; ++i) {
            cnt0[i] += cw[i];
          }
        }
        for (size_t i = 0; i < direct_span; ++i) {
          if (cnt0[i] == 0) {
            continue;
          }
          ++group_count;
          const int64_t kval = direct_lo + static_cast<int64_t>(i);
          std::vector<Value> row_vals;
          row_vals.reserve(plan_emit.size());
          for (const OutputSlotBinding& b : plan_emit) {
            if (b.kind == OutputSlotBinding::Kind::kKey) {
              row_vals.push_back(key_is_date ? Value::DateFromDays(kval)
                                             : Value(kval));
            } else if (b.kind == OutputSlotBinding::Kind::kAgg) {
              row_vals.emplace_back(static_cast<int64_t>(cnt0[i]));
            } else {
              row_vals.push_back(b.constant);
            }
          }
          out.AddRow(Row(std::move(row_vals)));
        }
      } else {
        auto& sum0 = direct_sums[0];
        auto& seen0 = direct_seen[0];
        for (size_t w = 1; w < workers; ++w) {
          const auto& sw = direct_sums[w];
          const auto& kw = direct_seen[w];
          for (size_t i = 0; i < direct_span; ++i) {
            if (kw[i]) {
              sum0[i] += sw[i];
              seen0[i] = 1;
            }
          }
        }
        for (size_t i = 0; i < direct_span; ++i) {
          if (!seen0[i]) {
            continue;
          }
          if (!std::isfinite(sum0[i])) {
            // Fall back to generic path for non-finite infinity provenance.
            return std::optional<Relation>{};
          }
          ++group_count;
          const int64_t kval = direct_lo + static_cast<int64_t>(i);
          std::vector<Value> row_vals;
          row_vals.reserve(plan_emit.size());
          for (const OutputSlotBinding& b : plan_emit) {
            if (b.kind == OutputSlotBinding::Kind::kKey) {
              row_vals.push_back(key_is_date ? Value::DateFromDays(kval)
                                             : Value(kval));
            } else if (b.kind == OutputSlotBinding::Kind::kAgg) {
              row_vals.emplace_back(sum0[i]);
            } else {
              row_vals.push_back(b.constant);
            }
          }
          out.AddRow(Row(std::move(row_vals)));
        }
      }
      RETURN_IF_FAIL(out.FinishSpill());
      std::vector<Column> out_cols;
      out_cols.reserve(plan_select_names.size());
      for (size_t i = 0; i < plan_select_names.size(); ++i) {
        std::string cname = plan_select_names[i].name;
        if (cname.empty()) {
          cname = "col_" + std::to_string(i);
        }
        const OutputSlotBinding& b = plan_emit[i];
        if (b.kind == OutputSlotBinding::Kind::kKey) {
          out_cols.emplace_back(std::move(cname), key_is_date
                                                      ? ValueType::kDate
                                                      : ValueType::kInt64);
        } else if (b.kind == OutputSlotBinding::Kind::kAgg) {
          out_cols.emplace_back(std::move(cname), direct_is_count
                                                      ? ValueType::kInt64
                                                      : ValueType::kDouble);
        } else {
          out_cols.emplace_back(std::move(cname), b.constant.type);
        }
      }
      out.schema = Schema("", std::move(out_cols));
      if (plan_runtime != nullptr) {
        const double scan_ms = ColumnarMsSince(scan_begin);
        plan_runtime->scan_ms += scan_ms;
        plan_runtime->filter_ms += scan_ms;
        plan_runtime->scan_rows += total_scanned.load();
        plan_runtime->scan_output_rows += total_matching.load();
        plan_runtime->aggregate_groups += group_count;
        plan_runtime->aggregate_input_rows += total_matching.load();
        plan_runtime->aggregate_ms += ColumnarMsSince(merge_begin);
      }
      return std::optional<Relation>(std::move(out));
    }

    size_t total_worker_groups = 0;
    if (single_int_key) {
      for (const auto& it : single_int_tables) {
        total_worker_groups += it->TotalGroups();
      }
    } else {
      for (const auto& wt : worker_tables) {
        total_worker_groups += wt->NumGroups();
      }
    }

    if (single_int_key && total_worker_groups <= 32768 && plan_having.empty() &&
        !plan_any_distinct && plan_agg_specs.size() == 1 &&
        plan_agg_specs[0].kind == ColAggKind::kCountStar &&
        plan_order.size() == 1 &&
        plan_order[0].order.KeyCollation() != Collation::kCaseInsensitive &&
        (plan_order[0].binding.kind == OutputSlotBinding::Kind::kAgg ||
         (plan_order[0].binding.kind == OutputSlotBinding::Kind::kKey &&
          (key_specs[plan_order[0].binding.index].kind ==
               ColKeyKind::kColInt64 ||
           key_specs[plan_order[0].binding.index].kind ==
               ColKeyKind::kColDate ||
           key_specs[plan_order[0].binding.index].kind ==
               ColKeyKind::kUtcTimestampExtract ||
           key_specs[plan_order[0].binding.index].kind ==
               ColKeyKind::kUtcTimestampTrunc))) &&
        std::all_of(plan_emit.begin(), plan_emit.end(),
                    [](const OutputSlotBinding& b) {
                      return b.kind == OutputSlotBinding::Kind::kKey ||
                             b.kind == OutputSlotBinding::Kind::kAgg ||
                             b.kind == OutputSlotBinding::Kind::kConstValue;
                    })) {
      SingleIntWorkerTable& merged = *single_int_tables[0];
      merged.Reserve(std::min<size_t>(total_worker_groups, 32768));
      for (size_t w = 1; w < workers; ++w) {
        SingleIntWorkerTable& src = *single_int_tables[w];
        for (uint32_t g = 0; g < src.NonNullCount(); ++g) {
          ASSIGN_OR_RETURN(uint32_t, dst_gid,
                           merged.FindOrInsertNonNull(src.KeyAt(g)));
          merged.CountAt(dst_gid) += src.CountAt(g);
        }
        if (src.HasNullGroup()) {
          merged.IncrementNullCount(src.NullCount());
        }
      }
      const OutputSlotBinding& ob0 = plan_order[0].binding;
      const SortKeyOrder& ord0 = plan_order[0].order;
      const bool order_is_agg = (ob0.kind == OutputSlotBinding::Kind::kAgg);
      const size_t n_nn = merged.NonNullCount();
      struct DirectSortItem {
        bool is_null{false};
        int64_t val{0};
        uint32_t gid{0};
      };
      std::vector<DirectSortItem> items;
      items.reserve(n_nn + (merged.HasNullGroup() ? 1 : 0));
      for (uint32_t g = 0; g < n_nn; ++g) {
        int64_t sv = 0;
        if (order_is_agg) {
          sv = merged.CountAt(g);
        } else if (packed_int32_pair) {
          const int64_t pk = merged.KeyAt(g);
          sv = (ob0.index == 0)
                   ? static_cast<int64_t>(static_cast<int32_t>(pk >> 32))
                   : static_cast<int64_t>(
                         static_cast<int32_t>(static_cast<uint32_t>(pk)));
        } else {
          sv = merged.KeyAt(g);
        }
        items.push_back({.is_null = false, .val = sv, .gid = g});
      }
      if (merged.HasNullGroup()) {
        items.push_back({.is_null = !order_is_agg,
                         .val = order_is_agg ? merged.NullCount() : 0,
                         .gid = UINT32_MAX});
      }
      size_t emit_count = items.size();
      if (stmt.HasLimit()) {
        const size_t cap =
            stmt.Offset() <= std::numeric_limits<size_t>::max() - stmt.Limit()
                ? stmt.Offset() + stmt.Limit()
                : std::numeric_limits<size_t>::max();
        emit_count = std::min(emit_count, cap);
      }
      auto dcmp = [&](const DirectSortItem& a,
                      const DirectSortItem& b) noexcept {
        if (a.is_null || b.is_null) {
          if (a.is_null != b.is_null) {
            return a.is_null ? ord0.NullsFirst() : !ord0.NullsFirst();
          }
          return a.gid < b.gid;
        }
        if (a.val != b.val) {
          return ord0.Ascending() ? (a.val < b.val) : (a.val > b.val);
        }
        return a.gid < b.gid;
      };
      if (!items.empty()) {
        if (emit_count < items.size() && emit_count * 2 < items.size()) {
          std::partial_sort(
              items.begin(),
              items.begin() + static_cast<std::ptrdiff_t>(emit_count),
              items.end(), dcmp);
        } else {
          std::sort(items.begin(), items.end(), dcmp);
        }
      }
      if (plan_publish_marker && plan_runtime != nullptr) {
        auto marker = std::make_shared<Relation>(plan_runtime);
        marker->schema = qualified_schema;
        plan_runtime->base_relations.emplace(fold_cache_key, std::move(marker));
      }
      Relation output(plan_runtime);
      output.rows.reserve(emit_count);
      std::vector<Column> out_cols;
      out_cols.reserve(plan_emit.size());
      for (size_t i = 0; i < plan_emit.size(); ++i) {
        ValueType vt = ValueType::kInt64;
        std::string cname = "col_" + std::to_string(i);
        if (i < plan_select_names.size()) {
          cname = ProjectionName(plan_select_names[i], i);
          if (plan_select_names[i].expression) {
            auto rt =
                plan_select_names[i].expression->ResultType(qualified_schema);
            vt = rt.GetType() == TypeTag::kDouble    ? ValueType::kDouble
                 : rt.GetType() == TypeTag::kDate    ? ValueType::kDate
                 : rt.GetType() == TypeTag::kVarChar ? ValueType::kVarChar
                                                     : ValueType::kInt64;
          }
        }
        out_cols.emplace_back(std::move(cname), vt);
      }
      std::vector<uint8_t> type_refined(out_cols.size(), 0);
      const size_t approx_row_bytes = plan_emit.size() * sizeof(Value);
      const size_t flush_gran = Relation::ChargeFlushGranularity();
      for (size_t r = 0; r < emit_count; ++r) {
        const uint32_t g = items[r].gid;
        const int64_t cnt =
            (g == UINT32_MAX) ? merged.NullCount() : merged.CountAt(g);
        std::vector<Value> row_vals;
        row_vals.reserve(plan_emit.size());
        for (size_t i = 0; i < plan_emit.size(); ++i) {
          const OutputSlotBinding& eb = plan_emit[i];
          Value v;
          if (eb.kind == OutputSlotBinding::Kind::kConstValue) {
            v = eb.constant;
          } else if (eb.kind == OutputSlotBinding::Kind::kAgg) {
            v = Value(cnt);
          } else if (g == UINT32_MAX) {
            v = Value();
          } else {
            const int64_t kv =
                packed_int32_pair
                    ? ((eb.index == 0)
                           ? static_cast<int64_t>(
                                 static_cast<int32_t>(merged.KeyAt(g) >> 32))
                           : static_cast<int64_t>(static_cast<int32_t>(
                                 static_cast<uint32_t>(merged.KeyAt(g)))))
                    : merged.KeyAt(g);
            const ColGroupKeySpec& ks = key_specs[eb.index];
            if (ks.kind == ColKeyKind::kColDate) {
              v = Value::DateFromDays(kv);
            } else if (ks.kind == ColKeyKind::kUtcTimestampTrunc) {
              ASSIGN_OR_RETURN(Value, ts_v, FormatUtcTimestampTruncKey(kv));
              v = std::move(ts_v);
            } else {
              v = Value(kv);
            }
          }
          if (type_refined[i] == 0U && !v.IsNull()) {
            out_cols[i] = Column(out_cols[i].Name(), ValueTypeOf(v));
            out_cols[i].SetUnsigned(v.IsUnsigned());
            type_refined[i] = 1;
          }
          row_vals.push_back(std::move(v));
        }
        output.AddRowWithKnownBytes(Row(std::move(row_vals)), approx_row_bytes,
                                    flush_gran);
      }
      output.schema = Schema("", std::move(out_cols));
      if (plan_runtime != nullptr) {
        const double scan_ms = ColumnarMsSince(scan_begin);
        plan_runtime->scan_ms += scan_ms;
        plan_runtime->filter_ms += scan_ms;
        plan_runtime->scan_rows += total_scanned.load();
        plan_runtime->scan_output_rows += total_matching.load();
        plan_runtime->typed_parallel_aggregations += 1;
        plan_runtime->aggregate_groups += merged.TotalGroups();
        plan_runtime->aggregate_input_rows += total_matching.load();
        plan_runtime->aggregate_ms += ColumnarMsSince(merge_begin);
      }
      return std::optional<Relation>(std::move(output));
    }

    const size_t merge_shards =
        (workers > 1 && total_worker_groups > 4096)
            ? (total_worker_groups > 65536
                   ? 64
                   : (total_worker_groups > 32768 ? 16 : 8))
            : 1;
    const uint64_t shard_mask = merge_shards - 1;

    struct ShardRef {
      uint32_t worker_id{0};
      uint32_t gid{0};
    };
    std::vector<std::vector<ShardRef>> shard_refs(merge_shards);
    for (size_t s = 0; s < merge_shards; ++s) {
      shard_refs[s].reserve((total_worker_groups / merge_shards) + 16);
    }
    if (single_int_key) {
      for (uint32_t w = 0; w < workers; ++w) {
        const auto& it = *single_int_tables[w];
        for (uint32_t g = 0; g < it.NonNullCount(); ++g) {
          const size_t s =
              merge_shards == 1
                  ? 0
                  : (static_cast<size_t>(it.HashAt(g)) & shard_mask);
          shard_refs[s].push_back({.worker_id = w, .gid = g});
        }
        if (it.HasNullGroup()) {
          const size_t s =
              merge_shards == 1
                  ? 0
                  : (static_cast<size_t>(SingleIntWorkerTable::kNullHash) &
                     shard_mask);
          shard_refs[s].push_back({.worker_id = w, .gid = UINT32_MAX});
        }
      }
    } else {
      for (uint32_t w = 0; w < workers; ++w) {
        const auto& wt = *worker_tables[w];
        for (uint32_t g = 0; g < wt.NumGroups(); ++g) {
          const size_t s =
              merge_shards == 1
                  ? 0
                  : (static_cast<size_t>(wt.HashAt(g)) & shard_mask);
          shard_refs[s].push_back({.worker_id = w, .gid = g});
        }
      }
    }

    enum class FastSingleOrderKind : uint8_t { kNone, kInt, kDouble };
    FastSingleOrderKind fast_order_kind = FastSingleOrderKind::kNone;
    if (plan_order.size() == 1 &&
        plan_order[0].order.KeyCollation() != Collation::kCaseInsensitive) {
      const OutputSlotBinding& b0 = plan_order[0].binding;
      if (b0.kind == OutputSlotBinding::Kind::kAgg) {
        switch (plan_agg_specs[b0.index].kind) {
          case ColAggKind::kCountStar:
          case ColAggKind::kCountCol:
          case ColAggKind::kCountByteLen:
          case ColAggKind::kSumInt64:
          case ColAggKind::kSumByteLen:
          case ColAggKind::kMinInt64:
          case ColAggKind::kMaxInt64:
          case ColAggKind::kMinByteLen:
          case ColAggKind::kMaxByteLen:
          case ColAggKind::kMinDate:
          case ColAggKind::kMaxDate:
            fast_order_kind = FastSingleOrderKind::kInt;
            break;
          case ColAggKind::kAvgDouble:
          case ColAggKind::kAvgByteLen:
          case ColAggKind::kSumDouble:
          case ColAggKind::kMinDouble:
          case ColAggKind::kMaxDouble:
            fast_order_kind = FastSingleOrderKind::kDouble;
            break;
          default:
            break;
        }
      } else if (b0.kind == OutputSlotBinding::Kind::kKey) {
        switch (key_specs[b0.index].kind) {
          case ColKeyKind::kColInt64:
          case ColKeyKind::kConstInt64:
          case ColKeyKind::kColDate:
          case ColKeyKind::kUtcTimestampExtract:
          case ColKeyKind::kUtcTimestampTrunc:
            fast_order_kind = FastSingleOrderKind::kInt;
            break;
          case ColKeyKind::kColDouble:
            fast_order_kind = FastSingleOrderKind::kDouble;
            break;
          default:
            break;
        }
      }
    }

    struct FinalGroupRow {
      uint32_t shard{0};
      uint32_t gid{0};
      bool num_null{false};
      int64_t num_int{0};
      double num_double{0.0};
      uint32_t order_offset{0};
    };
    std::vector<std::vector<Value>> shard_order_keys(merge_shards);
    auto cmp_rows = [&](const FinalGroupRow& a, const FinalGroupRow& b) {
      if (fast_order_kind != FastSingleOrderKind::kNone) {
        const SortKeyOrder& ord0 = plan_order[0].order;
        if (a.num_null || b.num_null) {
          if (a.num_null != b.num_null) {
            return a.num_null ? ord0.NullsFirst() : !ord0.NullsFirst();
          }
        } else if (fast_order_kind == FastSingleOrderKind::kInt) {
          if (a.num_int != b.num_int) {
            return ord0.Ascending() ? (a.num_int < b.num_int)
                                    : (a.num_int > b.num_int);
          }
        } else {
          const bool na = std::isnan(a.num_double);
          const bool nb = std::isnan(b.num_double);
          if (na != nb) {
            return ord0.Ascending() ? na : !na;
          }
          if (!na && a.num_double != b.num_double) {
            return ord0.Ascending() ? (a.num_double < b.num_double)
                                    : (a.num_double > b.num_double);
          }
        }
        return std::tie(a.shard, a.gid) < std::tie(b.shard, b.gid);
      }
      for (size_t k = 0; k < plan_order.size(); ++k) {
        const int c = plan_order[k].order.Compare(
            shard_order_keys[a.shard][a.order_offset + k],
            shard_order_keys[b.shard][b.order_offset + k]);
        if (c != 0) {
          return c < 0;
        }
      }
      return std::tie(a.shard, a.gid) < std::tie(b.shard, b.gid);
    };
    auto cmp_probe_row = [&](const std::vector<Value>& scratch,
                             const FinalGroupRow& probe,
                             const FinalGroupRow& row) {
      const std::vector<Value>& row_keys = shard_order_keys[row.shard];
      for (size_t k = 0; k < plan_order.size(); ++k) {
        const int c = plan_order[k].order.Compare(
            scratch[k], row_keys[row.order_offset + k]);
        if (c != 0) {
          return c < 0;
        }
      }
      return std::tie(probe.shard, probe.gid) < std::tie(row.shard, row.gid);
    };

    const bool bounded_topk = stmt.HasLimit() && !plan_order.empty();
    const size_t topk_cap =
        stmt.HasLimit() ? (stmt.Offset() <= std::numeric_limits<size_t>::max() -
                                                stmt.Limit()
                               ? stmt.Offset() + stmt.Limit()
                               : std::numeric_limits<size_t>::max())
                        : 0;

    std::vector<uint8_t> needed_agg_before_emit(plan_agg_specs.size(), 0);
    for (const PositionalHaving& clause : plan_having) {
      const bool fast_int_count =
          !clause.rhs_is_agg && clause.has_constant &&
          clause.constant.type == ValueType::kInt64 &&
          (plan_agg_specs[clause.agg_index].kind == ColAggKind::kCountStar ||
           plan_agg_specs[clause.agg_index].kind == ColAggKind::kCountCol ||
           plan_agg_specs[clause.agg_index].kind == ColAggKind::kCountByteLen);
      if (!fast_int_count) {
        needed_agg_before_emit[clause.agg_index] = 1;
        if (clause.rhs_is_agg) {
          needed_agg_before_emit[clause.rhs_agg_index] = 1;
        }
      }
    }
    if (fast_order_kind == FastSingleOrderKind::kNone) {
      for (const auto& ob : plan_order) {
        if (ob.binding.kind == OutputSlotBinding::Kind::kAgg) {
          needed_agg_before_emit[ob.binding.index] = 1;
        } else if (ob.binding.kind == OutputSlotBinding::Kind::kPostAgg) {
          std::fill(needed_agg_before_emit.begin(),
                    needed_agg_before_emit.end(), 1);
        }
      }
    }

    std::vector<QueryMemoryCharge> merge_charges(merge_shards);
    std::vector<std::unique_ptr<WorkerGroupTable>> merged_shards(merge_shards);
    std::vector<Status> merge_status(merge_shards, Status::kSuccess);
    std::vector<std::vector<FinalGroupRow>> shard_survivors(merge_shards);
    std::vector<Status> shard_extract_status(merge_shards, Status::kSuccess);

    auto merge_and_extract_fn = [&](size_t s) {
      const bool bypass_merge =
          (!single_int_key && merge_shards == 1 && workers == 1);
      if (!bypass_merge) {
        if (shard_refs[s].empty()) {
          return;
        }
        auto mt = std::make_unique<WorkerGroupTable>(
            &key_specs, &plan_agg_specs, &merge_charges[s]);
        Status st = mt->Reserve(shard_refs[s].size());
        if (st != Status::kSuccess) {
          merge_status[s] = std::move(st);
          return;
        }
        std::array<RuntimeKeyCell, 2> unpack_cells{};
        RuntimeAggState count_only_state;
        auto unpack_or_single = [&](int64_t packed,
                                    const RuntimeKeyCell** out_ptr) {
          if (packed_int32_pair) {
            unpack_cells[0] =
                RuntimeKeyCell{.int_val = static_cast<int64_t>(
                                   static_cast<int32_t>(packed >> 32))};
            unpack_cells[1] = RuntimeKeyCell{
                .int_val = static_cast<int64_t>(
                    static_cast<int32_t>(static_cast<uint32_t>(packed)))};
            *out_ptr = unpack_cells.data();
          } else {
            unpack_cells[0] = RuntimeKeyCell{.int_val = packed};
            *out_ptr = unpack_cells.data();
          }
        };
        for (const ShardRef& ref : shard_refs[s]) {
          Status mst = Status::kSuccess;
          if (single_int_key) {
            SingleIntWorkerTable& src = *single_int_tables[ref.worker_id];
            if (ref.gid == UINT32_MAX) {
              unpack_cells[0] = RuntimeKeyCell{.is_null = true};
              if (single_int_count_star) {
                count_only_state.count = src.NullCount();
                count_only_state.has_value = true;
                mst = mt->MergeFromGroup(unpack_cells.data(),
                                         SingleIntWorkerTable::kNullHash,
                                         {&count_only_state, 1}, true);
              } else {
                mst = mt->MergeFromGroup(
                    unpack_cells.data(), SingleIntWorkerTable::kNullHash,
                    {src.NullAggs(), plan_agg_specs.size()},
                    /*borrow_strings=*/true);
              }
            } else {
              const RuntimeKeyCell* kptr = nullptr;
              unpack_or_single(src.KeyAt(ref.gid), &kptr);
              const uint64_t kh =
                  packed_int32_pair
                      ? GroupKeySliceHasher::HashTyped(kptr, key_specs)
                      : src.HashAt(ref.gid);
              if (single_int_count_star) {
                count_only_state.count = src.CountAt(ref.gid);
                count_only_state.has_value = true;
                mst =
                    mt->MergeFromGroup(kptr, kh, {&count_only_state, 1}, true);
              } else {
                mst = mt->MergeFromGroup(
                    kptr, kh, {src.AggsAt(ref.gid), plan_agg_specs.size()},
                    /*borrow_strings=*/true);
              }
            }
          } else {
            WorkerGroupTable& src = *worker_tables[ref.worker_id];
            mst =
                mt->MergeFromGroup(src.KeysAt(ref.gid), src.HashAt(ref.gid),
                                   {src.AggsAt(ref.gid), plan_agg_specs.size()},
                                   /*borrow_strings=*/true);
          }
          if (mst != Status::kSuccess) {
            merge_status[s] = std::move(mst);
            return;
          }
        }
        merged_shards[s] = std::move(mt);
      }
      if (!merged_shards[s]) {
        return;
      }

      WorkerGroupTable& mt = *merged_shards[s];
      auto& out_vec = shard_survivors[s];
      std::vector<Value> finished_scratch(plan_agg_specs.size());
      std::vector<Value> cached_keys(key_specs.size());
      std::vector<Value> order_scratch;
      if (fast_order_kind == FastSingleOrderKind::kNone &&
          !plan_order.empty()) {
        order_scratch.resize(plan_order.size());
      }
      const size_t ng = mt.NumGroups();
      const bool use_shard_heap = bounded_topk && topk_cap > 0 && ng > topk_cap;
      if (use_shard_heap) {
        out_vec.reserve(topk_cap + 1);
      } else if (plan_having.empty()) {
        out_vec.reserve(ng);
      }

      for (uint32_t g = 0; g < ng; ++g) {
        const RuntimeAggState* ast = mt.AggsAt(g);
        auto having_or = EvalPositionalHavingOnStates(
            plan_having, plan_agg_specs, ast, &finished_scratch);
        if (!having_or.HasValue()) {
          shard_extract_status[s] = having_or.GetStatus();
          return;
        }
        if (!having_or.Value()) {
          continue;
        }

        if (fast_order_kind != FastSingleOrderKind::kNone) {
          FinalGroupRow probe{.shard = static_cast<uint32_t>(s), .gid = g};
          const OutputSlotBinding& b0 = plan_order[0].binding;
          if (b0.kind == OutputSlotBinding::Kind::kAgg) {
            const RuntimeAggState& st = ast[b0.index];
            switch (plan_agg_specs[b0.index].kind) {
              case ColAggKind::kCountStar:
              case ColAggKind::kCountCol:
              case ColAggKind::kCountByteLen:
                probe.num_int = st.count;
                break;
              case ColAggKind::kSumInt64:
              case ColAggKind::kSumByteLen:
              case ColAggKind::kMinInt64:
              case ColAggKind::kMaxInt64:
              case ColAggKind::kMinByteLen:
              case ColAggKind::kMaxByteLen:
              case ColAggKind::kMinDate:
              case ColAggKind::kMaxDate:
                probe.num_null = !st.has_value;
                probe.num_int = st.int_val;
                break;
              case ColAggKind::kAvgDouble:
              case ColAggKind::kAvgByteLen:
                probe.num_null = (st.count == 0);
                probe.num_double =
                    st.count == 0
                        ? 0.0
                        : (st.double_val / static_cast<double>(st.count));
                break;
              case ColAggKind::kSumDouble:
                probe.num_null = !st.has_value;
                probe.num_double = st.double_val;
                break;
              case ColAggKind::kMinDouble:
              case ColAggKind::kMaxDouble:
                probe.num_null = !st.has_value;
                probe.num_double =
                    st.saw_nan ? std::numeric_limits<double>::quiet_NaN()
                               : st.double_val;
                break;
              default:
                break;
            }
          } else {
            const RuntimeKeyCell& kc = mt.KeysAt(g)[b0.index];
            probe.num_null = kc.is_null;
            if (fast_order_kind == FastSingleOrderKind::kInt) {
              probe.num_int = kc.int_val;
            } else {
              probe.num_double = kc.double_val;
            }
          }
          if (bounded_topk && topk_cap == 0) {
            continue;
          }
          if (use_shard_heap) {
            if (out_vec.size() < topk_cap) {
              // NOLINTNEXTLINE(performance-move-const-arg) move is harmless
              out_vec.push_back(std::move(probe));
              std::push_heap(out_vec.begin(), out_vec.end(), cmp_rows);
            } else if (cmp_rows(probe, out_vec.front())) {
              std::pop_heap(out_vec.begin(), out_vec.end(), cmp_rows);
              out_vec.back() = probe;
              std::push_heap(out_vec.begin(), out_vec.end(), cmp_rows);
            }
          } else {
            // NOLINTNEXTLINE(performance-move-const-arg) move is harmless
            out_vec.push_back(std::move(probe));
          }
        } else if (!plan_order.empty()) {
          for (size_t a = 0; a < plan_agg_specs.size(); ++a) {
            if (needed_agg_before_emit[a]) {
              StatusOr<Value> finished_value =
                  FinishAggState(plan_agg_specs[a], ast[a]);
              if (!finished_value.HasValue()) {
                shard_extract_status[s] = finished_value.GetStatus();
                return;
              }
              finished_scratch[a] = finished_value.MoveValue();
            }
          }
          std::fill(cached_keys.begin(), cached_keys.end(), Value());
          for (size_t k = 0; k < plan_order.size(); ++k) {
            auto kv_or = EvaluateBindingValue(plan_order[k].binding, key_specs,
                                              mt.KeysAt(g), finished_scratch,
                                              &cached_keys);
            if (!kv_or.HasValue()) {
              shard_extract_status[s] = kv_or.GetStatus();
              return;
            }
            order_scratch[k] = std::move(kv_or.Value());
          }
          if (bounded_topk && topk_cap == 0) {
            continue;
          }
          std::vector<Value>& arena = shard_order_keys[s];
          if (use_shard_heap) {
            FinalGroupRow probe{.shard = static_cast<uint32_t>(s), .gid = g};
            if (out_vec.size() < topk_cap) {
              const auto offset = static_cast<uint32_t>(arena.size());
              for (size_t k = 0; k < plan_order.size(); ++k) {
                arena.push_back(std::move(order_scratch[k]));
              }
              probe.order_offset = offset;
              out_vec.push_back(probe);
              std::push_heap(out_vec.begin(), out_vec.end(), cmp_rows);
            } else if (cmp_probe_row(order_scratch, probe, out_vec.front())) {
              std::pop_heap(out_vec.begin(), out_vec.end(), cmp_rows);
              FinalGroupRow& evicted = out_vec.back();
              for (size_t k = 0; k < plan_order.size(); ++k) {
                arena[evicted.order_offset + k] = std::move(order_scratch[k]);
              }
              evicted.gid = g;
              std::push_heap(out_vec.begin(), out_vec.end(), cmp_rows);
            }
            continue;
          }
          const auto offset = static_cast<uint32_t>(arena.size());
          for (size_t k = 0; k < plan_order.size(); ++k) {
            arena.push_back(std::move(order_scratch[k]));
          }
          out_vec.push_back({.shard = static_cast<uint32_t>(s),
                             .gid = g,
                             .order_offset = offset});
        } else {
          out_vec.push_back({.shard = static_cast<uint32_t>(s), .gid = g});
          if (stmt.HasLimit() && out_vec.size() >= topk_cap) {
            break;
          }
        }
      }
    };

    if (!single_int_key && merge_shards == 1 && workers == 1) {
      merged_shards[0] = std::move(worker_tables[0]);
      merge_and_extract_fn(0);
    } else if (merge_shards == 1) {
      merge_and_extract_fn(0);
    } else {
      const size_t mworkers = std::min(workers, merge_shards);
      std::atomic<size_t> next_s{0};
      RETURN_IF_FAIL(RunParallelTasks(mworkers, [&](size_t) {
        while (true) {
          const size_t s = next_s.fetch_add(1, std::memory_order_relaxed);
          if (s >= merge_shards) {
            break;
          }
          merge_and_extract_fn(s);
        }
      }));
    }
    for (const Status& st : merge_status) {
      if (st == StatusCode::kMemoryLimitExceeded) {
        return std::optional<Relation>{};
      }
      RETURN_IF_FAIL(st);
    }
    for (const Status& st : shard_extract_status) {
      RETURN_IF_FAIL(st);
    }

    size_t total_merged_groups = 0;
    for (size_t s = 0; s < merge_shards; ++s) {
      if (merged_shards[s]) {
        total_merged_groups += merged_shards[s]->NumGroups();
      }
    }

    size_t total_survivors = 0;
    for (const auto& sv : shard_survivors) {
      total_survivors += sv.size();
    }
    std::vector<FinalGroupRow> survivors;
    survivors.reserve(total_survivors);
    for (auto& sv : shard_survivors) {
      for (auto& item : sv) {
        // NOLINTNEXTLINE(performance-move-const-arg) move is harmless
        survivors.push_back(std::move(item));
      }
    }

    size_t emit_count = survivors.size();
    if (stmt.HasLimit()) {
      emit_count = std::min(emit_count, topk_cap);
    }
    if (!plan_order.empty() && !survivors.empty()) {
      if (emit_count < survivors.size() && emit_count * 2 < survivors.size()) {
        std::partial_sort(
            survivors.begin(),
            survivors.begin() + static_cast<std::ptrdiff_t>(emit_count),
            survivors.end(), cmp_rows);
      } else if (
          ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_RADIX_SORT") ==
              nullptr &&
          !bounded_topk && fast_order_kind == FastSingleOrderKind::kInt &&
          plan_order.size() == 1 && survivors.size() >= 1024 &&
          // Lean proof:
          // lean/TinylambOpt/ResearchProofs/StableRadix.lean
          // (sound)
          TryStableRadixSortNullableInt64(
              survivors, plan_order[0].order.Ascending(),
              plan_order[0].order.NullsFirst(),
              [](const FinalGroupRow& r) noexcept { return r.num_null; },
              [](const FinalGroupRow& r) noexcept { return r.num_int; })) {
      } else {
        std::sort(survivors.begin(), survivors.end(), cmp_rows);
      }
    }

    if (plan_publish_marker && plan_runtime != nullptr) {
      auto marker = std::make_shared<Relation>(plan_runtime);
      marker->schema = qualified_schema;
      plan_runtime->base_relations.emplace(fold_cache_key, std::move(marker));
    }

    Relation output(plan_runtime);
    output.rows.reserve(emit_count);
    std::vector<Column> out_cols;
    out_cols.reserve(plan_emit.size());
    for (size_t i = 0; i < plan_emit.size(); ++i) {
      ValueType vt = ValueType::kInt64;
      std::string cname = "col_" + std::to_string(i);
      if (i < plan_select_names.size()) {
        cname = ProjectionName(plan_select_names[i], i);
        if (plan_select_names[i].expression) {
          auto rt =
              plan_select_names[i].expression->ResultType(qualified_schema);
          vt = rt.GetType() == TypeTag::kDouble    ? ValueType::kDouble
               : rt.GetType() == TypeTag::kDate    ? ValueType::kDate
               : rt.GetType() == TypeTag::kVarChar ? ValueType::kVarChar
                                                   : ValueType::kInt64;
        }
      }
      out_cols.emplace_back(std::move(cname), vt);
    }
    std::vector<uint8_t> type_refined(out_cols.size(), 0);
    std::vector<Value> full_finished(plan_agg_specs.size());
    std::vector<Value> cached_keys(key_specs.size());
    for (size_t r = 0; r < emit_count; ++r) {
      const FinalGroupRow& fg = survivors[r];
      WorkerGroupTable& mt = *merged_shards[fg.shard];
      const RuntimeAggState* ast = mt.AggsAt(fg.gid);
      for (size_t a = 0; a < plan_agg_specs.size(); ++a) {
        ASSIGN_OR_RETURN(Value, finished_value,
                         (FinishAggState(plan_agg_specs[a], ast[a])));
        full_finished[a] = std::move(finished_value);
      }
      std::vector<Value> row_vals;
      row_vals.reserve(plan_emit.size());
      std::fill(cached_keys.begin(), cached_keys.end(), Value());
      for (size_t i = 0; i < plan_emit.size(); ++i) {
        ASSIGN_OR_RETURN(
            Value, v,
            EvaluateBindingValue(plan_emit[i], key_specs, mt.KeysAt(fg.gid),
                                 full_finished, &cached_keys));
        if (type_refined[i] == 0U && !v.IsNull()) {
          out_cols[i] = Column(out_cols[i].Name(), ValueTypeOf(v));
          out_cols[i].SetUnsigned(v.IsUnsigned());
          type_refined[i] = 1;
        }
        row_vals.push_back(std::move(v));
      }
      output.AddRow(Row(std::move(row_vals)));
    }
    output.schema = Schema("", std::move(out_cols));

    if (dbg) {
      (void)std::fprintf(stderr,
                         "[TLCS-AGG-OK] grouped dir=%.2fms scan=%.2fms "
                         "merge=%.2fms cands=%zu/%u rows=%zu/%zu groups=%zu\n",
                         dir_ms, ColumnarMsSince(scan_begin), ColumnarMsSince(merge_begin),
                         candidate_blocks.size(), tlcs->BlockCount(),
                         total_scanned.load(), total_matching.load(),
                         total_merged_groups);
    }
    if (plan_runtime != nullptr) {
      const double scan_ms = ColumnarMsSince(scan_begin);
      plan_runtime->scan_ms += scan_ms;
      plan_runtime->filter_ms += scan_ms;
      plan_runtime->scan_rows += total_scanned.load();
      plan_runtime->scan_output_rows += total_matching.load();
      plan_runtime->typed_parallel_aggregations += 1;
      plan_runtime->aggregate_groups += total_merged_groups;
      plan_runtime->aggregate_input_rows += total_matching.load();
      plan_runtime->aggregate_ms += ColumnarMsSince(merge_begin);
    }
    return std::optional<Relation>(std::move(output));
  };

  // Lean proof: lean/TinylambOpt/GroupKeyAndDictionaryRewrites.lean
  // (two_stage_topn_late_agg_materialization_sound,
  // two_stage_late_agg_sql_in_null_group_bug)
  // CB-3: Two-Stage Top-N Late Aggregate Materialization (ClickBench Q10, Q22,
  // Q23, Q29). When HAVING and ORDER BY ... LIMIT K depend only on lightweight
  // ranking aggregates (`agg1`, e.g. `COUNT(*)`, `AVG(BYTE_LENGTH(...))`),
  // defer expensive payload aggregates (`MIN/MAX(VARCHAR)`, `COUNT(DISTINCT)`,
  // and provably non-overflowing unreferenced aggregates) to a second pass over
  // ONLY the `W <= K` winning group keys using NULL-safe key membership!
  // Lean proof: lean/TinylambOpt/CountBoundsAndBlockAggregates.lean
  //   (having_count_markov_bound_sound, having_two_stage_late_agg_sound,
  //    having_sum_signed_unbounded_groups_bug)
  uint64_t min_having_count = 0;
  for (const PositionalHaving& h : positional_having) {
    if (!h.has_constant || h.rhs_is_agg || h.agg_index >= agg_specs.size() ||
        h.constant.IsNull() || h.constant.IsUnsigned() ||
        h.constant.type != ValueType::kInt64) {
      continue;
    }
    const ColAggKind hk = agg_specs[h.agg_index].kind;
    if (hk != ColAggKind::kCountStar && hk != ColAggKind::kCountCol &&
        hk != ColAggKind::kCountByteLen) {
      continue;
    }
    const int64_t cval = h.constant.value.int_value;
    int64_t min_req = 0;
    if (h.op == BinaryOperation::kGreaterThan && cval >= 0 &&
        cval < std::numeric_limits<int64_t>::max()) {
      min_req = cval + 1;
    } else if ((h.op == BinaryOperation::kGreaterThanEquals ||
                h.op == BinaryOperation::kEquals) &&
               cval >= 1) {
      min_req = cval;
    }
    if (min_req >= 1 &&
        tlcs->TotalRows() / static_cast<uint64_t>(min_req) <= 1024ULL) {
      min_having_count = static_cast<uint64_t>(min_req);
      break;
    }
  }

  const bool markov_having_bounded = min_having_count != 0;
  const size_t outer_topk_cap =
      stmt.HasLimit()
          ? (stmt.Offset() <= std::numeric_limits<size_t>::max() - stmt.Limit()
                 ? stmt.Offset() + stmt.Limit()
                 : std::numeric_limits<size_t>::max())
          : 0;
  const bool topk_two_stage_eligible =
      stmt.HasLimit() && !order_bindings.empty() && outer_topk_cap > 0 &&
      outer_topk_cap <= 256;
  const bool try_certified_capture =
      markov_having_bounded && key_specs.size() == 1 &&
      key_specs[0].regex_capture_jit && tlcs->TotalRows() <= UINT32_MAX &&
      min_having_count <= UINT32_MAX && candidate_blocks.size() >= 128 &&
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_FUSED_CAPTURE_HAVING") ==
          nullptr &&
      std::ranges::all_of(agg_specs, [&](const ColAggSpec& agg) {
        return agg.kind == ColAggKind::kCountStar ||
               (agg.full_col == key_specs[0].full_col &&
                (agg.kind == ColAggKind::kMinVarChar ||
                 agg.kind == ColAggKind::kMaxVarChar ||
                 agg.kind == ColAggKind::kAvgByteLen));
      });
  using BucketProfiles = std::unordered_map<std::string, std::vector<uint32_t>>;
  // Process-wide capture profiles are intentional here; both reads and writes
  // below are held under profiles_mu, so thread_local is not needed.
  static std::mutex profiles_mu;
  static BucketProfiles profiles;
  std::string profile_key;
  std::optional<HavingBucketCertificate> capture_certificate;
  std::optional<std::vector<uint32_t>> observed_buckets;
  const auto publish_capture_profile = [&] {
    if (!observed_buckets) {
      return;
    }
    const std::scoped_lock lock(profiles_mu);
    if (profiles.contains(profile_key) || profiles.size() < 32) {
      profiles.insert_or_assign(profile_key, std::move(*observed_buckets));
    }
    observed_buckets.reset();
  };
  QueryMemoryCharge capture_mask_charge;
  constexpr size_t kHavingBuckets = 1U << 18;
  if (try_certified_capture && capture_mask_charge.TryAdd(kHavingBuckets)) {
    capture_certificate.emplace(HavingBucketCertificate{
        .tracked = std::vector<uint8_t>(kHavingBuckets),
        .minimum_count = static_cast<uint32_t>(min_having_count),
        .observe_only = true,
        .observed_buckets = &observed_buckets});
    const auto& capture_key = key_specs[0];
    profile_key =
        tlcs->Path() + "\n" + std::to_string(tlcs->LayoutGeneration()) + "\n" +
        std::to_string(capture_key.full_col) + "\n" +
        std::to_string(min_having_count) + "\n" + capture_key.regex_pattern +
        "\n" + capture_key.regex_replacement;
    {
      const std::scoped_lock lock(profiles_mu);
      if (const auto entry = profiles.find(profile_key);
          entry != profiles.end()) {
        for (uint32_t bucket : entry->second) {
          capture_certificate->tracked[bucket] = 1;
        }
        capture_certificate->observe_only = false;
      }
    }
    if (!capture_certificate->observe_only) {
      ASSIGN_OR_RETURN(
          std::optional<Relation>, fused,
          run_classified_plan(stmt, agg_specs, positional_having, emit_bindings,
                              order_bindings, stmt.SelectList(), runtime,
                              publish_lstable_marker, nullptr,
                              &*capture_certificate));
      publish_capture_profile();
      if (fused) {
        if (dbg) {
          (void)std::fprintf(stderr,
                             "[TLCS-AGG-OK] fused count-HAVING certificate\n");
        }
        return fused;
      }
    }
  }
  if (!is_group_all && (topk_two_stage_eligible || markov_having_bounded) &&
      !candidate_blocks.empty() && !agg_specs.empty()) {
    std::vector<uint8_t> is_early(agg_specs.size(), 0);
    for (const PositionalHaving& h : positional_having) {
      is_early[h.agg_index] = 1;
      if (h.rhs_is_agg) {
        is_early[h.rhs_agg_index] = 1;
      }
    }
    bool defer_order_to_s2 = false;
    if (markov_having_bounded) {
      for (const ColumnarOrderBinding& ob : order_bindings) {
        if (ob.binding.kind == OutputSlotBinding::Kind::kAgg &&
            is_early[ob.binding.index] == 0U) {
          defer_order_to_s2 = true;
        } else if (ob.binding.kind == OutputSlotBinding::Kind::kPostAgg &&
                   ob.binding.post_agg != nullptr) {
          for (const auto& s : ob.binding.post_agg->slots) {
            if (!s.is_key && s.index < is_early.size() &&
                is_early[s.index] == 0U) {
              defer_order_to_s2 = true;
            }
          }
        }
      }
      if (!stmt.HasLimit()) {
        defer_order_to_s2 = true;
      }
    }
    if (!defer_order_to_s2) {
      for (const ColumnarOrderBinding& ob : order_bindings) {
        if (ob.binding.kind == OutputSlotBinding::Kind::kAgg) {
          is_early[ob.binding.index] = 1;
        } else if (ob.binding.kind == OutputSlotBinding::Kind::kPostAgg &&
                   ob.binding.post_agg != nullptr) {
          for (const auto& s : ob.binding.post_agg->slots) {
            if (!s.is_key && s.index < is_early.size()) {
              is_early[s.index] = 1;
            }
          }
        }
      }
    }
    for (size_t a = 0; a < agg_specs.size(); ++a) {
      if (is_early[a] == 0U && agg_specs[a].kind == ColAggKind::kSumInt64) {
        ASSIGN_OR_RETURN(bool, safe_no_ovf,
                         sum_int64_provably_no_overflow(agg_specs[a].full_col));
        if (!safe_no_ovf) {
          is_early[a] = 1;
        }
      }
    }

    auto is_heavy_agg = [&](ColAggKind k) noexcept {
      if (k == ColAggKind::kMinVarChar || k == ColAggKind::kMaxVarChar ||
          k == ColAggKind::kCountDistinctInt64 ||
          k == ColAggKind::kCountDistinctDate ||
          k == ColAggKind::kCountDistinctDouble ||
          k == ColAggKind::kCountDistinctVarChar) {
        return true;
      }
      if (markov_having_bounded &&
          (k == ColAggKind::kAvgByteLen || k == ColAggKind::kSumByteLen ||
           k == ColAggKind::kMinByteLen || k == ColAggKind::kMaxByteLen)) {
        return true;
      }
      return false;
    };
    bool early_has_heavy = false;
    bool late_has_heavy = false;
    std::vector<size_t> orig_to_early(agg_specs.size(), SIZE_MAX);
    std::vector<ColAggSpec> early_aggs;
    early_aggs.reserve(agg_specs.size());
    std::vector<size_t> late_orig_idxs;
    late_orig_idxs.reserve(agg_specs.size());
    std::vector<ColAggSpec> late_aggs;
    late_aggs.reserve(agg_specs.size());
    for (size_t a = 0; a < agg_specs.size(); ++a) {
      if (is_early[a] != 0U) {
        if (is_heavy_agg(agg_specs[a].kind)) {
          early_has_heavy = true;
        }
        orig_to_early[a] = early_aggs.size();
        early_aggs.push_back(agg_specs[a]);
      } else {
        if (is_heavy_agg(agg_specs[a].kind)) {
          late_has_heavy = true;
        }
        late_orig_idxs.push_back(a);
        late_aggs.push_back(agg_specs[a]);
      }
    }
    // For large COUNT-first groups, deferring the numeric payload avoids
    // using the larger aggregate state for every hash-table entry.
    if (!late_has_heavy && candidate_blocks.size() >= 512 &&
        early_aggs.size() == 1 &&
        early_aggs[0].kind == ColAggKind::kCountStar && late_aggs.size() >= 2) {
      late_has_heavy = true;
    }
    // Ensure Stage 1 has at least one aggregate (`COUNT(*)` if ORDER BY is key)
    // and Stage 2 skips at least one heavy or multi-column payload aggregate.
    if (!early_has_heavy && late_has_heavy && !early_aggs.empty()) {
      std::vector<PositionalHaving> s1_having = positional_having;
      for (PositionalHaving& h : s1_having) {
        h.agg_index = orig_to_early[h.agg_index];
        if (h.rhs_is_agg) {
          h.rhs_agg_index = orig_to_early[h.rhs_agg_index];
        }
      }
      std::vector<ColumnarOrderBinding> s1_order;
      bool s1_order_ok = true;
      SelectStatement s1_stmt_storage;
      const SelectStatement* s1_stmt_ptr = &stmt;
      if (defer_order_to_s2) {
        s1_stmt_storage = stmt;
        s1_stmt_storage.SetOrderBy({});
        s1_stmt_storage.SetLimit(std::nullopt);
        s1_stmt_storage.SetOffset(0);
        s1_stmt_ptr = &s1_stmt_storage;
      } else {
        s1_order = order_bindings;
        for (ColumnarOrderBinding& ob : s1_order) {
          if (ob.binding.kind == OutputSlotBinding::Kind::kAgg) {
            ob.binding.index = orig_to_early[ob.binding.index];
          } else if (ob.binding.kind == OutputSlotBinding::Kind::kPostAgg) {
            // Clone PostAggExpr with remapped aggregate indices.
            auto cloned = std::make_shared<PostAggExpr>(*ob.binding.post_agg);
            for (auto& sl : cloned->slots) {
              if (!sl.is_key) {
                if (sl.index >= orig_to_early.size() ||
                    orig_to_early[sl.index] == SIZE_MAX) {
                  s1_order_ok = false;
                  break;
                }
                sl.index = orig_to_early[sl.index];
              }
            }
            ob.binding.post_agg = std::move(cloned);
          }
        }
      }
      if (s1_order_ok) {
        std::vector<OutputSlotBinding> s1_emit;
        s1_emit.reserve(key_specs.size() + early_aggs.size());
        for (size_t k = 0; k < key_specs.size(); ++k) {
          OutputSlotBinding kb;
          kb.kind = OutputSlotBinding::Kind::kKey;
          kb.index = k;
          s1_emit.push_back(std::move(kb));
        }
        for (size_t e = 0; e < early_aggs.size(); ++e) {
          OutputSlotBinding ab;
          ab.kind = OutputSlotBinding::Kind::kAgg;
          ab.index = e;
          s1_emit.push_back(std::move(ab));
        }
        Stage1BlockFilterMemo s1_memo;
        std::optional<Relation> s1_res;
        if (s1_having.empty() && preds.Empty()) {
          ASSIGN_OR_RETURN(
              std::optional<Relation>, metadata,
              TryFoldTlcsBoundaryAggregate(
                  *tlcs, *s1_stmt_ptr, table_schema, qualified_schema, preds,
                  key_specs, early_aggs, s1_emit, s1_order, {}, nullptr));
          s1_res = std::move(metadata);
        }
        if (!s1_res) {
          ASSIGN_OR_RETURN(
              std::optional<Relation>, scanned,
              run_classified_plan(
                  *s1_stmt_ptr, early_aggs, s1_having, s1_emit, s1_order, {},
                  /*plan_runtime=*/nullptr,
                  /*plan_publish_marker=*/false, &s1_memo,
                  capture_certificate && capture_certificate->observe_only
                      ? &*capture_certificate
                      : nullptr));
          publish_capture_profile();
          s1_res = std::move(scanned);
        }
        if (s1_res.has_value() && s1_res->rows.size() <= 1024) {
          Relation& s1_rel = *s1_res;
          const size_t n_win = s1_rel.rows.size();
          if (n_win == 0) {
            if (publish_lstable_marker && runtime != nullptr) {
              auto marker = std::make_shared<Relation>(runtime);
              marker->schema = qualified_schema;
              runtime->base_relations.emplace(fold_cache_key,
                                              std::move(marker));
            }
            Relation empty_out(runtime);
            std::vector<Column> out_cols;
            out_cols.reserve(stmt.SelectList().size());
            for (size_t i = 0; i < stmt.SelectList().size(); ++i) {
              ValueType vt = ValueType::kInt64;
              if (stmt.SelectList()[i].expression) {
                auto rt = stmt.SelectList()[i].expression->ResultType(
                    qualified_schema);
                vt = rt.GetType() == TypeTag::kDouble    ? ValueType::kDouble
                     : rt.GetType() == TypeTag::kDate    ? ValueType::kDate
                     : rt.GetType() == TypeTag::kVarChar ? ValueType::kVarChar
                                                         : ValueType::kInt64;
              }
              out_cols.emplace_back(ProjectionName(stmt.SelectList()[i], i),
                                    vt);
            }
            empty_out.schema = Schema("", std::move(out_cols));
            return std::optional<Relation>(std::move(empty_out));
          }

          ASSIGN_OR_RETURN(
              KeyProjectionMap, km,
              BuildKeyProjectionMap(*tlcs, candidate_blocks, key_specs));
          const size_t n_phys = km.phys_specs.size();
          std::vector<size_t> phys_to_full(n_phys, 0);
          for (size_t fk = 0; fk < key_specs.size(); ++fk) {
            if (km.full_to_phys[fk] >= 0) {
              phys_to_full[static_cast<size_t>(km.full_to_phys[fk])] = fk;
            }
          }

          // Build NULL-safe (`IS NOT DISTINCT FROM`) open-addressed lookup
          // table over the `n_win <= 256` winning physical key tuples!
          std::vector<RuntimeKeyCell> win_phys_keys(n_win * n_phys);
          std::vector<RuntimeKeyCell> win_full_keys(n_win * key_specs.size());
          bool key_conv_ok = true;
          auto value_to_cell = [](const ColGroupKeySpec& ks, const Value& v,
                                  RuntimeKeyCell* out) -> bool {
            *out = RuntimeKeyCell{};
            if (v.IsNull()) {
              out->is_null = true;
              return true;
            }
            switch (ks.kind) {
              case ColKeyKind::kColInt64:
              case ColKeyKind::kConstInt64:
              case ColKeyKind::kColInt64Delta:
              case ColKeyKind::kUtcTimestampExtract:
                out->int_val = v.value.int_value;
                return true;
              case ColKeyKind::kColDate:
                out->int_val = v.DateDays();
                return true;
              case ColKeyKind::kColDouble:
                out->double_val = v.value.double_value;
                return true;
              case ColKeyKind::kUtcTimestampTrunc: {
                const auto pk =
                    TryUtcTimestampTruncKey(v.AsStringView(), ks.trunc_unit);
                if (!pk.has_value()) {
                  return false;
                }
                out->int_val = *pk;
                return true;
              }
              case ColKeyKind::kColVarChar:
              case ColKeyKind::kCaseWhenVarChar:
              case ColKeyKind::kRegexReplaceVarChar: {
                const std::string_view sv = v.AsStringView();
                out->str_ptr = sv.data();
                out->str_len = static_cast<uint32_t>(sv.size());
                return true;
              }
            }
            return false;
          };

          for (size_t w = 0; w < n_win && key_conv_ok; ++w) {
            for (size_t fk = 0; fk < key_specs.size(); ++fk) {
              if (!value_to_cell(key_specs[fk], s1_rel.rows[w][fk],
                                 &win_full_keys[(w * key_specs.size()) + fk])) {
                key_conv_ok = false;
                break;
              }
            }
            for (size_t p = 0; p < n_phys; ++p) {
              win_phys_keys[(w * n_phys) + p] =
                  win_full_keys[(w * key_specs.size()) + phys_to_full[p]];
            }
          }

          if (key_conv_ok) {
            const size_t ht_cap =
                std::max<size_t>(16, std::bit_ceil(n_win * 4));
            const size_t ht_mask = ht_cap - 1;
            struct WinSlot {
              uint64_t hash{0};
              uint32_t win_plus_one{0};
            };
            std::vector<WinSlot> win_ht(ht_cap);
            bool win_p0_has_null = false;
            int64_t win_p0_min = std::numeric_limits<int64_t>::max();
            int64_t win_p0_max = std::numeric_limits<int64_t>::min();
            std::vector<int64_t> win_p0_values;
            const bool p0_int_prunable =
                n_phys >= 1 &&
                !table_schema.GetColumn(km.phys_specs[0].full_col)
                     .IsUnsigned() &&
                (km.phys_specs[0].kind == ColKeyKind::kColInt64 ||
                 km.phys_specs[0].kind == ColKeyKind::kColDate);
            if (p0_int_prunable) {
              win_p0_values.reserve(n_win);
            }

            for (uint32_t w = 0; w < n_win; ++w) {
              const RuntimeKeyCell* pk = &win_phys_keys[w * n_phys];
              if (p0_int_prunable) {
                if (pk[0].is_null) {
                  win_p0_has_null = true;
                } else {
                  win_p0_min = std::min(win_p0_min, pk[0].int_val);
                  win_p0_max = std::max(win_p0_max, pk[0].int_val);
                  win_p0_values.push_back(pk[0].int_val);
                }
              }
              const uint64_t h =
                  GroupKeySliceHasher::HashTyped(pk, km.phys_specs);
              size_t pos = static_cast<size_t>(h) & ht_mask;
              while (win_ht[pos].win_plus_one != 0) {
                pos = (pos + 1) & ht_mask;
              }
              win_ht[pos] = {.hash = h, .win_plus_one = w + 1};
            }

            // Prune `candidate_blocks` for Pass 2 using `km.phys_specs[0]`
            // integer zone maps when applicable (e.g. `RegionID` in Q10!).
            std::vector<uint32_t> p2_blocks;
            if (p0_int_prunable && !win_p0_has_null &&
                win_p0_min <= win_p0_max) {
              std::ranges::sort(win_p0_values);
              const uint32_t dg_count = tlcs->DirGroupCount();
              const uint32_t dg_size = tlcs->DirGroupSize();
              std::vector<std::shared_ptr<const TlcsFile::DirGroup>> dir_groups(
                  dg_count);
              for (uint32_t g = 0; g < dg_count; ++g) {
                ASSIGN_OR_RETURN(std::shared_ptr<const TlcsFile::DirGroup>, grp,
                                 tlcs->GetDirGroup(g));
                dir_groups[g] = std::move(grp);
              }
              const slot_t p0_col = km.phys_specs[0].full_col;
              p2_blocks.reserve(candidate_blocks.size());
              for (const uint32_t bidx : candidate_blocks) {
                const auto& blk =
                    dir_groups[bidx / dg_size]->blocks[bidx % dg_size];
                const auto& m = blk.columns[p0_col];
                if (m.encoding == TlcsColumnEncoding::kConstNull) {
                  continue;
                }
                if (m.has_min_max) {
                  // Lean: shortlist_interval_cannot_match. Test the exact
                  // shortlist against the zone, even inside the global hull.
                  const auto first =
                      std::ranges::lower_bound(win_p0_values, m.min_int);
                  if (first == win_p0_values.end() || *first > m.max_int) {
                    continue;
                  }
                }
                p2_blocks.push_back(bidx);
              }
            } else {
              p2_blocks = candidate_blocks;
            }

            if (s1_memo.complete) {
              std::vector<uint32_t> filtered_p2;
              filtered_p2.reserve(p2_blocks.size());
              for (const uint32_t bidx : p2_blocks) {
                if (bidx < s1_memo.block_has_match.size() &&
                    s1_memo.block_has_match[bidx] != 0U) {
                  filtered_p2.push_back(bidx);
                }
              }
              p2_blocks = std::move(filtered_p2);
            }

            const size_t n_late = late_aggs.size();
            const ColumnarStageSchedule p2_key_sched =
                BuildColumnarStageSchedule(table_schema, preds, km.phys_specs,
                                           {});
            // Predicate-free schedule used when Stage 1 cached exact surviving
            // `uint16_t` row offsets for a block (`s1_memo.block_rows[bidx]`).
            std::vector<slot_t> p2_memo_key_cols;
            std::vector<slot_t> p2_memo_late_cols;
            std::vector<uint8_t> p2_memo_late_len_only;
            {
              const ColumnarPredicateSet empty_preds;
              const ColumnarStageSchedule memo_ks = BuildColumnarStageSchedule(
                  table_schema, empty_preds, km.phys_specs, {});
              std::vector<uint8_t> in_memo_key(table_schema.ColumnCount(), 0);
              auto add_memo_key = [&](slot_t c) {
                if (in_memo_key[c] == 0U) {
                  in_memo_key[c] = 1;
                  p2_memo_key_cols.push_back(c);
                }
              };
              for (const slot_t c : memo_ks.stage1a_cols) {
                add_memo_key(c);
              }
              for (const slot_t c : memo_ks.stage1b_cols) {
                add_memo_key(c);
              }
              for (const slot_t c : memo_ks.stage2_cols) {
                add_memo_key(c);
              }
              for (const slot_t c : memo_ks.cond_cols) {
                add_memo_key(c);
              }
              std::vector<uint8_t> memo_late_seen(table_schema.ColumnCount(),
                                                  0);
              std::vector<uint8_t> memo_late_bytes(table_schema.ColumnCount(),
                                                   0);
              for (const ColAggSpec& ls : late_aggs) {
                if (ls.kind == ColAggKind::kCountStar) {
                  continue;
                }
                if (ls.double_arith != nullptr) {
                  for (const slot_t fc : ls.double_arith->referenced_cols) {
                    memo_late_seen[fc] = 1;
                  }
                  continue;
                }
                memo_late_seen[ls.full_col] = 1;
                if (ls.needs_bytes) {
                  memo_late_bytes[ls.full_col] = 1;
                }
              }
              for (size_t c = 0; c < table_schema.ColumnCount(); ++c) {
                if (memo_late_seen[c] != 0U && in_memo_key[c] == 0U) {
                  p2_memo_late_cols.push_back(static_cast<slot_t>(c));
                  p2_memo_late_len_only.push_back(memo_late_bytes[c] != 0U ? 0
                                                                           : 1);
                }
              }
            }
            const size_t ncols = table_schema.ColumnCount();
            std::vector<uint8_t> in_key_sched(ncols, 0);
            for (const slot_t c : p2_key_sched.stage1a_cols) {
              in_key_sched[c] = 1;
            }
            for (const slot_t c : p2_key_sched.stage1b_cols) {
              in_key_sched[c] = 1;
            }
            for (const slot_t c : p2_key_sched.stage1c_cols) {
              in_key_sched[c] = 1;
            }
            for (const slot_t c : p2_key_sched.stage2_cols) {
              in_key_sched[c] = 1;
            }
            for (const slot_t c : p2_key_sched.cond_cols) {
              in_key_sched[c] = 1;
            }

            // Check which already-scheduled columns still need full VARCHAR
            // bytes for `late_aggs` (`kMinVarChar` on `URL` in Q22/Q23 where
            // `URL` was already in `stage1a_cols` with full bytes!).
            std::vector<slot_t> p2_extra_cols;
            std::vector<uint8_t> p2_extra_len_only;
            std::vector<uint8_t> extra_seen(ncols, 0);
            std::vector<uint8_t> extra_bytes(ncols, 0);
            for (const ColAggSpec& ls : late_aggs) {
              if (ls.kind == ColAggKind::kCountStar) {
                continue;
              }
              if (ls.double_arith != nullptr) {
                for (const slot_t fc : ls.double_arith->referenced_cols) {
                  extra_seen[fc] = 1;
                }
                continue;
              }
              extra_seen[ls.full_col] = 1;
              if (ls.needs_bytes) {
                extra_bytes[ls.full_col] = 1;
              }
            }
            ColumnarStageSchedule p2_sched = p2_key_sched;
            for (size_t i = 0; i < p2_sched.stage1b_cols.size(); ++i) {
              if (extra_bytes[p2_sched.stage1b_cols[i]] != 0U) {
                p2_sched.stage1b_len_only[i] = 0;
              }
            }
            for (size_t i = 0; i < p2_sched.stage2_cols.size(); ++i) {
              if (extra_bytes[p2_sched.stage2_cols[i]] != 0U) {
                p2_sched.stage2_len_only[i] = 0;
              }
            }
            for (size_t c = 0; c < ncols; ++c) {
              if (extra_seen[c] != 0U && in_key_sched[c] == 0U) {
                p2_extra_cols.push_back(static_cast<slot_t>(c));
                p2_extra_len_only.push_back(extra_bytes[c] != 0U ? 0 : 1);
              }
            }

            // Separate integer `COUNT(DISTINCT)` late aggs for sharded
            // deduplication (handles skewed top-10 regions in ClickBench Q10
            // without building multi-million-entry maps per worker!).
            std::vector<size_t> int_dist_late_idxs;
            int_dist_late_idxs.reserve(n_late);
            for (size_t la = 0; la < n_late; ++la) {
              if (late_aggs[la].kind == ColAggKind::kCountDistinctInt64 ||
                  late_aggs[la].kind == ColAggKind::kCountDistinctDate ||
                  late_aggs[la].kind == ColAggKind::kCountDistinctDouble) {
                int_dist_late_idxs.push_back(la);
              }
            }
            const bool sharded_int_distinct =
                int_dist_late_idxs.size() == 1 && p2_blocks.size() > 16;
            const size_t int_dist_la =
                sharded_int_distinct ? int_dist_late_idxs[0] : SIZE_MAX;

            const size_t p2_workers = std::max<size_t>(
                1, std::min<size_t>({QueryParallelWorkerLimit(
                                         std::thread::hardware_concurrency()),
                                     std::max<size_t>(1, p2_blocks.size()),
                                     size_t{16}}));
            static constexpr size_t kDistShards = 64;
            struct WinDistPair {
              uint16_t win_id{0};
              int64_t dv{0};
            };
            std::vector<std::vector<std::vector<WinDistPair>>> dist_shards(
                sharded_int_distinct ? kDistShards : 0);
            for (auto& ds : dist_shards) {
              ds.resize(p2_workers);
            }

            std::vector<QueryMemoryCharge> p2_charges(p2_workers);
            std::vector<std::unique_ptr<PagedStringArena>> p2_arenas;
            p2_arenas.reserve(p2_workers);
            std::vector<std::vector<RuntimeAggState>> p2_worker_aggs(
                p2_workers);
            for (size_t w = 0; w < p2_workers; ++w) {
              p2_arenas.push_back(
                  std::make_unique<PagedStringArena>(&p2_charges[w]));
              p2_worker_aggs[w].resize(n_win * n_late);
            }
            std::vector<Status> p2_status(p2_workers, Status::kSuccess);
            std::atomic<size_t> next_p2{0};
            const bool single_direct_p0 =
                n_phys == 1 &&
                (km.phys_specs[0].kind == ColKeyKind::kColInt64 ||
                 km.phys_specs[0].kind == ColKeyKind::kColDate);
            const bool use_key_fence =
                n_phys > 1 && p0_int_prunable && !win_p0_has_null &&
                p2_blocks.size() >= 512 &&
                tlcs->DictionaryKind(km.phys_specs[0].full_col) !=
                    TlcsDictKind::kCompleteNarrow &&
                static_cast<__int128>(win_p0_max) - win_p0_min >= (1 << 20);
            std::array<uint64_t, 64> key_fence{};
            const auto fence_bit = [](int64_t value) {
              const auto bits = static_cast<uint64_t>(value);
              return (bits ^ (bits >> 32)) & 4095;
            };
            if (use_key_fence) {
              for (int64_t value : win_p0_values) {
                const uint64_t bit = fence_bit(value);
                key_fence[bit / 64] |= uint64_t{1} << (bit % 64);
              }
            }

            const bool p2_reads_text =
                std::ranges::any_of(late_aggs, [&](const ColAggSpec& spec) {
                  return spec.needs_bytes &&
                         table_schema.GetColumn(spec.full_col).Type() ==
                             ValueType::kVarChar;
                });
            auto p2_worker_fn = [&](size_t tid) {
              BlockDecodeScratch scratch;
              TlcsFile::DecodedBlock blk_extra;
              std::vector<uint8_t> io_extra;
              TimestampPrefixCache ts_cache;
              RegexReplaceCache regex_cache;
              std::string regex_scratch;
              std::vector<const TlcsFile::DecodedColumnChunk*> col_ptrs(
                  ncols, nullptr);
              std::vector<uint32_t> cand_rows;
              std::vector<RuntimeKeyCell> probe_keys(n_phys);
              std::vector<std::pair<uint32_t, uint32_t>> matched_wins;
              uint64_t local_scanned = 0;
              constexpr size_t kPrecheckSample = 16;
              size_t precheck_blocks = 0;
              size_t precheck_hits = 0;

              // 4096-slot L1 session dedup filter for sharded int distinct.
              static constexpr size_t kL1DistSlots = 4096;
              struct L1DistEntry {
                uint16_t win_plus_one{0};
                int64_t dv{0};
              };
              std::vector<L1DistEntry> l1_dist(
                  sharded_int_distinct ? kL1DistSlots : 0);

              while (true) {
                const size_t bi =
                    next_p2.fetch_add(1, std::memory_order_relaxed);
                if (bi >= p2_blocks.size()) {
                  break;
                }
                const uint32_t bidx = p2_blocks[bi];
                if (use_key_fence &&
                    (precheck_blocks < kPrecheckSample ||
                     precheck_hits * 4 < precheck_blocks * 3)) {
                  // Start with one column. Only blocks with a shortlisted
                  // leading value can need the other keys or late payloads.
                  const std::array columns{km.phys_specs[0].full_col};
                  auto& leading = scratch.blk_cond;
                  Status status = tlcs->DecodeBlockColumnsWithOptions(
                      bidx, columns, table_schema, &leading, true,
                      &scratch.io_cond, {});
                  if (!status.ok()) {
                    p2_status[tid] = std::move(status);
                    return;
                  }
                  ++precheck_blocks;
                  if (leading.row_count == 0) {
                    continue;
                  }
                  const auto& column = leading.columns[0];
                  bool hit = false;
                  for (uint32_t row = 0; row < leading.row_count; ++row) {
                    const int64_t value = column.int_cells[row];
                    const uint64_t bit = fence_bit(value);
                    if (((key_fence[bit / 64] >> (bit % 64)) & 1) != 0 &&
                        !column.IsNull(row) &&
                        std::ranges::binary_search(win_p0_values, value)) {
                      hit = true;
                      break;
                    }
                  }
                  if (!hit) {
                    continue;
                  }
                  ++precheck_hits;
                }
                const bool use_memo_rows = s1_memo.complete &&
                                           bidx < s1_memo.block_rows.size() &&
                                           !s1_memo.block_rows[bidx].empty();
                if (use_memo_rows) {
                  std::fill(col_ptrs.begin(), col_ptrs.end(), nullptr);
                  if (!p2_memo_key_cols.empty()) {
                    Status mst = tlcs->DecodeBlockColumnsWithOptions(
                        bidx, p2_memo_key_cols, table_schema, &scratch.blk1a,
                        true, &scratch.io1a, {});
                    if (mst != Status::kSuccess) {
                      p2_status[tid] = std::move(mst);
                      return;
                    }
                    for (size_t i = 0; i < p2_memo_key_cols.size(); ++i) {
                      col_ptrs[p2_memo_key_cols[i]] = &scratch.blk1a.columns[i];
                    }
                  }
                  const auto& saved = s1_memo.block_rows[bidx];
                  cand_rows.resize(saved.size());
                  for (size_t i = 0; i < saved.size(); ++i) {
                    cand_rows[i] = saved[i];
                  }
                } else {
                  auto dec_or = DecodeAndFilterBlock(
                      *tlcs, bidx, table_schema, preds, p2_sched, &scratch,
                      &col_ptrs, &cand_rows, &local_scanned);
                  if (!dec_or.HasValue()) {
                    p2_status[tid] = dec_or.GetStatus();
                    return;
                  }
                  if (!dec_or.Value()) {
                    continue;
                  }
                }

                matched_wins.clear();
                if (single_direct_p0) {
                  const auto* kch = col_ptrs[km.phys_specs[0].full_col];
                  const bool no_null = !kch->has_null;
                  for (const uint32_t r : cand_rows) {
                    const bool is_null = !no_null && kch->IsNull(r);
                    if (!is_null && !win_p0_has_null &&
                        (kch->int_cells[r] < win_p0_min ||
                         kch->int_cells[r] > win_p0_max)) {
                      continue;
                    }
                    probe_keys[0] =
                        is_null ? RuntimeKeyCell{.is_null = true}
                                : RuntimeKeyCell{.int_val = kch->int_cells[r]};
                    const uint64_t h = GroupKeySliceHasher::HashTyped(
                        probe_keys.data(), km.phys_specs);
                    size_t pos = static_cast<size_t>(h) & ht_mask;
                    while (win_ht[pos].win_plus_one != 0) {
                      const uint32_t w = win_ht[pos].win_plus_one - 1;
                      if (win_ht[pos].hash == h &&
                          GroupKeySliceHasher::Equals(
                              &win_phys_keys[w * n_phys], probe_keys.data(),
                              km.phys_specs)) {
                        matched_wins.emplace_back(r, w);
                        break;
                      }
                      pos = (pos + 1) & ht_mask;
                    }
                  }
                } else {
                  const auto* first_key =
                      use_key_fence ? col_ptrs[km.phys_specs[0].full_col]
                                    : nullptr;
                  for (const uint32_t r : cand_rows) {
                    if (first_key != nullptr) {
                      const uint64_t bit = fence_bit(first_key->int_cells[r]);
                      if (first_key->IsNull(r) ||
                          ((key_fence[bit / 64] >> (bit % 64)) & 1) == 0) {
                        continue;
                      }
                    }
                    Status kst = Status::kSuccess;
                    for (size_t p = 0; p < n_phys; ++p) {
                      if (!ExtractSingleKeyCell(
                              km.phys_specs[p], col_ptrs, r, &probe_keys[p],
                              &regex_scratch, &ts_cache, &kst, &regex_cache)) {
                        p2_status[tid] = std::move(kst);
                        return;
                      }
                    }
                    const uint64_t h = GroupKeySliceHasher::HashTyped(
                        probe_keys.data(), km.phys_specs);
                    size_t pos = static_cast<size_t>(h) & ht_mask;
                    while (win_ht[pos].win_plus_one != 0) {
                      const uint32_t w = win_ht[pos].win_plus_one - 1;
                      if (win_ht[pos].hash == h &&
                          GroupKeySliceHasher::Equals(
                              &win_phys_keys[w * n_phys], probe_keys.data(),
                              km.phys_specs)) {
                        matched_wins.emplace_back(r, w);
                        break;
                      }
                      pos = (pos + 1) & ht_mask;
                    }
                  }
                }

                if (matched_wins.empty()) {
                  continue;
                }

                const auto& need_late_cols =
                    use_memo_rows ? p2_memo_late_cols : p2_extra_cols;
                const auto& need_late_len =
                    use_memo_rows ? p2_memo_late_len_only : p2_extra_len_only;
                if (!need_late_cols.empty()) {
                  std::span<const uint32_t> text_rows;
                  if (p2_reads_text && matched_wins.size() <= 512) {
                    cand_rows.clear();
                    cand_rows.reserve(matched_wins.size());
                    for (const auto& match : matched_wins) {
                      cand_rows.push_back(match.first);
                    }
                    text_rows = cand_rows;
                  }
                  Status est = tlcs->DecodeBlockColumnsWithOptions(
                      bidx, need_late_cols, table_schema, &blk_extra, true,
                      &io_extra, need_late_len, false, text_rows);
                  if (est != Status::kSuccess) {
                    p2_status[tid] = std::move(est);
                    return;
                  }
                  for (size_t i = 0; i < need_late_cols.size(); ++i) {
                    col_ptrs[need_late_cols[i]] = &blk_extra.columns[i];
                  }
                }

                // Lean proofs:
                //   lean/TinylambOpt/ResearchProofs/AnalyticalOperators/FrequencyBoundsAndOrderedFetch.lean
                //     (OwnerDistinct_sound, OrderedCodeMin_sound)
                auto& my_aggs = p2_worker_aggs[tid];
                // OrderedCodeMin_sound: for dictionary-encoded VARCHAR
                // MIN/MAX late aggregates where the local dictionary is sorted
                // in binary string order, track only the extreme code per
                // winner in this block and decode/fold at most 1 row per
                // winner.
                std::vector<uint8_t> la_ordered_code_done(n_late, 0);
                for (size_t la = 0; la < n_late; ++la) {
                  const ColAggSpec& ls = late_aggs[la];
                  if (ls.kind != ColAggKind::kMinVarChar &&
                      ls.kind != ColAggKind::kMaxVarChar) {
                    continue;
                  }
                  const auto* vch = col_ptrs[ls.full_col];
                  if (vch == nullptr || !vch->dict_encoded ||
                      vch->dict == nullptr || vch->dict_codes.empty()) {
                    continue;
                  }
                  const size_t n_dict = vch->dict->StringEntryCount();
                  if (n_dict == 0 || n_dict > 255) {
                    continue;
                  }
                  bool sorted_dict = true;
                  for (size_t c = 1; c < n_dict; ++c) {
                    if (vch->dict->StringEntryAt(c - 1) >
                        vch->dict->StringEntryAt(c)) {
                      sorted_dict = false;
                      break;
                    }
                  }
                  if (!sorted_dict) {
                    continue;
                  }
                  const bool is_min = (ls.kind == ColAggKind::kMinVarChar);
                  std::vector<uint16_t> best_code(n_win, 0xFFFF);
                  std::vector<uint32_t> best_row(n_win, 0);
                  const uint8_t* __restrict codes = vch->dict_codes.data();
                  for (const auto& [r, w] : matched_wins) {
                    if (vch->IsNull(r)) {
                      continue;
                    }
                    const uint16_t code = codes[r];
                    if (best_code[w] == 0xFFFF ||
                        (is_min ? (code < best_code[w])
                                : (code > best_code[w]))) {
                      best_code[w] = code;
                      best_row[w] = r;
                    }
                  }
                  for (size_t w = 0; w < n_win; ++w) {
                    if (best_code[w] == 0xFFFF) {
                      continue;
                    }
                    Status fst = FoldRowIntoAggState(ls, col_ptrs, best_row[w],
                                                     my_aggs[(w * n_late) + la],
                                                     *p2_arenas[tid]);
                    if (fst != Status::kSuccess) {
                      p2_status[tid] = std::move(fst);
                      return;
                    }
                  }
                  la_ordered_code_done[la] = 1;
                }
                // Lean proof:
                //   lean/TinylambOpt/ResearchProofs/StorageExecution/BagKernelsA.lean
                //     (DictionaryAliases.DistinctOutput,
                //      DictionaryAliases.FilteredMoment)
                // For dictionary-encoded VARCHAR COUNT(DISTINCT) and
                // SUM/AVG(BYTE_LENGTH) late aggregates, accumulate per-winner
                // 256-bit code sets / multiplicities over matched rows and fold
                // each live dictionary code at most once per winner.
                for (size_t la = 0; la < n_late; ++la) {
                  if (la_ordered_code_done[la] != 0) {
                    continue;
                  }
                  const ColAggSpec& ls = late_aggs[la];
                  const auto* vch = col_ptrs[ls.full_col];
                  if (vch == nullptr || !vch->dict_encoded ||
                      vch->dict == nullptr || vch->dict_codes.empty()) {
                    continue;
                  }
                  const size_t n_dict = vch->dict->StringEntryCount();
                  if (n_dict == 0 || n_dict > 255) {
                    continue;
                  }
                  const uint8_t* __restrict codes = vch->dict_codes.data();
                  if (ls.kind == ColAggKind::kCountDistinctVarChar) {
                    std::vector<std::array<uint64_t, 4>> seen_codes(n_win);
                    for (const auto& [r, w] : matched_wins) {
                      if (vch->IsNull(r)) {
                        continue;
                      }
                      const uint8_t c = codes[r];
                      seen_codes[w][c >> 6U] |= (uint64_t{1} << (c & 63U));
                    }
                    for (size_t w = 0; w < n_win; ++w) {
                      RuntimeAggState& st = my_aggs[(w * n_late) + la];
                      for (size_t word = 0; word < 4; ++word) {
                        uint64_t bits = seen_codes[w][word];
                        while (bits != 0) {
                          const size_t c =
                              (word << 6U) +
                              static_cast<size_t>(std::countr_zero(bits));
                          bits &= (bits - 1);
                          if (c >= n_dict) {
                            continue;
                          }
                          if (st.distinct_strs == nullptr) {
                            st.distinct_strs =
                                std::make_unique<DistinctStringValues>();
                          }
                          const std::string_view sv =
                              vch->dict->StringEntryAt(c);
                          if (!st.distinct_strs->values.contains(sv)) {
                            if (!st.distinct_strs->charge.TryAdd(
                                    sizeof(std::string_view) + 24U)) {
                              p2_status[tid] = StatusError(
                                  StatusCode::kMemoryLimitExceeded,
                                  "columnar distinct set exceeded budget");
                              return;
                            }
                            auto interned = p2_arenas[tid]->Intern(sv);
                            if (!interned.HasValue()) {
                              p2_status[tid] = interned.GetStatus();
                              return;
                            }
                            st.distinct_strs->values.insert(interned.Value());
                            st.has_value = true;
                          }
                        }
                      }
                    }
                    la_ordered_code_done[la] = 1;
                  } else if ((ls.kind == ColAggKind::kSumByteLen ||
                              ls.kind == ColAggKind::kAvgByteLen) &&
                             n_win * n_dict <= 65536) {
                    std::vector<uint32_t> code_counts(n_win * n_dict, 0);
                    for (const auto& [r, w] : matched_wins) {
                      if (vch->IsNull(r)) {
                        continue;
                      }
                      const uint8_t c = codes[r];
                      if (c < n_dict) {
                        ++code_counts[(w * n_dict) + c];
                      }
                    }
                    for (size_t w = 0; w < n_win; ++w) {
                      RuntimeAggState& st = my_aggs[(w * n_late) + la];
                      const uint32_t* wc = &code_counts[w * n_dict];
                      for (size_t c = 0; c < n_dict; ++c) {
                        if (wc[c] == 0) {
                          continue;
                        }
                        const uint64_t cnt = wc[c];
                        const uint64_t len =
                            vch->dict->StringEntryAt(c).size();
                        if (ls.kind == ColAggKind::kSumByteLen) {
                          const __int128 prod = static_cast<__int128>(cnt) *
                                                static_cast<__int128>(len);
                          const __int128 next =
                              static_cast<__int128>(st.int_val) + prod;
                          if (next > std::numeric_limits<int64_t>::max()) {
                            p2_status[tid] = StatusError(
                                StatusCode::kIsInfinity,
                                "integer overflow in SUM");
                            return;
                          }
                          st.int_val = static_cast<int64_t>(next);
                        } else {
                          st.double_val +=
                              static_cast<double>(cnt) * static_cast<double>(len);
                        }
                        st.count += static_cast<int64_t>(cnt);
                        st.has_value = true;
                      }
                    }
                    la_ordered_code_done[la] = 1;
                  }
                }
                for (const auto& [r, w] : matched_wins) {
                  RuntimeAggState* base_st = &my_aggs[w * n_late];
                  for (size_t la = 0; la < n_late; ++la) {
                    if (la_ordered_code_done[la] != 0) {
                      continue;
                    }
                    if (la == int_dist_la) {
                      const ColAggSpec& ds = late_aggs[la];
                      const auto* dch = col_ptrs[ds.full_col];
                      if (dch->IsNull(r)) {
                        continue;
                      }
                      int64_t dv = dch->int_cells[r];
                      if (ds.kind == ColAggKind::kCountDistinctDouble) {
                        const auto d = std::bit_cast<double>(dv);
                        if (d == 0.0) {
                          dv = 0;
                        } else if (std::isnan(d)) {
                          dv = int64_t{0x7ff8000000000000LL};
                        }
                      }
                      const uint64_t dh = MixHash64(
                          static_cast<uint64_t>(dv) ^
                          (static_cast<uint64_t>(w) * 0x9e3779b97f4a7c15ULL));
                      L1DistEntry& le =
                          l1_dist[static_cast<size_t>(dh) & (kL1DistSlots - 1)];
                      if (le.win_plus_one == w + 1 && le.dv == dv) {
                        continue;
                      }
                      le = {.win_plus_one = static_cast<uint16_t>(w + 1),
                            .dv = dv};
                      const size_t s =
                          static_cast<size_t>(dh) & (kDistShards - 1);
                      auto& vec = dist_shards[s][tid];
                      if (vec.size() == vec.capacity()) {
                        const size_t grow =
                            std::max<size_t>(256, vec.capacity()) *
                            sizeof(WinDistPair);
                        if (!p2_charges[tid].TryAdd(grow)) {
                          p2_status[tid] = StatusError(
                              StatusCode::kMemoryLimitExceeded,
                              "two-stage distinct shard exceeded budget");
                          return;
                        }
                        vec.reserve(vec.capacity() +
                                    std::max<size_t>(256, vec.capacity()));
                      }
                      vec.push_back(
                          {.win_id = static_cast<uint16_t>(w), .dv = dv});
                      continue;
                    }
                    Status fst =
                        FoldRowIntoAggState(late_aggs[la], col_ptrs, r,
                                            base_st[la], *p2_arenas[tid]);
                    if (fst != Status::kSuccess) {
                      p2_status[tid] = std::move(fst);
                      return;
                    }
                  }
                }
              }
            };

            if (p2_workers == 1) {
              p2_worker_fn(0);
            } else {
              RETURN_IF_FAIL(RunParallelTasks(p2_workers, p2_worker_fn));
            }
            bool p2_mem_exceeded = false;
            for (const Status& st : p2_status) {
              if (st == StatusCode::kInvalidArgument ||
                  st == StatusCode::kMemoryLimitExceeded) {
                p2_mem_exceeded = true;
                break;
              }
              RETURN_IF_FAIL(st);
            }

            if (!p2_mem_exceeded) {
              QueryMemoryCharge merge_charge;
              PagedStringArena merge_arena(&merge_charge);
              std::vector<RuntimeAggState> merged_late(n_win * n_late);
              bool merge_mem_exceeded = false;
              for (size_t w_tid = 0; w_tid < p2_workers && !merge_mem_exceeded;
                   ++w_tid) {
                for (size_t w = 0; w < n_win; ++w) {
                  for (size_t la = 0; la < n_late; ++la) {
                    if (la == int_dist_la) {
                      continue;
                    }
                    Status mst = MergeAggState(
                        late_aggs[la], merged_late[(w * n_late) + la],
                        p2_worker_aggs[w_tid][(w * n_late) + la], merge_arena);
                    if (mst == StatusCode::kMemoryLimitExceeded) {
                      merge_mem_exceeded = true;
                      break;
                    }
                    RETURN_IF_FAIL(mst);
                  }
                }
              }

              std::vector<uint64_t> sharded_dist_counts(n_win, 0);
              if (!merge_mem_exceeded && sharded_int_distinct) {
                const size_t dworkers = std::min(p2_workers, kDistShards);
                std::vector<std::vector<uint64_t>> thread_dcounts(
                    dworkers, std::vector<uint64_t>(n_win, 0));
                std::vector<Status> dstatus(dworkers, Status::kSuccess);
                std::atomic<size_t> next_ds{0};
                RETURN_IF_FAIL(RunParallelTasks(dworkers, [&](size_t dtid) {
                  QueryMemoryCharge set_charge;
                  FlatIntKeyMap<uint8_t> dset;
                  while (true) {
                    const size_t s =
                        next_ds.fetch_add(1, std::memory_order_relaxed);
                    if (s >= kDistShards) {
                      break;
                    }
                    size_t total_pairs = 0;
                    for (size_t w = 0; w < p2_workers; ++w) {
                      total_pairs += dist_shards[s][w].size();
                    }
                    if (total_pairs == 0) {
                      continue;
                    }
                    dset.Clear();
                    for (size_t w = 0; w < p2_workers; ++w) {
                      for (const WinDistPair& pr : dist_shards[s][w]) {
                        // Pack `(win_id <= 255)` + 64-bit `dv` via secondary
                        // per-win bitmask inside `FlatIntKeyMap<uint8_t>` when
                        // `n_win <= 8`, or hash-mixed `(win_id, dv)` when `dv`
                        // fits 56 bits; use exact `unordered_set` fallback
                        // when 64-bit full width with `n_win > 1` is needed.
                        if (n_win <= 8) {
                          uint8_t* ptr = dset.TryEmplace(pr.dv);
                          const auto bit =
                              static_cast<uint8_t>(1U << pr.win_id);
                          if ((*ptr & bit) == 0U) {
                            *ptr |= bit;
                            ++thread_dcounts[dtid][pr.win_id];
                          }
                        }
                      }
                    }
                    if (n_win > 8) {
                      struct PairKey {
                        uint16_t w;
                        int64_t v;
                        bool operator==(const PairKey& o) const noexcept {
                          return w == o.w && v == o.v;
                        }
                      };
                      struct PairHash {
                        size_t operator()(const PairKey& k) const noexcept {
                          return static_cast<size_t>(
                              MixHash64(static_cast<uint64_t>(k.v) ^
                                        (static_cast<uint64_t>(k.w) *
                                         0x9e3779b97f4a7c15ULL)));
                        }
                      };
                      std::unordered_set<PairKey, PairHash> exact_set;
                      exact_set.reserve(total_pairs);
                      for (size_t w = 0; w < p2_workers; ++w) {
                        for (const WinDistPair& pr : dist_shards[s][w]) {
                          if (exact_set.insert({pr.win_id, pr.dv}).second) {
                            ++thread_dcounts[dtid][pr.win_id];
                          }
                        }
                      }
                    }
                    dist_shards[s].clear();
                  }
                }));
                for (size_t dtid = 0; dtid < dworkers; ++dtid) {
                  for (size_t w = 0; w < n_win; ++w) {
                    sharded_dist_counts[w] += thread_dcounts[dtid][w];
                  }
                }
              }

              if (!merge_mem_exceeded) {
                if (publish_lstable_marker && runtime != nullptr) {
                  auto marker = std::make_shared<Relation>(runtime);
                  marker->schema = qualified_schema;
                  runtime->base_relations.emplace(fold_cache_key,
                                                  std::move(marker));
                }
                Relation out(runtime);
                out.rows.reserve(n_win);
                std::vector<Column> out_cols;
                out_cols.reserve(stmt.SelectList().size());
                for (size_t i = 0; i < stmt.SelectList().size(); ++i) {
                  ValueType vt = ValueType::kInt64;
                  if (stmt.SelectList()[i].expression) {
                    auto rt = stmt.SelectList()[i].expression->ResultType(
                        qualified_schema);
                    vt = rt.GetType() == TypeTag::kDouble ? ValueType::kDouble
                         : rt.GetType() == TypeTag::kDate ? ValueType::kDate
                         : rt.GetType() == TypeTag::kVarChar
                             ? ValueType::kVarChar
                             : ValueType::kInt64;
                  }
                  out_cols.emplace_back(ProjectionName(stmt.SelectList()[i], i),
                                        vt);
                }
                std::vector<uint8_t> type_refined(out_cols.size(), 0);
                std::vector<Value> full_finished(agg_specs.size());
                std::vector<Value> cached_keys(key_specs.size());
                struct BuiltStage2Row {
                  std::vector<Value> ord_vals;
                  Row emit_row;
                };
                std::vector<BuiltStage2Row> built_rows;
                built_rows.reserve(n_win);
                for (size_t w = 0; w < n_win; ++w) {
                  for (size_t a = 0; a < agg_specs.size(); ++a) {
                    if (orig_to_early[a] != SIZE_MAX) {
                      full_finished[a] =
                          s1_rel.rows[w][key_specs.size() + orig_to_early[a]];
                    }
                  }
                  for (size_t la = 0; la < n_late; ++la) {
                    const size_t orig_a = late_orig_idxs[la];
                    if (la == int_dist_la) {
                      full_finished[orig_a] =
                          Value(static_cast<int64_t>(sharded_dist_counts[w]));
                    } else {
                      ASSIGN_OR_RETURN(
                          Value, finished_value,
                          (FinishAggState(late_aggs[la],
                                          merged_late[(w * n_late) + la])));
                      full_finished[orig_a] = std::move(finished_value);
                    }
                  }
                  std::fill(cached_keys.begin(), cached_keys.end(), Value());
                  for (size_t fk = 0; fk < key_specs.size(); ++fk) {
                    cached_keys[fk] = s1_rel.rows[w][fk];
                  }
                  std::vector<Value> ord_vals;
                  if (defer_order_to_s2 && !order_bindings.empty()) {
                    ord_vals.reserve(order_bindings.size());
                    for (const auto& ob : order_bindings) {
                      ASSIGN_OR_RETURN(Value, ov,
                                       EvaluateBindingValue(
                                           ob.binding, key_specs,
                                           &win_full_keys[w * key_specs.size()],
                                           full_finished, &cached_keys));
                      ord_vals.push_back(std::move(ov));
                    }
                  }
                  std::vector<Value> row_vals;
                  row_vals.reserve(emit_bindings.size());
                  for (size_t i = 0; i < emit_bindings.size(); ++i) {
                    ASSIGN_OR_RETURN(Value, v,
                                     EvaluateBindingValue(
                                         emit_bindings[i], key_specs,
                                         &win_full_keys[w * key_specs.size()],
                                         full_finished, &cached_keys));
                    if (type_refined[i] == 0U && !v.IsNull()) {
                      out_cols[i] = Column(out_cols[i].Name(), ValueTypeOf(v));
                      out_cols[i].SetUnsigned(v.IsUnsigned());
                      type_refined[i] = 1;
                    }
                    row_vals.push_back(std::move(v));
                  }
                  built_rows.push_back(
                      BuiltStage2Row{.ord_vals = std::move(ord_vals),
                                     .emit_row = Row(std::move(row_vals))});
                }
                if (defer_order_to_s2 && !order_bindings.empty()) {
                  std::stable_sort(
                      built_rows.begin(), built_rows.end(),
                      [&](const BuiltStage2Row& a, const BuiltStage2Row& b) {
                        for (size_t k = 0; k < order_bindings.size(); ++k) {
                          const int c = order_bindings[k].order.Compare(
                              a.ord_vals[k], b.ord_vals[k]);
                          if (c != 0) {
                            return c < 0;
                          }
                        }
                        return false;
                      });
                }
                const size_t emit_limit =
                    (defer_order_to_s2 && stmt.HasLimit())
                        ? std::min(built_rows.size(), outer_topk_cap)
                        : built_rows.size();
                for (size_t i = 0; i < emit_limit; ++i) {
                  out.AddRow(std::move(built_rows[i].emit_row));
                }
                out.schema = Schema("", std::move(out_cols));
                if (dbg) {
                  (void)std::fprintf(stderr,
                                     "[TLCS-AGG-OK] two-stage-topn winners=%zu "
                                     "p2_blocks=%zu/%zu "
                                     "total=%.2fms\n",
                                     n_win, p2_blocks.size(),
                                     candidate_blocks.size(),
                                     ColumnarMsSince(scan_begin));
                }
                if (runtime != nullptr) {
                  runtime->typed_parallel_aggregations += 1;
                  runtime->aggregate_groups += n_win;
                }
                return std::optional<Relation>(std::move(out));
              }
            }
          }
        }
      }
    }
  }

  return run_classified_plan(stmt, agg_specs, positional_having, emit_bindings,
                             order_bindings, stmt.SelectList(), runtime,
                             publish_lstable_marker);
}

}  // namespace tinylamb::relational_detail
