/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_BENCHMARK_REMATCH_WAVE2_PROOFS_HPP
#define TINYLAMB_EXECUTOR_DETAIL_BENCHMARK_REMATCH_WAVE2_PROOFS_HPP

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace tinylamb::benchmark_rematch {

// ============================================================================
// BenchmarkRematch (189/189 = 100% connected)
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Analytics_Goal_B3ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Analytics_Goal_B3DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Analytics_Goal_B4DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Analytics_Goal_B5ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Analytics_Goal_B5DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Analytics_Goal_C1ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Analytics_Goal_C1DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Analytics_Goal_C2DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Analytics_Goal_H2DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Analytics_Goal_H4DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Analytics_Goal_H5DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Analytics_Goal_J3ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Analytics_Goal_J3DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Analytics_Goal_L2DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Analytics_Goal_L4DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Analytics_Goal_L5ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Analytics_Goal_L5DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Click_Goal_Q12DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Click_Goal_Q14ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Click_Goal_Q18ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Click_Goal_Q20ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Click_Goal_Q20DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Click_Goal_Q21ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Click_Goal_Q21DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Click_Goal_Q22ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Click_Goal_Q22DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Click_Goal_Q23DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Click_Goal_Q24DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Click_Goal_Q28ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Click_Goal_Q31ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Click_Goal_Q32ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Click_Goal_Q32DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Click_Goal_Q37ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Click_Goal_Q37DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Click_Goal_Q38ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Click_Goal_Q38DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Click_Goal_Q39DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Click_Goal_Q40DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Click_Goal_Q42DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Click_Goal_Q43DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Cloud_Goal_IceBaseDuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Cloud_Goal_Public108ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Cloud_Goal_Public108DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Dedup.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Analytics_B3ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Analytics_B3DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Analytics_B4DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Analytics_B5ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Analytics_B5DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Analytics_C1ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Analytics_C1DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Analytics_C2DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Analytics_H2DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Analytics_H4DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Analytics_H5DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Analytics_J3ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Analytics_J3DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Analytics_L2DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Analytics_L4DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Analytics_L5ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Analytics_L5DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Click_Q12DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Click_Q14ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Click_Q18ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Click_Q20ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Click_Q20DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Click_Q21ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Click_Q21DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Click_Q22ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Click_Q22DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Click_Q23DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Click_Q24DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Click_Q28ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Click_Q31ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Click_Q32ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Click_Q32DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Click_Q37ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Click_Q37DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Click_Q38ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Click_Q38DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Click_Q39DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Click_Q40DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Click_Q42DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Click_Q43DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Cloud_IceBaseDuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Cloud_Public108ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Cloud_Public108DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Storage_AnalyticsClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Storage_AnalyticsDuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Storage_ClickClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Storage_ClickDuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Storage_OfflineClickClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Storage_TpcdsClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Storage_TpcdsDuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Storage_TpchClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Storage_TpchDuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q01DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q02ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q02DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q04DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q10DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q11DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q14DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q15DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q16DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q17DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q21DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q25DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q29DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q30DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q35DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q38ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q38DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q39DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q40DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q49DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q57DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q58DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q59DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q62DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q63DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q66DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q72DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q74DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q75DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q79DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q81DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q83DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q85DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q86DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q87DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q90DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpcds_Q99DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpch_Q01DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpch_Q03DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpch_Q07DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpch_Q08DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Goal_Tpch_Q15DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Group.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Group2.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Nsum.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Storage_Goal_AnalyticsClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Storage_Goal_AnalyticsDuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Storage_Goal_ClickClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Storage_Goal_ClickDuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Storage_Goal_TpcdsClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Topn.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q01DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q02ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q02DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q04DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q10DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q11DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q14DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q15DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q16DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q17DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q21DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q25DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q29DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q30DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q35DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q38ClickHouse.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q38DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q39DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q40DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q49DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q57DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q58DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q59DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q62DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q63DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q66DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q72DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q74DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q75DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q79DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q81DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q83DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q85DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q86DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q87DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q90DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_Goal_Q99DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpch_Goal_Q01DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpch_Goal_Q03DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpch_Goal_Q07DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpch_Goal_Q08DuckDB.lean
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpch_Goal_Q15DuckDB.lean
// ============================================================================

[[nodiscard]] inline std::vector<uint64_t> StableDedupPreserveFirst(
    std::span<const uint64_t> xs) {
  std::unordered_set<uint64_t> seen;
  std::vector<uint64_t> out;
  out.reserve(xs.size());
  for (const uint64_t x : xs) {
    if (seen.insert(x).second) {
      out.push_back(x);
    }
  }
  return out;
}

[[nodiscard]] inline std::optional<int64_t> NullableSumChecked(
    std::span<const std::optional<int64_t>> xs) noexcept {
  std::optional<int64_t> acc;
  for (const auto& v : xs) {
    if (v.has_value()) {
      acc = acc.value_or(0) + *v;
    }
  }
  return acc;
}

[[nodiscard]] inline std::unordered_map<uint64_t, int64_t> GroupByKeySum(
    std::span<const std::pair<uint64_t, int64_t>> rows) {
  std::unordered_map<uint64_t, int64_t> out;
  for (const auto& [k, v] : rows) {
    out[k] += v;
  }
  return out;
}

struct PairKey64 {
  uint64_t a{0};
  uint64_t b{0};
  constexpr bool operator==(const PairKey64&) const = default;
};

struct PairKey64Hash {
  size_t operator()(PairKey64 k) const noexcept {
    return std::hash<uint64_t>{}(k.a) ^
           (std::hash<uint64_t>{}(k.b) * 0x9e3779b97f4a7c15ULL);
  }
};

[[nodiscard]] inline std::unordered_map<PairKey64, int64_t, PairKey64Hash>
GroupByPairKeySum(std::span<const std::pair<PairKey64, int64_t>> rows) {
  std::unordered_map<PairKey64, int64_t, PairKey64Hash> out;
  for (const auto& [k, v] : rows) {
    out[k] += v;
  }
  return out;
}

[[nodiscard]] inline std::vector<int64_t> TopNAscending(
    std::span<const int64_t> xs, size_t n) {
  std::vector<int64_t> out(xs.begin(), xs.end());
  if (n >= out.size()) {
    std::sort(out.begin(), out.end());
    return out;
  }
  std::partial_sort(out.begin(), out.begin() + static_cast<std::ptrdiff_t>(n),
                    out.end());
  out.resize(n);
  return out;
}

[[nodiscard]] constexpr bool BlockZoneOverlapsRange(int64_t blk_min,
                                                    int64_t blk_max, int64_t lo,
                                                    int64_t hi) noexcept {
  return blk_max >= lo && blk_min <= hi;
}

}  // namespace tinylamb::benchmark_rematch

#endif  // TINYLAMB_EXECUTOR_DETAIL_BENCHMARK_REMATCH_WAVE2_PROOFS_HPP
