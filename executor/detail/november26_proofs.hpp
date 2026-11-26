/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_NOVEMBER26_PROOFS_HPP
#define TINYLAMB_EXECUTOR_DETAIL_NOVEMBER26_PROOFS_HPP

#include "executor/detail/query_rewrite_analytics.hpp"
#include "executor/detail/query_rewrite_clickbench.hpp"
#include "executor/detail/query_rewrite_tpcds.hpp"
#include "executor/detail/query_rewrite_tpch.hpp"

namespace tinylamb::benchmark_rematch {

// ============================================================================
// November26 (83/83 = 100% connected via query_rewrite_{tpch,tpcds,clickbench,
// analytics}.hpp)
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/November26/Proof_Analytics_B3.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Analytics_B4.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Analytics_B5.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Analytics_C1.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Analytics_C2.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Analytics_H2.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Analytics_H4.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Analytics_H5.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Analytics_J1.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Analytics_J2.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Analytics_J3.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Analytics_J5.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Analytics_L2.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Analytics_L3.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Analytics_L4.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Analytics_L5.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Click_Q12.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Click_Q14.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Click_Q16.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Click_Q18.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Click_Q20.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Click_Q21.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Click_Q22.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Click_Q23.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Click_Q24.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Click_Q27.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Click_Q28.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Click_Q31.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Click_Q32.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Click_Q36.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Click_Q37.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Click_Q38.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Click_Q39.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Click_Q40.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Click_Q42.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Click_Q43.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Cloud_IceBase.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Cloud_Public108.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q02.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q04.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q11.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q14.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q15.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q16.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q17.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q21.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q25.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q29.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q30.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q35.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q38.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q39.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q40.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q41.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q45.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q49.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q57.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q58.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q59.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q62.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q63.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q66.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q72.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q74.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q75.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q79.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q80.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q81.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q83.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q85.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q86.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q87.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q89.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q90.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpcds_Q99.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpch_Q01.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpch_Q03.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpch_Q06.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpch_Q07.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpch_Q08.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpch_Q10.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpch_Q19.lean
// lean/TinylambOpt/ResearchProofs/November26/Proof_Tpch_Q20.lean
// ============================================================================

inline constexpr size_t kNovember26ConnectedProofCount = 83;

}  // namespace tinylamb::benchmark_rematch

#endif  // TINYLAMB_EXECUTOR_DETAIL_NOVEMBER26_PROOFS_HPP
