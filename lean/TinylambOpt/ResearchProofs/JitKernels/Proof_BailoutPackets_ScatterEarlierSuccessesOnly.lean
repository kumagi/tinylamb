import TinylambOpt.ResearchGoals.JitKernels.BailoutPackets
import TinylambOpt.ResearchProofs.JitKernels.BailoutPacketsLemmas

/-! The output prefix ends exactly at the first fault, so committing the
materialized values of the preceding physical prefix preserves the compacted
output. Later rows are left for replay. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace Nov26.Proof_BailoutPackets_ScatterEarlierSuccessesOnly

open TinylambOpt.ResearchGoals.JitKernels.BailoutPackets
open TinylambOpt.ResearchGoals.JitKernels.BailoutPackets.ScatterEarlierSuccessesOnly
open Nov26.BailoutPacketsLemmas

theorem sound : Goal := by
  intro xs i e h
  exact prefixOut_eq_take xs i e h

end Nov26.Proof_BailoutPackets_ScatterEarlierSuccessesOnly
