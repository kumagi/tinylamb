import TinylambOpt.ResearchGoals.JitKernels.BailoutPackets
import TinylambOpt.ResearchProofs.JitKernels.BailoutPacketsLemmas

/-! When a vector packet contains no fault, its output prefix is complete and
can be committed before replaying the next packet. NULL lanes are dropped by the
output filter in both the split and joined forms. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace Nov26.Proof_BailoutPackets_WordOfFaultsReplayPrefix

open TinylambOpt.ResearchGoals.JitKernels.BailoutPackets
open TinylambOpt.ResearchGoals.JitKernels.BailoutPackets.WordOfFaultsReplayPrefix
open Nov26.BailoutPacketsLemmas

theorem sound : Goal := by
  intro a b hnone
  change firstAt a 0 = none at hnone
  have hno : ∀ c ∈ a, bad c = false := first_none_no_bad a 0 hnone
  exact prefixOut_append_of_nobad a b hno

end Nov26.Proof_BailoutPackets_WordOfFaultsReplayPrefix
