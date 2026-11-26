import TinylambOpt.ResearchGoals.JitKernels.BorrowMasks
import TinylambOpt.ResearchProofs.JitKernels.BorrowMasksLemmas

/-! A borrow can only survive into a lane above an actual zero byte, so both
masks agree on whether any marked lane lies before a short-tail boundary. The
no-hit branch may ignore all bits beyond that live prefix. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace Nov26.Proof_BorrowMasks_ActivePrefixMissProof

open TinylambOpt.ResearchGoals.JitKernels.BorrowMasks
open TinylambOpt.ResearchGoals.JitKernels.BorrowMasks.ActivePrefixMissProof
open Nov26.BorrowMasksLemmas

theorem sound : Goal := by
  intro x n
  rw [marked_any_lt_eq_mask_all (fast x) n,
      marked_any_lt_eq_mask_all (TinylambOpt.ResearchGoals.SwarDigitBallot.zeroBytes x) n]
  exact prefix_fast_any_eq x (min n 8) (by omega)

end Nov26.Proof_BorrowMasks_ActivePrefixMissProof
