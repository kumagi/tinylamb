import TinylambOpt.ResearchGoals.JitKernels.BorrowMasks
import TinylambOpt.ResearchProofs.JitKernels.BorrowMasksLemmas

/-! Extra lane marks produced by borrow propagation always follow a true zero
byte. The two masks therefore have the same first marked lane, so execution may
use either mask to choose the next literal row. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace Nov26.Proof_BorrowMasks_BorrowEarliestHit

open TinylambOpt.ResearchGoals.JitKernels.BorrowMasks
open TinylambOpt.ResearchGoals.JitKernels.BorrowMasks.BorrowEarliestHit
open Nov26.BorrowMasksLemmas

theorem sound : Goal := by
  intro x
  rw [marked_head_eq_ctz (fast x) (fast_high_only x),
      marked_head_eq_ctz (TinylambOpt.ResearchGoals.SwarDigitBallot.zeroBytes x) (zeroBytes_high_only x)]
  by_cases h0 : fast x = 0
  · have hz : TinylambOpt.ResearchGoals.SwarDigitBallot.zeroBytes x = 0 := (fast_zero_iff x).mp h0
    simp [h0, hz]
  · have hz : TinylambOpt.ResearchGoals.SwarDigitBallot.zeroBytes x ≠ 0 := by
      intro hz
      exact h0 ((fast_zero_iff x).mpr hz)
    rw [ite_eq_right h0, ite_eq_right hz, fast_ctz_eq x]

end Nov26.Proof_BorrowMasks_BorrowEarliestHit
