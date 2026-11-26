import TinylambOpt.ResearchGoals.JitKernels.BorrowMasks
import TinylambOpt.ResearchProofs.JitKernels.BorrowMasksLemmas

/-!
Replicated-needle XOR maps equal lanes to zero bytes, and the borrow mask's
first marked lane is the first zero byte. This allows SQL literal search to
replace eight scalar equality checks with one word-wide XOR and mask scan while
preserving the reported first match.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_BorrowMasks_NeedleXorFirstMatch

open TinylambOpt.ResearchGoals.SwarDigitBallot
open TinylambOpt.ResearchGoals.JitKernels.BorrowMasks
open Nov26.BorrowMasksLemmas

set_option maxHeartbeats 0
set_option maxRecDepth 10000

theorem sound : TinylambOpt.ResearchGoals.JitKernels.BorrowMasks.NeedleXorFirstMatch.Goal := by
  intro x b
  let y : W := x ^^^ repeatByte b
  have hfirst : (marked (fast y)).head? =
      (marked (TinylambOpt.ResearchGoals.SwarDigitBallot.zeroBytes y)).head? := by
    rw [marked_head_eq_ctz (fast y) (fast_high_only y),
      marked_head_eq_ctz (TinylambOpt.ResearchGoals.SwarDigitBallot.zeroBytes y)
        (zeroBytes_high_only y)]
    by_cases h0 : fast y = 0
    · have hz : TinylambOpt.ResearchGoals.SwarDigitBallot.zeroBytes y = 0 :=
        (fast_zero_iff y).mp h0
      simp [h0, hz]
    · have hz : TinylambOpt.ResearchGoals.SwarDigitBallot.zeroBytes y ≠ 0 := by
        intro hz
        exact h0 ((fast_zero_iff y).mpr hz)
      rw [ite_eq_right h0, ite_eq_right hz, fast_ctz_eq y]
  rw [show x ^^^ repeatByte b = y by rfl, hfirst]
  rw [marked_zeroBytes_xor x b]
  exact List.head?_filter

end TinylambOpt.ResearchProofs.JitKernels.Goal_BorrowMasks_NeedleXorFirstMatch
