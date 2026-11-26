import TinylambOpt.ResearchGoals.JitKernels.BorrowMasks
import TinylambOpt.ResearchProofs.JitKernels.BorrowMasksLemmas

/-! XOR with a replicated needle turns equal lanes into zero bytes. Borrow
propagation can add later marks but never moves the first mark, so one word
search locates the first literal byte without per-lane comparisons. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace Nov26.Proof_BorrowMasks_NeedleXorFirstMatch

open TinylambOpt.ResearchGoals.JitKernels.BorrowMasks
open TinylambOpt.ResearchGoals.JitKernels.BorrowMasks.NeedleXorFirstMatch
open Nov26.BorrowMasksLemmas

theorem sound : Goal := by
  intro x b
  let y := x ^^^ repeatByte b
  have hfirst : (marked (fast y)).head? = (marked (TinylambOpt.ResearchGoals.SwarDigitBallot.zeroBytes y)).head? := by
    rw [marked_head_eq_ctz (fast y) (fast_high_only y),
        marked_head_eq_ctz (TinylambOpt.ResearchGoals.SwarDigitBallot.zeroBytes y) (zeroBytes_high_only y)]
    by_cases h0 : fast y = 0
    · have hz : TinylambOpt.ResearchGoals.SwarDigitBallot.zeroBytes y = 0 := (fast_zero_iff y).mp h0
      simp [h0, hz]
    · have hz : TinylambOpt.ResearchGoals.SwarDigitBallot.zeroBytes y ≠ 0 := by
        intro hz
        exact h0 ((fast_zero_iff y).mpr hz)
      rw [ite_eq_right h0, ite_eq_right hz, fast_ctz_eq y]
  rw [show x ^^^ repeatByte b = y by rfl, hfirst]
  rw [marked_zeroBytes_xor x b]
  exact List.head?_filter

end Nov26.Proof_BorrowMasks_NeedleXorFirstMatch
