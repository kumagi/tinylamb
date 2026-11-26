import TinylambOpt.ResearchGoals.JitKernels.BorrowMasks
import TinylambOpt.ResearchProofs.JitKernels.BorrowMasksLemmas

/-! The cheap subtract-and-mask word test is empty exactly when the exact
zero-byte mask is empty. A SQL literal scan may therefore take the word-wide
all-miss branch without changing the modeled miss result. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_BorrowMasks_BorrowAnyByteHit

open TinylambOpt.ResearchGoals.JitKernels.BorrowMasks
open Nov26.BorrowMasksLemmas

theorem sound : TinylambOpt.ResearchGoals.JitKernels.BorrowMasks.BorrowAnyByteHit.Goal := by
  intro x
  by_cases h0 : fast x = 0
  · have hz : TinylambOpt.ResearchGoals.SwarDigitBallot.zeroBytes x = 0 := (fast_zero_iff x).mp h0
    simp [h0, hz]
  · have hz : TinylambOpt.ResearchGoals.SwarDigitBallot.zeroBytes x ≠ 0 := by
      intro hz
      exact h0 ((fast_zero_iff x).mpr hz)
    have hf : (fast x == 0) = false := by
      change decide (fast x = 0) = false
      exact decide_eq_false h0
    have hz' : (TinylambOpt.ResearchGoals.SwarDigitBallot.zeroBytes x == 0) = false := by
      change decide (TinylambOpt.ResearchGoals.SwarDigitBallot.zeroBytes x = 0) = false
      exact decide_eq_false hz
    rw [hf, hz']

end TinylambOpt.ResearchProofs.JitKernels.Goal_BorrowMasks_BorrowAnyByteHit
