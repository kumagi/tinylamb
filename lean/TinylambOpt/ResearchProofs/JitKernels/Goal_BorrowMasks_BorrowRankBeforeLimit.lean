import TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

/-! The subtract mask is refuted as a ballot: a zero byte propagates a borrow
    into the following byte, creating an extra marked lane. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_BorrowMasks_BorrowRankBeforeLimit

open TinylambOpt.ResearchGoals.JitKernels.BorrowMasks
open TinylambOpt.ResearchGoals.SwarDigitBallot

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.BorrowMasks.BorrowRankBeforeLimit.Goal := by
  intro h
  let x : W := 0x0100
  have hh := h x 2 (by omega)
  have hne : (((marked (fast x)).filter (fun i => i < 2)).length ≠
      ((marked (zeroBytes x)).filter (fun i => i < 2)).length) := by native_decide
  exact hne hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_BorrowMasks_BorrowRankBeforeLimit
