import TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

/-
Refutes `BorrowRankBeforeLimit`: borrow-contaminated lanes inflate the
popcount of the subtract mask before a LIMIT boundary. The witness `0x0100`
with `n = 2` counts two subtract-mask lanes but one zero byte.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.BorrowMasks.Goal_BorrowRankBeforeLimit

open TinylambOpt.ResearchGoals.SwarDigitBallot
open TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

theorem refute : ¬ BorrowRankBeforeLimit.Goal := by
  intro h
  have heq := h 0x0202020202020100 2 (by omega)
  have hf : marked (fast 0x0202020202020100) = [0,1] := by native_decide
  have hz : marked (zeroBytes 0x0202020202020100) = [0] := by native_decide
  rw [hf, hz] at heq
  contradiction

end TinylambOpt.ResearchProofs.JitKernels.BorrowMasks.Goal_BorrowRankBeforeLimit
