import TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

/-
Refutes `AdjacentDelimiterIntersection`: a borrow-contaminated lane can be
intersected into a false two-byte literal match. For `0x0100` with both needles
`0`, lane 0 passes both subtract-mask tests although the second byte is `0x01`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.BorrowMasks.Goal_AdjacentDelimiterIntersection

open TinylambOpt.ResearchGoals.SwarDigitBallot
open TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

theorem refute : ¬ AdjacentDelimiterIntersection.Goal := by
  intro h
  have heq := h 0x0100 0 0
  have hl : ((List.range 7).filter (fun i =>
      (marked (fast (0x0100 ^^^ repeatByte (0 : BitVec 8)))).contains i &&
      (marked (fast (0x0100 ^^^ repeatByte (0 : BitVec 8)))).contains (i+1))) = [0,1,2,3,4,5,6] := by native_decide
  have hr : (List.range 7).filter (fun i => lane 0x0100 i == (0 : BitVec 8).toNat && lane 0x0100 (i+1) == (0 : BitVec 8).toNat) = [2,3,4,5,6] := by native_decide
  rw [hl, hr] at heq
  contradiction

end TinylambOpt.ResearchProofs.JitKernels.BorrowMasks.Goal_AdjacentDelimiterIntersection
