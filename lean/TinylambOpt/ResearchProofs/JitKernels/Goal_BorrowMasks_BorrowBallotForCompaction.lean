import TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

/-
Refutes `BorrowBallotForCompaction`: a zero byte followed by byte `0x01`
continues the borrow and makes the subtract mask flag a non-zero lane. The
witness `0x0100` flags lane 1 in `fast` but not in `zeroBytes`, so a SQL scan
cannot compact candidates with the raw subtract mask.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.BorrowMasks.Goal_BorrowBallotForCompaction

open TinylambOpt.ResearchGoals.SwarDigitBallot
open TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

theorem refute : ¬ BorrowBallotForCompaction.Goal := by
  intro h
  have heq := h 0x0100
  have hf : marked (fast 0x0100) = [0,1,2,3,4,5,6,7] := by native_decide
  have hz : marked (zeroBytes 0x0100) = [0,2,3,4,5,6,7] := by native_decide
  rw [hf, hz] at heq
  contradiction

end TinylambOpt.ResearchProofs.JitKernels.BorrowMasks.Goal_BorrowBallotForCompaction
