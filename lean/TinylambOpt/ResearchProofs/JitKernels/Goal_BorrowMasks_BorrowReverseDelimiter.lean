import TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

/-
Refutes `BorrowReverseDelimiter`: a trailing `0x01` after the last zero byte
keeps borrowing, so the subtract mask's last lane is later than the last real
zero byte. A reverse delimiter scan must verify the candidate mask.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_BorrowMasks_BorrowReverseDelimiter

open TinylambOpt.ResearchGoals.SwarDigitBallot
open TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.BorrowMasks.BorrowReverseDelimiter.Goal := by
  intro h
  have heq := h 0x0202020202020100
  have hf : marked (fast 0x0202020202020100) = [0,1] := by native_decide
  have hz : marked (zeroBytes 0x0202020202020100) = [0] := by native_decide
  rw [hf, hz] at heq
  contradiction

end TinylambOpt.ResearchProofs.JitKernels.Goal_BorrowMasks_BorrowReverseDelimiter
