import TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

/-
Refutes `InactivePrefixBorrowContamination`: a zero before `start` can carry
a borrow into a later `0x01`, so masking the subtract result to the suffix
still reports a phantom hit. The witness `0x0100, start = 1` has no zero byte
in the suffix but does have a subtract-mask lane there.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.BorrowMasks.Goal_InactivePrefixBorrowContamination

open TinylambOpt.ResearchGoals.SwarDigitBallot
open TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

theorem refute : ¬ InactivePrefixBorrowContamination.Goal := by
  intro h
  have heq := h 0x0202020202020100 1
  have hf : marked (fast 0x0202020202020100) = [0,1] := by native_decide
  have hz : marked (zeroBytes 0x0202020202020100) = [0] := by native_decide
  rw [hf, hz] at heq
  contradiction

end TinylambOpt.ResearchProofs.JitKernels.BorrowMasks.Goal_InactivePrefixBorrowContamination
