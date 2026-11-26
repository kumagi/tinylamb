import TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_BorrowMasks_InactivePrefixBorrowContamination

open TinylambOpt.ResearchGoals.SwarDigitBallot
open TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.BorrowMasks.InactivePrefixBorrowContamination.Goal := by
  intro h
  let x : W := 0x0000000000000100
  have hbad :
      ((marked (fast x)).filter (1 ≤ ·)).head? ≠
        ((marked (zeroBytes x)).filter (1 ≤ ·)).head? := by
    native_decide
  exact hbad (h x 1)

end TinylambOpt.ResearchProofs.JitKernels.Goal_BorrowMasks_InactivePrefixBorrowContamination
