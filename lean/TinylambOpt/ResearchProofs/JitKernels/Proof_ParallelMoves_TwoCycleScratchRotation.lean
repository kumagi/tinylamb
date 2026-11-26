import TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

/-!
A two-cycle PHI swap is broken with a scratch register holding the old value of
`a`. Every register other than the scratch observes the same value as the
parallel reference transfer, including when the two swapped registers coincide.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_ParallelMoves_TwoCycleScratchRotation
open TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

theorem sound : TwoCycleScratchRotation.Goal := by
  intro s a b tmp hta htb r hrt
  have hbt : b ≠ tmp := by omega
  simp [serial, parallelCopy, put, hta, hbt, hrt]

end Nov26.Proof_ParallelMoves_TwoCycleScratchRotation
