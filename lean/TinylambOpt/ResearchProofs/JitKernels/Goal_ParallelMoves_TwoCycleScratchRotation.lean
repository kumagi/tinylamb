import TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

/-!
A two-register swap can be lowered as a three-move rotation through a scratch
register. Every register other than the scratch observes the same transfer as
the parallel swap; the proof splits on whether the observed register is one of
the two swapped registers (including the case where they coincide).

Proved

- `TwoCycleScratchRotation` — the scratch-based rotation may replace the
  parallel swap bundle at a join, preserving every non-scratch output.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ParallelMoves_TwoCycleScratchRotation

open TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

theorem sound : TinylambOpt.ResearchGoals.JitKernels.ParallelMoves.TwoCycleScratchRotation.Goal := by
  intro s a b tmp hta htb r hr
  have hat : a ≠ tmp := by omega
  have hbt : b ≠ tmp := by omega
  by_cases hab : a = b
  · subst b
    simp [serial, parallelCopy, put, hta, hat, hr]
  · by_cases hra : r = a
    · subst r
      simp [serial, parallelCopy, put, hta, htb, hat, hbt, hab]
    · by_cases hrb : r = b
      · subst r
        simp [serial, parallelCopy, put, hta, htb, hat, hbt, hab]
      · simp [serial, parallelCopy, put, hta, htb, hat, hbt, hab, hra, hrb, hr]

end TinylambOpt.ResearchProofs.JitKernels.Goal_ParallelMoves_TwoCycleScratchRotation
