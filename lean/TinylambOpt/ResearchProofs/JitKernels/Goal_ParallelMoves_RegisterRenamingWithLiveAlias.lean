import TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

/-!
Renaming move destinations injectively is not enough to preserve the observed
transfer when sources are renamed by the same non-injective map: a renamed
destination can clobber the byte that a later renamed move reads from the
current register file. This file refutes the claim with a two-move bundle.

Refuted by counterexample: `f` collapses `0` and `2` to `0`, maps `1` to `1`,
and the register file agrees with its image at both sources. The renamed
sequence reads back the first move's result instead of the original second
source.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ParallelMoves_RegisterRenamingWithLiveAlias

open TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

theorem refute : ¬ RegisterRenamingWithLiveAlias.Goal := by
  intro h
  let s : Nat → Nat := fun x => if x = 1 then 1 else 0
  let f : Nat → Nat := fun x => if x = 1 then 1 else 0
  have hnodup : ([(0, 1), (1, 2)].map (fun m => f m.1)).Nodup := by native_decide
  have hlive : ∀ x ∈ [(0, 1), (1, 2)], s (f x.2) = s x.2 := by native_decide
  have heq := h s [(0, 1), (1, 2)] f hnodup hlive (1, 2) (by native_decide)
  exact (by native_decide :
    serial s [(f 0, f 1), (f 1, f 2)] (f 1) ≠ serial s [(0, 1), (1, 2)] 1) heq

end TinylambOpt.ResearchProofs.JitKernels.Goal_ParallelMoves_RegisterRenamingWithLiveAlias
