import TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

/-!
Renaming both sides of a move bundle can turn a renamed destination into a
source of a later move. Matching initial values at renamed sources does not
help once that source is clobbered: `[(0,1),(1,2)]` renamed by
`0↦2, 1↦1, 2↦2` makes the second move read the first move's destination.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_ParallelMoves_RegisterRenamingWithLiveAlias
open TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

theorem refute : ¬ RegisterRenamingWithLiveAlias.Goal := by
  intro h
  let f : Nat → Nat := fun r => if r == 0 then 2 else if r == 1 then 1 else if r == 2 then 2 else 0
  let s : File := fun r => 100 + r
  have hnodup : (([(0, 1), (1, 2)] : List Move).map (fun m => f m.1)).Nodup := by native_decide
  have hsrc : ∀ x ∈ ([(0, 1), (1, 2)] : List Move), s (f x.2) = s x.2 := by native_decide
  have heq := h s [(0, 1), (1, 2)] f hnodup hsrc (1, 2) (by native_decide)
  exact (by native_decide :
    serial s ([(0, 1), (1, 2)].map (fun x => (f x.1, f x.2))) (f 1) ≠
      serial s [(0, 1), (1, 2)] 1) heq

end Nov26.Proof_ParallelMoves_RegisterRenamingWithLiveAlias
