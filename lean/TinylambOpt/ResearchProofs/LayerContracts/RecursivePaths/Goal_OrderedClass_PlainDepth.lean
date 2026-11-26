import TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Dominance

/-!
A recursive query uses visited states and frontier updates to avoid redundant work while
retaining visits that affect path acceptance.

Proved

- `OrderedClass.PlainDepth` — Aggressive recursion cap: one round per graph vertex even
  with path constraints.
-/

set_option maxHeartbeats 0
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_OrderedClass_PlainDepth

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths
open TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Generic

theorem sound : OrderedClass.PlainDepth.Goal := by
  refine plainDepth_of_monotone _ (by intro q; rfl) id ?_ ?_
  · intro q x r h
    dsimp only [id]
    simp only [OrderedClass.model] at h
    split at h
    · cases h
      simp at *; omega
    · cases h
  · intro q q' x r' hq h
    dsimp only [id] at hq ⊢
    simp only [OrderedClass.model] at h ⊢
    split at h
    · cases h
      rename_i hc
      refine ⟨x, ?_, by simp⟩
      rw [ite_eq_left]
      simp at hc ⊢; omega
    · cases h

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_OrderedClass_PlainDepth
