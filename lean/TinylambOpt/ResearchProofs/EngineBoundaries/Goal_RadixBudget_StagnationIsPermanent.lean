import TinylambOpt.ResearchGoals.EngineBoundaries.RadixBudget

/-!
Partitioning for a spillable SQL operator must satisfy the modeled size or resource
bound for its partitions.

Refuted by counterexample

- `StagnationIsPermanent` — A heavy first bit need not imply that deeper bits are
  useless.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_RadixBudget_StagnationIsPermanent

open TinylambOpt.ResearchGoals.EngineBoundaries.RadixBudget

theorem refute : ¬ StagnationIsPermanent.Goal := by
  intro h
  let rs : Batch := [⟨0,6⟩, ⟨1,6⟩]
  have hchild : 6 < size (bucket 1 false rs) := by native_decide
  have hstag : stagnant (size (bucket 1 false rs)) (size rs) := by native_decide
  have hh := h rs 1 6 (by decide) hchild hstag
  have hbad : ¬ 6 < max (size (bucket 0 true (bucket 1 false rs)))
      (size (bucket 0 false (bucket 1 false rs))) := by native_decide
  exact hbad hh

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_RadixBudget_StagnationIsPermanent
