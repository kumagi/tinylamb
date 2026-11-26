import TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

/-!
A recursive query uses visited states and frontier updates to avoid redundant work while
retaining visits that affect path acceptance.

Refuted by counterexample

- `AtLeastOneRed.PlainDepth` — Aggressive recursion cap: one round per graph vertex
  even with path constraints.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_AtLeastOneRed_PlainDepth

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths
open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths.AtLeastOneRed

theorem refute : ¬ AtLeastOneRed.PlainDepth.Goal := by
  intro h
  let g : List Edge := [⟨0,1,0⟩, ⟨1,0,1⟩]
  have hi := h g 0 3 1
  have ha : 1 ∈ query AtLeastOneRed.model g 0 3 id := by native_decide
  have hb : ¬ (1 ∈ query AtLeastOneRed.model g 0 (2 + 0) id) := by native_decide
  have hb' : ¬ (1 ∈ query AtLeastOneRed.model g 0 2 id) := by native_decide
  exact hb' (hi ha)

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_AtLeastOneRed_PlainDepth
