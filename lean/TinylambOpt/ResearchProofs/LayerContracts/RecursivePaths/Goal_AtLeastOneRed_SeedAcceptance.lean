import TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

/-!
A recursive query uses visited states and frontier updates to avoid redundant work while
retaining visits that affect path acceptance.

Refuted by counterexample

- `AtLeastOneRed.SeedAcceptance` — Stopping on first physical encounter with a vertex
  is enough to decide acceptance.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_AtLeastOneRed_SeedAcceptance

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths
open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths.AtLeastOneRed

theorem refute : ¬ AtLeastOneRed.SeedAcceptance.Goal := by
  intro h
  let ls : List Nat := [0,0,0]
  let a : Nat := 0
  let b : List Nat := [1]
  have hi := h ls a b
  have h1 : advance AtLeastOneRed.model 0 ls = some a := by native_decide
  have h2 : advance AtLeastOneRed.model a b ≠ none := by native_decide
  have h3 : (advance AtLeastOneRed.model a b).any AtLeastOneRed.model.accept := by native_decide
  have h4 : ¬ AtLeastOneRed.model.accept a := by native_decide
  exact h4 (hi h1 h2 h3)

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_AtLeastOneRed_SeedAcceptance
