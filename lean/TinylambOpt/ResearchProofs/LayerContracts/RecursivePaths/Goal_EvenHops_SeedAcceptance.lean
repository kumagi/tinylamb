import TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

/-!
A recursive query uses visited states and frontier updates to avoid redundant work while
retaining visits that affect path acceptance.

Refuted by counterexample

- `EvenHops.SeedAcceptance` — Stopping on first physical encounter with a vertex is
  enough to decide acceptance.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_EvenHops_SeedAcceptance

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths
open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths.EvenHops

theorem refute : ¬ EvenHops.SeedAcceptance.Goal := by
  intro h
  let ls : List Nat := [0,0,0]
  let a : Nat := 1
  let b : List Nat := [0]
  have hi := h ls a b
  have h1 : advance EvenHops.model 0 ls = some a := by native_decide
  have h2 : advance EvenHops.model a b ≠ none := by native_decide
  have h3 : (advance EvenHops.model a b).any EvenHops.model.accept := by native_decide
  have h4 : ¬ EvenHops.model.accept a := by native_decide
  exact h4 (hi h1 h2 h3)

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_EvenHops_SeedAcceptance
