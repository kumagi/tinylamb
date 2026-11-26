import TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

/-!
A recursive query uses visited states and frontier updates to avoid redundant work while
retaining visits that affect path acceptance.

Refuted by counterexample

- `SingleRed.SeedAcceptance` — Stopping on first physical encounter with a vertex is
  enough to decide acceptance.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_SingleRed_SeedAcceptance

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths
open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths.SingleRed

theorem refute : ¬ SingleRed.SeedAcceptance.Goal := by
  intro h
  let ls : List Nat := [0,0,0]
  let a : Nat := 0
  let b : List Nat := [1]
  have hi := h ls a b
  have h1 : advance SingleRed.model 0 ls = some a := by native_decide
  have h2 : advance SingleRed.model a b ≠ none := by native_decide
  have h3 : (advance SingleRed.model a b).any SingleRed.model.accept := by native_decide
  have h4 : ¬ SingleRed.model.accept a := by native_decide
  exact h4 (hi h1 h2 h3)

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_SingleRed_SeedAcceptance
