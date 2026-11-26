import TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

/-!
A recursive query uses visited states and frontier updates to avoid redundant work while
retaining visits that affect path acceptance.

Refuted by counterexample

- `PatternAB.SeedAcceptance` — Stopping on first physical encounter with a vertex is
  enough to decide acceptance.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_PatternAB_SeedAcceptance

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths
open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths.PatternAB

theorem refute : ¬ PatternAB.SeedAcceptance.Goal := by
  intro h
  let ls : List Nat := [0,0,1]
  let a : Nat := 1
  let b : List Nat := [2]
  have hi := h ls a b
  have h1 : advance PatternAB.model 0 ls = some a := by native_decide
  have h2 : advance PatternAB.model a b ≠ none := by native_decide
  have h3 : (advance PatternAB.model a b).any PatternAB.model.accept := by native_decide
  have h4 : ¬ PatternAB.model.accept a := by native_decide
  exact h4 (hi h1 h2 h3)

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_PatternAB_SeedAcceptance
