import TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_LastBlue_SeedAcceptance

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths
open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths.LastBlue

theorem refute : ¬ LastBlue.SeedAcceptance.Goal := by
  intro h
  let ls : List Nat := [0,0,0]
  let a : Nat := 0
  let b : List Nat := [2]
  have hi := h ls a b
  have h1 : advance LastBlue.model 0 ls = some a := by native_decide
  have h2 : advance LastBlue.model a b ≠ none := by native_decide
  have h3 : (advance LastBlue.model a b).any LastBlue.model.accept := by native_decide
  have h4 : ¬ LastBlue.model.accept a := by native_decide
  exact h4 (hi h1 h2 h3)

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_LastBlue_SeedAcceptance
