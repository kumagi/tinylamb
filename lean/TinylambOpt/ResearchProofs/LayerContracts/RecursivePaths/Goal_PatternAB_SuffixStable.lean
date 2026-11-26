import TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_PatternAB_SuffixStable

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

set_option maxHeartbeats 0
open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

def paF (q x : Nat) : Option Nat := some (if q == 2 || (q == 1 && x == 2) then 2 else if x == 1 then 1 else 0)

theorem paFold2 : ∀ (ls : List Nat), List.foldlM paF 2 ls = some 2 := by
  intro ls
  induction ls with
  | nil => simp
  | cons l ls ih =>
      rw [List.foldlM_cons]
      simp [paF, ih]

theorem sound : PatternAB.SuffixStable.Goal := by
  intro ls
  change List.foldlM paF 2 ls = some 2
  exact paFold2 ls

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_PatternAB_SuffixStable
