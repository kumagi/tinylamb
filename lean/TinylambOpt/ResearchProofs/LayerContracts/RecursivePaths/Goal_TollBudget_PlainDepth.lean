import TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Dominance

set_option maxHeartbeats 0
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_TollBudget_PlainDepth

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths
open TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Generic

theorem sound : TollBudget.PlainDepth.Goal := by
  refine plainDepth_of_monotone _ (by intro q; rfl) id ?_ ?_
  · intro q x r h
    dsimp only [id]
    simp only [TollBudget.model] at h
    split at h
    · cases h; omega
    · cases h
  · intro q q' x r' hq h
    dsimp only [id] at hq ⊢
    simp only [TollBudget.model] at h ⊢
    split at h
    · cases h
      refine ⟨q + x, ?_, by simp; omega⟩
      rw [ite_eq_left (by omega)]
    · cases h

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_TollBudget_PlainDepth
