import TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_TollBudget_SeedAcceptance

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

theorem sound : TollBudget.SeedAcceptance.Goal := by
  intro labels a b h1 h2 h3
  simp [TollBudget.model]

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_TollBudget_SeedAcceptance
