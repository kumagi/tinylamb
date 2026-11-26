import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_ResetOnNull_DropHistory

open TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits limit)

theorem sound : ResetOnNull.DropHistory.Goal := by
  intro a b s t
  dsimp only
  have hnone : whole ResetOnNull.update [none] = (0, 0) := by
    simp [whole, ResetOnNull.update, compose]
  have hmain : whole ResetOnNull.update (a ++ [none] ++ b) =
      compose (whole ResetOnNull.update b) (0, 0) := by
    rw [whole_append, whole_append, hnone]
    have hz : compose (0, 0) (whole ResetOnNull.update a) = (0, 0) := by
      rcases whole ResetOnNull.update a with ⟨A, B⟩
      simp [compose]
    rw [hz]
  rw [hmain]
  rcases whole ResetOnNull.update b with ⟨A, B⟩
  simp [apply, compose]


end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_ResetOnNull_DropHistory
