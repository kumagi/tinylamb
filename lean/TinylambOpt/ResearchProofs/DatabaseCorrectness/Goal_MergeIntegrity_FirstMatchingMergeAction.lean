import TinylambOpt.ResearchGoals.DatabaseCorrectness.MergeIntegrity

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_MergeIntegrity_FirstMatchingMergeAction

open TinylambOpt.ResearchGoals.DatabaseCorrectness.MergeIntegrity


theorem sound : FirstMatchingMergeAction.Goal := by
  intro before after a i hbefore
  simp [decideMatched]
  induction before with
  | nil =>
      rfl
  | cons c cs ih =>
      have hc : c.1 = false := hbefore c (by simp)
      simp [hc]
      apply ih
      intro x hx
      exact hbefore x (by simp [hx])

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_MergeIntegrity_FirstMatchingMergeAction
