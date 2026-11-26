import TinylambOpt.ResearchGoals.DatabaseCorrectness.MergeIntegrity

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_MergeIntegrity_NoActionCannotBecomeInsert

open TinylambOpt.ResearchGoals.DatabaseCorrectness.MergeIntegrity


theorem sound : NoActionCannotBecomeInsert.Goal := by
  intro target src k i hget hk hkin
  intro hmem
  simp [unmatched] at hmem
  have hfk := hmem.2 k hkin
  simp [eqOn, hk] at hfk

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_MergeIntegrity_NoActionCannotBecomeInsert
