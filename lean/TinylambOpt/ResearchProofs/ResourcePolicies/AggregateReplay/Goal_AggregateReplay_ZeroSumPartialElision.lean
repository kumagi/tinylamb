import TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Refutes `ZeroSumPartialElision`: dropping a partial whose SUM component is zero can
change COUNT-style bookkeeping. When the rest of the group is empty, the dropped
partial makes the group non-empty, turning SQL SUM's all-NULL `none` into `some 0`.
Witness: one record with state (0, 1) for key 0 and an empty tail. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_AggregateReplay_ZeroSumPartialElision
open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

theorem refute : ¬ ZeroSumPartialElision.Goal := by
  intro h
  exact absurd (h ⟨0, 0, (0, 1)⟩ [] 0) (by decide)

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_AggregateReplay_ZeroSumPartialElision
