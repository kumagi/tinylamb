import TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Refutes `ValueDeduplicatedPartials`: two distinct records (different ids) may share
the same group key and partial state; deduplicating by (key, state) merges them into
one partial and loses one contribution to the group's sum/count. Witness: two records
with id 1 and 2, both key 0 and state (5, 1); replay gives (10, 2) but dedup gives (5, 1). -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_AggregateReplay_ValueDeduplicatedPartials
open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

theorem refute : ¬ ValueDeduplicatedPartials.Goal := by
  intro h
  exact absurd (h [⟨1, 0, (5, 1)⟩, ⟨2, 0, (5, 1)⟩] 0) (by decide)

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_AggregateReplay_ValueDeduplicatedPartials
