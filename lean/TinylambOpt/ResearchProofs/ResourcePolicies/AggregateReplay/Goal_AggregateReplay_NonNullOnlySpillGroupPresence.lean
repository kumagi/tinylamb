import TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Refutes `NonNullOnlySpillGroupPresence`: dropping rows with NULL aggregates also
drops their group keys, so a group that only ever appeared with a NULL value vanishes
from a sparse-state flush. Witness: a single row `(1, none)` loses key `1`. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_AggregateReplay_NonNullOnlySpillGroupPresence
open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

theorem refute : ¬ NonNullOnlySpillGroupPresence.Goal := by
  intro h
  exact absurd (h [(1, none)]) (by decide)

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_AggregateReplay_NonNullOnlySpillGroupPresence
