import TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.JoinFanout_SymmetricHotKeySalting
open TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout

/--
Salting both sides by local ordinal does NOT preserve every duplicate match:
a probe row only matches build rows in its own stripe, so pairs whose ordinals
fall in different stripes are lost. With 2 rows, 2 probes and 2 stripes the
salted self-join yields 2 matched pairs, not 4.
-/
theorem refute : ¬ SymmetricHotKeySalting.Goal := by
  intro hh
  have key : ¬ ((List.range 2).map fun i =>
      ((List.range 2).filter fun j => i % 2 == j % 2).length).sum =
      (2 : Nat) * 2 := by
    decide
  exact key (hh 2 2 2 (by omega))

end TinylambOpt.ResearchProofs.ResourcePolicies.JoinFanout_SymmetricHotKeySalting
