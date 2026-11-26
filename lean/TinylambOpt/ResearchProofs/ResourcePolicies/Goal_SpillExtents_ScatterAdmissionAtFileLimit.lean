import TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents
/-!
Registers the checked result for `SpillExtents.ScatterAdmissionAtFileLimit.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SpillExtents_ScatterAdmissionAtFileLimit
open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents ScatterAdmissionAtFileLimit

theorem sound : Goal := by
  intro holes requested used cap hnodup hlt hused
  constructor <;> intro h <;> omega
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SpillExtents_ScatterAdmissionAtFileLimit
