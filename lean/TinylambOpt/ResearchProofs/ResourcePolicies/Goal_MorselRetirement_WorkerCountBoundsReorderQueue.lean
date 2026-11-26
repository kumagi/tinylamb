import TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement.WorkerCountBoundsReorderQueue.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Worker count does NOT bound the reorder queue when workers may claim new morsels
behind a stalled head.

Claim (`WorkerCountBoundsReorderQueue.Goal`): a schedule list of (worker, morsel)
events — where every event names a distinct in-range morsel — has length at most
`workers`.

Refuted: a single worker can complete several *different* morsels (one after
another) while an earlier id stays missing, so the distinct-morsel event list can
exceed the worker count. Counterexample: `schedule = [(0,1),(0,2)]` with
`workers = 1`: both events name worker 0 and distinct positive morsel ids, yet
`schedule.length = 2 > 1`. Capping the queue therefore needs a per-worker
outstanding-morsel limit, not just a worker-count limit.
-/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MorselRetirement_WorkerCountBoundsReorderQueue
open TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement

theorem refute : ¬ WorkerCountBoundsReorderQueue.Goal := by
  intro h
  exact absurd (h [(0, 1), (0, 2)] 1 (by decide) (by decide)) (by decide)

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MorselRetirement_WorkerCountBoundsReorderQueue
