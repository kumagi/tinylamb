import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.MorselRetirement.Goal_WorkerCountBoundsReorderQueue

open TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement


/-!
Refuted: bounding worker ids does not bound the reorder queue. A worker can
successively claim several morsels behind a stalled head, so distinct positive
sequence numbers can accumulate without bound even with a single worker.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement.WorkerCountBoundsReorderQueue.Goal := by
  intro h
  let sched : List (Nat × Nat) := [(0, 1), (0, 2)]
  have hb := h sched 1 (by decide) (by decide)
  have hbad : ¬ sched.length ≤ 1 := by decide
  exact hbad hb
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.MorselRetirement.Goal_WorkerCountBoundsReorderQueue
