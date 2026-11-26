import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.EvictionWriteback.Goal_RepeatedFlushCompletion

open TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback


/-!
Completion callbacks are idempotent. A second callback for the same ticket is
either fenced in the same way as the first, or sees its own image already on
disk and is rejected by the revision fence, so dirty state and backing bytes
are unchanged.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback.RepeatedFlushCompletion.Goal := by
  intro s t wal
  unfold finish
  by_cases h : (t.incarnation != s.live.incarnation || wal < t.wal ||
      s.disk.any (fun d => d.incarnation == t.incarnation && t.revision < d.revision)) = true
  · simp [h]
  · simp [h]
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.EvictionWriteback.Goal_RepeatedFlushCompletion
