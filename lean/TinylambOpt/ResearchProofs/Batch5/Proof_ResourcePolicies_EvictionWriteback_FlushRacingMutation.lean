import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.EvictionWriteback.Goal_FlushRacingMutation

open TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback


/-!
A flush completion that races with a mutation cannot mark the page clean.
The ticket always captures the pre-mutation revision, so either the WAL /
disk fence rejects it or the completion sets dirty from a live image whose
revision differs by one. Hence a concurrent write cannot be lost by eviction.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback.FlushRacingMutation.Goal := by
  intro s lsn data durable
  unfold finish reclaimable mutate
  simp only
  by_cases h : (durable < s.live.wal ||
      s.disk.any (fun d => d.incarnation == s.live.incarnation && s.live.revision < d.revision)) = true
  · simp [h]
  · simp [h]
    intro _ heq
    have hrev := congrArg Image.revision heq
    simp at hrev
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.EvictionWriteback.Goal_FlushRacingMutation
