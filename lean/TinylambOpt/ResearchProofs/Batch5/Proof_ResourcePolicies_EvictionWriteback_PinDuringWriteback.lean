import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.EvictionWriteback.Goal_PinDuringWriteback

open TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback


/-!
A completion arriving after the resident page has been pinned may write the
backing bytes, but it never changes the pin count. A pinned slot therefore
remains non-reclaimable, preventing eviction of a page an active reader holds.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback.PinDuringWriteback.Goal := by
  intro s t wal h
  unfold finish reclaimable
  by_cases g : (t.incarnation != s.live.incarnation || wal < t.wal ||
      s.disk.any (fun d => d.incarnation == t.incarnation && t.revision < d.revision)) = true
  · simp [g]
    omega
  · simp [g]
    omega
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.EvictionWriteback.Goal_PinDuringWriteback
