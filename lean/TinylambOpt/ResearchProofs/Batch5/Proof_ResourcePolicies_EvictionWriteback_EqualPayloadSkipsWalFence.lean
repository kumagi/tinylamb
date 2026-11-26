import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.EvictionWriteback.Goal_EqualPayloadSkipsWalFence

open TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback


/-!
Refuted: equal payload and incarnation do not imply that the durable image
satisfies the live page's WAL dependency. The backing copy can be an older
revision with the same bytes but a lower redo LSN; skipping the WAL fence
would publish bytes whose redo log may not survive recovery.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback.EqualPayloadSkipsWalFence.Goal := by
  intro h
  let d : Image := Image.mk 0 0 0 0
  let s : Slot := Slot.mk (Image.mk 0 0 1 0) (some d) false 0
  have hb := h s d (by simp [s, d]) (by simp [s, d]) (by simp [s, d])
  exact (by decide : ¬ (d.wal ≥ s.live.wal)) hb
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.EvictionWriteback.Goal_EqualPayloadSkipsWalFence
