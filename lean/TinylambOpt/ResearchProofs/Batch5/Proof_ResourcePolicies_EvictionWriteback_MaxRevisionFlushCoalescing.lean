import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.EvictionWriteback.Goal_MaxRevisionFlushCoalescing

open TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback


/-!
Refuted: coalescing flushes by keeping the maximum revision is unsafe when the
newer revision carries a higher WAL dependency. The older ticket becomes
durable, then the newer completion is fenced by its LSN, so the backing image
stays at the older revision — unlike a direct flush of the newest image.
The counterexample is the one already recorded as a `#guard` in the goal file.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback.MaxRevisionFlushCoalescing.Goal := by
  intro h
  let a : Image := Image.mk 0 0 0 0
  let b : Image := Image.mk 0 1 1 0
  let s : Slot := Slot.mk b none false 0
  have hb := h s a b 0 (by simp [s, b]) (by simp [a, b]) (by simp [a, b]) (by simp [a, b])
  have hbad : (finish (finish s a 0) b 0).disk ≠ (finish s b 0).disk := by decide
  exact hbad hb
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.EvictionWriteback.Goal_MaxRevisionFlushCoalescing
