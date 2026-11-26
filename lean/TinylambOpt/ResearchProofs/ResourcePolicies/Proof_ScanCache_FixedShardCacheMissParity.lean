import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache

namespace TinylambOpt.ResearchProofs.ResourcePolicies.ScanCache.Goal_FixedShardCacheMissParity

open TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache


/-!
Static per-shard byte quotas do not reproduce a pooled cache: two even keys
reuse a one-byte shard badly while the pooled two-byte cache keeps both.  The
counterexample corresponds to two scan streams sharing a fixed cache partition
instead of a common byte budget.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false



open TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache.FixedShardCacheMissParity.Goal := by
  intro h
  have hbad := h [0, 2, 0] 1
  simp [demandMisses, touch, fit, run] at hbad
end TinylambOpt.ResearchProofs.ResourcePolicies.ScanCache.Goal_FixedShardCacheMissParity
