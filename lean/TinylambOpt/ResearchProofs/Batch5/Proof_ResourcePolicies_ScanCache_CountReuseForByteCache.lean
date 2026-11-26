import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.ScanCache.Goal_CountReuseForByteCache

open TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache


/-!
A byte cap cannot be replaced by a count-based reuse test: one intervening
block can consume the whole cap.  The counterexample touches a one-byte key,
then a hundred-byte block under a two-byte cap; the count bound holds but the
first key is evicted.  This models why a scan-cache admission test must sum
block sizes instead of counting distinct keys.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false



open TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache.CountReuseForByteCache.Goal := by
  intro h
  let bytes : Nat → Nat := fun i => if i == 0 then 1 else 100
  have hit := h bytes 2 0 [1] (by simp) (by simp [bytes]) (by decide)
  simp [bytes, run, touch, fit, List.eraseDups, List.erase] at hit
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.ScanCache.Goal_CountReuseForByteCache
