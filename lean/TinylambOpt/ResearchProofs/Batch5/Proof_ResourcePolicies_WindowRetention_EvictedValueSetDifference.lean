import TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.WindowRetention.Goal_EvictedValueSetDifference

open TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention


/-!
Unconditional erase on eviction is not a set difference for a sliding DISTINCT
window: the deduplicated input collapses repeated values before the erase, so a
value that is still present in the frame is dropped together with the expired
copy. The counterexample loses the surviving `0`.
-/

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention.EvictedValueSetDifference.Goal := by
  intro h
  have hPerm := h 0 [0] 1
  have hnot : ¬ (([0, 0] : List Nat).eraseDups.erase 0 ++ [1]).eraseDups.Perm
      ([0, 1] : List Nat).eraseDups := by native_decide
  exact hnot hPerm
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.WindowRetention.Goal_EvictedValueSetDifference
