import TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages

/-!
Proof contribution for ResourcePolicies_ClusteredPages_CoalescingPhantomRangeCost. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.ClusteredPages.Goal_CoalescingPhantomRangeCost

open TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages


theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages.CoalescingPhantomRangeCost.Goal := by
  intro h
  have hres := h ⟨[0], 8⟩ ⟨[10], 8⟩ 5 5
  have hbad : ¬ readBytes [⟨[0] ++ [10], 8 + 8⟩] 5 5 ≤
      readBytes [⟨[0], 8⟩, ⟨[10], 8⟩] 5 5 + max 8 8 := by
    native_decide
  exact hbad hres

end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.ClusteredPages.Goal_CoalescingPhantomRangeCost
