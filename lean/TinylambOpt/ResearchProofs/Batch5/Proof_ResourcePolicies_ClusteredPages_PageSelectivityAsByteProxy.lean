import TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages

/-!
Proof contribution for ResourcePolicies_ClusteredPages_PageSelectivityAsByteProxy. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.ClusteredPages.Goal_PageSelectivityAsByteProxy

open TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages


theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages.PageSelectivityAsByteProxy.Goal := by
  intro h
  have hlt : (([⟨[0], 100⟩] : List Page).filter (candidate 0 0)).length <
      (([⟨[0], 1⟩, ⟨[0], 1⟩] : List Page).filter (candidate 0 0)).length := by native_decide
  have hres := h [⟨[0], 100⟩] [⟨[0], 1⟩, ⟨[0], 1⟩] 0 0 hlt
  have hbad : ¬ readBytes [⟨[0], 100⟩] 0 0 ≤ readBytes [⟨[0], 1⟩, ⟨[0], 1⟩] 0 0 := by
    native_decide
  exact hbad hres

end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.ClusteredPages.Goal_PageSelectivityAsByteProxy
