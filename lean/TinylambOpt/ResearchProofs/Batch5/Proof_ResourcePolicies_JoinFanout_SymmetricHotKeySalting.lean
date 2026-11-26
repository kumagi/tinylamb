import TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout

/-!
Proof contribution for ResourcePolicies_JoinFanout_SymmetricHotKeySalting. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.JoinFanout.Goal_SymmetricHotKeySalting

open TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout


theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout.SymmetricHotKeySalting.Goal := by
  intro h
  have hres := h 3 3 2 (by decide)
  have hbad' : ¬
      ((List.range 3).map (fun i => ((List.range 3).filter (fun j => i % 2 == j % 2)).length)).sum = 3 * 3 := by
    native_decide
  exact hbad' hres

end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.JoinFanout.Goal_SymmetricHotKeySalting
