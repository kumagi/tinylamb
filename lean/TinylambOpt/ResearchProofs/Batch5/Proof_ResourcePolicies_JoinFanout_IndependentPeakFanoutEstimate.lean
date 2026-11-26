import TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout

/-!
Proof contribution for ResourcePolicies_JoinFanout_IndependentPeakFanoutEstimate. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.JoinFanout.Goal_IndependentPeakFanoutEstimate

open TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout


theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout.IndependentPeakFanoutEstimate.Goal := by
  intro h
  have hres := h [some 0] [some 0, some 1, some 1]
  have hbad : ¬ (let maxA := ((hist [some 0]).map Prod.snd).max?.getD 0
      let maxB := ((hist [some 0, some 1, some 1]).map Prod.snd).max?.getD 0
      0 < product [some 0] [some 0, some 1, some 1] →
        maxA * maxB ≤ product [some 0] [some 0, some 1, some 1]) := by
    native_decide
  exact hbad hres

end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.JoinFanout.Goal_IndependentPeakFanoutEstimate
