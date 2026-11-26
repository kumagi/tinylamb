import TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.JoinFanout_IndependentPeakFanoutEstimate
open TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout

/--
Pairing separately measured peak frequencies overestimates the join output:
the heavy keys of the two sides need not be the keys that actually match, so
`maxA * maxB` can far exceed the true product while it is still positive.
-/
theorem refute : ¬ IndependentPeakFanoutEstimate.Goal := by
  intro hh
  have key : ¬ (0 < product [some 0, some 0, some 1, some 1, some 1] [some 0, some 2, some 2] →
      (((hist [some 0, some 0, some 1, some 1, some 1]).map Prod.snd).max?.getD 0) *
        (((hist [some 0, some 2, some 2]).map Prod.snd).max?.getD 0) ≤
      product [some 0, some 0, some 1, some 1, some 1] [some 0, some 2, some 2]) := by
    native_decide
  -- Only key 0 matches (2 build rows × 1 probe row = 2 pairs), but the peak
  -- frequencies are 3 and 2, and 3*2 = 6 > 2.
  exact key (hh [some 0, some 0, some 1, some 1, some 1] [some 0, some 2, some 2])

end TinylambOpt.ResearchProofs.ResourcePolicies.JoinFanout_IndependentPeakFanoutEstimate
