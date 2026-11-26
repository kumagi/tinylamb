import TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages

/-!
Proof contribution for ResourcePolicies_ClusteredPages_ReclusterPaybackEpoch. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
namespace TinylambOpt.ResearchProofs.ResourcePolicies.ClusteredPages.Goal_ReclusterPaybackEpoch

open TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages


theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages.ReclusterPaybackEpoch.Goal := by
  intro a b qs rewrite n hb
  let A := score a qs
  let B := score b qs
  change B ≤ A at hb
  change rewrite + n * B ≤ n * A ↔ rewrite ≤ n * (A - B)
  rw [Nat.mul_sub_left_distrib]
  exact (Nat.le_sub_iff_add_le (Nat.mul_le_mul_left n hb)).symm

end TinylambOpt.ResearchProofs.ResourcePolicies.ClusteredPages.Goal_ReclusterPaybackEpoch
