import TinylambOpt.ResearchGoals.Atlas.JoinPlans
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.FriendBridgeCapsAndCollisionCorrection

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved

- `FriendBridge.FractionalCap` — `∀ n db b weights den, cover pattern weights den →
  (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
  ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_FriendBridge_FractionalCap

open TinylambOpt.ResearchGoals.Atlas.JoinPlans.FriendBridge

/-- The fractional edge-cover bound for the LDBC-style four-edge friend/like/
message/country path: the support count of the four-way join, raised to `den`,
is at most the product of the per-atom support counts raised to the cover
weights.  The proof is the fully elaborated argument in the per-model
`Remaining` module (indicator expansion, three integral cover bounds
`N ≤ sA*sC`, `N ≤ sB*sC`, `N ≤ sB*sD`, and the splitting
`den = α + β + γ` that distributes the cover weights over those bounds). -/
theorem sound_FriendBridge_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.FriendBridge.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.FriendBridgeCapsAndCollisionCorrection.sound_FractionalCap

end TinylambOpt.ResearchProofs.Atlas.Goal_FriendBridge_FractionalCap
