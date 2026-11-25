import TinylambOpt.ResearchGoals.Atlas.JoinPlans
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_FriendBridge_Remaining

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
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.FriendBridge.Remaining.sound_FractionalCap

end TinylambOpt.ResearchProofs.Atlas.Goal_FriendBridge_FractionalCap
