import TinylambOpt.ResearchGoals.Atlas.JoinPlans

import TinylambOpt.ResearchProofs.Atlas.JoinPlans.PostDiamondCapsAndCollisionCorrection
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.VoteBadgeFractionalCap
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.VoteBadgeCollisionCorrection
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.ReplyCycleCapsAndCollisionCorrection
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.FriendBridgeCapsAndCollisionCorrection

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved (8 claims, including the following)

- `FriendBridge.CollisionCorrection` — `∀ n db b, injective pattern n db b =
  ((partitions pattern.freeVars).map (fun bs => mobius bs * collapsed pattern n db b
  bs)).sum`
- `FriendBridge.FractionalCap` — `∀ n db b weights den, cover pattern weights den →
  (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
  ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod`
- `PostDiamond.CollisionCorrection` — `∀ n db b, injective pattern n db b =
  ((partitions pattern.freeVars).map (fun bs => mobius bs * collapsed pattern n db b
  bs)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans.SocialJoinCapsAndCollisionCorrections

/-! The eight assigned goals are all true.  Each theorem below re-exports an
existing proof from the corresponding per-model "Remaining"
module, using fully qualified goal names. -/

theorem PostDiamond_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.PostDiamond.FractionalCap.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.PostDiamondCapsAndCollisionCorrection.sound_FractionalCap

theorem PostDiamond_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.PostDiamond.CollisionCorrection.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.PostDiamondCapsAndCollisionCorrection.sound_CollisionCorrection

theorem VoteBadge_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.VoteBadge.FractionalCap.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.VoteBadgeFractionalCap.sound_FractionalCap

theorem VoteBadge_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.VoteBadge.CollisionCorrection.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.VoteBadgeCollisionCorrection.sound_CollisionCorrection

theorem ReplyCycle_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.ReplyCycle.FractionalCap.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.ReplyCycleCapsAndCollisionCorrection.sound_FractionalCap

theorem ReplyCycle_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.ReplyCycle.CollisionCorrection.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.ReplyCycleCapsAndCollisionCorrection.sound_CollisionCorrection

theorem FriendBridge_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.FriendBridge.FractionalCap.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.FriendBridgeCapsAndCollisionCorrection.sound_FractionalCap

theorem FriendBridge_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.FriendBridge.CollisionCorrection.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.FriendBridgeCapsAndCollisionCorrection.sound_CollisionCorrection

end TinylambOpt.ResearchProofs.Atlas.JoinPlans.SocialJoinCapsAndCollisionCorrections
