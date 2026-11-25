import TinylambOpt.ResearchGoals.Atlas.JoinPlans

import TinylambOpt.ResearchProofs.Atlas_JoinPlans_PostDiamond_Remaining
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_VoteBadge_Remaining
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_VoteBadge_Remaining_Round2
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_ReplyCycle_Remaining
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_FriendBridge_Remaining

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Auto_JoinPlans_J2

/-! The eight assigned goals are all true.  Each theorem below re-exports an
existing proof from the corresponding per-model "Remaining"
module, using fully qualified goal names. -/

theorem PostDiamond_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.PostDiamond.FractionalCap.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.PostDiamond.Remaining.sound_FractionalCap

theorem PostDiamond_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.PostDiamond.CollisionCorrection.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.PostDiamond.Remaining.sound_CollisionCorrection

theorem VoteBadge_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.VoteBadge.FractionalCap.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.VoteBadge.Remaining.sound_FractionalCap

theorem VoteBadge_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.VoteBadge.CollisionCorrection.Goal := by
  exact TinylambOpt.ResearchProofs.Round2.Task67.sound_CollisionCorrection

theorem ReplyCycle_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.ReplyCycle.FractionalCap.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.ReplyCycle.Remaining.sound_FractionalCap

theorem ReplyCycle_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.ReplyCycle.CollisionCorrection.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.ReplyCycle.Remaining.sound_CollisionCorrection

theorem FriendBridge_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.FriendBridge.FractionalCap.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.FriendBridge.Remaining.sound_FractionalCap

theorem FriendBridge_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.FriendBridge.CollisionCorrection.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.FriendBridge.Remaining.sound_CollisionCorrection

end TinylambOpt.ResearchProofs.Atlas.Auto_JoinPlans_J2
