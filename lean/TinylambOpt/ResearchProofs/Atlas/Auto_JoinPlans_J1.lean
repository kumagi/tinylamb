import TinylambOpt.ResearchProofs.Atlas_JoinPlans_MovieCredits_Remaining
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_KeywordCompany_Remaining
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_CastOverlap_Remaining
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_DoubleBilling_Remaining

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Auto_JoinPlans_J1

/-! The eight requested goals are already established in the per-model
`Remaining` modules. Each theorem below supplies the required fully qualified
statement by reusing the corresponding checked proof. -/

theorem MovieCredits_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.MovieCredits.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.MovieCredits.Remaining.sound_FractionalCap

theorem MovieCredits_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.MovieCredits.CollisionCorrection.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.MovieCredits.Remaining.sound_CollisionCorrection

theorem KeywordCompany_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.KeywordCompany.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.KeywordCompany.Remaining.sound_FractionalCap

theorem KeywordCompany_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.KeywordCompany.CollisionCorrection.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.KeywordCompany.Remaining.sound_CollisionCorrection

theorem CastOverlap_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.CastOverlap.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.CastOverlap.Remaining.sound_FractionalCap

theorem CastOverlap_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.CastOverlap.CollisionCorrection.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.CastOverlap.Remaining.sound_CollisionCorrection

theorem DoubleBilling_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.DoubleBilling.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.DoubleBilling.Remaining.sound_FractionalCap

theorem DoubleBilling_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.DoubleBilling.CollisionCorrection.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.DoubleBilling.Remaining.sound_CollisionCorrection

end TinylambOpt.ResearchProofs.Atlas.Auto_JoinPlans_J1
