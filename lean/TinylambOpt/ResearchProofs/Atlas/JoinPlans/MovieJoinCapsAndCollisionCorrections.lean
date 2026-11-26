import TinylambOpt.ResearchProofs.Atlas.JoinPlans.MovieCreditsCapsAndCollisionCorrection
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.KeywordCompanyCapsAndCollisionCorrection
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.CastOverlapCapsAndCollisionCorrection
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.DoubleBillingCapsAndCollisionCorrection

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved (8 claims, including the following)

- `CastOverlap.CollisionCorrection` — `∀ n db b, injective pattern n db b =
  ((partitions pattern.freeVars).map (fun bs => mobius bs * collapsed pattern n db b
  bs)).sum`
- `CastOverlap.FractionalCap` — `∀ n db b weights den, cover pattern weights den →
  (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
  ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod`
- `DoubleBilling.CollisionCorrection` — `∀ n db b, injective pattern n db b =
  ((partitions pattern.freeVars).map (fun bs => mobius bs * collapsed pattern n db b
  bs)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans.MovieJoinCapsAndCollisionCorrections

/-! The eight requested goals are already established in the per-model
`Remaining` modules. Each theorem below supplies the required fully qualified
statement by reusing the corresponding checked proof. -/

theorem MovieCredits_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.MovieCredits.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.MovieCreditsCapsAndCollisionCorrection.sound_FractionalCap

theorem MovieCredits_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.MovieCredits.CollisionCorrection.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.MovieCreditsCapsAndCollisionCorrection.sound_CollisionCorrection

theorem KeywordCompany_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.KeywordCompany.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.KeywordCompanyCapsAndCollisionCorrection.sound_FractionalCap

theorem KeywordCompany_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.KeywordCompany.CollisionCorrection.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.KeywordCompanyCapsAndCollisionCorrection.sound_CollisionCorrection

theorem CastOverlap_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.CastOverlap.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.CastOverlapCapsAndCollisionCorrection.sound_FractionalCap

theorem CastOverlap_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.CastOverlap.CollisionCorrection.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.CastOverlapCapsAndCollisionCorrection.sound_CollisionCorrection

theorem DoubleBilling_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.DoubleBilling.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.DoubleBillingCapsAndCollisionCorrection.sound_FractionalCap

theorem DoubleBilling_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.DoubleBilling.CollisionCorrection.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.DoubleBillingCapsAndCollisionCorrection.sound_CollisionCorrection

end TinylambOpt.ResearchProofs.Atlas.JoinPlans.MovieJoinCapsAndCollisionCorrections
