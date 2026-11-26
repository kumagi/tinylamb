import TinylambOpt.ResearchGoals.Atlas.JoinPlans
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.ForumBowtieCapsAndCollisionCorrection
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.CoLikesCapsAndCollisionCorrection
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.CountryReturnDeltaForecast
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.CountryReturnCapsAndCollisionCorrection
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.OrderSupplierFractionalCap
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.OrderSupplierCollisionCorrection

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved (9 claims, including the following)

- `CoLikes.CollisionCorrection` — `∀ n db b, injective pattern n db b = ((partitions
  pattern.freeVars).map (fun bs => mobius bs * collapsed pattern n db b bs)).sum`
- `CoLikes.FractionalCap` — `∀ n db b weights den, cover pattern weights den →
  (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
  ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod`
- `CountryReturn.CollisionCorrection` — `∀ n db b, injective pattern n db b =
  ((partitions pattern.freeVars).map (fun bs => mobius bs * collapsed pattern n db b
  bs)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans.ForumAndSupplierJoinCapsAndCorrections

namespace ForumFractionalCover

theorem sound_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.ForumBowtie.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.ForumBowtieCapsAndCollisionCorrection.sound_FractionalCap

end ForumFractionalCover

namespace ForumCollisionCorrection

theorem sound_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.ForumBowtie.CollisionCorrection.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.ForumBowtieCapsAndCollisionCorrection.sound_CollisionCorrection

end ForumCollisionCorrection

theorem CoLikes_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.CoLikes.FractionalCap.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.CoLikesCapsAndCollisionCorrection.sound_FractionalCap

theorem CoLikes_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.CoLikes.CollisionCorrection.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.CoLikesCapsAndCollisionCorrection.sound_CollisionCorrection

theorem ForumBowtie_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.ForumBowtie.FractionalCap.Goal := by
  exact ForumFractionalCover.sound_FractionalCap

theorem ForumBowtie_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.ForumBowtie.CollisionCorrection.Goal := by
  exact ForumCollisionCorrection.sound_CollisionCorrection

theorem CountryReturn_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.CountryReturn.FractionalCap.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.CountryReturnCapsAndCollisionCorrection.sound_FractionalCap

theorem CountryReturn_DeltaForecast :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.CountryReturn.DeltaForecast.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.CountryReturnDeltaForecast.sound_DeltaForecast

theorem CountryReturn_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.CountryReturn.CollisionCorrection.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.CountryReturnCapsAndCollisionCorrection.sound_CollisionCorrection

theorem OrderSupplier_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.OrderSupplier.FractionalCap.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.OrderSupplierFractionalCap.sound_FractionalCap

theorem OrderSupplier_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.OrderSupplier.CollisionCorrection.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.OrderSupplierCollisionCorrection.sound_CollisionCorrection

end TinylambOpt.ResearchProofs.Atlas.JoinPlans.ForumAndSupplierJoinCapsAndCorrections
