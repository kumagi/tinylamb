import TinylambOpt.ResearchGoals.Atlas.JoinPlans

-- Reuse the already-registered proofs for these model goals.
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.ReturnedItemCapsAndCollisionCorrection
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.ChannelBridgeFractionalCap
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.ChannelBridgeCollisionCorrection
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.PromoInventoryCollisionCorrectionAndDeltaForecast
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.PromoInventoryFractionalCap
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.TwoHopAntiWitnessCapsAndCollisionCorrection

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved (9 claims, including the following)

- `ChannelBridge.CollisionCorrection` — `∀ n db b, injective pattern n db b =
  ((partitions pattern.freeVars).map (fun bs => mobius bs * collapsed pattern n db b
  bs)).sum`
- `ChannelBridge.FractionalCap` — `∀ n db b weights den, cover pattern weights den →
  (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
  ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod`
- `PromoInventory.CollisionCorrection` — `∀ n db b, injective pattern n db b =
  ((partitions pattern.freeVars).map (fun bs => mobius bs * collapsed pattern n db b
  bs)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans.ReturnChannelAndInventoryJoinCaps

/-- ReturnedItem: fractional edge cover bounds the projected support. -/
theorem ReturnedItem_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.ReturnedItem.FractionalCap.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.ReturnedItemCapsAndCollisionCorrection.sound_FractionalCap

/-- ReturnedItem: inclusion-exclusion over equality partitions. -/
theorem ReturnedItem_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.ReturnedItem.CollisionCorrection.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.ReturnedItemCapsAndCollisionCorrection.sound_CollisionCorrection

/-- ChannelBridge: fractional edge cover bounds the projected support. -/
theorem ChannelBridge_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.ChannelBridge.FractionalCap.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.ChannelBridgeFractionalCap.sound_FractionalCap

/-- ChannelBridge: inclusion-exclusion over equality partitions. -/
theorem ChannelBridge_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.ChannelBridge.CollisionCorrection.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.ChannelBridgeCollisionCorrection.sound_CollisionCorrection

/-- PromoInventory: fractional edge cover bounds the projected support. -/
theorem PromoInventory_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.PromoInventory.FractionalCap.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.PromoInventoryFractionalCap.sound_FractionalCap

/-- PromoInventory: Newton forward expansion in the changed-table degree. -/
theorem PromoInventory_DeltaForecast :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.PromoInventory.DeltaForecast.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.PromoInventoryCollisionCorrectionAndDeltaForecast.sound_DeltaForecast

/-- PromoInventory: inclusion-exclusion over equality partitions. -/
theorem PromoInventory_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.PromoInventory.CollisionCorrection.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.PromoInventoryCollisionCorrectionAndDeltaForecast.sound_CollisionCorrection

/-- TwoHopAntiWitness: fractional edge cover bounds the projected support. -/
theorem TwoHopAntiWitness_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.TwoHopAntiWitness.FractionalCap.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.TwoHopAntiWitnessCapsAndCollisionCorrection.sound_FractionalCap

/-- TwoHopAntiWitness: inclusion-exclusion over equality partitions. -/
theorem TwoHopAntiWitness_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.TwoHopAntiWitness.CollisionCorrection.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.TwoHopAntiWitnessCapsAndCollisionCorrection.sound_CollisionCorrection

end TinylambOpt.ResearchProofs.Atlas.JoinPlans.ReturnChannelAndInventoryJoinCaps
