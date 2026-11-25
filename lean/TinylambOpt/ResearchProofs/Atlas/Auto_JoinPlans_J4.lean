import TinylambOpt.ResearchGoals.Atlas.JoinPlans

-- Reuse the already-registered proofs for these model goals.
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_ReturnedItem_Remaining
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_ChannelBridge_Remaining
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_ChannelBridge_Remaining_Round2
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_PromoInventory_Remaining
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_PromoInventory_Remaining_Round2
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_TwoHopAntiWitness_Remaining

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Auto_JoinPlans_J4

/-- ReturnedItem: fractional edge cover bounds the projected support. -/
theorem ReturnedItem_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.ReturnedItem.FractionalCap.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.ReturnedItem.Remaining.sound_FractionalCap

/-- ReturnedItem: inclusion-exclusion over equality partitions. -/
theorem ReturnedItem_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.ReturnedItem.CollisionCorrection.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.ReturnedItem.Remaining.sound_CollisionCorrection

/-- ChannelBridge: fractional edge cover bounds the projected support. -/
theorem ChannelBridge_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.ChannelBridge.FractionalCap.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.ChannelBridge.Remaining.sound_FractionalCap

/-- ChannelBridge: inclusion-exclusion over equality partitions. -/
theorem ChannelBridge_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.ChannelBridge.CollisionCorrection.Goal := by
  exact TinylambOpt.ResearchProofs.Round2.Task75.sound_CollisionCorrection

/-- PromoInventory: fractional edge cover bounds the projected support. -/
theorem PromoInventory_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.PromoInventory.FractionalCap.Goal := by
  exact TinylambOpt.ResearchProofs.Round2.Task76.sound_FractionalCap

/-- PromoInventory: Newton forward expansion in the changed-table degree. -/
theorem PromoInventory_DeltaForecast :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.PromoInventory.DeltaForecast.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.PromoInventory.Remaining.sound_DeltaForecast

/-- PromoInventory: inclusion-exclusion over equality partitions. -/
theorem PromoInventory_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.PromoInventory.CollisionCorrection.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.PromoInventory.Remaining.sound_CollisionCorrection

/-- TwoHopAntiWitness: fractional edge cover bounds the projected support. -/
theorem TwoHopAntiWitness_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.TwoHopAntiWitness.FractionalCap.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.TwoHopAntiWitness.Remaining.sound_FractionalCap

/-- TwoHopAntiWitness: inclusion-exclusion over equality partitions. -/
theorem TwoHopAntiWitness_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.TwoHopAntiWitness.CollisionCorrection.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.TwoHopAntiWitness.Remaining.sound_CollisionCorrection

end TinylambOpt.ResearchProofs.Atlas.Auto_JoinPlans_J4
