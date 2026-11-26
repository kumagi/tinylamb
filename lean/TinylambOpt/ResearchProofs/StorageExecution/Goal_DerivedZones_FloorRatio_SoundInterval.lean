import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_FloorRatio_SoundInterval

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.FloorRatio

theorem sound_SoundInterval : FloorRatio.SoundInterval.Goal := by
  intro t a b x y hx hy
  obtain ⟨ha1, ha2⟩ := hx
  obtain ⟨hb1, hb2⟩ := hy
  constructor
  · exact Nat.div_le_div (Nat.mul_le_mul ha1 (Nat.le_refl t)) (by omega) (by omega)
  · exact Nat.div_le_div (Nat.mul_le_mul ha2 (Nat.le_refl t)) (by omega) (by omega)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_FloorRatio_SoundInterval
