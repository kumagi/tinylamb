import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_Vertical_AlignedPlane

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.Vertical

-- Sanity checks on small inputs satisfying the hypotheses.
#eval (List.range 9).map (fun b => (Vertical.layout.bit 8 9 0 b) % 8 == 0)

theorem sound_AlignedPlane : Vertical.AlignedPlane.Goal := by
  intro n w b hn hb
  dsimp [Vertical.layout, layout]
  have h8n : 8 ∣ n := Nat.dvd_of_mod_eq_zero hn
  have h8bn : 8 ∣ b * n := Nat.dvd_mul_left_of_dvd h8n b
  exact Nat.mod_eq_zero_of_dvd h8bn

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_Vertical_AlignedPlane
