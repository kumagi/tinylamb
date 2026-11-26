import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_SlotPlacement

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_ForwardGrowth_Reuse

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.ForwardGrowth.Reuse.Goal := by
  intro r cap u hfit
  rw [quick]
  apply find?_eq_some_of_sorted (probes_sorted ForwardGrowth.model r u)
  · rw [mem_probes]
    left
    simp [ForwardGrowth.model]
  · exact hfit
  · intro b hb hblt
    rw [mem_probes] at hb
    rcases hb with hb | ⟨x, hx, hb⟩
    · simp [ForwardGrowth.model] at hb
      omega
    · simp [ForwardGrowth.model] at hb
      omega

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_ForwardGrowth_Reuse
