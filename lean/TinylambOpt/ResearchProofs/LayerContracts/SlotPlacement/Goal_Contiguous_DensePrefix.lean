import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_SlotPlacement

/-!
Page allocation for stored SQL rows must preserve the byte ranges, alignment, and live
data required by the modeled layout.

Proved

- `Contiguous.DensePrefix` — `∀ r n cap, 0 < r.bytes → n+r.bytes ≤ cap → quick model r
  cap (band 0 n) = some n`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_Contiguous_DensePrefix

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.Contiguous.DensePrefix.Goal := by
    intro r n cap hb hn
    have hff : firstFit Contiguous.model := by
      apply firstFit_of Contiguous.model ContiguousFacts.prev
      · exact ContiguousFacts.fix
      · exact ContiguousFacts.prevSpec
    rw [hff r cap (band 0 n)]
    apply (List.find?_range_eq_some).2
    refine ⟨?_, by
      have h1 : n + 1 ≤ n + r.bytes := Nat.add_le_add_left hb n
      have h2 : n.succ ≤ cap := Nat.le_trans h1 hn
      simp [Nat.lt_of_succ_le h2], ?_⟩
    · simp [fits, Contiguous.model, band, List.all_eq_true]
      constructor
      · omega
      · intro x hx
        have hxlt : n + x < n + r.bytes := Nat.add_lt_add_left hx n
        omega
    · intro j hj
      by_cases hfj : fits Contiguous.model r cap (band 0 n) j = true
      · have h' := hfj
        simp [fits, Contiguous.model, band, List.all_eq_true] at h'
        have hmem : j ∈ band 0 n := by simp [band]; omega
        have hbad := h'.2 0 hb
        simp [hmem] at hbad
        omega
      · simp [hfj]

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_Contiguous_DensePrefix
