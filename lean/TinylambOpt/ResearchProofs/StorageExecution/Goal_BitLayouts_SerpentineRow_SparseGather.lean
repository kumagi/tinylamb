import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SerpentineRow_SparseGather

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SerpentineRow

/-! `fetch` reads only the bytes `l.bit n w i c / 8` for `c < w`, i.e. exactly
the bytes in `footprint l n w i`. For every selected row `i`, that footprint is
part of `addressSet l n w rows`, so the two readers agree on every byte that
matters. This argument is layout-independent, hence it applies to the
serpentine row layout. -/

theorem sound_SparseGather : SerpentineRow.SparseGather.Goal := by
  intro n w rows a a' h
  apply List.map_congr_left
  intro i hi
  simp only [fetch]
  apply congrArg List.sum
  apply List.map_congr_left
  intro c hc
  have hcw : c < w := by simpa [lanes] using hc
  have hfp : layout.bit n w i c / 8 ∈ footprint layout n w i := by
    simp only [footprint, List.mem_eraseDups]
    exact List.mem_map.mpr ⟨c, by simpa [lanes] using hcw, rfl⟩
  have has : layout.bit n w i c / 8 ∈ addressSet layout n w rows := by
    simp only [addressSet, List.mem_eraseDups]
    exact List.mem_flatMap.mpr ⟨i, hi, hfp⟩
  have heq := h (layout.bit n w i c / 8) has
  simp [heq]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SerpentineRow_SparseGather
