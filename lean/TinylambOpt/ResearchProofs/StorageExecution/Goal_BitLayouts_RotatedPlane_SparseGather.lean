import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RotatedPlane_SparseGather

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RotatedPlane

/-- `fetch` reads only the bytes in `footprint`, so any two readers that agree
    on `addressSet` agree on every selected row. This argument does not use
    anything specific to a particular layout. -/
private theorem sparseGather_of_layout (l : Layout) : sparseGather l := by
  intro n w rows a b h
  apply List.map_congr_left
  intro i hi
  unfold fetch
  apply congrArg List.sum
  apply List.map_congr_left
  intro b' hb'
  let p := l.bit n w i b'
  have hfp : p / 8 ∈ footprint l n w i := by
    unfold footprint
    apply List.mem_eraseDups.mpr
    apply List.mem_map.mpr
    exact ⟨b', hb', rfl⟩
  have haddr : p / 8 ∈ addressSet l n w rows := by
    unfold addressSet
    apply List.mem_eraseDups.mpr
    apply List.mem_flatMap.mpr
    exact ⟨i, hi, hfp⟩
  have hab : a (p / 8) = b (p / 8) := h (p / 8) haddr
  simp [p, hab]

theorem sound_SparseGather : RotatedPlane.SparseGather.Goal := by
  exact sparseGather_of_layout RotatedPlane.layout

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RotatedPlane_SparseGather
