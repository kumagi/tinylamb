import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `RowMajor.SparseGather` — Only source bytes requested by selected ordinals need to
  survive cache eviction.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RowMajor_SparseGather

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RowMajor

theorem sound_SparseGather : RowMajor.SparseGather.Goal := by
  intro n w rows a b h
  apply List.map_congr_left
  intro i hi
  unfold fetch
  apply congrArg List.sum
  apply List.map_congr_left
  intro z hz
  have hmem : (i * w + z) / 8 ∈ addressSet layout n w rows := by
    simp only [addressSet, List.mem_eraseDups, List.mem_flatMap]
    refine ⟨i, hi, ?_⟩
    simp only [footprint, List.mem_eraseDups, lanes, List.mem_map, RowMajor.layout]
    exact ⟨z, by simpa [lanes] using hz, rfl⟩
  simp [RowMajor.layout, h ((i * w + z) / 8) hmem]
  rfl

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RowMajor_SparseGather
