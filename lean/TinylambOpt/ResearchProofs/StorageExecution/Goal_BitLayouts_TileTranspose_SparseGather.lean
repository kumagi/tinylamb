import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_TileTranspose_SparseGather

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.TileTranspose

theorem sound_SparseGather : TileTranspose.SparseGather.Goal := by
  intro n w rows a b h
  apply List.map_congr_left
  intro i hi
  unfold fetch
  apply congrArg List.sum
  apply List.map_congr_left
  intro z hz
  have hmem : (((i / 8) * 8 * w + z * 8 + i % 8) / 8) ∈ addressSet layout n w rows := by
    simp only [addressSet, List.mem_eraseDups, List.mem_flatMap]
    refine ⟨i, hi, ?_⟩
    simp only [footprint, List.mem_eraseDups, lanes, List.mem_map, TileTranspose.layout]
    exact ⟨z, by simpa [lanes] using hz, rfl⟩
  simp [TileTranspose.layout, h (((i / 8) * 8 * w + z * 8 + i % 8) / 8) hmem]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_TileTranspose_SparseGather
