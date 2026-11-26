import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_ByteAlignedRow_SparseGather

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedRow

/-- バイト整列行では、行 `i` のレーン `z` の物理ビット位置は `i * align 8 w + z`。
    このビットが属するバイトは `footprint layout n w i` に含まれるので、
    選択行のアドレス集合 `addressSet` に必ず入る。よってそのバイトで
    二つの read 関数が一致すれば、すべてのレーンの読み値も一致する。 -/
theorem sound_SparseGather : ByteAlignedRow.SparseGather.Goal := by
  intro n w rows a b h
  apply List.map_congr_left
  intro i hi
  unfold fetch
  apply congrArg List.sum
  apply List.map_congr_left
  intro z hz
  have hmem : (i * align 8 w + z) / 8 ∈ addressSet layout n w rows := by
    simp only [addressSet, List.mem_eraseDups, List.mem_flatMap]
    refine ⟨i, hi, ?_⟩
    simp only [footprint, List.mem_eraseDups, lanes, List.mem_map, ByteAlignedRow.layout]
    exact ⟨z, by simpa [lanes] using hz, rfl⟩
  simp [ByteAlignedRow.layout, h ((i * align 8 w + z) / 8) hmem]
  rfl

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_ByteAlignedRow_SparseGather
