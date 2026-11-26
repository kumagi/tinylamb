import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `SplitNibble.SparseGather` — Only source bytes requested by selected ordinals need to
  survive cache eviction.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SplitNibble_SparseGather

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SplitNibble

/-- `fetch` が参照するのはビット位置 `p` の所属バイト `p / 8` だけである。
    選択行 `i ∈ rows` の各レーン `z < w` について `p / 8` は
    `footprint layout n w i` に含まれ、したがって `addressSet` にも入る。
    仮説により二つの read 関数はそのバイトで一致するので、レーンごとの読み値が
    等しくなり、選択行全体の fetch 結果も一致する。これは SplitNibble の
    具体的な配置には依存しない一般的な議論である。 -/
theorem sound_SparseGather : SplitNibble.SparseGather.Goal := by
  intro n w rows a b h
  apply List.map_congr_left
  intro i hi
  unfold fetch
  apply congrArg List.sum
  apply List.map_congr_left
  intro z hz
  have hmem : layout.bit n w i z / 8 ∈ addressSet layout n w rows := by
    simp only [addressSet, List.mem_eraseDups, List.mem_flatMap]
    refine ⟨i, hi, ?_⟩
    simp only [footprint, List.mem_eraseDups, lanes, List.mem_map]
    exact ⟨z, by simpa [lanes] using hz, rfl⟩
  dsimp
  rw [h (layout.bit n w i z / 8) hmem]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SplitNibble_SparseGather
