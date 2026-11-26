import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SplitNibble_LowNibbleLocal

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SplitNibble

-- 小入力での真偽確認（w ≥ 4 かつ b < 4 の範囲を総当たり）。
private def check (N W : Nat) : Bool :=
  (List.range N).all fun n => (List.range W).all fun w =>
    (List.range (2 * W + 3)).all fun i =>
      (List.range 8).all fun b =>
        decide (4 ≤ w → b < 4 → layout.bit n w i b / 8 = i / 2)

#eval check 6 12

/-- `4 ≤ w` のとき `min 4 w = 4` なので、下位ニブル (`b < 4`) の物理位置は
    `i * 4 + b` になる。`i = 2 * (i / 2) + i % 2` とおくと、
    `i * 4 + b = 8 * (i / 2) + ((i % 2) * 4 + b)` であり、
    `i % 2 < 2` かつ `b < 4` より端数 `(i % 2) * 4 + b` は 8 未満。
    したがって 8 で割ると `i / 2` になる。 -/
theorem sound_LowNibbleLocal : SplitNibble.LowNibbleLocal.Goal := by
  intro n w i b hw hb
  simp [layout, Nat.min_eq_left hw, hb]
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SplitNibble_LowNibbleLocal
