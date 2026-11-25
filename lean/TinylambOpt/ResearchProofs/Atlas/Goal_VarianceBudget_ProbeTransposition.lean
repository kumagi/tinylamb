import TinylambOpt.ResearchGoals.Atlas.RangePlans
import TinylambOpt.ResearchProofs.Atlas.RangePlans4

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_VarianceBudget_ProbeTransposition

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.VarianceBudget

/-! 有限領域内の整数座標では、符号付きスタンシルの四隅の重み和が
    `qualifies` の指示器になり、境界イベントへの転置は各プローブの
    寄与を各build行で読み直すなどして総和を保つ。
    共通補題 `probeTransposition_of_stencil` にこのモデルの
    スタンシル識別子を渡すだけで証明できる。小さい入力の総当たり
    （b=1, 座標 -1..1, プローブ/行リスト長 0..2）も一致することを確認済み。 -/
theorem sound_VarianceBudget_ProbeTransposition :
    TinylambOpt.ResearchGoals.Atlas.RangePlans.VarianceBudget.ProbeTransposition.Goal := by
  intro b ps rs hb
  exact (TinylambOpt.ResearchProofs.Atlas.RangePlans4.VarianceBudget.sound_VarianceBudget_ProbeTransposition)
    b ps rs hb

end TinylambOpt.ResearchProofs.Atlas.Goal_VarianceBudget_ProbeTransposition
