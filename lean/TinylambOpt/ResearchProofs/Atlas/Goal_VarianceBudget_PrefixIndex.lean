import TinylambOpt.ResearchGoals.Atlas.RangePlans
import TinylambOpt.ResearchProofs.Atlas.RangePlans4

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_VarianceBudget_PrefixIndex

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.VarianceBudget

/-! 符号付き矩形スタンシルの四隅の累積和による読み取りは、
    有限領域内の整数座標では実際の走査と一致する。
    `RangePlans4` の共通補題 `prefixIndex_of_stencil` を、
    このモデルのスタンシル識別子に適用した証明を再利用する。 -/
theorem sound_VarianceBudget_PrefixIndex :
    TinylambOpt.ResearchGoals.Atlas.RangePlans.VarianceBudget.PrefixIndex.Goal :=
  TinylambOpt.ResearchProofs.Atlas.RangePlans4.VarianceBudget.sound_VarianceBudget_PrefixIndex

end TinylambOpt.ResearchProofs.Atlas.Goal_VarianceBudget_PrefixIndex
