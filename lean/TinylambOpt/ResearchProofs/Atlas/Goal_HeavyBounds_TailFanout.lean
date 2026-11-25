import TinylambOpt.ResearchGoals.Atlas.SynopsisBounds
import TinylambOpt.ResearchProofs.Atlas.SynopsisBounds

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_HeavyBounds_TailFanout

open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.HeavyBounds

/-! 小入力（左右 27 通り × 27 通り × hot 9 通り）の総当たりで
    `joinSize a b ≤ upper a b hot` に反例がないことを確認したうえで、
    `SynopsisBounds.lean` の一般命題の証明をこの個別ゴールへ移す。 -/

theorem sound_HeavyBounds_TailFanout : TailFanout.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.SynopsisBounds.sound_HeavyBounds_TailFanout

end TinylambOpt.ResearchProofs.Atlas.Goal_HeavyBounds_TailFanout
