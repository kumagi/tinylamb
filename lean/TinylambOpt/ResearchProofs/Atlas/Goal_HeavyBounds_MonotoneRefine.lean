import TinylambOpt.ResearchGoals.Atlas.SynopsisBounds
import TinylambOpt.ResearchProofs.Atlas.SynopsisBounds

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_HeavyBounds_MonotoneRefine

open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.HeavyBounds

/-! 小入力（各列 長さ ≤2・値 0..2 の全列 × hot 全列 × h∈{0,1,2}）の総当たりで
    `upper a b (h::hot) ≤ upper a b hot` に反例がないことを確認したうえで、
    `SynopsisBounds.lean` の一般命題の証明をこの個別ゴールへ移す。 -/

theorem sound_HeavyBounds_MonotoneRefine : MonotoneRefine.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.SynopsisBounds.sound_HeavyBounds_MonotoneRefine

end TinylambOpt.ResearchProofs.Atlas.Goal_HeavyBounds_MonotoneRefine
