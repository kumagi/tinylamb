import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_P9

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_GrayTile_DeleteProgression

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.GrayTile

/-- GrayTile の物理行は正の stride を持つ `striped` レーンに分割され、
    各レーン上で位置削除の総和は `deletedSum` に還元される。
    既存の一般補題 `delete_from_rows` をそのまま適用する。 -/
theorem sound_GrayTile_DeleteProgression : GrayTile.DeleteProgression.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_P9.GrayTile.GrayTile_DeleteProgression

end TinylambOpt.ResearchProofs.Atlas.Goal_GrayTile_DeleteProgression
