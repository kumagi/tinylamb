import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_P6

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PeriodicPatch_DeleteProgression

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.PeriodicPatch

/-- `PeriodicPatch` の行集合は `striped` なレーンの `flatMap` と等しく、
    各レーンでは物理位置の削除が `deletedSum` の減算になる。
    この一般命題は `Auto_EncodedPages_P6` で証明済みなので、それを再利用する。 -/
theorem sound_PeriodicPatch_DeleteProgression : PeriodicPatch.DeleteProgression.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_P6.sound_PeriodicPatch_DeleteProgression

end TinylambOpt.ResearchProofs.Atlas.Goal_PeriodicPatch_DeleteProgression
