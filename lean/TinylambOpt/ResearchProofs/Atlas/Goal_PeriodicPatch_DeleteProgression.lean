import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.EncodedPages.PeriodicPatchAlignment

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `PeriodicPatch.DeleteProgression` — `∀ c mask, (((rows layout c).filter (fun r =>
  !contains mask r.1)).map Prod.snd).sum = ((layout.lanes c).map (fun a => partialSum
  a a.n - deletedSum a mask)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PeriodicPatch_DeleteProgression

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.PeriodicPatch

/-- `PeriodicPatch` の行集合は `striped` なレーンの `flatMap` と等しく、
    各レーンでは物理位置の削除が `deletedSum` の減算になる。
    この一般命題は `Auto_EncodedPages_P6` で証明済みなので、それを再利用する。 -/
theorem sound_PeriodicPatch_DeleteProgression : PeriodicPatch.DeleteProgression.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.EncodedPages.PeriodicPatchAlignment.sound_PeriodicPatch_DeleteProgression

end TinylambOpt.ResearchProofs.Atlas.Goal_PeriodicPatch_DeleteProgression
