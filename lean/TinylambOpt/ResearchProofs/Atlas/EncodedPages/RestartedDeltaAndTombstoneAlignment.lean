import TinylambOpt.ResearchGoals.Atlas.EncodedPages

import TinylambOpt.ResearchProofs.Atlas.EncodedPages.RestartedDeltaAlignedProductAndDeleteProgression
import TinylambOpt.ResearchProofs.Atlas.EncodedPages.PagedTombstonesEpochsAndProgressions

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `PagedTombstones.AlignedProduct` — `∀ c (b : Lane), 0 < b.stride → ((rows layout
  c).map (fun r => ((List.range b.n).map (fun i => if r.1 == b.start+i*b.stride then
  r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum = ((layout.lanes c).map (aligned ·
  b)).sum`
- `PagedTombstones.DeleteProgression` — `∀ c mask, (((rows layout c).filter (fun r =>
  !contains mask r.1)).map Prod.snd).sum = ((layout.lanes c).map (fun a => partialSum
  a a.n - deletedSum a mask)).sum`
- `PagedTombstones.ResidueJoin` — `∀ c (b : Lane), ((rows layout c).map (fun r =>
  (List.range b.n).countP (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum =
  ((layout.lanes c).map (joinCount · b)).sum`
- `RestartedDelta.AlignedProduct` — `∀ c (b : Lane), 0 < b.stride → ((rows layout
  c).map (fun r => ((List.range b.n).map (fun i => if r.1 == b.start+i*b.stride then
  r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum = ((layout.lanes c).map (aligned ·
  b)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages.RestartedDeltaAndTombstoneAlignment

/-- RestartedDelta: aligned products over the two restarted lanes.
    The proof is carried by the (already verified) Remaining module. -/
theorem RestartedDelta_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.RestartedDelta.AlignedProduct.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.EncodedPages.RestartedDeltaAlignedProductAndDeleteProgression.sound_AlignedProduct

/-- PagedTombstones: per-row equality join count against an arbitrary lane. -/
theorem PagedTombstones_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.PagedTombstones.ResidueJoin.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.EncodedPages.PagedTombstonesEpochsAndProgressions.sound_ResidueJoin

/-- PagedTombstones: periodic position deletion sum. -/
theorem PagedTombstones_DeleteProgression :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.PagedTombstones.DeleteProgression.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.EncodedPages.PagedTombstonesEpochsAndProgressions.sound_DeleteProgression

/-- PagedTombstones: aligned products over the even-tile lanes. -/
theorem PagedTombstones_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.PagedTombstones.AlignedProduct.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.EncodedPages.PagedTombstonesEpochsAndProgressions.sound_AlignedProduct

end TinylambOpt.ResearchProofs.Atlas.EncodedPages.RestartedDeltaAndTombstoneAlignment
