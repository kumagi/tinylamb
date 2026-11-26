import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.EncodedPages.DeltaProgressionsAndTombstoneEpochs

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `RestartedDelta.ResidueJoin` — `∀ c (b : Lane), ((rows layout c).map (fun r =>
  (List.range b.n).countP (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum =
  ((layout.lanes c).map (joinCount · b)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RestartedDelta_ResidueJoin

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.RestartedDelta

/-- The two RestartedDelta lanes partition the physical rows (even when the
switch point lies outside the page).  The per-row join multiplicity therefore
splits into one `joinCount` term per lane; this is established in
`Auto_EncodedPages_E7.RD`, where `rows_lanes_rd` supplies the row/lane
correspondence and `residueJoin_of_perm` the generic reduction. -/
theorem sound_RestartedDelta_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.RestartedDelta.ResidueJoin.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.EncodedPages.DeltaProgressionsAndTombstoneEpochs.RD.RD_ResidueJoin

end TinylambOpt.ResearchProofs.Atlas.Goal_RestartedDelta_ResidueJoin
