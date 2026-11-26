import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.EncodedPages.TailRunAndWrappingResidueJoins

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `WrappingSequence.ResidueJoin` — `∀ c (b : Lane), ((rows layout c).map (fun r =>
  (List.range b.n).countP (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum =
  ((layout.lanes c).map (joinCount · b)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_WrappingSequence_ResidueJoin

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.WrappingSequence

/-- The cyclic layout's rows are a permutation of the points of its stripes,
    so the per-row join multiplicities add up to the sum of `joinCount` over
    the stripes.  The row/lane correspondence and the generic reduction are
    established in `Auto_EncodedPages_E2`. -/
theorem sound_WrappingSequence_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.WrappingSequence.ResidueJoin.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.EncodedPages.TailRunAndWrappingResidueJoins.WrappingSequence_ResidueJoin

end TinylambOpt.ResearchProofs.Atlas.Goal_WrappingSequence_ResidueJoin
