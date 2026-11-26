import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.EncodedPages.SparseSampleProgressions

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `SparseSample.AlignedProduct` — `∀ c (b : Lane), 0 < b.stride → ((rows layout c).map
  (fun r => ((List.range b.n).map (fun i => if r.1 == b.start+i*b.stride then
  r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum = ((layout.lanes c).map (aligned ·
  b)).sum`
- `SparseSample.DeleteProgression` — `∀ c mask, (((rows layout c).filter (fun r =>
  !contains mask r.1)).map Prod.snd).sum = ((layout.lanes c).map (fun a => partialSum
  a a.n - deletedSum a mask)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! Batch P4: remaining `SparseSample` Atlas EncodedPages goals.

The CRT core and the generic row/lane permutation lemmas for these two goals
live in `TinylambOpt.ResearchProofs.Atlas.EncodedPages.SparseSampleProgressions`
(a self-contained module importing only the goal file).  That module proves
all three `SparseSample` goals; here we expose the two goals assigned to this
batch under fully qualified statements so they are picked up by proof checks.
-/
namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages.SparseSampleDeletesAndProduct

theorem SparseSample_DeleteProgression :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.SparseSample.DeleteProgression.Goal :=
  TinylambOpt.ResearchProofs.Atlas.EncodedPages.SparseSampleProgressions.sound_DeleteProgression

theorem SparseSample_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.SparseSample.AlignedProduct.Goal :=
  TinylambOpt.ResearchProofs.Atlas.EncodedPages.SparseSampleProgressions.sound_AlignedProduct

end TinylambOpt.ResearchProofs.Atlas.EncodedPages.SparseSampleDeletesAndProduct
