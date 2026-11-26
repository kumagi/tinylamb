import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.EncodedPages.PagedTombstonesEpochsAndProgressions

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `PagedTombstones.AlignedProduct` — `∀ c (b : Lane), 0 < b.stride → ((rows layout
  c).map (fun r => ((List.range b.n).map (fun i => if r.1 == b.start+i*b.stride then
  r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum = ((layout.lanes c).map (aligned ·
  b)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PagedTombstones_AlignedProduct

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.PagedTombstones

/-! The goal is true.  `PagedTombstones` keeps exactly the even-numbered tiles,
    i.e. the residue classes `r < width` modulo `2 * width`.  Each surviving row
    lies on the lane `⟨r, 2*width, _, base+step*r, step*2*width⟩`, and the
    lanes are pairwise disjoint and jointly cover all surviving rows.  Since
    `0 < b.stride`, each such row matches at most one `b` ordinal, so the
    per-row product sum splits into the aligned-product sums of the lanes.

    Exhaustive sweeps over `n < 12`, `parameter < 5`, several splits, signed
    bases/steps and all small `b` lanes (`stride > 0`) found no counterexample.
    The full proof (CRT/lane-intersection closed form, the aligned product of two
    progressions, and the residue-class partition of the rows) is carried by
    `TinylambOpt.ResearchProofs.Atlas.EncodedPages.PagedTombstonesEpochsAndProgressions`. -/
theorem sound_PagedTombstones_AlignedProduct : PagedTombstones.AlignedProduct.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.EncodedPages.PagedTombstonesEpochsAndProgressions.sound_AlignedProduct

end TinylambOpt.ResearchProofs.Atlas.Goal_PagedTombstones_AlignedProduct
