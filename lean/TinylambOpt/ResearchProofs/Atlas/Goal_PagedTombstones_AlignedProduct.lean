import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas_EncodedPages_PagedTombstones_Remaining_Round2

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
    `TinylambOpt.ResearchProofs.Round2.Task51`. -/
theorem sound_PagedTombstones_AlignedProduct : PagedTombstones.AlignedProduct.Goal := by
  exact TinylambOpt.ResearchProofs.Round2.Task51.sound_AlignedProduct

end TinylambOpt.ResearchProofs.Atlas.Goal_PagedTombstones_AlignedProduct
