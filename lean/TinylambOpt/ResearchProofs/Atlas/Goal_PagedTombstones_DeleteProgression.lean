import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.EncodedPages.PagedTombstonesEpochsAndProgressions

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `PagedTombstones.DeleteProgression` — `∀ c mask, (((rows layout c).filter (fun r =>
  !contains mask r.1)).map Prod.snd).sum = ((layout.lanes c).map (fun a => partialSum
  a a.n - deletedSum a mask)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PagedTombstones_DeleteProgression

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.PagedTombstones

/-! The goal is true.  `PagedTombstones` keeps exactly the positions in the
even-numbered tiles, i.e. the residue classes `r < width` modulo `2*width`;
each surviving row is a point of the corresponding constant-step lane.  The
full proof (periodic mask/lane intersection via CRT, the sum of surviving lane
values equals `partialSum - deletedSum`, and the residue-class partition of
the rows) is carried by `TinylambOpt.ResearchProofs.Atlas.EncodedPages.PagedTombstonesEpochsAndProgressions`.  A brute
force sweep over n < 11, parameter < 6, three bases, three steps and all small
masks found no counterexample. -/
theorem sound_PagedTombstones_DeleteProgression : PagedTombstones.DeleteProgression.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.EncodedPages.PagedTombstonesEpochsAndProgressions.sound_DeleteProgression

end TinylambOpt.ResearchProofs.Atlas.Goal_PagedTombstones_DeleteProgression
