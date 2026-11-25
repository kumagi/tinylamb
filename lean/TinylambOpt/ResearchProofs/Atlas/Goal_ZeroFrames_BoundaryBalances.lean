import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
import TinylambOpt.ResearchProofs.Atlas_OrderedMeasures_ZeroFrames_Remaining

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ZeroFrames_BoundaryBalances

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.ZeroFrames

/-!
The statement was first checked exhaustively (all series of length <= 3 over
`[-2,-1,0,1,2]`, via `native_decide`) and found true.  A full structural proof
by induction on the left series already exists in the (unmodified) module
`Atlas_OrderedMeasures_ZeroFrames_Remaining`: it splits the windows of
`b :: xs ++ y` into those staying in `b :: xs` and those ending in `y`,
using `windows_cons`, `range_split` and `sum_take_append_right`; the crossing
windows are exactly the `(suffs (b :: xs)) × (prefs y)` pairs whose sums cancel.
We reuse that checked proof term here.
-/

theorem sound_ZeroFrames_BoundaryBalances : ZeroFrames.BoundaryBalances.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.ZeroFrames.Remaining.sound_BoundaryBalances

end TinylambOpt.ResearchProofs.Atlas.Goal_ZeroFrames_BoundaryBalances
