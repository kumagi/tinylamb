import TinylambOpt.ResearchGoals.Atlas.EncodedPages

import TinylambOpt.ResearchProofs.Atlas_EncodedPages_RestartedDelta_Remaining
import TinylambOpt.ResearchProofs.Atlas_EncodedPages_PagedTombstones_Remaining_Round2

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_P10

/-- RestartedDelta: aligned products over the two restarted lanes.
    The proof is carried by the (already verified) Remaining module. -/
theorem RestartedDelta_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.RestartedDelta.AlignedProduct.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.EncodedPages.RestartedDelta.Remaining.sound_AlignedProduct

/-- PagedTombstones: per-row equality join count against an arbitrary lane. -/
theorem PagedTombstones_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.PagedTombstones.ResidueJoin.Goal := by
  exact TinylambOpt.ResearchProofs.Round2.Task51.sound_ResidueJoin

/-- PagedTombstones: periodic position deletion sum. -/
theorem PagedTombstones_DeleteProgression :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.PagedTombstones.DeleteProgression.Goal := by
  exact TinylambOpt.ResearchProofs.Round2.Task51.sound_DeleteProgression

/-- PagedTombstones: aligned products over the even-tile lanes. -/
theorem PagedTombstones_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.PagedTombstones.AlignedProduct.Goal := by
  exact TinylambOpt.ResearchProofs.Round2.Task51.sound_AlignedProduct

end TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_P10
