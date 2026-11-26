import TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Common_PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.BackFields_PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Specific_PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Refutations_PackedKeys

/-!
SQL grouping or ordering can use a packed key when the representation preserves the
relevant comparisons and validity bits.

Proved

- `MixedDirection.DistinctGroups` — Use only packed keys to build DISTINCT for admitted
  rows, decoding representatives once.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Goal_MixedDirection_DistinctGroups

open TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys
open TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys.MixedDirection
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Common
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.BackFields
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Specific

theorem sound : MixedDirection.DistinctGroups.Goal := distinctGroups_of_hits MixedDirection.codec (recover_MixedDirection) (hits_eq_beq MixedDirection.codec (collisionFree_of_recover MixedDirection.codec (recover_MixedDirection)))

end TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Goal_MixedDirection_DistinctGroups
