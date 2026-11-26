import TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Common_PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.BackFields_PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Specific_PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Refutations_PackedKeys

/-!
SQL grouping or ordering can use a packed key when the representation preserves the
relevant comparisons and validity bits.

Proved

- `ZigzagPair.NearbyNegative` — `encode codec (pair 0 (-1))=some 1`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Goal_ZigzagPair_NearbyNegative

open TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys
open TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys.ZigzagPair
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Common
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.BackFields
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Specific

theorem sound : ZigzagPair.NearbyNegative.Goal := nearbyNegative

end TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Goal_ZigzagPair_NearbyNegative
