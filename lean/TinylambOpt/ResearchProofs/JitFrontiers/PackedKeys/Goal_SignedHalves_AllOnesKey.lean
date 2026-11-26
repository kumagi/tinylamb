import TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Common_PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.BackFields_PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Specific_PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Refutations_PackedKeys

/-!
SQL grouping or ordering can use a packed key when the representation preserves the
relevant comparisons and validity bits.

Proved

- `SignedHalves.AllOnesKey` — `encode codec (pair (-1) (-1))=some (-1)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Goal_SignedHalves_AllOnesKey

open TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys
open TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys.SignedHalves
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Common
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.BackFields
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Specific

theorem sound : SignedHalves.AllOnesKey.Goal := allOnes

end TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Goal_SignedHalves_AllOnesKey
