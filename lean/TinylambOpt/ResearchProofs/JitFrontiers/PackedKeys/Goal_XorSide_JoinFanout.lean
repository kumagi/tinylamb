import TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Common_PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.BackFields_PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Specific_PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Refutations_PackedKeys

/-!
SQL grouping or ordering can use a packed key when the representation preserves the
relevant comparisons and validity bits.

Proved

- `XorSide.JoinFanout` — Use one native word comparison without changing
  duplicate-preserving join fanout.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Goal_XorSide_JoinFanout

open TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys
open TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys.XorSide
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Common
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.BackFields
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Specific

theorem sound : XorSide.JoinFanout.Goal := joinFanout_of_hits XorSide.codec (hits_eq_beq XorSide.codec (collisionFree_of_recover XorSide.codec (recover_XorSide)))

end TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Goal_XorSide_JoinFanout
