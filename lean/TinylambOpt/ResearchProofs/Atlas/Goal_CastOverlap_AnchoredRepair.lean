import TinylambOpt.ResearchGoals.Atlas.JoinPlans
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_G1

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! `CastOverlap.AnchoredRepair` is the generic anchored-repair telescoping
identity specialised to the four-edge cast-overlap pattern.  Truth was checked
first exhaustively on small inputs: for n = 2, all 16 boolean databases on the
16 relevant entries of relation 0, all pinned bindings and both flipped
databases, `#eval` reported `true` for every case (see the assigned scratch
check).  The generic lemma `anchored_repair_generic` in
`Atlas_JoinPlans_G1` proves the identity for every pattern with distinct free
variables and no self-loop atoms; both side conditions hold for this pattern
by computation. -/

namespace TinylambOpt.ResearchProofs.Atlas.Goal_CastOverlap_AnchoredRepair

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.CastOverlap

theorem sound_CastOverlap_AnchoredRepair : CastOverlap.AnchoredRepair.Goal := by
  intro n old new b
  exact TinylambOpt.ResearchProofs.Atlas_JoinPlans_G1.anchored_repair_generic
    pattern n old new b (by native_decide) (by native_decide)

end TinylambOpt.ResearchProofs.Atlas.Goal_CastOverlap_AnchoredRepair
