import TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads

/-!
Proves `GroupLastLaneBoundsHoist`: the last active lane of a packed field group is bounded by the group extent, making the hoisted bounds check safe.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PackedFieldLoads_GroupLastLaneBoundsHoist

open TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads

theorem sound : GroupLastLaneBoundsHoist.Goal := by
  intro bs w last bytes h i hi
  unfold within at h ⊢
  have hmul : i * w ≤ last * w := Nat.mul_le_mul_right w hi
  have hdiv : i * w / 8 ≤ last * w / 8 := Nat.div_le_div_right hmul
  omega

end TinylambOpt.ResearchProofs.JitKernels.Goal_PackedFieldLoads_GroupLastLaneBoundsHoist
