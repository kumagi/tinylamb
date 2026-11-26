import TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads

/-!
**Proved: bounding the last lane's footprint licenses every earlier lane.**
The read footprint `within bs w i bytes = i * w / 8 + bytes ≤ bs.length` grows
monotonically in the lane index `i`, so if a group's last lane fits inside the
buffer together with its `bytes`-wide read, so does each earlier lane. SQL
execution may check the speculative read bounds once per group instead of per
lane; the check remains sound for all decoded values.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.PackedFieldLoads_GroupLastLaneBoundsHoist
open TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads
open TinylambOpt.ResearchGoals.Compression.Wire

theorem sound : GroupLastLaneBoundsHoist.Goal := by
  intro bs w last bytes h i hi
  unfold within at *
  have h2 : w * i ≤ w * last := Nat.mul_le_mul_left w hi
  have h3 : w * i / 8 ≤ w * last / 8 := Nat.div_le_div_right h2
  have cw1 : w * last = last * w := Nat.mul_comm _ _
  have cw2 : w * i = i * w := Nat.mul_comm _ _
  omega

end TinylambOpt.ResearchProofs.JitKernels.PackedFieldLoads_GroupLastLaneBoundsHoist
