import TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_NullWordFusion_WordAlignedSliceReuse

open TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

theorem sound : WordAlignedSliceReuse.Goal := by
  intro ws base n
  unfold lanes
  rw [List.range_add]
  simp only [List.map_append, List.map_map]
  simp
  intro a ha
  have hdiv : (64 * base + a) / 64 = base + a / 64 := by
    rw [Nat.add_comm]
    rw [Nat.add_mul_div_left a base (by omega : 0 < 64)]
    omega
  rw [hdiv]

end TinylambOpt.ResearchProofs.JitKernels.Goal_NullWordFusion_WordAlignedSliceReuse
