import TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads
set_option linter.unusedSimpArgs false

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PackedFieldLoads_BytePhaseLoopVersion

open TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads

theorem sound : BytePhaseLoopVersion.Goal := by
  intro w i period hdiv
  rcases hdiv with ⟨k, hk⟩
  have hmain : (i + period) * w = i * w + 8 * k := by
    rw [Nat.add_mul, hk]
  have hmainW : w * (i + period) = i * w + 8 * k := by
    rw [Nat.mul_comm w (i + period), hmain]
  have hpk : period * w / 8 = k := by
    rw [hk]
    omega
  unfold bitOffset
  apply Prod.ext
  · rw [hmain]
    exact Nat.add_mul_mod_self_left (i * w) 8 k
  · rw [hmainW, hpk]
    rw [show w * i = i * w from Nat.mul_comm w i]
    exact Nat.add_mul_div_left (i * w) k (by omega : 0 < 8)

end TinylambOpt.ResearchProofs.JitKernels.Goal_PackedFieldLoads_BytePhaseLoopVersion
