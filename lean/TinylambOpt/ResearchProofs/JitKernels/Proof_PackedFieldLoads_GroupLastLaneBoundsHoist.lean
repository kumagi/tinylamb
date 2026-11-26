import TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads

/-!
Once the last lane's fixed-width read fits in the allocation, every earlier
lane's read fits too: the byte offset `i*w/8` is monotone in `i`. This licenses
hoisting one bounds guard for a whole group of speculative lane loads.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_PackedFieldLoads_GroupLastLaneBoundsHoist
open TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads

theorem sound : GroupLastLaneBoundsHoist.Goal := by
  intro bs w last bytes h i hi
  unfold within at h ⊢
  have hle : i * w / 8 ≤ last * w / 8 :=
    Nat.div_le_div_right (Nat.mul_le_mul_right w hi)
  omega

end Nov26.Proof_PackedFieldLoads_GroupLastLaneBoundsHoist
