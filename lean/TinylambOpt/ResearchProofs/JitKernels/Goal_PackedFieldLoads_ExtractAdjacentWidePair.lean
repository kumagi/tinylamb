import TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads
import TinylambOpt.ResearchProofs.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PackedFieldLoads_ExtractAdjacentWidePair

open TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchProofs.Compression.Wire


theorem sound : TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads.ExtractAdjacentWidePair.Goal := by
  intro bs w j hcanon
  rw [sound_LocalRead bs (2*w) j hcanon,
      sound_LocalRead bs w (2*j) hcanon,
      sound_LocalRead bs w (2*j+1) hcanon]
  unfold TinylambOpt.ResearchGoals.Compression.Wire.get
  have hp : (2*j)*w = j*(2*w) := by
    rw [show 2*j = j*2 by omega]
    rw [Nat.mul_assoc]
  have hp2 : (2*j+1)*w = j*(2*w) + w := by
    rw [Nat.add_mul, Nat.one_mul, hp]
  rw [hp, hp2]
  have h2w : 2^(2*w) = 2^w * 2^w := by
    rw [show 2*w = w+w by omega, Nat.pow_add]
  rw [h2w]
  have hdiv : fromLittle bs / 2^(j*(2*w)+w) = (fromLittle bs / 2^(j*(2*w))) / 2^w := by
    rw [Nat.pow_add, Nat.div_div_eq_div_mul]
  rw [hdiv]
  exact Nat.mod_mul (a := 2^w) (b := 2^w) (x := fromLittle bs / 2^(j*(2*w)))

end TinylambOpt.ResearchProofs.JitKernels.Goal_PackedFieldLoads_ExtractAdjacentWidePair