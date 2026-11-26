import TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Common_PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.BackFields_PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Specific_PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Refutations_PackedKeys

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Goal_ScaledKey_Stride

open TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys
open TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys.ScaledKey
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Common
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.BackFields
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Specific

/-- The scaled field value `100 * k` (as an `Int`). -/
def X (k : Nat) : Int := (100 * k : Nat)

theorem xmod (k : Nat) : X k % 100 = 0 := by
  show ((100 * k : Nat) : Int) % 100 = 0
  omega

theorem xdiv (k : Nat) : X k / 100 = (k : Int) := by
  show ((100 * k : Nat) : Int) / 100 = _
  omega

theorem uX (k : Nat) : unsigned (X k / 100) = some k := by
  rw [xdiv, unsigned, if_neg (by omega : ¬((k : Int) < 0))]
  exact congrArg some (by omega)

theorem u0 : unsigned (0 : Int) = some 0 := by
  unfold unsigned
  rw [if_neg (by omega : ¬((0 : Int) < 0))]
  rfl

theorem stride_fields (k : Nat) (hk : k < 2 ^ 32) :
    ScaledKey.codec.fields (pair (X k) 0) = some (k, 0) := by
  have hcx : (X k % 100 == 0) = true := by
    have h := xmod k
    simp [h]
  show (if (X k % 100 == 0) then
      Bind.bind (unsigned (X k / 100)) fun a =>
        Option.map (fun b => (a, b)) (unsigned (0 : Int))
      else none) = some (k, 0)
  rw [if_pos hcx, uX, u0]
  rfl

theorem stride : Stride.Goal := by
  intro k hk
  have hf : ScaledKey.codec.fields (pair (100 * (k : Nat)) 0) = some (k, 0) :=
    stride_fields k hk
  rw [encode_eq_some ScaledKey.codec k 0 hf hk (by decide : 0 < 2 ^ 32)]
  unfold ScaledKey.codec
  rw [Nat.add_zero]

end TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Goal_ScaledKey_Stride
