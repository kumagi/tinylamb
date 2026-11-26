import TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads
import TinylambOpt.ResearchProofs.Compression.Wire

/-!
Two adjacent `w`-bit lanes form one `2*w`-bit lane at the first lane's bit
offset. After replacing bounded windows by the canonical `get` model, this is
the ordinary base-`2^w` decomposition of a bit chunk: the low lane is the
remainder and the high lane is the quotient's remainder. This licenses reading
a pair through one wider load without changing either decoded value.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_PackedFieldLoads_ExtractAdjacentWidePair
open TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchProofs.Compression.Wire

private theorem mod_mul_self (v m : Nat) :
    v % (m * m) = v % m + m * ((v / m) % m) := by
  calc
    v % (m * m) = m * (v % (m * m) / m) + v % (m * m) % m :=
      (Nat.div_add_mod (v % (m * m)) m).symm
    _ = m * (v / m % m) + v % m := by
      rw [Nat.mod_mul_right_div_self, Nat.mod_mul_right_mod]
    _ = v % m + m * ((v / m) % m) := by omega

theorem sound : ExtractAdjacentWidePair.Goal := by
  intro bs w j hbs
  rw [sound_LocalRead bs (2 * w) j hbs]
  rw [sound_LocalRead bs w (2 * j) hbs]
  rw [sound_LocalRead bs w (2 * j + 1) hbs]
  unfold TinylambOpt.ResearchGoals.Compression.Wire.get
  let N := fromLittle bs
  let e := j * (2 * w)
  let q := N / 2 ^ e
  have he : (2 * j) * w = j * (2 * w) := by
    ac_rfl
  have hlow : N / 2 ^ ((2 * j) * w) = q := by
    dsimp [q, e]
    rw [he]
  have hhigh : N / 2 ^ ((2 * j + 1) * w) = q / 2 ^ w := by
    dsimp [q, e]
    rw [Nat.div_div_eq_div_mul]
    rw [← Nat.pow_add]
    have he2 : (2 * j + 1) * w = j * (2 * w) + w := by
      rw [Nat.add_mul, Nat.one_mul, he]
    rw [he2]
  have hmod : q % 2 ^ (2 * w) = q % 2 ^ w + 2 ^ w * ((q / 2 ^ w) % 2 ^ w) := by
    have hp : 2 ^ w * 2 ^ w = 2 ^ (2 * w) := by
      rw [← Nat.pow_add]
      congr 1
      omega
    rw [← hp]
    exact mod_mul_self q (2 ^ w)
  rw [hlow, hhigh, hmod]

end Nov26.Proof_PackedFieldLoads_ExtractAdjacentWidePair
