import TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads
import TinylambOpt.ResearchProofs.Compression.Wire

/-! When a packed page ends on a byte boundary, the bitstream splice is also a
    byte splice: the second page's encoding is shifted by a whole number of
    little-endian bytes. Concatenating the physical page bytes therefore emits
    the same stream as repacking the concatenated lanes, so SQL can run one
    decoder across adjacent aligned frames without copying or re-encoding. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PackedFieldLoads_ByteAlignedFrameConcatenation

open TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchProofs.Compression.Wire

theorem sound : TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads.ByteAlignedFrameConcatenation.Goal := by
  have little_succ : ∀ (n k : Nat), little n (k + 1) = (n % 256) :: little (n / 256) k := by
    intro n k
    apply List.ext_getElem
    · simp [little]
    · intro i hi₁ hi₂
      cases i with
      | zero => simp [little]
      | succ i =>
          simp [little, List.getElem_map, List.getElem_range, Nat.div_div_eq_div_mul,
            Nat.pow_succ, Nat.mul_comm]
  have little_of_canonical : ∀ (bs : Bytes) (hbs : ∀ b ∈ bs, b < 256),
      little (fromLittle bs) bs.length = bs := by
    intro bs hbs
    induction bs with
    | nil => simp [little, fromLittle]
    | cons b bs ih =>
        have hb : b < 256 := hbs b (by simp)
        have hbs' : ∀ x ∈ bs, x < 256 := by
          intro x hx
          exact hbs x (by simp [hx])
        rw [List.length_cons, little_succ]
        simp only [fromLittle]
        have hhead : (b + 256 * fromLittle bs) % 256 = b := by
          rw [Nat.add_mul_mod_self_left b 256 (fromLittle bs)]
          exact Nat.mod_eq_of_lt hb
        have hdiv : (b + 256 * fromLittle bs) / 256 = fromLittle bs := by
          rw [Nat.add_mul_div_left b (fromLittle bs) (by decide : 0 < 256)]
          rw [Nat.div_eq_of_lt hb, Nat.zero_add]
        rw [hhead, hdiv, ih hbs']
  have fromLittle_lt_pow : ∀ (bs : Bytes) (hbs : ∀ b ∈ bs, b < 256),
      fromLittle bs < 256 ^ bs.length := by
    intro bs hbs
    induction bs with
    | nil => simp [fromLittle]
    | cons b bs ih =>
        have hb : b < 256 := hbs b (by simp)
        have hbs' : ∀ x ∈ bs, x < 256 := by
          intro x hx
          exact hbs x (by simp [hx])
        have ih' := ih hbs'
        simp only [fromLittle, List.length_cons, Nat.pow_succ]
        have hle : fromLittle bs + 1 ≤ 256 ^ bs.length := Nat.succ_le_of_lt ih'
        omega
  have little_add_shift : ∀ (x y m k : Nat), x < 256 ^ m →
      little (x + 256 ^ m * y) (m + k) = little x m ++ little y k := by
    intro x y m k hx
    induction m generalizing x with
    | zero =>
        have hx0 : x = 0 := by simpa using hx
        rw [hx0]
        simp [little]
    | succ m ih =>
        have hx' : x / 256 < 256 ^ m := by
          rw [Nat.div_lt_iff_lt_mul (by decide : 0 < 256)]
          simpa [Nat.pow_succ, Nat.mul_comm] using hx
        have hhead : (x + 256 ^ (m + 1) * y) % 256 = x % 256 := by
          have hpow : 256 ^ (m + 1) * y = 256 * (256 ^ m * y) := by
            rw [Nat.pow_succ, Nat.mul_comm (256 ^ m) 256, Nat.mul_assoc]
          rw [hpow]
          exact Nat.add_mul_mod_self_left x 256 (256 ^ m * y)
        have hdiv : (x + 256 ^ (m + 1) * y) / 256 = x / 256 + 256 ^ m * y := by
          have hpow : 256 ^ (m + 1) * y = 256 * (256 ^ m * y) := by
            rw [Nat.pow_succ, Nat.mul_comm (256 ^ m) 256, Nat.mul_assoc]
          rw [hpow]
          exact Nat.add_mul_div_left x (256 ^ m * y) (by decide : 0 < 256)
        rw [show m + 1 + k = (m + k) + 1 by omega]
        rw [little_succ, little_succ, hhead, hdiv]
        rw [ih (x / 256) hx']
        simp
  have pack_canonical : ∀ (w : Nat) (xs : List Nat), ∀ b ∈ pack w xs, b < 256 := by
    intro w xs b hb
    unfold pack little at hb
    rw [List.mem_map] at hb
    rcases hb with ⟨i, hi, rfl⟩
    exact Nat.mod_lt _ (by decide : 0 < 256)
  intro w a b halign
  rw [sound_BitSplice w a b]
  have ha_length : byteCount (a.length * w) = a.length * w / 8 := by
    unfold byteCount
    rcases halign with ⟨k, hk⟩
    rw [hk]
    rw [show 8 * k + 7 = 7 + 8 * k by omega]
    rw [Nat.add_mul_div_left 7 k (by decide : 0 < 8)]
    simp
  have hab_length : byteCount ((a.length + b.length) * w) =
      a.length * w / 8 + byteCount (b.length * w) := by
    unfold byteCount at ha_length ⊢
    rw [Nat.add_mul]
    rcases halign with ⟨k, hk⟩
    rw [hk]
    rw [show 8 * k + b.length * w + 7 = (b.length * w + 7) + 8 * k by omega]
    rw [Nat.add_mul_div_left (b.length * w + 7) k (by decide : 0 < 8)]
    simp [Nat.add_comm]
  let m := a.length * w / 8
  have hcancel : 8 * m = a.length * w := by
    dsimp [m]
    exact Nat.mul_div_cancel' halign
  have hm_pow : 256 ^ m = 2 ^ (a.length * w) := by
    rw [← hcancel]
    have h256 : (256 : Nat) = 2 ^ 8 := by decide
    rw [h256, Nat.pow_mul]
  have hlen' : (pack w a).length = m := by
    rw [sound_PackedCharge w a, ha_length]
  have hfroma_lt : fromLittle (pack w a) < 256 ^ m := by
    rw [← hlen']
    exact fromLittle_lt_pow (pack w a) (pack_canonical w a)
  rw [hab_length]
  rw [Nat.mul_comm w a.length, ← hm_pow]
  rw [← hlen']
  rw [show a.length * w / 8 = (pack w a).length by exact hlen'.symm]
  rw [little_add_shift (fromLittle (pack w a)) (fromLittle (pack w b))
    ((pack w a).length) (byteCount (b.length * w)) (by rw [hlen']; exact hfroma_lt)]
  rw [little_of_canonical (pack w a) (pack_canonical w a)]
  rw [← sound_PackedCharge w b]
  rw [little_of_canonical (pack w b) (pack_canonical w b)]

end TinylambOpt.ResearchProofs.JitKernels.Goal_PackedFieldLoads_ByteAlignedFrameConcatenation
