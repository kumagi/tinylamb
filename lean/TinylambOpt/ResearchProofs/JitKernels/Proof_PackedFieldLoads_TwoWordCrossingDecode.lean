import TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads
import TinylambOpt.ResearchProofs.Compression.Wire

/-!
A 16-byte window contains every field of width at most 64 bits regardless of
its bit phase. The extra word only contributes bits beyond `2^w` after the
phase shift, so masking recovers exactly the same value as the narrow
`takeAt` window. This licenses a single two-word speculation for wide packed
loads without changing the decoded lane value.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_PackedFieldLoads_TwoWordCrossingDecode
open TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchProofs.Compression.Wire

private theorem fromLittle_append (l₁ l₂ : Bytes) :
    fromLittle (l₁ ++ l₂) = fromLittle l₁ + 256 ^ l₁.length * fromLittle l₂ := by
  induction l₁ with
  | nil => simp [fromLittle]
  | cons b l₁ ih =>
      simp only [List.cons_append, fromLittle]
      rw [ih]
      simp only [List.length_cons, Nat.pow_succ]
      rw [Nat.mul_comm (256 ^ l₁.length) 256, Nat.mul_assoc]
      omega

private theorem fromLittle_take_mod (bs : Bytes) (m : Nat) (hbs : ∀ b ∈ bs, b < 256) :
    fromLittle (bs.take m) = fromLittle bs % 256 ^ m := by
  induction bs generalizing m with
  | nil => simp [fromLittle, Nat.zero_mod]
  | cons b bs ih =>
      cases m with
      | zero => simp [fromLittle, Nat.mod_one]
      | succ m =>
          have hb : b < 256 := hbs b (by simp)
          have hbs' : ∀ x ∈ bs, x < 256 := by intro x hx; exact hbs x (by simp [hx])
          simp only [List.take_succ_cons, fromLittle]
          rw [ih m hbs']
          have hMpos : 0 < 256 ^ m := Nat.pow_pos (by decide : 0 < 256)
          have hpowsucc : 256 ^ (m + 1) = 256 ^ m * 256 := by rw [Nat.pow_succ]
          rw [hpowsucc]
          have hlt : b + 256 * (fromLittle bs % 256 ^ m) < 256 ^ m * 256 := by
            have hmodlt := Nat.mod_lt (fromLittle bs) hMpos
            omega
          have hdecomp : b + 256 * fromLittle bs =
              (b + 256 * (fromLittle bs % 256 ^ m)) + (256 ^ m * 256) * (fromLittle bs / 256 ^ m) := by
            have hmad := Nat.mod_add_div (fromLittle bs) (256 ^ m)
            calc
              b + 256 * fromLittle bs
                  = b + 256 * (fromLittle bs % 256 ^ m + 256 ^ m * (fromLittle bs / 256 ^ m)) := by rw [hmad]
              _ = b + (256 * (fromLittle bs % 256 ^ m) + 256 * (256 ^ m * (fromLittle bs / 256 ^ m))) := by rw [Nat.mul_add]
              _ = (b + 256 * (fromLittle bs % 256 ^ m)) + 256 * (256 ^ m * (fromLittle bs / 256 ^ m)) := by rw [Nat.add_assoc]
              _ = (b + 256 * (fromLittle bs % 256 ^ m)) + (256 ^ m * 256) * (fromLittle bs / 256 ^ m) := by ac_rfl
          rw [hdecomp, Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hlt]

private theorem fromLittle_drop_div (bs : Bytes) (s : Nat) (hbs : ∀ b ∈ bs, b < 256) :
    fromLittle (bs.drop s) = fromLittle bs / 256 ^ s := by
  induction s generalizing bs with
  | zero => simp
  | succ s ih =>
      cases bs with
      | nil => simp [fromLittle, Nat.zero_div]
      | cons b bs =>
          have hb : b < 256 := hbs b (by simp)
          have hbs' : ∀ x ∈ bs, x < 256 := by intro x hx; exact hbs x (by simp [hx])
          simp only [List.drop_succ_cons, fromLittle]
          rw [ih bs hbs']
          rw [Nat.add_comm b (256 * fromLittle bs)]
          rw [Nat.pow_succ, Nat.mul_comm (256 ^ s) 256, ← Nat.div_div_eq_div_mul]
          rw [Nat.add_comm (256 * fromLittle bs) b]
          rw [Nat.add_mul_div_left b (fromLittle bs) (by decide : 0 < 256)]
          rw [Nat.div_eq_of_lt hb, Nat.zero_add]

theorem sound : TwoWordCrossingDecode.Goal := by
  intro bs w i hw hbs
  rw [sound_LocalRead bs w i hbs]
  unfold load
  let start := i * w / 8
  let shift := i * w % 8
  have hfl : fromLittle ((bs.drop start).take 16) = (fromLittle bs / 256 ^ start) % 256 ^ 16 := by
    rw [fromLittle_take_mod _ _ (by intro b hb; exact hbs b (List.mem_of_mem_drop hb))]
    rw [fromLittle_drop_div _ _ hbs]
  change (fromLittle ((bs.drop start).take 16) / 2 ^ shift) % 2 ^ w =
    (fromLittle bs / 2 ^ (i * w)) % 2 ^ w
  rw [hfl]
  have hshiftlt : shift < 8 := Nat.mod_lt _ (by decide : 0 < 8)
  have hwide : shift + w ≤ 8 * 16 := by omega
  have hpow256 : 256 ^ 16 = 2 ^ (8 * 16) := by
    have h256 : (256 : Nat) = 2 ^ 8 := by decide
    rw [h256, Nat.pow_mul]
  let q := fromLittle bs / 256 ^ start
  let r := q % 256 ^ 16
  let t := q / 256 ^ 16
  have hq : q = r + 256 ^ 16 * t := by
    dsimp [r, t]
    exact (Nat.mod_add_div q (256 ^ 16)).symm
  have hdivq : q / 2 ^ shift = r / 2 ^ shift + 2 ^ (8 * 16 - shift) * t := by
    rw [hq, hpow256]
    have hfactor : 2 ^ (8 * 16) = 2 ^ shift * 2 ^ (8 * 16 - shift) := by
      rw [← Nat.pow_add]
      congr 1
      omega
    rw [hfactor, Nat.mul_assoc]
    exact Nat.add_mul_div_left r (2 ^ (8 * 16 - shift) * t) (Nat.two_pow_pos shift)
  have hmod_eq : (q / 2 ^ shift) % 2 ^ w = (r / 2 ^ shift) % 2 ^ w := by
    rw [hdivq]
    have hfactor : 2 ^ (8 * 16 - shift) * t = 2 ^ w * (2 ^ (8 * 16 - shift - w) * t) := by
      have hexp : w + (8 * 16 - shift - w) = 8 * 16 - shift := by omega
      calc
        2 ^ (8 * 16 - shift) * t = 2 ^ (w + (8 * 16 - shift - w)) * t := by rw [hexp]
        _ = (2 ^ w * 2 ^ (8 * 16 - shift - w)) * t := by rw [Nat.pow_add]
        _ = 2 ^ w * (2 ^ (8 * 16 - shift - w) * t) := by rw [Nat.mul_assoc]
    rw [hfactor]
    exact Nat.add_mul_mod_self_left (r / 2 ^ shift) (2 ^ w)
      (2 ^ (8 * 16 - shift - w) * t)
  have h256start : 256 ^ start = 2 ^ (8 * start) := by
    have h256 : (256 : Nat) = 2 ^ 8 := by decide
    rw [h256, Nat.pow_mul]
  have hqv : q / 2 ^ shift = fromLittle bs / 2 ^ (i * w) := by
    dsimp [q]
    rw [h256start, Nat.div_div_eq_div_mul]
    have hp : 2 ^ (8 * start) * 2 ^ shift = 2 ^ (i * w) := by
      rw [← Nat.pow_add]
      congr 1
      dsimp [start, shift]
      have hmad := Nat.div_add_mod (i * w) 8
      omega
    rw [hp]
  rw [← hmod_eq, hqv]

end Nov26.Proof_PackedFieldLoads_TwoWordCrossingDecode
