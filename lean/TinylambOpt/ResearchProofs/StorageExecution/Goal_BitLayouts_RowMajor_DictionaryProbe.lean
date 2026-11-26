import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `RowMajor.DictionaryProbe` — Read only lanes requested by a probe and translate them
  through a possibly short dictionary.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RowMajor_DictionaryProbe

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RowMajor

/-! ## bit decomposition helpers -/

private theorem bits_sum_lt (a w : Nat) :
    ((List.range w).map (fun b => if a.testBit b then 2^b else 0)).sum < 2^w := by
  induction w with
  | zero => simp
  | succ w ih =>
      rw [List.range_succ, List.map_append, List.sum_append]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Nat.add_zero]
      by_cases h : a.testBit w
      · simp [h, Nat.pow_succ]
        have hp : 0 < 2^w := Nat.two_pow_pos w
        omega
      · simp [h]
        exact Nat.lt_trans ih (Nat.pow_lt_pow_right (by decide : 1 < 2) (by omega))

private theorem bits_sum_testBit (a w j : Nat) :
    (((List.range w).map (fun b => if a.testBit b then 2^b else 0)).sum).testBit j =
      if j < w then a.testBit j else false := by
  induction w with
  | zero => simp
  | succ w ih =>
      rw [List.range_succ, List.map_append, List.sum_append]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Nat.add_zero]
      by_cases h : a.testBit w
      · simp [h]
        rw [Nat.add_comm]
        by_cases hj : j < w
        · rw [Nat.testBit_two_pow_add_gt hj, ih]
          have hjlt : j < w + 1 := by omega
          simp [hj, hjlt]
        · by_cases hjw : j = w
          · subst j
            rw [Nat.testBit_two_pow_add_eq]
            have hSfalse : (((List.range w).map (fun b => if a.testBit b then 2^b else 0)).sum).testBit w = false :=
              Nat.testBit_lt_two_pow (bits_sum_lt a w)
            simp [hSfalse, h]
          · have hlt : 2^w + ((List.range w).map (fun b => if a.testBit b then 2^b else 0)).sum < 2^j := by
              have hSlt := bits_sum_lt a w
              have hpowle : 2^(w+1) ≤ 2^j := Nat.pow_le_pow_right (by decide : 0 < 2) (by omega)
              rw [Nat.pow_add_one'] at hpowle
              have hp : 0 < 2^w := Nat.two_pow_pos w
              omega
            rw [Nat.testBit_lt_two_pow hlt]
            have hjnot : ¬ j < w + 1 := by omega
            simp [hjnot]
      · simp [h, ih]
        by_cases hj : j < w
        · have hjlt : j < w + 1 := by omega
          simp [hj, hjlt]
        · by_cases hjw : j = w
          · subst j
            have hjnot : ¬ w < w := by omega
            simp [hjnot, h]
          · have hjnot : ¬ j < w + 1 := by omega
            simp [hj, hjnot]

private theorem bits_sum (a w : Nat) :
    ((List.range w).map (fun b => if a.testBit b then 2^b else 0)).sum = a % 2^w := by
  apply Nat.eq_of_testBit_eq
  intro j
  rw [Nat.testBit_mod_two_pow, bits_sum_testBit]
  by_cases hj : j < w <;> simp [hj]

/-! ## the row-major image is the packed word -/

private theorem foldPaint_testBit_true (bs : List Nat) (base x acc p : Nat) :
    (bs.foldl (fun a b => if x.testBit b then a ||| 2^(base+b) else a) acc).testBit p = true ↔
      acc.testBit p = true ∨ ∃ b ∈ bs, x.testBit b = true ∧ base+b = p := by
  induction bs generalizing acc with
  | nil => simp
  | cons b bs ih =>
      rw [List.foldl_cons, ih]
      constructor
      · intro h
        rcases h with hA | hTail
        · by_cases hxb : x.testBit b
          · simp [hxb, Nat.testBit_or] at hA
            rcases hA with hAcc | hPow
            · exact Or.inl hAcc
            · have hbp : base+b = p := by
                simpa [Nat.testBit_two_pow] using hPow
              exact Or.inr ⟨b, by simp, hxb, hbp⟩
          · have hAcc : acc.testBit p = true := by simpa [hxb] using hA
            exact Or.inl hAcc
        · rcases hTail with ⟨z, hz, hxz, hzp⟩
          exact Or.inr ⟨z, by simp [hz], hxz, hzp⟩
      · intro h
        rcases h with hAcc | hHead
        · by_cases hxb : x.testBit b
          · simp [hxb, Nat.testBit_or, hAcc]
          · have h : (if x.testBit b then acc ||| 2^(base+b) else acc).testBit p = true := by
              simpa [hxb] using hAcc
            exact Or.inl h
        · rcases hHead with ⟨z, hz, hxz, hzp⟩
          by_cases hzb : z = b
          · subst z
            subst p
            simp [hxz, Nat.testBit_or]
          · have hzin : z ∈ bs := by simpa [hzb] using hz
            right
            exact ⟨z, hzin, hxz, hzp⟩

private def paintBits (w base : Nat) (x acc : Nat) : Nat :=
  (List.range w).foldl (fun a b => if x.testBit b then a ||| 2^(base+b) else a) acc

private theorem paintBits_testBit_true (w base x acc p : Nat) :
    (paintBits w base x acc).testBit p = true ↔
      acc.testBit p = true ∨ (base ≤ p ∧ p < base+w ∧ x.testBit (p-base) = true) := by
  unfold paintBits
  rw [foldPaint_testBit_true]
  constructor
  · intro h
    rcases h with hacc | ⟨b, hbmem, hxb, hbp⟩
    · exact Or.inl hacc
    · have hb : b < w := by simpa using hbmem
      have hbase : base ≤ p := by omega
      have hp : p < base+w := by omega
      have hsub : p-base = b := by omega
      subst p
      exact Or.inr ⟨hbase, hp, by simpa [hsub] using hxb⟩
  · intro h
    rcases h with hacc | ⟨hbase, hp, hx⟩
    · exact Or.inl hacc
    · exact Or.inr ⟨p-base, by simp; omega, hx, by omega⟩

private def imageAtAcc (w start acc : Nat) (xs : List Nat) : Nat :=
  (xs.zipIdx start).foldl (fun a (x,i) => paintBits w (i*w) x a) acc

private theorem imageAtAcc_testBit_true (w start acc p : Nat) (xs : List Nat) :
    (imageAtAcc w start acc xs).testBit p = true ↔
      acc.testBit p = true ∨ ∃ j b, j < xs.length ∧ b < w ∧
        p = (start+j)*w+b ∧ (xs[j]?.getD 0).testBit b = true := by
  induction xs generalizing start acc with
  | nil => simp [imageAtAcc]
  | cons x xs ih =>
      rw [imageAtAcc, List.zipIdx_cons, List.foldl_cons]
      change (imageAtAcc w (start+1) (paintBits w (start*w) x acc) xs).testBit p = true ↔ _
      rw [ih, paintBits_testBit_true]
      constructor
      · intro h
        rcases h with (hAcc | hHead) | hTail
        · exact Or.inl hAcc
        · rcases hHead with ⟨hlo, hhi, hx⟩
          have hb : p - start*w < w := by omega
          have hp : p = start*w + (p-start*w) := by omega
          exact Or.inr ⟨0, p-start*w, by simp, hb, hp, by simpa using hx⟩
        · rcases hTail with ⟨j, b, hj, hb, hp, hx⟩
          exact Or.inr ⟨j+1, b, by simp [hj], hb,
            by simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using hp, by simpa using hx⟩
      · intro h
        rcases h with hAcc | hFound
        · exact Or.inl (Or.inl hAcc)
        · rcases hFound with ⟨j, b, hj, hb, hp, hx⟩
          cases j with
          | zero =>
              simp at hp
              apply Or.inl
              apply Or.inr
              have hb' : start*w ≤ p := by omega
              have hb'' : p < start*w+w := by omega
              have hsub : p-start*w = b := by omega
              exact ⟨hb', hb'', by simpa [hsub] using hx⟩
          | succ j =>
              apply Or.inr
              refine ⟨j, b, by simpa using hj, hb, ?_, by simpa using hx⟩
              simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using hp

private theorem word_testBit_true (w p : Nat) (xs : List Nat) :
    (word w xs).testBit p = true ↔
      ∃ j b, j < xs.length ∧ b < w ∧ p = j*w+b ∧ (xs[j]?.getD 0).testBit b = true := by
  induction xs generalizing p with
  | nil => simp [word]
  | cons x xs ih =>
      rw [word, Nat.add_comm]
      rw [Nat.testBit_two_pow_mul_add (word w xs) (Nat.mod_lt x (Nat.two_pow_pos w)) p]
      by_cases hp : p < w
      · simp [hp]
        constructor
        · intro hxbit
          refine ⟨0, by simp, p, hp, ?_, ?_⟩
          · simp
          · simpa using hxbit
        · intro h
          rcases h with ⟨j, hj, b, hb, hpeq, hxb⟩
          have hj0 : j = 0 := by
            by_cases hzero : j = 0
            · exact hzero
            · have hjge : 1 ≤ j := by omega
              have hmul : w ≤ j*w := by
                simpa [Nat.one_mul] using Nat.mul_le_mul_right w hjge
              omega
          subst j
          simp at hpeq
          subst p
          simpa using hxb
      · simp [hp]
        rw [ih]
        constructor
        · intro h
          rcases h with ⟨j, b, hj, hb, hpeq, hxb⟩
          refine ⟨j+1, by simp [hj], b, hb, ?_, ?_⟩
          · rw [Nat.add_mul, Nat.one_mul]
            omega
          · simpa using hxb
        · intro h
          rcases h with ⟨j, hj, b, hb, hpeq, hxb⟩
          cases j with
          | zero =>
              simp at hpeq
              omega
          | succ j =>
              refine ⟨j, b, by simpa using hj, hb, ?_, by simpa using hxb⟩
              rw [Nat.add_mul, Nat.one_mul] at hpeq
              omega

private theorem image_eq_imageAtAcc (w : Nat) (xs : List Nat) :
    image layout w xs = imageAtAcc w 0 0 xs := by
  simp [image, layout, imageAtAcc, paintBits, lanes]

private theorem image_eq_word (w : Nat) (xs : List Nat) :
    image layout w xs = word w xs := by
  rw [image_eq_imageAtAcc]
  apply Nat.eq_of_testBit_eq
  intro p
  apply (Bool.eq_iff_iff).2
  constructor
  · intro h
    rcases (imageAtAcc_testBit_true w 0 0 p xs).1 h with hacc | hfound
    · simp at hacc
    · rcases hfound with ⟨j, b, hj, hb, hp, hxb⟩
      simp at hp
      exact (word_testBit_true w p xs).2 ⟨j, b, hj, hb, hp, hxb⟩
  · intro h
    rcases (word_testBit_true w p xs).1 h with ⟨j, b, hj, hb, hp, hxb⟩
    exact (imageAtAcc_testBit_true w 0 0 p xs).2 (Or.inr ⟨j, b, hj, hb, by simpa using hp, hxb⟩)

private theorem file_eq_pack (w : Nat) (xs : List Nat) :
    file layout w xs = pack w xs := by
  simp only [file, pack]
  rw [image_eq_word]
  rfl

/-! ## reading a physical bit back from the serialized bytes -/

private theorem bits_le_byteCount (bits : Nat) : bits ≤ 8 * byteCount bits := by
  unfold byteCount
  have hm := Nat.mod_lt (bits + 7) (by decide : 0 < 8)
  have hd := Nat.mod_add_div (bits + 7) 8
  omega

private theorem atByte_little (m k j : Nat) (hj : j < k) :
    atByte (little m k) j = (m / 256^j) % 256 := by
  simp [atByte, little, hj]

private theorem byte_testBit_little (m k p : Nat) (hk : p/8 < k) :
    (atByte (little m k) (p/8)).testBit (p%8) = m.testBit p := by
  rw [atByte_little m k (p/8) hk]
  have h256 : (256 : Nat) = 2^8 := by decide
  rw [h256, ← Nat.pow_mul]
  have hr : p % 8 < 8 := Nat.mod_lt _ (by decide : 0 < 8)
  simp only [Nat.testBit_mod_two_pow, decide_true, Bool.true_and]
  rw [← Nat.testBit_add]
  simp [hr]
  rw [Nat.mod_add_div]

private theorem fetch_rowmajor (w : Nat) (xs : List Nat) (i : Nat) (hi : i < xs.length) :
    fetch layout xs.length w i (atByte (file layout w xs)) = xs[i]?.getD 0 % 2^w := by
  let n := xs.length
  let k := byteCount (n*w)
  let m := word w xs
  have hfile : file layout w xs = little m k := by
    dsimp [m, k, n]
    rw [file_eq_pack]
    rfl
  have hbit (b : Nat) (hb : b < w) :
      (atByte (file layout w xs) ((i*w+b)/8)).testBit ((i*w+b)%8) =
        (xs[i]?.getD 0).testBit b := by
    have hiw : (i+1)*w ≤ n*w := by
      dsimp [n]
      exact Nat.mul_le_mul_right w (Nat.succ_le_of_lt hi)
    have hp_lt : i*w+b < n*w := by
      have : i*w + b < (i+1)*w := by
        rw [Nat.add_mul, Nat.one_mul]
        omega
      omega
    have hk : (i*w+b)/8 < k := by
      dsimp [k]
      exact Nat.div_lt_of_lt_mul (Nat.lt_of_lt_of_le hp_lt (bits_le_byteCount (n*w)))
    rw [hfile, byte_testBit_little m k (i*w+b) hk]
    dsimp [m]
    apply (Bool.eq_iff_iff).2
    constructor
    · intro h
      have h' := (word_testBit_true w (i*w+b) xs).1 h
      rcases h' with ⟨j, c, hj, hc, hp, hxc⟩
      have hwpos : 0 < w := by omega
      have hmod := congrArg (fun x : Nat => x % w) hp
      have hcb : c = b := by
        simpa [Nat.add_mod, Nat.mul_mod_left, Nat.mod_eq_of_lt hb, Nat.mod_eq_of_lt hc] using hmod.symm
      subst c
      have hij : i = j := Nat.mul_right_cancel hwpos (Nat.add_right_cancel hp)
      subst j
      simpa using hxc
    · intro h
      exact (word_testBit_true w (i*w+b) xs).2 ⟨i, b, by simpa [n] using hi, hb, rfl, h⟩
  unfold fetch
  change ((lanes w).map (fun b =>
    if (atByte (file layout w xs) ((i*w+b)/8)).testBit ((i*w+b)%8) then 2^b else 0)).sum = _
  rw [show ((lanes w).map (fun b =>
    if (atByte (file layout w xs) ((i*w+b)/8)).testBit ((i*w+b)%8) then 2^b else 0)).sum =
      ((List.range w).map (fun b => if (xs[i]?.getD 0).testBit b then 2^b else 0)).sum by
        apply congrArg List.sum
        apply List.map_congr_left
        intro b hbmem
        have hb : b < w := by simpa [lanes] using hbmem
        simp [hbit b hb]]
  exact bits_sum (xs[i]?.getD 0) w

theorem sound_DictionaryProbe : RowMajor.DictionaryProbe.Goal := by
  intro w xs dict keys hkeys
  apply List.map_congr_left
  intro i hi
  have := fetch_rowmajor w xs i (hkeys i hi)
  rw [this]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RowMajor_DictionaryProbe
