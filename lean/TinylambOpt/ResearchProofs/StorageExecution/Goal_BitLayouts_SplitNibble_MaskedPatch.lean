import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SplitNibble_MaskedPatch

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SplitNibble

/-! Shared, self-contained bit-level lemmas.  The patched value is compared to
the original one by extensionality on `Nat.testBit`; all that is needed is that
`layout.bit` is injective on its (row, lane) arguments. -/

private theorem foldl_gen_testBit {α : Type} (xs : List α) (step : Nat → α → Nat)
    (a p : Nat) (g : α → Bool)
    (hstep : ∀ acc x, (step acc x).testBit p = (acc.testBit p || g x)) :
    (xs.foldl step a).testBit p = (a.testBit p || xs.any g) := by
  induction xs generalizing a with
  | nil => simp
  | cons x xs ih =>
      rw [List.foldl_cons, ih (step a x), hstep a x, List.any_cons]
      simp [Bool.or_assoc]

private theorem foldl_if_testBit {α : Type} (xs : List α) (f : α → Nat)
    (c : α → Bool) (a p : Nat) :
    (xs.foldl (fun acc x => if c x then acc ||| f x else acc) a).testBit p =
      (a.testBit p || xs.any (fun x => c x && (f x).testBit p)) := by
  apply foldl_gen_testBit
  intro acc x
  by_cases hx : c x = true
  · rw [if_pos hx, Nat.testBit_or]
    simp [hx]
  · have hx' : c x = false := Bool.eq_false_iff.mpr hx
    rw [if_neg hx]
    simp [hx']

private theorem rowMask_testBit (l : Layout) (n w i p : Nat) :
    (rowMask l n w i).testBit p =
      (lanes w).any (fun b => decide (l.bit n w i b = p)) := by
  unfold rowMask lanes
  rw [foldl_gen_testBit (List.range w)
    (fun acc b => acc ||| 2 ^ l.bit n w i b) (0 : Nat) p
    (fun b => decide (l.bit n w i b = p))]
  · simp [Nat.testBit_two_pow]
  · intro acc b
    rw [Nat.testBit_or, Nat.testBit_two_pow]

private theorem rowBits_testBit (l : Layout) (n w i x p : Nat) :
    (rowBits l n w i x).testBit p =
      (lanes w).any (fun b => x.testBit b && decide (l.bit n w i b = p)) := by
  unfold rowBits lanes
  rw [foldl_if_testBit]
  simp [Nat.testBit_two_pow]

private theorem any_zipIdx_eq_range (xs : List Nat) (f : Nat → Nat → Bool) :
    xs.zipIdx.any (fun z => f z.1 z.2) =
      (List.range xs.length).any (fun i => f (xs[i]?.getD 0) i) := by
  apply Bool.eq_iff_iff.mpr
  simp only [List.any_eq_true]
  constructor
  · rintro ⟨⟨x, i⟩, hz, hf⟩
    have hi : i < xs.length := (List.mem_zipIdx' hz).1
    have hx : x = xs[i] := (List.mem_zipIdx' hz).2
    have hval : xs[i]?.getD 0 = xs[i] := by
      rw [List.getElem?_eq_getElem hi]
      simp
    refine ⟨i, by simpa using hi, ?_⟩
    simpa [hx, hval] using hf
  · rintro ⟨i, hi, hf⟩
    have hi' : i < xs.length := by simpa using hi
    have hx : xs[i]? = some (xs[i]?.getD 0) := by
      rw [List.getElem?_eq_getElem hi']
      simp
    refine ⟨(xs[i]?.getD 0, i), ?_, hf⟩
    simpa using (List.mem_zipIdx_iff_getElem?.mpr hx)

private theorem image_testBit (l : Layout) (w : Nat) (ys : List Nat) (p : Nat) :
    (image l w ys).testBit p =
      (List.range ys.length).any
        (fun j => (rowBits l ys.length w j (ys[j]?.getD 0)).testBit p) := by
  unfold image
  rw [foldl_gen_testBit ys.zipIdx
    (fun acc z => (lanes w).foldl
      (fun a b => if z.1.testBit b then a ||| 2 ^ l.bit ys.length w z.2 b else a) acc)
    (0 : Nat) p (fun z => (rowBits l ys.length w z.2 z.1).testBit p)]
  · simpa using any_zipIdx_eq_range ys
      (fun y j => (rowBits l ys.length w j y).testBit p)
  · intro acc z
    rcases z with ⟨x, i⟩
    simp only [Prod.fst, Prod.snd]
    rw [foldl_if_testBit, rowBits_testBit]
    simp [Nat.testBit_two_pow]

private theorem image_testBit_iff (l : Layout) (w : Nat) (ys : List Nat) (p : Nat) :
    (image l w ys).testBit p ↔
      ∃ j, j < ys.length ∧ ∃ b, b < w ∧ (ys[j]?.getD 0).testBit b ∧
        l.bit ys.length w j b = p := by
  rw [image_testBit]
  constructor
  · intro h
    obtain ⟨j, hj, hrow⟩ := List.any_eq_true.mp h
    rw [rowBits_testBit] at hrow
    obtain ⟨b, hb, hcond⟩ := List.any_eq_true.mp hrow
    have hb' : b < w := by simpa [lanes] using hb
    simp only [Bool.and_eq_true, decide_eq_true_eq] at hcond
    exact ⟨j, by simpa using hj, b, hb', hcond.1, hcond.2⟩
  · rintro ⟨j, hj, b, hb, hval, heq⟩
    refine List.any_eq_true.mpr ⟨j, by simpa using hj, ?_⟩
    rw [rowBits_testBit]
    exact List.any_eq_true.mpr ⟨b, by simpa [lanes] using hb, by simp [hval, heq]⟩

private theorem rowBits_at_value (l : Layout) (hno : nonoverlap l)
    (n w i x b : Nat) (hi : i < n) (hb : b < w) :
    (rowBits l n w i x).testBit (l.bit n w i b) = x.testBit b := by
  rw [rowBits_testBit]
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    obtain ⟨a, ha, hcond⟩ := List.any_eq_true.mp h
    have haw : a < w := by simpa [lanes] using ha
    simp only [Bool.and_eq_true, decide_eq_true_eq] at hcond
    have ha_eq : a = b := ((hno n w i i a b hi hi haw hb).mp hcond.2).2
    simpa [ha_eq] using hcond.1
  · intro h
    exact List.any_eq_true.mpr ⟨b, by simpa [lanes] using hb, by simp [h]⟩

private theorem maskedPatch_of_nonoverlap (l : Layout) (hno : nonoverlap l) :
    maskedPatch l := by
  intro w xs i x hi
  apply Nat.eq_of_testBit_eq
  intro p
  by_cases hm : (lanes w).any (fun b => decide (l.bit xs.length w i b = p)) = true
  · obtain ⟨b0, hb0mem, hbp0⟩ := List.any_eq_true.mp hm
    have hb0 : b0 < w := by simpa [lanes] using hb0mem
    have hpos : l.bit xs.length w i b0 = p := by simpa using hbp0
    have him_set : (image l w (xs.set i x)).testBit p ↔ x.testBit b0 := by
      rw [image_testBit_iff, List.length_set]
      constructor
      · rintro ⟨j, hj, b, hb, hval, heq⟩
        have heq' : l.bit xs.length w j b = l.bit xs.length w i b0 := by
          rw [heq, hpos]
        obtain ⟨rfl, hba⟩ :=
          (hno xs.length w j i b b0 hj hi hb hb0).mp heq'
        subst b
        simpa [List.getElem?_set_self, hi] using hval
      · intro hx
        refine ⟨i, hi, b0, hb0, ?_, ?_⟩
        · simpa [List.getElem?_set_self, hi] using hx
        · exact hpos
    have him_orig : (image l w xs).testBit p ↔ (xs[i]?.getD 0).testBit b0 := by
      rw [image_testBit_iff]
      constructor
      · rintro ⟨j, hj, b, hb, hval, heq⟩
        have heq' : l.bit xs.length w j b = l.bit xs.length w i b0 := by
          rw [heq, hpos]
        obtain ⟨rfl, hba⟩ :=
          (hno xs.length w j i b b0 hj hi hb hb0).mp heq'
        subst b
        exact hval
      · intro hv
        exact ⟨i, hi, b0, hb0, hv, hpos⟩
    have hrb : (rowBits l xs.length w i x).testBit p = x.testBit b0 := by
      rw [← hpos]
      exact rowBits_at_value l hno xs.length w i x b0 hi hb0
    apply Bool.eq_iff_iff.mpr
    rw [him_set, Nat.testBit_or, Nat.testBit_xor, Nat.testBit_and,
      rowMask_testBit, hm, hrb]
    cases himv : (image l w xs).testBit p <;>
      cases xv : (xs[i]?.getD 0).testBit b0 <;>
      cases xb : x.testBit b0 <;> simp [himv, xv, xb] at him_orig ⊢
  · have hmf : (lanes w).any (fun b => decide (l.bit xs.length w i b = p)) = false :=
      Bool.not_eq_true _ |>.mp hm
    have haway : ∀ b, b < w → l.bit xs.length w i b ≠ p := by
      intro b hb heq
      apply hm
      exact List.any_eq_true.mpr ⟨b, by simpa [lanes] using hb, by simp [heq]⟩
    have him_eq : (image l w (xs.set i x)).testBit p = (image l w xs).testBit p := by
      apply Bool.eq_iff_iff.mpr
      rw [image_testBit_iff, image_testBit_iff, List.length_set]
      constructor
      · rintro ⟨j, hj, b, hb, hval, heq⟩
        have hij : i ≠ j := by
          intro hij
          subst j
          exact haway b hb heq
        exact ⟨j, hj, b, hb,
          by simpa [List.getElem?_set_ne hij] using hval, heq⟩
      · rintro ⟨j, hj, b, hb, hval, heq⟩
        have hij : i ≠ j := by
          intro hij
          subst j
          exact haway b hb heq
        exact ⟨j, hj, b, hb,
          by simpa [List.getElem?_set_ne hij] using hval, heq⟩
    have hrb : (rowBits l xs.length w i x).testBit p = false := by
      rw [rowBits_testBit]
      apply Bool.eq_false_iff.mpr
      intro h
      obtain ⟨b, hb, hcond⟩ := List.any_eq_true.mp h
      have hb' : b < w := by simpa [lanes] using hb
      simp only [Bool.and_eq_true, decide_eq_true_eq] at hcond
      exact haway b hb' hcond.2
    rw [Nat.testBit_or, Nat.testBit_xor, Nat.testBit_and,
      rowMask_testBit, hmf, hrb]
    simpa [him_eq]

/-! `SplitNibble` places lanes 0..3 next to each row and the remaining lanes in
row-major order on separate planes.  The low part ends before the high part
starts, so every (row, lane) pair gets a distinct physical bit. -/

private theorem splitNibble_nonoverlap : nonoverlap layout := by
  intro n w i j a b hi hj ha hb
  constructor
  · intro h
    simp only [layout] at h
    by_cases ha4 : a < 4
    · by_cases hb4 : b < 4
      · simp only [ha4, hb4, ↓reduceIte] at h
        by_cases hw4 : w ≤ 4
        · have hm : min 4 w = w := Nat.min_eq_right hw4
          rw [hm] at h
          have hwpos : 0 < w := by omega
          have hab : a = b := by
            have hmod := congrArg (fun t => t % w) h
            simpa [Nat.add_mod, Nat.mul_mod_left, Nat.mod_eq_of_lt ha,
              Nat.mod_eq_of_lt hb] using hmod
          subst b
          exact ⟨Nat.mul_right_cancel hwpos (Nat.add_right_cancel h), rfl⟩
        · have hm : min 4 w = 4 := Nat.min_eq_left (by omega : 4 ≤ w)
          rw [hm] at h
          have hab : a = b := by
            have hmod := congrArg (fun t => t % 4) h
            simpa [Nat.add_mod, Nat.mul_mod_left, Nat.mod_eq_of_lt ha4,
              Nat.mod_eq_of_lt hb4] using hmod
          subst b
          exact ⟨Nat.mul_right_cancel (by decide : 0 < 4) (Nat.add_right_cancel h), rfl⟩
      · simp only [ha4, hb4, ↓reduceIte] at h
        have hmpos : 0 < min 4 w := by
          by_cases hw4 : w ≤ 4
          · rw [Nat.min_eq_right hw4]; omega
          · rw [Nat.min_eq_left (by omega : 4 ≤ w)]; omega
        have ha_m : a < min 4 w := by
          by_cases hw4 : w ≤ 4
          · simpa [Nat.min_eq_right hw4] using ha
          · simpa [Nat.min_eq_left (by omega : 4 ≤ w)] using ha4
        have hmul : (i + 1) * min 4 w ≤ n * min 4 w :=
          Nat.mul_le_mul_right (min 4 w) (by omega)
        have hsucc : (i + 1) * min 4 w = i * min 4 w + min 4 w := by
          simp [Nat.add_mul]
        omega
    · by_cases hb4 : b < 4
      · simp only [ha4, hb4, ↓reduceIte] at h
        have hmpos : 0 < min 4 w := by
          by_cases hw4 : w ≤ 4
          · rw [Nat.min_eq_right hw4]; omega
          · rw [Nat.min_eq_left (by omega : 4 ≤ w)]; omega
        have hb_m : b < min 4 w := by
          by_cases hw4 : w ≤ 4
          · simpa [Nat.min_eq_right hw4] using hb
          · simpa [Nat.min_eq_left (by omega : 4 ≤ w)] using hb4
        have hmul : (j + 1) * min 4 w ≤ n * min 4 w :=
          Nat.mul_le_mul_right (min 4 w) (by omega)
        have hsucc : (j + 1) * min 4 w = j * min 4 w + min 4 w := by
          simp [Nat.add_mul]
        omega
      · simp only [ha4, hb4, ↓reduceIte] at h
        have h' : (a - 4) * n + i = (b - 4) * n + j := by omega
        have hab : a = b := by
          by_cases hne : a = b
          · exact hne
          · rcases Nat.lt_or_gt_of_ne hne with hlt | hgt
            · have hmul : (a - 4 + 1) * n ≤ (b - 4) * n :=
                Nat.mul_le_mul_right n (by omega)
              have hsucc : (a - 4 + 1) * n = (a - 4) * n + n := by
                simp [Nat.add_mul]
              omega
            · have hmul : (b - 4 + 1) * n ≤ (a - 4) * n :=
                Nat.mul_le_mul_right n (by omega)
              have hsucc : (b - 4 + 1) * n = (b - 4) * n + n := by
                simp [Nat.add_mul]
              omega
        subst b
        exact ⟨by omega, rfl⟩
  · rintro ⟨rfl, rfl⟩
    rfl

theorem sound_MaskedPatch : MaskedPatch.Goal :=
  maskedPatch_of_nonoverlap layout splitNibble_nonoverlap

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SplitNibble_MaskedPatch
