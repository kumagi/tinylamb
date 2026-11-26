import TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RowMajor_Nonoverlap

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `RowMajor.MaskedPatch` — Replace one lane by clearing its mask and inserting a new
  value.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RowMajor_MaskedPatch

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RowMajor
open TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RowMajor_Nonoverlap

private theorem mem_lanes {b w : Nat} : b ∈ lanes w ↔ b < w := by
  simp [lanes]

private theorem foldl_or_eq_if (f : Nat → Nat) (cond : Nat → Bool) (bs : List Nat) (acc : Nat) :
    bs.foldl (fun a b => if cond b then a ||| 2 ^ f b else a) acc =
      acc ||| bs.foldl (fun a b => if cond b then a ||| 2 ^ f b else a) 0 := by
  induction bs generalizing acc with
  | nil => simp
  | cons b bs ih =>
      by_cases hc : cond b = true
      · calc
          bs.foldl (fun a c => if cond c then a ||| 2 ^ f c else a) (if cond b then acc ||| 2 ^ f b else acc)
              = bs.foldl (fun a c => if cond c then a ||| 2 ^ f c else a) (acc ||| 2 ^ f b) := by rw [if_pos hc]
          _ = (acc ||| 2 ^ f b) ||| bs.foldl (fun a c => if cond c then a ||| 2 ^ f c else a) 0 := ih (acc ||| 2 ^ f b)
          _ = acc ||| (2 ^ f b ||| bs.foldl (fun a c => if cond c then a ||| 2 ^ f c else a) 0) := Nat.or_assoc _ _ _
          _ = acc ||| bs.foldl (fun a c => if cond c then a ||| 2 ^ f c else a) (2 ^ f b) := by
                rw [ih (2 ^ f b)]
          _ = acc ||| bs.foldl (fun a c => if cond c then a ||| 2 ^ f c else a)
                (if cond b then 0 ||| 2 ^ f b else 0) := by rw [if_pos hc, Nat.zero_or]
          _ = acc ||| (b :: bs).foldl (fun a c => if cond c then a ||| 2 ^ f c else a) 0 := rfl
      · have hc' : cond b = false := Bool.eq_false_iff.mpr hc
        calc
          bs.foldl (fun a c => if cond c then a ||| 2 ^ f c else a) (if cond b then acc ||| 2 ^ f b else acc)
              = bs.foldl (fun a c => if cond c then a ||| 2 ^ f c else a) acc := by rw [if_neg (by simp [hc'])]
          _ = acc ||| bs.foldl (fun a c => if cond c then a ||| 2 ^ f c else a) 0 := ih acc
          _ = acc ||| bs.foldl (fun a c => if cond c then a ||| 2 ^ f c else a)
                (if cond b then 0 ||| 2 ^ f b else 0) := by rw [if_neg (by simp [hc'])]
          _ = acc ||| (b :: bs).foldl (fun a c => if cond c then a ||| 2 ^ f c else a) 0 := rfl

private theorem testBit_foldl_zero_if (f : Nat → Nat) (cond : Nat → Bool)
    (bs : List Nat) (p : Nat) :
    ((bs.foldl (fun a b => if cond b then a ||| 2 ^ f b else a) 0).testBit p) =
      bs.any (fun b => cond b && decide (f b = p)) := by
  induction bs with
  | nil => simp
  | cons b bs ih =>
      by_cases hc : cond b = true
      · rw [List.foldl_cons, if_pos hc, Nat.zero_or]
        calc
          (bs.foldl (fun a c => if cond c then a ||| 2 ^ f c else a) (2 ^ f b)).testBit p
              = (2 ^ f b ||| bs.foldl (fun a c => if cond c then a ||| 2 ^ f c else a) 0).testBit p := by
                  rw [foldl_or_eq_if f cond bs (2 ^ f b)]
          _ = ((2 ^ f b).testBit p || (bs.foldl (fun a c => if cond c then a ||| 2 ^ f c else a) 0).testBit p) :=
                  Nat.testBit_or _ _ _
          _ = (decide (f b = p) || bs.any (fun c => cond c && decide (f c = p))) := by
                 rw [Nat.testBit_two_pow, ih]
          _ = (b :: bs).any (fun c => cond c && decide (f c = p)) := by simp [hc, Bool.or_assoc]
      · have hc' : cond b = false := Bool.eq_false_iff.mpr hc
        rw [List.foldl_cons, if_neg (by simp [hc']), ih]
        simp [hc']

private theorem testBit_foldl_or_if (f : Nat → Nat) (cond : Nat → Bool)
    (bs : List Nat) (acc p : Nat) :
    ((bs.foldl (fun a b => if cond b then a ||| 2 ^ f b else a) acc).testBit p) =
      (acc.testBit p || bs.any (fun b => cond b && decide (f b = p))) := by
  calc
    (bs.foldl (fun a b => if cond b then a ||| 2 ^ f b else a) acc).testBit p
        = (acc ||| bs.foldl (fun a b => if cond b then a ||| 2 ^ f b else a) 0).testBit p := by
            rw [foldl_or_eq_if f cond bs acc]
    _ = (acc.testBit p || (bs.foldl (fun a b => if cond b then a ||| 2 ^ f b else a) 0).testBit p) :=
          Nat.testBit_or _ _ _
    _ = (acc.testBit p || bs.any (fun b => cond b && decide (f b = p))) := by rw [testBit_foldl_zero_if f cond bs p]

private theorem testBit_rowMask (l : Layout) (n w i p : Nat) :
    (rowMask l n w i).testBit p = (lanes w).any (fun b => decide (l.bit n w i b = p)) := by
  unfold rowMask
  have h := testBit_foldl_or_if (fun b => l.bit n w i b) (fun _ => true) (lanes w) 0 p
  have hz : Nat.testBit 0 p = false := by simp
  rw [hz] at h
  simp only [Bool.false_or, Bool.true_and] at h
  exact h

private theorem testBit_rowBits (l : Layout) (n w i x p : Nat) :
    (rowBits l n w i x).testBit p =
      (lanes w).any (fun b => x.testBit b && decide (l.bit n w i b = p)) := by
  unfold rowBits
  have h := testBit_foldl_or_if (fun b => l.bit n w i b) (fun b => x.testBit b) (lanes w) 0 p
  have hz : Nat.testBit 0 p = false := by simp
  rw [hz] at h
  simp only [Bool.false_or] at h
  exact h

private theorem foldl_or_pairs (f : Nat × Nat → Nat) (bs : List (Nat × Nat)) (acc : Nat) :
    bs.foldl (fun a x => a ||| f x) acc = acc ||| bs.foldl (fun a x => a ||| f x) 0 := by
  induction bs generalizing acc with
  | nil => simp
  | cons x xs ih =>
      calc
        xs.foldl (fun a y => a ||| f y) (acc ||| f x)
            = (acc ||| f x) ||| xs.foldl (fun a y => a ||| f y) 0 := ih (acc ||| f x)
        _ = acc ||| (f x ||| xs.foldl (fun a y => a ||| f y) 0) := Nat.or_assoc _ _ _
        _ = acc ||| xs.foldl (fun a y => a ||| f y) (f x) := by rw [ih (f x)]
        _ = acc ||| (x :: xs).foldl (fun a y => a ||| f y) 0 := by rw [List.foldl_cons, Nat.zero_or]

private theorem testBit_generic_fold (f : Nat × Nat → Nat) (bs : List (Nat × Nat)) (p : Nat) :
    ((bs.foldl (fun acc x => acc ||| f x) 0).testBit p) = bs.any (fun x => (f x).testBit p) := by
  induction bs with
  | nil => simp
  | cons x xs ih =>
      rw [List.foldl_cons, Nat.zero_or]
      calc
        (xs.foldl (fun acc y => acc ||| f y) (f x)).testBit p
            = (f x ||| xs.foldl (fun acc y => acc ||| f y) 0).testBit p := by rw [foldl_or_pairs f xs (f x)]
        _ = ((f x).testBit p || (xs.foldl (fun acc y => acc ||| f y) 0).testBit p) := Nat.testBit_or _ _ _
        _ = ((f x).testBit p || xs.any (fun y => (f y).testBit p)) := by rw [ih]
        _ = (x :: xs).any (fun y => (f y).testBit p) := by simp [Bool.or_assoc]

private theorem image_fold (l : Layout) (w : Nat) (xs : List Nat) :
    image l w xs = xs.zipIdx.foldl (fun acc (x, i) => acc ||| rowBits l xs.length w i x) 0 := by
  unfold image
  unfold rowBits
  have hgen : ∀ (lxs : List (Nat × Nat)) (acc : Nat),
      lxs.foldl (fun acc xi => (lanes w).foldl
        (fun a b => if xi.1.testBit b then a ||| 2 ^ l.bit xs.length w xi.2 b else a) acc) acc =
      lxs.foldl (fun acc xi => acc ||| (lanes w).foldl
        (fun a b => if xi.1.testBit b then a ||| 2 ^ l.bit xs.length w xi.2 b else a) 0) acc := by
    intro lxs
    induction lxs with
    | nil => intro acc; simp
    | cons xi rest ih =>
        intro acc
        rw [List.foldl_cons, List.foldl_cons]
        have hrow := foldl_or_eq_if (fun b => l.bit xs.length w xi.2 b) (fun b => xi.1.testBit b) (lanes w) acc
        rw [hrow]
        exact ih (acc ||| (lanes w).foldl
          (fun a b => if xi.1.testBit b then a ||| 2 ^ l.bit xs.length w xi.2 b else a) 0)
  rw [hgen xs.zipIdx 0]

private theorem testBit_image (l : Layout) (w : Nat) (xs : List Nat) (p : Nat) :
    (image l w xs).testBit p =
      xs.zipIdx.any (fun (x, i) =>
        (lanes w).any (fun b => x.testBit b && decide (l.bit xs.length w i b = p))) := by
  rw [image_fold]
  rw [testBit_generic_fold]
  apply List.any_congr rfl
  intro y
  exact testBit_rowBits l xs.length w y.2 y.1 p

private theorem testBit_image_row (l : Layout) (w : Nat) (xs : List Nat) (i b : Nat)
    (hi : i < xs.length) (hb : b < w)
    (hno : ∀ j k a c, j < xs.length → k < xs.length → a < w → c < w →
      l.bit xs.length w j a = l.bit xs.length w k c → j = k ∧ a = c) :
    (image l w xs).testBit (l.bit xs.length w i b) = (xs[i]?.getD 0).testBit b := by
  rw [testBit_image]
  rw [Bool.eq_iff_iff]
  constructor
  · intro h
    rcases List.any_eq_true.mp h with ⟨⟨y, j⟩, hjmem, hjany⟩
    rcases List.any_eq_true.mp hjany with ⟨c, hcmem, hcond⟩
    simp only [Bool.and_eq_true, decide_eq_true_eq] at hcond
    have hjl : j < xs.length := by
      have ht := List.mem_zipIdx_iff_getElem?.mp hjmem
      exact (List.getElem?_eq_some_iff.mp ht).1
    have hc : c < w := by simpa [mem_lanes] using hcmem
    have heq := hno j i c b hjl hi hc hb hcond.2
    have hji : j = i := heq.1
    have hcb : c = b := heq.2
    have hjmem' : (y, i) ∈ xs.zipIdx := by simpa [hji] using hjmem
    have ht := List.mem_zipIdx_iff_getElem?.mp hjmem'
    rcases (List.getElem?_eq_some_iff.mp ht) with ⟨hidx, hval⟩
    have hy : y = xs[i]?.getD 0 := by simpa [hidx] using hval.symm
    rw [← hy]
    simpa [hcb] using hcond.1
  · intro h
    rw [List.any_eq_true]
    refine ⟨(xs[i]?.getD 0, i), ?_, ?_⟩
    · rw [List.mem_zipIdx_iff_getElem?]
      simp [List.getElem?_eq_some_iff, hi]
    · rw [List.any_eq_true]
      exact ⟨b, by simp [mem_lanes, hb], by simp [h]⟩

private theorem maskedPatch_of_injective (l : Layout)
    (hno : ∀ n w i j a b, i < n → j < n → a < w → b < w →
      l.bit n w i a = l.bit n w j b → i = j ∧ a = b) :
    maskedPatch l := by
  intro w xs i x hi
  apply Nat.eq_of_testBit_eq
  intro p
  rw [Nat.testBit_or, Nat.testBit_xor, Nat.testBit_and]
  have hlen : (xs.set i x).length = xs.length := List.length_set ..
  rw [testBit_rowMask l xs.length w i p, testBit_rowBits l xs.length w i x p]
  have hno_row : ∀ j k a c, j < xs.length → k < xs.length → a < w → c < w →
      l.bit xs.length w j a = l.bit xs.length w k c → j = k ∧ a = c := by
    intro j k a c hj hk ha hc heq
    exact hno xs.length w j k a c hj hk ha hc heq
  by_cases hpi : (lanes w).any (fun b => decide (l.bit xs.length w i b = p)) = true
  · rcases List.any_eq_true.mp hpi with ⟨b0, hb0mem, hbp0⟩
    simp only [decide_eq_true_eq] at hbp0
    have hb0 : b0 < w := by simpa [mem_lanes] using hb0mem
    have hset_im : (image l w (xs.set i x)).testBit p = x.testBit b0 := by
      have hno_set : ∀ j k a c, j < (xs.set i x).length → k < (xs.set i x).length → a < w → c < w →
          l.bit (xs.set i x).length w j a = l.bit (xs.set i x).length w k c → j = k ∧ a = c := by
        intro j k a c hj hk ha hc heq
        rw [hlen] at hj hk heq
        exact hno xs.length w j k a c hj hk ha hc heq
      have hi_set : i < (xs.set i x).length := by simpa [hlen] using hi
      have hrow := testBit_image_row l w (xs.set i x) i b0 hi_set hb0 hno_set
      rw [hlen] at hrow
      rw [← hbp0, hrow]
      simp [List.getElem?_set_self hi]
    have horig : (image l w xs).testBit p = (xs[i]?.getD 0).testBit b0 := by
      rw [← hbp0]
      exact testBit_image_row l w xs i b0 hi hb0 hno_row
    have hrb : (lanes w).any (fun b => x.testBit b && decide (l.bit xs.length w i b = p)) =
        x.testBit b0 := by
      rw [Bool.eq_iff_iff]
      constructor
      · intro h
        rcases List.any_eq_true.mp h with ⟨b, hbmem, hcond⟩
        simp only [Bool.and_eq_true, decide_eq_true_eq] at hcond
        have hb' : b < w := by simpa [mem_lanes] using hbmem
        have heq := hno xs.length w i i b b0 hi hi hb' hb0 (hcond.2.trans hbp0.symm)
        simpa [heq.2] using hcond.1
      · intro h
        rw [List.any_eq_true]
        exact ⟨b0, hb0mem, by simp [hbp0, h]⟩
    rw [hpi, hrb, hset_im, horig]
    cases (xs[i]?.getD 0).testBit b0 <;> cases x.testBit b0 <;> simp
  · have hmask : (lanes w).any (fun b => decide (l.bit xs.length w i b = p)) = false :=
      Bool.eq_false_iff.mpr hpi
    have hrb : (lanes w).any (fun b => x.testBit b && decide (l.bit xs.length w i b = p)) = false := by
      rw [Bool.eq_false_iff, ne_eq, List.any_eq_true]
      rintro ⟨b, hbmem, hcond⟩
      simp only [Bool.and_eq_true, decide_eq_true_eq] at hcond
      apply hpi
      rw [List.any_eq_true]
      exact ⟨b, hbmem, by simp [hcond.2]⟩
    have hagree : (image l w (xs.set i x)).testBit p = (image l w xs).testBit p := by
      rw [testBit_image l w (xs.set i x) p, testBit_image l w xs p, hlen]
      rw [Bool.eq_iff_iff]
      constructor
      · intro h
        rcases List.any_eq_true.mp h with ⟨⟨y, j⟩, hjmem, hjany⟩
        rcases List.any_eq_true.mp hjany with ⟨b, hbmem, hcond⟩
        simp only [Bool.and_eq_true, decide_eq_true_eq] at hcond
        have hji : j ≠ i := by
          intro hji'; subst j
          apply hpi
          rw [List.any_eq_true]
          exact ⟨b, hbmem, by simp [hcond.2]⟩
        rw [List.any_eq_true]
        refine ⟨(y, j), ?_, ?_⟩
        · rw [List.mem_zipIdx_iff_getElem?]
          rw [← List.getElem?_set_ne (Ne.symm hji)]
          exact List.mem_zipIdx_iff_getElem?.mp hjmem
        · rw [List.any_eq_true]
          refine ⟨b, hbmem, ?_⟩
          simp only [Bool.and_eq_true, decide_eq_true_eq]
          constructor
          · simpa [hji] using hcond.1
          · exact hcond.2
      · intro h
        rcases List.any_eq_true.mp h with ⟨⟨y, j⟩, hjmem, hjany⟩
        rcases List.any_eq_true.mp hjany with ⟨b, hbmem, hcond⟩
        simp only [Bool.and_eq_true, decide_eq_true_eq] at hcond
        have hji : j ≠ i := by
          intro hji'; subst j
          apply hpi
          rw [List.any_eq_true]
          exact ⟨b, hbmem, by simp [hcond.2]⟩
        rw [List.any_eq_true]
        refine ⟨(y, j), ?_, ?_⟩
        · rw [List.mem_zipIdx_iff_getElem?]
          rw [List.getElem?_set_ne (Ne.symm hji)]
          exact List.mem_zipIdx_iff_getElem?.mp hjmem
        · rw [List.any_eq_true]
          refine ⟨b, hbmem, ?_⟩
          simp only [Bool.and_eq_true, decide_eq_true_eq]
          constructor
          · simpa [hji] using hcond.1
          · exact hcond.2
    rw [hagree, hmask, hrb]
    cases (image l w xs).testBit p <;> simp

private theorem rowMajor_nonoverlap : ∀ n w i j a b, i < n → j < n → a < w → b < w →
    layout.bit n w i a = layout.bit n w j b → i = j ∧ a = b := by
  intro n w i j a b hi hj ha hb heq
  exact (sound_Nonoverlap n w i j a b hi hj ha hb).mp heq

theorem sound_MaskedPatch : RowMajor.MaskedPatch.Goal := by
  exact maskedPatch_of_injective layout rowMajor_nonoverlap

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RowMajor_MaskedPatch
