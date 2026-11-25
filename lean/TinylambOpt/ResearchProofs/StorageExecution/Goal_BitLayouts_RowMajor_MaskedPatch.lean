import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RowMajor_MaskedPatch

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RowMajor

/-- Testing a bit of a left fold that only ORs bits in. -/
private theorem testBit_foldl_cond_or (l : List Nat) (p : Nat → Bool) (f : Nat → Nat)
    (acc k : Nat) :
    (l.foldl (fun a b => if p b then a ||| f b else a) acc).testBit k =
      acc.testBit k || l.any (fun b => p b && (f b).testBit k) := by
  induction l generalizing acc with
  | nil => simp
  | cons b bs ih =>
      rw [List.foldl_cons, ih, List.any_cons]
      by_cases hp : p b
      · simp [hp, Nat.testBit_or, Bool.or_assoc, Bool.or_comm, Bool.or_left_comm]
      · simp [hp]

private theorem testBit_foldl_or_aux (l : List Nat) (f : Nat → Nat) (acc k : Nat) :
    (l.foldl (fun a b => a ||| f b) acc).testBit k =
      acc.testBit k || l.any (fun b => (f b).testBit k) := by
  simpa using (testBit_foldl_cond_or l (fun _ => true) f acc k)

/-- Folding conditional ORs from `acc` is `acc` OR the fold from zero. -/
private theorem foldl_cond_or_acc (l : List Nat) (p : Nat → Bool) (f : Nat → Nat)
    (acc : Nat) :
    l.foldl (fun a b => if p b then a ||| f b else a) acc =
      acc ||| l.foldl (fun a b => if p b then a ||| f b else a) 0 := by
  induction l generalizing acc with
  | nil => simp
  | cons b bs ih =>
      simp only [List.foldl_cons]
      rw [ih, ih]
      by_cases hp : p b
      · simp [hp, Nat.or_assoc, Nat.or_comm, Nat.or_left_comm]
      · simp [hp]

/-- `List.any` over one bit position of a row-major row. -/
private theorem range_any_bit_eq (w i x k : Nat) (hw : 0 < w) :
    (List.range w).any (fun b => x.testBit b && decide (i * w + b = k)) =
      (if k / w = i then x.testBit (k % w) else false) := by
  by_cases hki : k / w = i
  · rw [if_pos hki]
    have hmodlt : k % w < w := Nat.mod_lt k hw
    by_cases hx : x.testBit (k % w) = true
    · apply (List.any_eq_true).2
      refine ⟨k % w, List.mem_range.2 hmodlt, ?_⟩
      have hEq : i * w + k % w = k := by
        rw [← Nat.div_add_mod k w, hki, Nat.mul_comm i w]
      simp [hx, hEq]
    · have hx' : x.testBit (k % w) = false := by
        cases h : x.testBit (k % w) <;> simp_all
      apply (List.any_eq_false).2
      intro b hb hbtrue
      simp only [Bool.and_eq_true] at hbtrue
      rcases hbtrue with ⟨hxb, hdec⟩
      have hbw : b < w := List.mem_range.1 hb
      have hEq : i * w + b = k := of_decide_eq_true hdec
      have hmod : (i * w + b) % w = b := by
        rw [Nat.mul_comm i w, Nat.mul_add_mod_self_left, Nat.mod_eq_of_lt hbw]
      have hb_eq : b = k % w := by rw [← hmod, hEq]
      rw [hb_eq] at hxb
      exact hx hxb
  · rw [if_neg hki]
    apply (List.any_eq_false).2
    intro b hb hbtrue
    simp only [Bool.and_eq_true] at hbtrue
    rcases hbtrue with ⟨_, hdec⟩
    have hbw : b < w := List.mem_range.1 hb
    have hEq : i * w + b = k := of_decide_eq_true hdec
    have hdiv : (i * w + b) / w = i := by
      rw [Nat.mul_comm i w, Nat.mul_add_div hw i b, Nat.div_eq_of_lt hbw, Nat.add_zero]
    exact hki (by rw [← hEq, hdiv])

private theorem rowBits_testBit (w i x k : Nat) :
    (rowBits RowMajor.layout 0 w i x).testBit k =
      (if w > 0 ∧ k / w = i then x.testBit (k % w) else false) := by
  unfold rowBits RowMajor.layout lanes
  rw [testBit_foldl_cond_or]
  simp only [false_or, Nat.testBit_two_pow]
  by_cases hw : w = 0
  · subst w
    simp
  · have hwpos : 0 < w := Nat.pos_of_ne_zero hw
    by_cases hki : k / w = i
    · simp [hwpos, hki, range_any_bit_eq w i x k hwpos]
    · simp [hwpos, hki, range_any_bit_eq w i x k hwpos]

private theorem rowMask_testBit (w i k : Nat) :
    (rowMask RowMajor.layout 0 w i).testBit k = decide (w > 0 ∧ k / w = i) := by
  unfold rowMask RowMajor.layout lanes
  rw [testBit_foldl_cond_or]
  simp only [false_or, Nat.testBit_two_pow]
  by_cases hw : w = 0
  · subst w
    simp
  · have hwpos : 0 < w := Nat.pos_of_ne_zero hw
    have hrange : (List.range w).any (fun b => decide (i * w + b = k)) =
        decide (k / w = i) := by
      by_cases hki : k / w = i
      · have htrue : (List.range w).any (fun b => decide (i * w + b = k)) = true := by
          apply (List.any_eq_true).2
          refine ⟨k % w, List.mem_range.2 (Nat.mod_lt k hwpos), ?_⟩
          have hEq : i * w + k % w = k := by
            rw [← Nat.div_add_mod k w, hki, Nat.mul_comm i w]
          simp [hEq]
        simp [hki, htrue]
      · have hfalse : (List.range w).any (fun b => decide (i * w + b = k)) = false := by
          apply (List.any_eq_false).2
          intro b hb hbt
          have hEq : i * w + b = k := of_decide_eq_true hbt
          have hdiv : (i * w + b) / w = i := by
            rw [Nat.mul_comm i w, Nat.mul_add_div hwpos i b,
              Nat.div_eq_of_lt (List.mem_range.1 hb), Nat.add_zero]
          exact hki (by rw [← hEq, hdiv])
        simp [hki, hfalse]
    simp [hwpos, hrange]

private theorem foldl_zipIdx_rowBits (w base : Nat) (xs : List Nat) :
    (xs.zipIdx base).foldl (fun a q =>
        (List.range w).foldl (fun z b => if q.1.testBit b then z ||| 2 ^ (q.2 * w + b) else z) a) 0 =
      (xs.zipIdx base).foldl (fun a q => a ||| rowBits RowMajor.layout 0 w q.2 q.1) 0 := by
  symm
  refine List.foldl_hom id ?_
  intro x q
  unfold rowBits RowMajor.layout lanes
  exact (foldl_cond_or_acc (List.range w) (fun b => q.1.testBit b)
    (fun b => 2 ^ (q.2 * w + b)) x).symm

private theorem image_eq_foldl_or (w : Nat) (xs : List Nat) :
    image RowMajor.layout w xs =
      xs.zipIdx.foldl (fun acc q => acc ||| rowBits RowMajor.layout 0 w q.2 q.1) 0 := by
  unfold image RowMajor.layout lanes
  exact foldl_zipIdx_rowBits w 0 xs

/-- The bits of a zipIdx fold that writes at index `i`. -/
private theorem zipIdx_any_rowBits (w k base : Nat) (xs : List Nat) :
    (xs.zipIdx base).any (fun q => (rowBits RowMajor.layout 0 w q.2 q.1).testBit k) =
      (if w > 0 ∧ base ≤ k / w ∧ k / w < base + xs.length then
        (xs[k / w - base]?.getD 0).testBit (k % w)
       else false) := by
  induction xs generalizing base with
  | nil => simp
  | cons a tl ih =>
      rw [List.zipIdx_cons, List.any_cons, rowBits_testBit, ih, List.length_cons]
      by_cases hlt : k / w < base
      · have h1 : ¬ (w > 0 ∧ k / w = base) := by omega
        have h2 : ¬ (w > 0 ∧ base + 1 ≤ k / w ∧ k / w < base + 1 + tl.length) := by omega
        have h3 : ¬ (w > 0 ∧ base ≤ k / w ∧ k / w < base + (tl.length + 1)) := by omega
        simp [h1, h2, h3]
      · have hge : base ≤ k / w := by omega
        by_cases heq : k / w = base
        · rw [heq]
          simp
        · have hgt : base < k / w := by omega
          have h2 : (w > 0 ∧ base + 1 ≤ k / w ∧ k / w < base + 1 + tl.length) ↔
              (w > 0 ∧ k / w < base + 1 + tl.length) := by omega
          have h3 : (w > 0 ∧ base ≤ k / w ∧ k / w < base + (tl.length + 1)) ↔
              (w > 0 ∧ k / w < base + 1 + tl.length) := by omega
          have hshift : k / w - base = (k / w - (base + 1)) + 1 := by omega
          simp [hgt, h2, h3, hshift]

private theorem image_testBit (w k : Nat) (xs : List Nat) :
    (image RowMajor.layout w xs).testBit k =
      (if w > 0 ∧ k / w < xs.length then (xs[k / w]?.getD 0).testBit (k % w) else false) := by
  rw [image_eq_foldl_or]
  rw [testBit_foldl_or_aux]
  simp only [false_or]
  simpa using (zipIdx_any_rowBits w k 0 xs)

theorem sound_MaskedPatch : RowMajor.MaskedPatch.Goal := by
  intro w xs i x hi
  apply Nat.eq_of_testBit_eq
  intro k
  rw [Nat.testBit_or, Nat.testBit_xor, Nat.testBit_and]
  rw [image_testBit, rowMask_testBit, rowBits_testBit]
  rw [List.getElem?_set]
  by_cases hw : w = 0
  · subst w
    simp
  · have hwpos : 0 < w := Nat.pos_of_ne_zero hw
    by_cases hki : k / w = i
    · have hlen : k / w < xs.length := by omega
      simp [hw, hwpos, hki, hi, hlen]
    · by_cases hlen : k / w < xs.length
      · have hne : i ≠ k / w := by omega
        simp [hw, hwpos, hki, hne, hi, hlen]
      · simp [hw, hwpos, hki, hi, hlen]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RowMajor_MaskedPatch
