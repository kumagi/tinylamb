import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `ByteAlignedRow.MaskedPatch` — Replace one lane by clearing its mask and inserting a
  new value.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_ByteAlignedRow_MaskedPatch

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedRow

/-- `align 8 w` rounds `w` up to a multiple of eight, hence is at least `w`. -/
private theorem align8_ge (w : Nat) : w ≤ align 8 w := by
  unfold align
  have hdiv := Nat.div_add_mod (w + 7) 8
  rw [Nat.mul_comm 8 ((w + 7) / 8)] at hdiv
  have hmod := Nat.mod_lt (w + 7) (by omega : 0 < 8)
  omega

/-- A fold whose step leaves the accumulator untouched is the identity. -/
private theorem foldl_noop {α : Type} (l : List α) (a : Nat) :
    l.foldl (fun acc _ => acc) a = a := by
  induction l generalizing a with
  | nil => simp
  | cons b bs ih => simpa using ih a

/-- A zero default has no true bits, so a true bit proves the index exists. -/
private theorem getD_testBit_true_implies_lt : ∀ (xs : List Nat) (j b : Nat),
    (xs[j]?.getD 0).testBit b = true → j < xs.length := by
  intro xs
  induction xs with
  | nil => intro j b h; simp at h
  | cons a tl ih =>
      intro j b h
      cases j with
      | zero => simp
      | succ j =>
          have h' : (tl[j]?.getD 0).testBit b = true := by simpa using h
          have hj := ih j b h'
          simpa using hj

/-- Bit-set form of a fold that ORs conditional bit contributions. -/
private theorem fold_cond_or_bit_iff {α : Type} (l : List α) (p : α → Bool) (f : α → Nat)
    (acc k : Nat) :
    (l.foldl (fun a b => if p b then a ||| f b else a) acc).testBit k = true ↔
      acc.testBit k = true ∨ ∃ b ∈ l, p b = true ∧ (f b).testBit k = true := by
  induction l generalizing acc with
  | nil => simp
  | cons b bs ih =>
      simp only [List.foldl_cons]
      by_cases hp : p b = true
      · simp [hp, Nat.testBit_or, ih, Bool.or_assoc, Bool.or_comm, Bool.or_left_comm, or_assoc]
      · simp [hp, ih]

private theorem fold_or_bit_iff {α : Type} (l : List α) (f : α → Nat) (acc k : Nat) :
    (l.foldl (fun a b => a ||| f b) acc).testBit k = true ↔
      acc.testBit k = true ∨ ∃ b ∈ l, (f b).testBit k = true := by
  simpa using (fold_cond_or_bit_iff l (fun _ => true) f acc k)

/-- Folding conditional ORs from `acc` is `acc` OR the fold from zero. -/
private theorem foldl_cond_or_acc (l : List Nat) (p : Nat → Bool) (f : Nat → Nat)
    (acc : Nat) :
    l.foldl (fun a b => if p b then a ||| f b else a) acc =
      acc ||| l.foldl (fun a b => if p b then a ||| f b else a) 0 := by
  induction l generalizing acc with
  | nil => simp
  | cons b bs ih =>
      by_cases hp : p b = true
      · simp only [List.foldl_cons, hp, ite_true]
        rw [ih (acc ||| f b), ih (0 ||| f b)]
        simp only [Nat.zero_or]
        exact Nat.or_assoc acc (f b) (bs.foldl (fun a b => if p b = true then a ||| f b else a) 0)
      · simp only [List.foldl_cons, hp, ite_false]
        exact ih acc

/-- In a byte-aligned row, lane `b` of row `i` sits at `i * align 8 w + b`. -/
private theorem rowBits_bit_iff (n w i x k : Nat) (hw : 0 < w) :
    (rowBits layout n w i x).testBit k = true ↔
      k / align 8 w = i ∧ k % align 8 w < w ∧ x.testBit (k % align 8 w) = true := by
  have hwA : w ≤ align 8 w := align8_ge w
  have hApos : 0 < align 8 w := by omega
  unfold rowBits layout lanes
  rw [fold_cond_or_bit_iff]
  constructor
  · rintro (hacc | ⟨b, hbm, hpb, hbit⟩)
    · have hz : (0 : Nat).testBit k = false := by simp [Nat.testBit]
      rw [hz] at hacc
      cases hacc
    · have hbw : b < w := by simpa using hbm
      have hEq : i * align 8 w + b = k := by simpa [Nat.testBit_two_pow] using hbit
      have hbA : b < align 8 w := by omega
      have hmod : k % align 8 w = b := by
        rw [← hEq, Nat.mul_comm i (align 8 w), Nat.mul_add_mod_self_left, Nat.mod_eq_of_lt hbA]
      have hdiv : k / align 8 w = i := by
        rw [← hEq, Nat.mul_comm i (align 8 w), Nat.mul_add_div hApos i b,
          Nat.div_eq_of_lt hbA, Nat.add_zero]
      exact ⟨hdiv, by omega, by simpa [hmod] using hpb⟩
  · rintro ⟨hdiv, hltw, hxb⟩
    right
    refine ⟨k % align 8 w, by simpa using hltw, hxb, ?_⟩
    have hEq' : align 8 w * (k / align 8 w) + k % align 8 w = k :=
      Nat.div_add_mod k (align 8 w)
    rw [hdiv] at hEq'
    have hEq : i * align 8 w + k % align 8 w = k := by simpa [Nat.mul_comm] using hEq'
    simpa [hEq]

private theorem rowMask_bit_iff (n w i k : Nat) (hw : 0 < w) :
    (rowMask layout n w i).testBit k = true ↔
      k / align 8 w = i ∧ k % align 8 w < w := by
  have hwA : w ≤ align 8 w := align8_ge w
  have hApos : 0 < align 8 w := by omega
  unfold rowMask layout lanes
  rw [fold_or_bit_iff]
  constructor
  · intro hor
    have hex : ∃ b ∈ List.range w, (2 ^ (i * align 8 w + b)).testBit k = true := by
      rcases hor with hacc | hex
      · have hz : (0 : Nat).testBit k = false := by simp [Nat.testBit]
        rw [hz] at hacc
        cases hacc
      · exact hex
    rcases hex with ⟨b, hbm, hbit⟩
    have hbw : b < w := by simpa using hbm
    have hEq : i * align 8 w + b = k := by simpa [Nat.testBit_two_pow] using hbit
    have hbA : b < align 8 w := by omega
    have hmod : k % align 8 w = b := by
      rw [← hEq, Nat.mul_comm i (align 8 w), Nat.mul_add_mod_self_left, Nat.mod_eq_of_lt hbA]
    have hdiv : k / align 8 w = i := by
      rw [← hEq, Nat.mul_comm i (align 8 w), Nat.mul_add_div hApos i b,
        Nat.div_eq_of_lt hbA, Nat.add_zero]
    exact ⟨hdiv, by omega⟩
  · rintro ⟨hdiv, hltw⟩
    right
    refine ⟨k % align 8 w, by simpa using hltw, ?_⟩
    have hEq' : align 8 w * (k / align 8 w) + k % align 8 w = k :=
      Nat.div_add_mod k (align 8 w)
    rw [hdiv] at hEq'
    have hEq : i * align 8 w + k % align 8 w = k := by simpa [Nat.mul_comm] using hEq'
    simpa [hEq]

private theorem foldl_zipIdx_rowBits (w : Nat) (xs : List Nat) :
    (xs.zipIdx).foldl (fun a q =>
      (List.range w).foldl (fun z b => if q.1.testBit b then z ||| 2 ^ (q.2 * align 8 w + b) else z) a) 0 =
    (xs.zipIdx).foldl (fun a q => a ||| rowBits layout xs.length w q.2 q.1) 0 := by
  symm
  refine List.foldl_hom id ?_
  intro x q
  unfold rowBits layout lanes
  exact (foldl_cond_or_acc (List.range w) (fun b => q.1.testBit b)
    (fun b => 2 ^ (q.2 * align 8 w + b)) x).symm

private theorem image_eq_foldl_or (w : Nat) (xs : List Nat) :
    image layout w xs =
      xs.zipIdx.foldl (fun acc q => acc ||| rowBits layout xs.length w q.2 q.1) 0 := by
  unfold image layout lanes
  exact foldl_zipIdx_rowBits w xs

/-- Which zipIdx entry can contribute a given bit cell? -/
private theorem zipIdx_fold_bit_iff (n w k base : Nat) (xs : List Nat) (hw : 0 < w) :
    (∃ q ∈ xs.zipIdx base, (rowBits layout n w q.2 q.1).testBit k = true) ↔
      base ≤ k / align 8 w ∧ k / align 8 w < base + xs.length ∧ k % align 8 w < w ∧
        (xs[k / align 8 w - base]?.getD 0).testBit (k % align 8 w) = true := by
  induction xs generalizing base with
  | nil => simp
  | cons a tl ih =>
      constructor
      · rintro ⟨q, hqmem, hqbit⟩
        rw [List.zipIdx_cons] at hqmem
        rw [List.mem_cons] at hqmem
        rcases hqmem with hqhead | hqtail
        · subst q
          have hh := (rowBits_bit_iff n w base a k hw).mp hqbit
          rcases hh with ⟨hdiv, hltw, hbit⟩
          have hge : base ≤ k / align 8 w := by rw [hdiv]; exact Nat.le_refl base
          have hlt : k / align 8 w < base + (a :: tl).length := by rw [hdiv]; simp
          have hidx : k / align 8 w - base = 0 := by rw [hdiv, Nat.sub_self]
          exact ⟨hge, hlt, hltw, by simpa [hidx] using hbit⟩
        · have ht := (ih (base + 1)).mp ⟨q, hqtail, hqbit⟩
          rcases ht with ⟨hge, hlt, hltw, hbit⟩
          have hge' : base ≤ k / align 8 w := Nat.le_trans (Nat.le_succ base) hge
          have hlt' : k / align 8 w < base + (a :: tl).length := by
            simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using hlt
          have hsub : k / align 8 w - base = (k / align 8 w - (base + 1)) + 1 := by omega
          exact ⟨hge', hlt', hltw, by simpa [hsub] using hbit⟩
      · rintro ⟨hge, hlt, hltw, hbit⟩
        by_cases heq : k / align 8 w = base
        · refine ⟨(a, base), by simp, ?_⟩
          have hidx : k / align 8 w - base = 0 := by rw [heq, Nat.sub_self]
          exact (rowBits_bit_iff n w base a k hw).mpr ⟨heq, hltw, by simpa [hidx] using hbit⟩
        · have hgt : base + 1 ≤ k / align 8 w := by omega
          have hsub : k / align 8 w - base = (k / align 8 w - (base + 1)) + 1 := by omega
          have hlt'' : k / align 8 w < base + 1 + tl.length := by
            simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using hlt
          have ht : ∃ q ∈ tl.zipIdx (base + 1), (rowBits layout n w q.2 q.1).testBit k = true :=
            (ih (base + 1)).mpr ⟨hgt, hlt'', hltw, by simpa [hsub] using hbit⟩
          rcases ht with ⟨q, hqmem, hqbit⟩
          exact ⟨q, by simp [hqmem], hqbit⟩

private theorem image_bit_iff (w k : Nat) (xs : List Nat) (hw : 0 < w) :
    (image layout w xs).testBit k = true ↔
      0 < xs.length ∧ k / align 8 w < xs.length ∧ k % align 8 w < w ∧
        (xs[k / align 8 w]?.getD 0).testBit (k % align 8 w) = true := by
  rw [image_eq_foldl_or]
  rw [fold_or_bit_iff]
  have hz : (0 : Nat).testBit k = false := by simp [Nat.testBit]
  rw [hz]
  cases xs with
  | nil => simp
  | cons a tl =>
      have ht := zipIdx_fold_bit_iff (a :: tl).length w k 0 (a :: tl) hw
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ht

theorem sound_MaskedPatch : ByteAlignedRow.MaskedPatch.Goal := by
  intro w xs i x hi
  by_cases hw : w = 0
  · subst w
    simp [image, rowMask, rowBits, lanes, layout, foldl_noop]
  · have hwpos : 0 < w := Nat.pos_of_ne_zero hw
    let n := xs.length
    have hn : 0 < n := by omega
    have hlen : (xs.set i x).length = n := by simp [n]
    apply Nat.eq_of_testBit_eq
    intro k
    by_cases hkw : k % align 8 w < w
    · by_cases hri : k / align 8 w = i
      · have hAiff : (image layout w xs).testBit k = true ↔
            (xs[i]?.getD 0).testBit (k % align 8 w) = true := by
          constructor
          · intro h
            have r := (image_bit_iff w k xs hwpos).mp h
            have r' : 0 < n ∧ k / align 8 w < n ∧ k % align 8 w < w ∧
                (xs[k / align 8 w]?.getD 0).testBit (k % align 8 w) = true := by
              simpa [n] using r
            simpa [hri] using r'.2.2.2
          · intro h
            apply (image_bit_iff w k xs hwpos).mpr
            have hlt : k / align 8 w < xs.length := by rw [hri]; simpa [n] using hi
            simpa [n, hlt, hkw, hri] using
              (And.intro hn (And.intro hlt (And.intro hkw h)))
        have hDiff : (image layout w (xs.set i x)).testBit k = true ↔
            x.testBit (k % align 8 w) = true := by
          constructor
          · intro h
            have r := (image_bit_iff w k (xs.set i x) hwpos).mp h
            have r' : 0 < n ∧ k / align 8 w < n ∧ k % align 8 w < w ∧
                ((xs.set i x)[k / align 8 w]?.getD 0).testBit (k % align 8 w) = true := by
              simpa [n, hlen] using r
            have hx : (xs.set i x)[k / align 8 w]?.getD 0 = x := by simp [hri, hi]
            simpa [hx] using r'.2.2.2
          · intro h
            apply (image_bit_iff w k (xs.set i x) hwpos).mpr
            have hx : ((xs.set i x)[k / align 8 w]?.getD 0).testBit (k % align 8 w) = true := by
              simpa [hri, hi] using h
            have hlt : k / align 8 w < (xs.set i x).length :=
              getD_testBit_true_implies_lt (xs.set i x) (k / align 8 w) (k % align 8 w) hx
            simpa [hlen, n, hlt, hkw, hx] using
              (And.intro (by omega : 0 < (xs.set i x).length)
                (And.intro hlt (And.intro hkw hx)))
        have hB : (rowMask layout n w i).testBit k = true :=
          (rowMask_bit_iff n w i k hwpos).mpr ⟨hri, hkw⟩
        have hCiff : (rowBits layout n w i x).testBit k = true ↔
            x.testBit (k % align 8 w) = true := by
          rw [rowBits_bit_iff n w i x k hwpos]
          simp [hri, hkw]
        have hA : (image layout w xs).testBit k = (xs[i]?.getD 0).testBit (k % align 8 w) :=
          Bool.eq_iff_iff.mpr hAiff
        have hD : (image layout w (xs.set i x)).testBit k = x.testBit (k % align 8 w) :=
          Bool.eq_iff_iff.mpr hDiff
        have hC : (rowBits layout n w i x).testBit k = x.testBit (k % align 8 w) :=
          Bool.eq_iff_iff.mpr hCiff
        by_cases ha : (xs[i]?.getD 0).testBit (k % align 8 w) <;>
          by_cases hc : x.testBit (k % align 8 w) <;>
            simp [n, Nat.testBit_or, Nat.testBit_xor, Nat.testBit_and, hA, hB, hC, hD, ha, hc]
      · have hAiff : (image layout w xs).testBit k = true ↔
            (xs[k / align 8 w]?.getD 0).testBit (k % align 8 w) = true := by
          constructor
          · intro h
            have r := (image_bit_iff w k xs hwpos).mp h
            have r' : 0 < n ∧ k / align 8 w < n ∧ k % align 8 w < w ∧
                (xs[k / align 8 w]?.getD 0).testBit (k % align 8 w) = true := by
              simpa [n] using r
            exact r'.2.2.2
          · intro h
            apply (image_bit_iff w k xs hwpos).mpr
            have hlt : k / align 8 w < xs.length :=
              getD_testBit_true_implies_lt xs (k / align 8 w) (k % align 8 w) h
            simpa [n, hlt, hkw] using
              (And.intro hn (And.intro hlt (And.intro hkw h)))
        have hDiff : (image layout w (xs.set i x)).testBit k = true ↔
            (xs[k / align 8 w]?.getD 0).testBit (k % align 8 w) = true := by
          constructor
          · intro h
            have r := (image_bit_iff w k (xs.set i x) hwpos).mp h
            have r' : 0 < n ∧ k / align 8 w < n ∧ k % align 8 w < w ∧
                ((xs.set i x)[k / align 8 w]?.getD 0).testBit (k % align 8 w) = true := by
              simpa [n, hlen] using r
            have hne : i ≠ k / align 8 w := by intro heq; exact hri heq.symm
            have hx : (xs.set i x)[k / align 8 w]?.getD 0 = xs[k / align 8 w]?.getD 0 := by
              simp [List.getElem?_set, hne]
            simpa [hx] using r'.2.2.2
          · intro h
            apply (image_bit_iff w k (xs.set i x) hwpos).mpr
            have hne : i ≠ k / align 8 w := by intro heq; exact hri heq.symm
            have hx : ((xs.set i x)[k / align 8 w]?.getD 0).testBit (k % align 8 w) = true := by
              simpa [hlen, List.getElem?_set, hne] using h
            have hlt : k / align 8 w < (xs.set i x).length :=
              getD_testBit_true_implies_lt (xs.set i x) (k / align 8 w) (k % align 8 w) hx
            simpa [hlen, n, hlt, hkw, hx] using
              (And.intro (by omega : 0 < (xs.set i x).length)
                (And.intro hlt (And.intro hkw hx)))
        have hB : (rowMask layout n w i).testBit k = false := by
          cases h : (rowMask layout n w i).testBit k with
          | false => simpa using h
          | true => exact False.elim (hri (((rowMask_bit_iff n w i k hwpos).mp h).1))
        have hC : (rowBits layout n w i x).testBit k = false := by
          cases h : (rowBits layout n w i x).testBit k with
          | false => simpa using h
          | true => exact False.elim (hri (((rowBits_bit_iff n w i x k hwpos).mp h).1))
        have hA : (image layout w xs).testBit k = (xs[k / align 8 w]?.getD 0).testBit (k % align 8 w) :=
          Bool.eq_iff_iff.mpr hAiff
        have hD : (image layout w (xs.set i x)).testBit k =
            (xs[k / align 8 w]?.getD 0).testBit (k % align 8 w) :=
          Bool.eq_iff_iff.mpr hDiff
        by_cases ha : (xs[k / align 8 w]?.getD 0).testBit (k % align 8 w) <;>
          simp [n, Nat.testBit_or, Nat.testBit_xor, Nat.testBit_and, hA, hB, hC, hD, ha]
    · have hA : (image layout w xs).testBit k = false := by
        cases h : (image layout w xs).testBit k with
        | false => simpa using h
        | true =>
            have r := (image_bit_iff w k xs hwpos).mp h
            exact False.elim (hkw (by simpa [n] using r.2.2.1))
      have hD : (image layout w (xs.set i x)).testBit k = false := by
        cases h : (image layout w (xs.set i x)).testBit k with
        | false => simpa using h
        | true =>
            have r := (image_bit_iff w k (xs.set i x) hwpos).mp h
            exact False.elim (hkw (by simpa [n, hlen] using r.2.2.1))
      have hB : (rowMask layout n w i).testBit k = false := by
        cases h : (rowMask layout n w i).testBit k with
        | false => simpa using h
        | true => exact False.elim (hkw (((rowMask_bit_iff n w i k hwpos).mp h).2))
      have hC : (rowBits layout n w i x).testBit k = false := by
        cases h : (rowBits layout n w i x).testBit k with
        | false => simpa using h
        | true => exact False.elim (hkw (((rowBits_bit_iff n w i x k hwpos).mp h).2.1))
      simp [n, Nat.testBit_or, Nat.testBit_xor, Nat.testBit_and, hA, hB, hC, hD]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_ByteAlignedRow_MaskedPatch
