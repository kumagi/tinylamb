import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_Vertical_MaskedPatch

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.Vertical

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

private theorem nat_or_left_comm (a b c : Nat) : a ||| (b ||| c) = b ||| (a ||| c) := by
  rw [← Nat.or_assoc, Nat.or_comm a b, Nat.or_assoc]

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

/-- In a vertical plane, lane `b` of row `i` sits at `b*n+i`. -/
private theorem rowBits_bit_iff (n w i x k : Nat) (hn : 0 < n) (hi : i < n) :
    (rowBits Vertical.layout n w i x).testBit k = true ↔
      k % n = i ∧ k / n < w ∧ x.testBit (k / n) = true := by
  unfold rowBits Vertical.layout lanes
  rw [fold_cond_or_bit_iff]
  constructor
  · rintro (hacc | ⟨b, hbm, hpb, hbit⟩)
    · have hz : (0 : Nat).testBit k = false := by simp [Nat.testBit]
      rw [hz] at hacc
      cases hacc
    · have hbw : b < w := by simpa using hbm
      have hEq : b * n + i = k := by simpa [Vertical.layout, Nat.testBit_two_pow] using hbit
      have hmod : k % n = i := by
        rw [← hEq, Nat.mul_comm b n, Nat.mul_add_mod_self_left, Nat.mod_eq_of_lt hi]
      have hdiv : k / n = b := by
        rw [← hEq, Nat.mul_comm b n, Nat.mul_add_div hn b i, Nat.div_eq_of_lt hi, Nat.add_zero]
      exact ⟨hmod, by simpa [hdiv] using hbw, by simpa [hdiv] using hpb⟩
  · rintro ⟨hmod, hltw, hxb⟩
    right
    refine ⟨k / n, by simpa using hltw, hxb, ?_⟩
    have hEq : (k / n) * n + i = k := by rw [← hmod, Nat.mul_comm, Nat.div_add_mod]
    simpa [Vertical.layout, hEq]

private theorem rowMask_bit_iff (n w i k : Nat) (hn : 0 < n) (hi : i < n) :
    (rowMask Vertical.layout n w i).testBit k = true ↔
      k % n = i ∧ k / n < w := by
  unfold rowMask Vertical.layout lanes
  rw [fold_or_bit_iff]
  constructor
  · intro hor
    have hex : ∃ b ∈ List.range w, (2 ^ (Vertical.layout.bit n w i b)).testBit k = true := by
      rcases hor with hacc | hex
      · have hz : (0 : Nat).testBit k = false := by simp [Nat.testBit]
        rw [hz] at hacc
        cases hacc
      · exact hex
    rcases hex with ⟨b, hbm, hbit⟩
    have hbw : b < w := by simpa using hbm
    have hEq : b * n + i = k := by simpa [Vertical.layout, Nat.testBit_two_pow] using hbit
    have hmod : k % n = i := by
      rw [← hEq, Nat.mul_comm b n, Nat.mul_add_mod_self_left, Nat.mod_eq_of_lt hi]
    have hdiv : k / n = b := by
      rw [← hEq, Nat.mul_comm b n, Nat.mul_add_div hn b i, Nat.div_eq_of_lt hi, Nat.add_zero]
    exact ⟨hmod, by simpa [hdiv] using hbw⟩
  · rintro ⟨hmod, hltw⟩
    right
    refine ⟨k / n, by simpa using hltw, ?_⟩
    have hEq : (k / n) * n + i = k := by rw [← hmod, Nat.mul_comm, Nat.div_add_mod]
    simpa [Vertical.layout, hEq]

private theorem foldl_zipIdx_rowBits (w : Nat) (xs : List Nat) :
    (xs.zipIdx).foldl (fun a q =>
      (List.range w).foldl (fun z b => if q.1.testBit b then z ||| 2 ^ (b * xs.length + q.2) else z) a) 0 =
    (xs.zipIdx).foldl (fun a q => a ||| rowBits Vertical.layout xs.length w q.2 q.1) 0 := by
  symm
  refine List.foldl_hom id ?_
  intro x q
  unfold rowBits Vertical.layout lanes
  exact (foldl_cond_or_acc (List.range w) (fun b => q.1.testBit b)
    (fun b => 2 ^ (b * xs.length + q.2)) x).symm

private theorem image_eq_foldl_or (w : Nat) (xs : List Nat) :
    image Vertical.layout w xs =
      xs.zipIdx.foldl (fun acc q => acc ||| rowBits Vertical.layout xs.length w q.2 q.1) 0 := by
  unfold image Vertical.layout lanes
  exact foldl_zipIdx_rowBits w xs

/-- Which zipIdx entry can contribute a given bit cell? -/
private theorem zipIdx_fold_bit_iff (n w k base : Nat) (xs : List Nat)
    (hlen : base + xs.length = n) (hn : 0 < n) :
    (∃ q ∈ xs.zipIdx base, (rowBits Vertical.layout n w q.2 q.1).testBit k = true) ↔
      base ≤ k % n ∧ k % n < base + xs.length ∧ k / n < w ∧
        (xs[k % n - base]?.getD 0).testBit (k / n) = true := by
  induction xs generalizing base with
  | nil =>
      have hb : base = n := by omega
      subst n
      have hmodlt : k % base < base := Nat.mod_lt k hn
      simp [hmodlt]
  | cons a tl ih =>
      have hlenpos : 0 < (a :: tl).length := by simp
      have hbaselt : base < n := by omega
      have hlen2 := hlen
      simp only [List.length_cons] at hlen2
      have hlen' : base + 1 + tl.length = n := by omega
      constructor
      · rintro ⟨q, hqmem, hqbit⟩
        rw [List.zipIdx_cons] at hqmem
        rw [List.mem_cons] at hqmem
        rcases hqmem with hqhead | hqtail
        · subst q
          have hh := (rowBits_bit_iff n w base a k hn hbaselt).mp hqbit
          rcases hh with ⟨hmod, hltw, hbit⟩
          have hge : base ≤ k % n := by omega
          have hlt : k % n < base + (a :: tl).length := by
            rw [hlen]
            exact Nat.mod_lt k hn
          have hidx : k % n - base = 0 := by omega
          exact ⟨hge, hlt, hltw, by simpa [hidx] using hbit⟩
        · have ht := (ih (base + 1) hlen').mp ⟨q, hqtail, hqbit⟩
          rcases ht with ⟨hge, hlt, hltw, hbit⟩
          have hge' : base ≤ k % n := by omega
          have hlt' : k % n < base + (a :: tl).length := by
            rw [hlen]
            exact Nat.mod_lt k hn
          have hgt : base + 1 ≤ k % n := hge
          have hsub : k % n - base = (k % n - (base + 1)) + 1 := by
            have hcancel := Nat.sub_add_cancel hgt
            omega
          exact ⟨hge', hlt', hltw, by simpa [hsub] using hbit⟩
      · rintro ⟨hge, hlt, hltw, hbit⟩
        by_cases heq : k % n = base
        · refine ⟨(a, base), by simp, ?_⟩
          have hidx : k % n - base = 0 := by omega
          exact (rowBits_bit_iff n w base a k hn hbaselt).mpr
            ⟨heq, hltw, by simpa [hidx] using hbit⟩
        · have hgt : base + 1 ≤ k % n := by omega
          have hsub : k % n - base = (k % n - (base + 1)) + 1 := by
            have hcancel := Nat.sub_add_cancel hgt
            omega
          have hlt'' : k % n < base + 1 + tl.length := by
            rw [hlen']
            exact Nat.mod_lt k hn
          have ht : ∃ q ∈ tl.zipIdx (base + 1), (rowBits Vertical.layout n w q.2 q.1).testBit k = true :=
            (ih (base + 1) hlen').mpr ⟨hgt, hlt'', hltw, by simpa [hsub] using hbit⟩
          rcases ht with ⟨q, hqmem, hqbit⟩
          exact ⟨q, by simp [hqmem], hqbit⟩

private theorem image_bit_iff (w k : Nat) (xs : List Nat) :
    (image Vertical.layout w xs).testBit k = true ↔
      0 < xs.length ∧ k / xs.length < w ∧
        (xs[k % xs.length]?.getD 0).testBit (k / xs.length) = true := by
  rw [image_eq_foldl_or]
  rw [fold_or_bit_iff]
  have hz : (0 : Nat).testBit k = false := by simp [Nat.testBit]
  rw [hz]
  cases xs with
  | nil => simp
  | cons a tl =>
      have hn : 0 < (a :: tl).length := by simp
      have hmodlt : k % (a :: tl).length < (a :: tl).length := Nat.mod_lt k hn
      have hmodlt' : k % (tl.length + 1) < tl.length + 1 := Nat.mod_lt k (by simp)
      simpa [hn, hmodlt, hmodlt'] using (zipIdx_fold_bit_iff (a :: tl).length w k 0 (a :: tl) (by simp) hn)

theorem sound_MaskedPatch : Vertical.MaskedPatch.Goal := by
  intro w xs i x hi
  let n := xs.length
  have hn : 0 < n := by omega
  have hlen : (xs.set i x).length = n := by simp [n]
  apply Nat.eq_of_testBit_eq
  intro k
  by_cases hkw : k / n < w
  · by_cases hri : k % n = i
    · have hAiff : (image Vertical.layout w xs).testBit k = true ↔
          (xs[i]?.getD 0).testBit (k / n) = true := by
        constructor
        · intro h
          have r := (image_bit_iff w k xs).mp h
          simpa [n, hn, hkw, hri] using r.2.2
        · intro h
          exact (image_bit_iff w k xs).mpr (by simpa [n, hn, hkw, hri] using And.intro hn (And.intro hkw h))
      have hDiff : (image Vertical.layout w (xs.set i x)).testBit k = true ↔
          x.testBit (k / n) = true := by
        constructor
        · intro h
          have r := (image_bit_iff w k (xs.set i x)).mp h
          have r' : 0 < n ∧ k / n < w ∧ ((xs.set i x)[k % n]?.getD 0).testBit (k / n) = true := by
            simpa [n, hlen] using r
          have hx : (xs.set i x)[k % n]?.getD 0 = x := by simp [hri, hi]
          simpa [hx] using r'.2.2
        · intro h
          apply (image_bit_iff w k (xs.set i x)).mpr
          simpa [n, hlen, hn, hkw, hri, hi, h] using
            (And.intro hn (And.intro hkw h))
      have hB : (rowMask Vertical.layout n w i).testBit k = true :=
        (rowMask_bit_iff n w i k hn hi).mpr ⟨hri, hkw⟩
      have hCiff : (rowBits Vertical.layout n w i x).testBit k = true ↔
          x.testBit (k / n) = true := by
        rw [rowBits_bit_iff n w i x k hn hi]
        simp [hri, hkw]
      have hA : (image Vertical.layout w xs).testBit k = (xs[i]?.getD 0).testBit (k / n) :=
        Bool.eq_iff_iff.mpr hAiff
      have hD : (image Vertical.layout w (xs.set i x)).testBit k = x.testBit (k / n) :=
        Bool.eq_iff_iff.mpr hDiff
      have hC : (rowBits Vertical.layout n w i x).testBit k = x.testBit (k / n) :=
        Bool.eq_iff_iff.mpr hCiff
      by_cases ha : (xs[i]?.getD 0).testBit (k / n) <;>
        by_cases hc : x.testBit (k / n) <;>
          simp [n, Nat.testBit_or, Nat.testBit_xor, Nat.testBit_and, hA, hB, hC, hD, ha, hc]
    · have hAiff : (image Vertical.layout w xs).testBit k = true ↔
          (xs[k % n]?.getD 0).testBit (k / n) = true := by
        constructor
        · intro h
          have r := (image_bit_iff w k xs).mp h
          simpa [n, hn, hkw] using r.2.2
        · intro h
          exact (image_bit_iff w k xs).mpr (by simpa [n, hn, hkw] using And.intro hn (And.intro hkw h))
      have hDiff : (image Vertical.layout w (xs.set i x)).testBit k = true ↔
          (xs[k % n]?.getD 0).testBit (k / n) = true := by
        constructor
        · intro h
          have r := (image_bit_iff w k (xs.set i x)).mp h
          have r' : 0 < n ∧ k / n < w ∧ ((xs.set i x)[k % n]?.getD 0).testBit (k / n) = true := by
            simpa [n, hlen] using r
          have hne : i ≠ k % n := by omega
          have hx : (xs.set i x)[k % n]?.getD 0 = xs[k % n]?.getD 0 := by
            simp [List.getElem?_set, hne]
          simpa [hx] using r'.2.2
        · intro h
          apply (image_bit_iff w k (xs.set i x)).mpr
          have hne : i ≠ k % n := by omega
          have h' : ((xs.set i x)[k % (xs.set i x).length]?.getD 0).testBit (k / (xs.set i x).length) = true := by
            simpa [hlen, n, List.getElem?_set, hne] using h
          simpa [hlen, n, hn, hkw] using (And.intro hn (And.intro hkw h'))
      have hB : (rowMask Vertical.layout n w i).testBit k = false := by
        cases h : (rowMask Vertical.layout n w i).testBit k with
        | false => simpa using h
        | true => exact False.elim (hri (((rowMask_bit_iff n w i k hn hi).mp h).1))
      have hC : (rowBits Vertical.layout n w i x).testBit k = false := by
        cases h : (rowBits Vertical.layout n w i x).testBit k with
        | false => simpa using h
        | true => exact False.elim (hri (((rowBits_bit_iff n w i x k hn hi).mp h).1))
      have hA : (image Vertical.layout w xs).testBit k = (xs[k % n]?.getD 0).testBit (k / n) :=
        Bool.eq_iff_iff.mpr hAiff
      have hD : (image Vertical.layout w (xs.set i x)).testBit k = (xs[k % n]?.getD 0).testBit (k / n) :=
        Bool.eq_iff_iff.mpr hDiff
      by_cases ha : (xs[k % n]?.getD 0).testBit (k / n) <;>
        simp [n, Nat.testBit_or, Nat.testBit_xor, Nat.testBit_and, hA, hB, hC, hD, ha]
  · have hA : (image Vertical.layout w xs).testBit k = false := by
      cases h : (image Vertical.layout w xs).testBit k with
      | false => simpa using h
      | true =>
          have r := (image_bit_iff w k xs).mp h
          exact False.elim (hkw (by simpa [n] using r.2.1))
    have hD : (image Vertical.layout w (xs.set i x)).testBit k = false := by
      cases h : (image Vertical.layout w (xs.set i x)).testBit k with
      | false => simpa using h
      | true =>
          have r := (image_bit_iff w k (xs.set i x)).mp h
          exact False.elim (hkw (by simpa [n, hlen] using r.2.1))
    have hB : (rowMask Vertical.layout n w i).testBit k = false := by
      cases h : (rowMask Vertical.layout n w i).testBit k with
      | false => simpa using h
      | true => exact False.elim (hkw (((rowMask_bit_iff n w i k hn hi).mp h).2))
    have hC : (rowBits Vertical.layout n w i x).testBit k = false := by
      cases h : (rowBits Vertical.layout n w i x).testBit k with
      | false => simpa using h
      | true => exact False.elim (hkw (((rowBits_bit_iff n w i x k hn hi).mp h).2.1))
    simp [n, Nat.testBit_or, Nat.testBit_xor, Nat.testBit_and, hA, hB, hC, hD]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_Vertical_MaskedPatch
