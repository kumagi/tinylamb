import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_ByteAlignedPlane_MaskedPatch

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedPlane

-- Truth checks on small inputs (masked patch preserves every bit cell).
#eval image layout 3 ([1, 2, 3].set 1 7) ==
  ((image layout 3 [1, 2, 3] ^^^ (image layout 3 [1, 2, 3] &&& rowMask layout 3 3 1)) ||| rowBits layout 3 3 1 7)
#eval image layout 0 [] == 0
#eval image layout 5 ([31, 0, 7, 255, 16].set 2 0) ==
  ((image layout 5 [31, 0, 7, 255, 16] ^^^ (image layout 5 [31, 0, 7, 255, 16] &&& rowMask layout 5 5 2)) ||| rowBits layout 5 5 2 0)
#eval image layout 4 ([0, 0, 0, 0].set 3 15) ==
  ((image layout 4 [0, 0, 0, 0] ^^^ (image layout 4 [0, 0, 0, 0] &&& rowMask layout 4 4 3)) ||| rowBits layout 4 4 3 15)

/-! ## Generic fold infrastructure -/

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

/-! ## ByteAlignedPlane bit cells -/

/-- In a byte-aligned plane, lane `b` of row `i` sits at `b * align 8 n + i`.
Since `i < n ≤ align 8 n`, the offset `i` survives reduction modulo the stride. -/
private theorem rowBits_bit_iff (n w i x k : Nat) (hn : 0 < n) (hi : i < n) :
    (rowBits layout n w i x).testBit k = true ↔
      k % align 8 n = i ∧ k / align 8 n < w ∧ x.testBit (k / align 8 n) = true := by
  have hAge : n ≤ align 8 n := by unfold align; omega
  have hApos : 0 < align 8 n := by omega
  have hiA : i < align 8 n := by omega
  unfold rowBits layout lanes
  rw [fold_cond_or_bit_iff]
  constructor
  · rintro (hacc | ⟨b, hbm, hpb, hbit⟩)
    · have hz : (0 : Nat).testBit k = false := by simp [Nat.testBit]
      rw [hz] at hacc
      cases hacc
    · have hbw : b < w := by simpa using hbm
      have hEq : b * align 8 n + i = k := by
        simpa [layout, Nat.testBit_two_pow] using hbit
      have hmod : k % align 8 n = i := by
        rw [← hEq, Nat.mul_comm b (align 8 n), Nat.mul_add_mod_self_left, Nat.mod_eq_of_lt hiA]
      have hdiv : k / align 8 n = b := by
        rw [← hEq, Nat.mul_comm b (align 8 n), Nat.mul_add_div hApos b i,
          Nat.div_eq_of_lt hiA, Nat.add_zero]
      exact ⟨hmod, by simpa [hdiv] using hbw, by simpa [hdiv] using hpb⟩
  · rintro ⟨hmod, hltw, hxb⟩
    right
    refine ⟨k / align 8 n, by simpa using hltw, hxb, ?_⟩
    have hEq : (k / align 8 n) * align 8 n + i = k := by
      rw [← hmod, Nat.mul_comm, Nat.div_add_mod]
    simpa [layout, hEq]

private theorem rowMask_bit_iff (n w i k : Nat) (hn : 0 < n) (hi : i < n) :
    (rowMask layout n w i).testBit k = true ↔
      k % align 8 n = i ∧ k / align 8 n < w := by
  have hAge : n ≤ align 8 n := by unfold align; omega
  have hApos : 0 < align 8 n := by omega
  have hiA : i < align 8 n := by omega
  unfold rowMask layout lanes
  rw [fold_or_bit_iff]
  constructor
  · intro hor
    have hex : ∃ b ∈ List.range w, (2 ^ (layout.bit n w i b)).testBit k = true := by
      rcases hor with hacc | hex
      · have hz : (0 : Nat).testBit k = false := by simp [Nat.testBit]
        rw [hz] at hacc
        cases hacc
      · exact hex
    rcases hex with ⟨b, hbm, hbit⟩
    have hbw : b < w := by simpa using hbm
    have hEq : b * align 8 n + i = k := by
      simpa [layout, Nat.testBit_two_pow] using hbit
    have hmod : k % align 8 n = i := by
      rw [← hEq, Nat.mul_comm b (align 8 n), Nat.mul_add_mod_self_left, Nat.mod_eq_of_lt hiA]
    have hdiv : k / align 8 n = b := by
      rw [← hEq, Nat.mul_comm b (align 8 n), Nat.mul_add_div hApos b i,
        Nat.div_eq_of_lt hiA, Nat.add_zero]
    exact ⟨hmod, by simpa [hdiv] using hbw⟩
  · rintro ⟨hmod, hltw⟩
    right
    refine ⟨k / align 8 n, by simpa using hltw, ?_⟩
    have hEq : (k / align 8 n) * align 8 n + i = k := by
      rw [← hmod, Nat.mul_comm, Nat.div_add_mod]
    simpa [layout, hEq]

private theorem foldl_zipIdx_rowBits (w : Nat) (xs : List Nat) :
    (xs.zipIdx).foldl (fun a q =>
      (List.range w).foldl (fun z b => if q.1.testBit b then z ||| 2 ^ (b * align 8 xs.length + q.2) else z) a) 0 =
    (xs.zipIdx).foldl (fun a q => a ||| rowBits layout xs.length w q.2 q.1) 0 := by
  symm
  refine List.foldl_hom id ?_
  intro x q
  unfold rowBits layout lanes
  exact (foldl_cond_or_acc (List.range w) (fun b => q.1.testBit b)
    (fun b => 2 ^ (b * align 8 xs.length + q.2)) x).symm

private theorem image_eq_foldl_or (w : Nat) (xs : List Nat) :
    image layout w xs =
      xs.zipIdx.foldl (fun acc q => acc ||| rowBits layout xs.length w q.2 q.1) 0 := by
  unfold image layout lanes
  exact foldl_zipIdx_rowBits w xs

/-- Which zipIdx entry can contribute a given bit cell?  A strand offset that
falls in the byte-alignment padding simply reads back as zero via `getD`. -/
private theorem zipIdx_fold_bit_iff (n w k base : Nat) (xs : List Nat)
    (hlen : base + xs.length = n) (hn : 0 < n) :
    (∃ q ∈ xs.zipIdx base, (rowBits layout n w q.2 q.1).testBit k = true) ↔
      base ≤ k % align 8 n ∧ k / align 8 n < w ∧
        (xs[k % align 8 n - base]?.getD 0).testBit (k / align 8 n) = true := by
  induction xs generalizing base with
  | nil =>
      have hb : base = n := by omega
      subst n
      simp
  | cons a tl ih =>
      have hlenpos : 0 < (a :: tl).length := by simp
      have hlen2 := hlen
      simp only [List.length_cons] at hlen2
      have hbaselt : base < n := by omega
      have hlen' : base + 1 + tl.length = n := by omega
      constructor
      · rintro ⟨q, hqmem, hqbit⟩
        rw [List.zipIdx_cons] at hqmem
        rw [List.mem_cons] at hqmem
        rcases hqmem with hqhead | hqtail
        · subst q
          have hh := (rowBits_bit_iff n w base a k hn hbaselt).mp hqbit
          rcases hh with ⟨hmod, hltw, hbit⟩
          have hge : base ≤ k % align 8 n := by omega
          have hidx : k % align 8 n - base = 0 := by omega
          exact ⟨hge, hltw, by simpa [hidx] using hbit⟩
        · have ht := (ih (base + 1) hlen').mp ⟨q, hqtail, hqbit⟩
          rcases ht with ⟨hge, hltw, hbit⟩
          have hge' : base ≤ k % align 8 n := by omega
          have hgt : base + 1 ≤ k % align 8 n := hge
          have hsub : k % align 8 n - base = (k % align 8 n - (base + 1)) + 1 := by omega
          exact ⟨hge', hltw, by simpa [hsub] using hbit⟩
      · rintro ⟨hge, hltw, hbit⟩
        by_cases heq : k % align 8 n = base
        · refine ⟨(a, base), by simp, ?_⟩
          have hidx : k % align 8 n - base = 0 := by omega
          exact (rowBits_bit_iff n w base a k hn hbaselt).mpr
            ⟨heq, hltw, by simpa [hidx] using hbit⟩
        · have hgt : base + 1 ≤ k % align 8 n := by omega
          have hsub : k % align 8 n - base = (k % align 8 n - (base + 1)) + 1 := by omega
          have ht : ∃ q ∈ tl.zipIdx (base + 1), (rowBits layout n w q.2 q.1).testBit k = true :=
            (ih (base + 1) hlen').mpr ⟨hgt, hltw, by simpa [hsub] using hbit⟩
          rcases ht with ⟨q, hqmem, hqbit⟩
          exact ⟨q, by simp [hqmem], hqbit⟩

private theorem image_bit_iff (w k : Nat) (xs : List Nat) :
    (image layout w xs).testBit k = true ↔
      0 < xs.length ∧ k / align 8 xs.length < w ∧
        (xs[k % align 8 xs.length]?.getD 0).testBit (k / align 8 xs.length) = true := by
  rw [image_eq_foldl_or]
  rw [fold_or_bit_iff]
  have hz : (0 : Nat).testBit k = false := by simp [Nat.testBit]
  rw [hz]
  cases xs with
  | nil => simp
  | cons a tl =>
      have hn : 0 < (a :: tl).length := by simp
      simpa [hn] using
        (zipIdx_fold_bit_iff (a :: tl).length w k 0 (a :: tl) (by simp) hn)

theorem sound_MaskedPatch : ByteAlignedPlane.MaskedPatch.Goal := by
  intro w xs i x hi
  let n := xs.length
  have hn : 0 < n := by omega
  have hlen : (xs.set i x).length = n := by simp [n]
  apply Nat.eq_of_testBit_eq
  intro k
  by_cases hkw : k / align 8 n < w
  · by_cases hri : k % align 8 n = i
    · have hAiff : (image layout w xs).testBit k = true ↔
          (xs[i]?.getD 0).testBit (k / align 8 n) = true := by
        constructor
        · intro h
          have r := (image_bit_iff w k xs).mp h
          simpa [n, hri] using r.2.2
        · intro h
          apply (image_bit_iff w k xs).mpr
          simpa [n, hn, hkw, hri] using (And.intro hn (And.intro hkw h))
      have hDiff : (image layout w (xs.set i x)).testBit k = true ↔
          x.testBit (k / align 8 n) = true := by
        constructor
        · intro h
          have r := (image_bit_iff w k (xs.set i x)).mp h
          have r' : 0 < n ∧ k / align 8 n < w ∧
              ((xs.set i x)[k % align 8 n]?.getD 0).testBit (k / align 8 n) = true := by
            simpa [n, hlen] using r
          have hx : (xs.set i x)[k % align 8 n]?.getD 0 = x := by simp [hri, hi]
          simpa [hx] using r'.2.2
        · intro h
          apply (image_bit_iff w k (xs.set i x)).mpr
          have hxmod : k % align 8 (xs.set i x).length = i := by simpa [hlen] using hri
          have hget : ((xs.set i x)[k % align 8 (xs.set i x).length]?.getD 0).testBit
                (k / align 8 (xs.set i x).length) = true := by
            rw [hxmod]
            simpa [hlen, hi] using h
          have hkw' : k / align 8 (xs.set i x).length < w := by simpa [hlen] using hkw
          exact (by
            simpa [hlen, n, hn] using
              (And.intro hn (And.intro hkw' hget)))
      have hB : (rowMask layout n w i).testBit k = true :=
        (rowMask_bit_iff n w i k hn hi).mpr ⟨hri, hkw⟩
      have hCiff : (rowBits layout n w i x).testBit k = true ↔
          x.testBit (k / align 8 n) = true := by
        rw [rowBits_bit_iff n w i x k hn hi]
        simp [hri, hkw]
      have hA : (image layout w xs).testBit k = (xs[i]?.getD 0).testBit (k / align 8 n) :=
        Bool.eq_iff_iff.mpr hAiff
      have hD : (image layout w (xs.set i x)).testBit k = x.testBit (k / align 8 n) :=
        Bool.eq_iff_iff.mpr hDiff
      have hC : (rowBits layout n w i x).testBit k = x.testBit (k / align 8 n) :=
        Bool.eq_iff_iff.mpr hCiff
      by_cases ha : (xs[i]?.getD 0).testBit (k / align 8 n) <;>
        by_cases hc : x.testBit (k / align 8 n) <;>
          simp [n, Nat.testBit_or, Nat.testBit_xor, Nat.testBit_and, hA, hB, hC, hD, ha, hc]
    · have hAiff : (image layout w xs).testBit k = true ↔
          (xs[k % align 8 n]?.getD 0).testBit (k / align 8 n) = true := by
        constructor
        · intro h
          have r := (image_bit_iff w k xs).mp h
          simpa [n] using r.2.2
        · intro h
          apply (image_bit_iff w k xs).mpr
          simpa [n, hn, hkw] using (And.intro hn (And.intro hkw h))
      have hDiff : (image layout w (xs.set i x)).testBit k = true ↔
          (xs[k % align 8 n]?.getD 0).testBit (k / align 8 n) = true := by
        constructor
        · intro h
          have r := (image_bit_iff w k (xs.set i x)).mp h
          have r' : 0 < n ∧ k / align 8 n < w ∧
              ((xs.set i x)[k % align 8 n]?.getD 0).testBit (k / align 8 n) = true := by
            simpa [n, hlen] using r
          have hne : i ≠ k % align 8 n := by omega
          have hx : (xs.set i x)[k % align 8 n]?.getD 0 = xs[k % align 8 n]?.getD 0 := by
            simp [List.getElem?_set, hne]
          simpa [hx] using r'.2.2
        · intro h
          apply (image_bit_iff w k (xs.set i x)).mpr
          have hne : i ≠ k % align 8 n := by omega
          have hget : (xs.set i x)[k % align 8 (xs.set i x).length]?.getD 0 =
              xs[k % align 8 n]?.getD 0 := by
            rw [hlen]
            simp [List.getElem?_set, hne]
          have h' : ((xs.set i x)[k % align 8 (xs.set i x).length]?.getD 0).testBit
                (k / align 8 (xs.set i x).length) = true := by
            rw [hget]
            simpa [hlen] using h
          have hkw' : k / align 8 (xs.set i x).length < w := by simpa [hlen] using hkw
          exact (by
            simpa [hlen, n, hn] using
              (And.intro hn (And.intro hkw' h')))
      have hB : (rowMask layout n w i).testBit k = false := by
        cases h : (rowMask layout n w i).testBit k with
        | false => simpa using h
        | true => exact False.elim (hri (((rowMask_bit_iff n w i k hn hi).mp h).1))
      have hC : (rowBits layout n w i x).testBit k = false := by
        cases h : (rowBits layout n w i x).testBit k with
        | false => simpa using h
        | true => exact False.elim (hri (((rowBits_bit_iff n w i x k hn hi).mp h).1))
      have hA : (image layout w xs).testBit k = (xs[k % align 8 n]?.getD 0).testBit (k / align 8 n) :=
        Bool.eq_iff_iff.mpr hAiff
      have hD : (image layout w (xs.set i x)).testBit k = (xs[k % align 8 n]?.getD 0).testBit (k / align 8 n) :=
        Bool.eq_iff_iff.mpr hDiff
      by_cases ha : (xs[k % align 8 n]?.getD 0).testBit (k / align 8 n) <;>
        simp [n, Nat.testBit_or, Nat.testBit_xor, Nat.testBit_and, hA, hB, hC, hD, ha]
  · have hA : (image layout w xs).testBit k = false := by
      cases h : (image layout w xs).testBit k with
      | false => simpa using h
      | true =>
          have r := (image_bit_iff w k xs).mp h
          exact False.elim (hkw (by simpa [n] using r.2.1))
    have hD : (image layout w (xs.set i x)).testBit k = false := by
      cases h : (image layout w (xs.set i x)).testBit k with
      | false => simpa using h
      | true =>
          have r := (image_bit_iff w k (xs.set i x)).mp h
          exact False.elim (hkw (by simpa [n, hlen] using r.2.1))
    have hB : (rowMask layout n w i).testBit k = false := by
      cases h : (rowMask layout n w i).testBit k with
      | false => simpa using h
      | true => exact False.elim (hkw (((rowMask_bit_iff n w i k hn hi).mp h).2))
    have hC : (rowBits layout n w i x).testBit k = false := by
      cases h : (rowBits layout n w i x).testBit k with
      | false => simpa using h
      | true => exact False.elim (hkw (((rowBits_bit_iff n w i x k hn hi).mp h).2.1))
    simp [n, Nat.testBit_or, Nat.testBit_xor, Nat.testBit_and, hA, hB, hC, hD]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_ByteAlignedPlane_MaskedPatch
