import TinylambOpt.ResearchGoals.Atlas.TextExecution

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DevinBatch.Atlas_TextExecution_B

open TinylambOpt.ResearchGoals.Atlas.TextExecution
open Wavelet
open Lcp

/-! ### Shared list helpers -/

/-- `(l.take n).filter p` is a prefix of `l.filter p` of the same length. -/
private theorem filter_take_eq {α : Type} (ys : List α) (p : α → Bool) (i : Nat) :
    (ys.take i).filter p = (ys.filter p).take ((ys.take i).filter p).length := by
  induction i generalizing ys with
  | zero => simp
  | succ i ih =>
      cases ys with
      | nil => simp
      | cons y ys =>
          rw [List.take_cons (by omega : 0 < i + 1)]
          simp only [Nat.add_sub_cancel_right]
          by_cases hp : p y = true
          · simp only [List.filter_cons, hp, ite_true, List.length_cons]
            rw [ih]
            have hle : (List.filter p (List.take i ys)).length ≤ (List.filter p ys).length :=
              (List.Sublist.filter p (List.take_sublist i ys)).length_le
            rw [List.length_take, Nat.min_eq_left hle]
            rw [List.take_cons (by omega : 0 < (List.filter p (List.take i ys)).length + 1)]
            simp
          · simp only [List.filter_cons, hp]
            rw [ih]
            simp

/-! ### Wavelet.RangeMap -/

theorem RangeMap_sound :
    TinylambOpt.ResearchGoals.Atlas.TextExecution.Wavelet.RangeMap.Goal := by
  intro bit l w xs high
  show (((xs.drop l).take w).filter (fun x => x.testBit bit == high)) =
    (((xs.filter (fun x => x.testBit bit == high)).drop
        ((xs.take l).countP (fun x => x.testBit bit == high))).take
      (((xs.drop l).take w).countP (fun x => x.testBit bit == high)))
  have hsplit : xs.filter (fun x => x.testBit bit == high) =
      (xs.take l).filter (fun x => x.testBit bit == high) ++
        (xs.drop l).filter (fun x => x.testBit bit == high) := by
    have h := List.filter_append (p := fun x => x.testBit bit == high) (xs.take l) (xs.drop l)
    rw [List.take_append_drop] at h
    exact h
  rw [hsplit]
  simp only [List.countP_eq_length_filter]
  rw [List.drop_left]
  exact filter_take_eq (xs.drop l) (fun x => x.testBit bit == high) w

/-! ### Wavelet.SameBranch -/

private theorem perm_lower_upper (bit : Nat) (b : Word) :
    (lower bit b ++ upper bit b).Perm b := by
  have h2 : (upper bit b ++ lower bit b).Perm b := by
    have h3 := List.filter_append_perm (fun y : Nat => y.testBit bit) b
    exact h3
  exact List.perm_append_comm.trans h2

private theorem count_eq_lower_add_upper (bit : Nat) (x : Nat) (b : Word) :
    b.count x = (lower bit b).count x + (upper bit b).count x := by
  have hc := (perm_lower_upper bit b).count x
  rw [List.count_append] at hc
  exact hc.symm

private theorem count_lower_eq_zero {bit : Nat} {x : Nat} {b : Word} (hx : x.testBit bit = true) :
    (lower bit b).count x = 0 := by
  rw [List.count_eq_zero]
  intro hmem
  have hmem' : x ∈ b.filter (fun y => !y.testBit bit) := hmem
  have h := (List.mem_filter.mp hmem').2
  rw [hx] at h
  simp at h

private theorem count_upper_eq_zero {bit : Nat} {x : Nat} {b : Word} (hx : x.testBit bit = false) :
    (upper bit b).count x = 0 := by
  rw [List.count_eq_zero]
  intro hmem
  have hmem' : x ∈ b.filter (fun y => y.testBit bit) := hmem
  have h := (List.mem_filter.mp hmem').2
  rw [hx] at h
  simp at h

private theorem count_eq_lower_of_testBit_false {bit : Nat} {x : Nat} {b : Word}
    (hx : x.testBit bit = false) :
    b.count x = (lower bit b).count x := by
  have h := count_eq_lower_add_upper bit x b
  have hz := count_upper_eq_zero (b := b) hx
  omega

private theorem count_eq_upper_of_testBit_true {bit : Nat} {x : Nat} {b : Word}
    (hx : x.testBit bit = true) :
    b.count x = (upper bit b).count x := by
  have h := count_eq_lower_add_upper bit x b
  have hz := count_lower_eq_zero (b := b) hx
  omega

theorem SameBranch_sound :
    TinylambOpt.ResearchGoals.Atlas.TextExecution.Wavelet.SameBranch.Goal := by
  intro a b bit
  induction a with
  | nil => simp [pairs, lower, upper]
  | cons x xs ih =>
      have hsplit : pairs (x :: xs) b = b.count x + pairs xs b := by
        simp [pairs]
      rw [hsplit, ih]
      cases hxb : x.testBit bit with
      | false =>
          have hxl : lower bit (x :: xs) = x :: lower bit xs := by
            simp [lower, hxb]
          have hxu : upper bit (x :: xs) = upper bit xs := by
            simp [upper, hxb]
          rw [hxl, hxu]
          have hcount : b.count x = (lower bit b).count x :=
            count_eq_lower_of_testBit_false hxb
          simp only [pairs, List.map_cons, List.sum_cons]
          rw [hcount]
          omega
      | true =>
          have hxl : lower bit (x :: xs) = lower bit xs := by
            simp [lower, hxb]
          have hxu : upper bit (x :: xs) = x :: upper bit xs := by
            simp [upper, hxb]
          rw [hxl, hxu]
          have hcount : b.count x = (upper bit b).count x :=
            count_eq_upper_of_testBit_true hxb
          simp only [pairs, List.map_cons, List.sum_cons]
          rw [hcount]
          omega

/-! ### Wavelet.BetweenCodes -/

theorem BetweenCodes_sound :
    TinylambOpt.ResearchGoals.Atlas.TextExecution.Wavelet.BetweenCodes.Goal := by
  intro xs level lo hi hlo
  apply List.countP_congr
  intro x hx
  simp only [Bool.and_eq_true_iff, decide_eq_true_eq]
  constructor
  · rintro ⟨h1, h2⟩
    exact ⟨(Nat.le_div_iff_mul_le (Nat.two_pow_pos level)).mpr h1,
           (Nat.div_lt_iff_lt_mul (Nat.two_pow_pos level)).mpr h2⟩
  · rintro ⟨h1, h2⟩
    exact ⟨(Nat.le_div_iff_mul_le (Nat.two_pow_pos level)).mp h1,
           (Nat.div_lt_iff_lt_mul (Nat.two_pow_pos level)).mp h2⟩

/-! ### Wavelet.DictionaryGrowth -/

theorem DictionaryGrowth_sound :
    TinylambOpt.ResearchGoals.Atlas.TextExecution.Wavelet.DictionaryGrowth.Goal := by
  intro d old more k hold hmore hk
  have hlow_old : lower d old = old := by
    apply List.filter_eq_self.mpr
    intro x hx
    have hb := Nat.testBit_lt_two_pow (hold x hx)
    simp [hb]
  have hup_old : upper d old = [] := by
    apply List.filter_eq_nil_iff.mpr
    intro x hx
    have hb := Nat.testBit_lt_two_pow (hold x hx)
    simp [hb]
  have hlow_more : lower d more = [] := by
    apply List.filter_eq_nil_iff.mpr
    intro y hy
    have h2 := hmore y hy
    have hb : y.testBit d = true :=
      Nat.testBit_of_two_pow_le_and_two_pow_add_one_gt h2.1 h2.2
    simp [hb]
  have hup_more : upper d more = more := by
    apply List.filter_eq_self.mpr
    intro y hy
    have h2 := hmore y hy
    have hb : y.testBit d = true :=
      Nat.testBit_of_two_pow_le_and_two_pow_add_one_gt h2.1 h2.2
    simp [hb]
  have hlow_app : lower d (old ++ more) = old := by
    show (old ++ more).filter (fun x => !x.testBit d) = old
    rw [List.filter_append]
    have e1 : old.filter (fun x => !x.testBit d) = old := by
      apply List.filter_eq_self.mpr
      intro x hx
      have hb := Nat.testBit_lt_two_pow (hold x hx)
      simp [hb]
    have e2 : more.filter (fun x => !x.testBit d) = [] := by
      apply List.filter_eq_nil_iff.mpr
      intro y hy
      have h2 := hmore y hy
      have hb : y.testBit d = true :=
        Nat.testBit_of_two_pow_le_and_two_pow_add_one_gt h2.1 h2.2
      simp [hb]
    rw [e1, e2]
    simp
  have hup_app : upper d (old ++ more) = more := by
    show (old ++ more).filter (fun x => x.testBit d) = more
    rw [List.filter_append]
    have e1 : old.filter (fun x => x.testBit d) = [] := by
      apply List.filter_eq_nil_iff.mpr
      intro x hx
      have hb := Nat.testBit_lt_two_pow (hold x hx)
      simp [hb]
    have e2 : more.filter (fun x => x.testBit d) = more := by
      apply List.filter_eq_self.mpr
      intro y hy
      have h2 := hmore y hy
      have hb : y.testBit d = true :=
        Nat.testBit_of_two_pow_le_and_two_pow_add_one_gt h2.1 h2.2
      simp [hb]
    rw [e1, e2]
    simp
  show (let zs := lower d (old ++ more)
        if k < zs.length then kth d zs k
        else 2 ^ d + kth d (upper d (old ++ more)) (k - zs.length)) =
      if k < old.length then kth d old k else 2 ^ d + kth d more (k - old.length)
  rw [hlow_app, hup_app]

/-! ### Wavelet.Quantile machinery -/

private theorem pow_succ_two (d : Nat) : 2 ^ (d + 1) = 2 ^ d * 2 := by
  rw [Nat.pow_succ]

private theorem sorted_mergeSortNat (ys : List Nat) :
    ys.mergeSort.Pairwise (fun a b : Nat => a ≤ b) := by
  have h := List.pairwise_mergeSort (le := fun a b : Nat => a ≤ b)
    (by intro a b c hab hbc
        exact decide_eq_true (by
          have hab' : a ≤ b := of_decide_eq_true hab
          have hbc' : b ≤ c := of_decide_eq_true hbc
          omega))
    (by intro a b
        by_cases h : a ≤ b
        · simp [h]
        · have h' : b ≤ a := by omega
          simp [h']) ys
  exact h.imp (by intro a b hab; exact of_decide_eq_true hab)

private theorem perm_sorted_eq (l₁ l₂ : List Nat) (hp : l₁.Perm l₂)
    (h1 : l₁.Pairwise (fun a b : Nat => a ≤ b))
    (h2 : l₂.Pairwise (fun a b : Nat => a ≤ b)) : l₁ = l₂ := by
  induction l₁ generalizing l₂ with
  | nil =>
      exact List.Perm.nil_eq hp
  | cons a xs ih =>
      cases l₂ with
      | nil =>
          have hnil := List.Perm.eq_nil hp
          simp at hnil
      | cons b ys =>
          have hb1 : b ∈ a :: xs := hp.mem_iff.mpr (by simp)
          have ha2 : a ∈ b :: ys := hp.mem_iff.mp (by simp)
          simp only [List.mem_cons] at hb1 ha2
          have hab : a ≤ b := by
            rcases hb1 with hbeq | hb1
            · omega
            · exact (List.pairwise_cons.mp h1).1 b hb1
          have hba : b ≤ a := by
            rcases ha2 with haeq | ha2
            · omega
            · exact (List.pairwise_cons.mp h2).1 a ha2
          have heq : a = b := by omega
          cases heq
          rw [ih ys (List.Perm.cons_inv hp) (List.Pairwise.of_cons h1) (List.Pairwise.of_cons h2)]

private theorem mergeSort_split (low high : List Nat) (d : Nat)
    (hlow : ∀ x ∈ low, x < 2 ^ d) (hhigh : ∀ x ∈ high, 2 ^ d ≤ x) :
    (low ++ high).mergeSort = low.mergeSort ++ high.mergeSort := by
  apply perm_sorted_eq
  · exact (List.mergeSort_perm (low ++ high) (fun a b : Nat => a ≤ b)).trans
      ((List.Perm.append (List.mergeSort_perm low (fun a b : Nat => a ≤ b))
        (List.mergeSort_perm high (fun a b : Nat => a ≤ b))).symm)
  · exact sorted_mergeSortNat _
  · rw [List.pairwise_append]
    refine ⟨sorted_mergeSortNat _, sorted_mergeSortNat _, ?_⟩
    intro a ha b hb
    have ha' : a ∈ low := (List.mergeSort_perm low (fun x y : Nat => x ≤ y)).mem_iff.mp ha
    have hb' : b ∈ high := (List.mergeSort_perm high (fun x y : Nat => x ≤ y)).mem_iff.mp hb
    have := hlow a ha'
    have := hhigh b hb'
    omega

private theorem mergeSort_map_add (low : List Nat) (c : Nat) :
    (low.map fun x => c + x).mergeSort = low.mergeSort.map fun x => c + x := by
  apply perm_sorted_eq
  · exact (List.mergeSort_perm (low.map fun x => c + x) (fun a b : Nat => a ≤ b)).trans
      (((List.mergeSort_perm low (fun a b : Nat => a ≤ b)).map fun x => c + x).symm)
  · exact sorted_mergeSortNat _
  · have hs := sorted_mergeSortNat low
    exact hs.map (fun x => c + x) (by intro a b hab; omega)

/-- If the low `d` bits of `c` vanish, adding `c` does not change `testBit i` for `i < d`. -/
private theorem testBit_add_of_mod (c y i d : Nat) (hc : c % 2 ^ d = 0) (hi : i < d) :
    (c + y).testBit i = y.testBit i := by
  have hi1 : i + 1 ≤ d := hi
  have hcdvd : 2 ^ (i + 1) ∣ c := by
    have h1 : 2 ^ (i + 1) ∣ 2 ^ d := Nat.pow_dvd_pow 2 hi1
    have h2 : 2 ^ d ∣ c := Nat.dvd_of_mod_eq_zero hc
    exact Nat.dvd_trans h1 h2
  have hcmod : c % 2 ^ (i + 1) = 0 := Nat.mod_eq_zero_of_dvd hcdvd
  have hmod : (c + y) % 2 ^ (i + 1) = y % 2 ^ (i + 1) := by
    rw [Nat.add_mod, hcmod, Nat.zero_add]
    exact Nat.mod_eq_of_lt (Nat.mod_lt _ (Nat.two_pow_pos _))
  have key : ∀ n : Nat, n.testBit i = (n % 2 ^ (i + 1)).testBit i := by
    intro n
    have hlt : i < i + 1 := Nat.lt_succ_self i
    rw [Nat.testBit_mod_two_pow]
    simp [hlt]
  rw [key (c + y), key y, hmod]

/-- `kth` is invariant under adding a multiple of `2^d` to every element. -/
private theorem kth_map_add_shift (d : Nat) (ys : List Nat) (c j : Nat)
    (hc : c % 2 ^ d = 0) (hj : j < ys.length) :
    kth d (ys.map fun y => c + y) j = kth d ys j := by
  induction d generalizing ys c j with
  | zero => rfl
  | succ d ih =>
      have hbit : ∀ y : Nat, (c + y).testBit d = y.testBit d :=
        fun y => testBit_add_of_mod c y d (d + 1) hc (Nat.lt_succ_self d)
      have hlow : lower d (ys.map fun y => c + y) = (lower d ys).map fun y => c + y := by
        show (ys.map fun y => c + y).filter (fun x => !x.testBit d) = _
        rw [List.filter_map]
        apply congrArg (List.map fun y => c + y)
        apply List.filter_congr
        intro y hy
        simp only [Function.comp_apply]
        rw [hbit y]
      have hup : upper d (ys.map fun y => c + y) = (upper d ys).map fun y => c + y := by
        show (ys.map fun y => c + y).filter (fun x => x.testBit d) = _
        rw [List.filter_map]
        apply congrArg (List.map fun y => c + y)
        apply List.filter_congr
        intro y hy
        simp only [Function.comp_apply]
        rw [hbit y]
      have hlen_low : (lower d (ys.map fun y => c + y)).length = (lower d ys).length := by
        rw [hlow, List.length_map]
      have hlen_up : (upper d (ys.map fun y => c + y)).length = (upper d ys).length := by
        rw [hup, List.length_map]
      have hc' : c % 2 ^ d = 0 := by
        have h1 : 2 ^ d ∣ 2 ^ (d + 1) := Nat.pow_dvd_pow 2 (Nat.le_succ d)
        have h2 : 2 ^ (d + 1) ∣ c := Nat.dvd_of_mod_eq_zero hc
        exact Nat.mod_eq_zero_of_dvd (Nat.dvd_trans h1 h2)
      have hperm : ys.Perm (lower d ys ++ upper d ys) := by
        have h := List.filter_append_perm (fun y : Nat => y.testBit d) ys
        exact h.symm.trans List.perm_append_comm
      have hlen : (lower d ys).length + (upper d ys).length = ys.length := by
        have := hperm.length_eq
        rw [List.length_append] at this
        exact this.symm
      show (if j < (lower d (ys.map fun y => c + y)).length then
              kth d (lower d (ys.map fun y => c + y)) j
            else 2 ^ d + kth d (upper d (ys.map fun y => c + y))
              (j - (lower d (ys.map fun y => c + y)).length)) =
          (if j < (lower d ys).length then kth d (lower d ys) j
            else 2 ^ d + kth d (upper d ys) (j - (lower d ys).length))
      rw [hlow, hup]
      simp only [List.length_map]
      by_cases hjl : j < (lower d ys).length
      · simp [hjl]
        exact ih (lower d ys) c j hc' hjl
      · simp [hjl]
        apply ih (upper d ys) c (j - (lower d ys).length) hc'
        omega

private theorem kth_succ (d : Nat) (xs : List Nat) (k : Nat) :
    kth (d + 1) xs k =
      if k < (lower d xs).length then kth d (lower d xs) k
      else 2 ^ d + kth d (upper d xs) (k - (lower d xs).length) := by
  rfl

theorem Quantile_sound :
    TinylambOpt.ResearchGoals.Atlas.TextExecution.Wavelet.Quantile.Goal := by
  intro d
  induction d with
  | zero =>
      intro xs k hbound hk
      have hklen : k < xs.mergeSort.length := by
        have hlen := (List.mergeSort_perm xs (fun a b : Nat => a ≤ b)).length_eq
        omega
      have hopt : xs.mergeSort[k]? = some (xs.mergeSort[k]) :=
        List.getElem?_eq_some_iff.mpr ⟨hklen, rfl⟩
      rw [hopt]
      have hmem : xs.mergeSort[k] ∈ xs :=
        List.mem_mergeSort.mp (List.getElem_mem hklen)
      have hzero : xs.mergeSort[k] = 0 := by
        have h1 : xs.mergeSort[k] < 1 := hbound _ hmem
        omega
      rw [hzero]
      rfl
  | succ d ih =>
      intro xs k hbound hk
      have hbit_old : ∀ x ∈ lower d xs, x < 2 ^ d := by
        intro x hx
        have hx' : x ∈ xs := (List.mem_filter.mp hx).1
        have hz : (!x.testBit d) = true := (List.mem_filter.mp hx).2
        have hbit : x.testBit d = false := by simpa using hz
        have hlt : x < 2 ^ (d + 1) := hbound x hx'
        by_cases hge : 2 ^ d ≤ x
        · exfalso
          have htrue := Nat.testBit_of_two_pow_le_and_two_pow_add_one_gt hge hlt
          rw [hbit] at htrue
          exact absurd htrue (by decide)
        · exact Nat.lt_of_not_ge hge
      have hbit_up : ∀ x ∈ upper d xs, 2 ^ d ≤ x := by
        intro x hx
        have hbit : x.testBit d = true := (List.mem_filter.mp hx).2
        exact Nat.ge_two_pow_of_testBit hbit
      have hperm : xs.Perm (lower d xs ++ upper d xs) := by
        have h := List.filter_append_perm (fun y : Nat => y.testBit d) xs
        exact h.symm.trans List.perm_append_comm
      have hms : xs.mergeSort = (lower d xs).mergeSort ++ (upper d xs).mergeSort := by
        have h1 : xs.mergeSort = (lower d xs ++ upper d xs).mergeSort := by
          apply perm_sorted_eq
          · exact (List.mergeSort_perm xs _).trans
              (hperm.trans (List.mergeSort_perm _ _).symm)
          · exact sorted_mergeSortNat _
          · exact sorted_mergeSortNat _
        rw [h1]
        exact mergeSort_split (lower d xs) (upper d xs) d hbit_old hbit_up
      have hlen_zs : (lower d xs).length + (upper d xs).length = xs.length := by
        have := hperm.length_eq
        rw [List.length_append] at this
        exact this.symm
      have hml : (lower d xs).mergeSort.length = (lower d xs).length :=
        (List.mergeSort_perm _ _).length_eq
      rw [hms, List.getElem?_append]
      by_cases hkz : k < (lower d xs).length
      · rw [if_pos (by rw [hml]; exact hkz)]
        rw [ih (lower d xs) k hbit_old hkz, kth_succ, if_pos hkz]
      · rw [if_neg (by rw [hml]; exact hkz)]
        rw [hml, kth_succ, if_neg hkz]
        -- upper branch via lows = upper.map (· % 2^d)
        have hmod : ∀ x ∈ upper d xs, x % 2 ^ d = x - 2 ^ d := by
          intro x hx
          have h1 : 2 ^ d ≤ x := hbit_up x hx
          have h2 : x < 2 ^ (d + 1) := hbound x ((List.mem_filter.mp hx).1)
          have hpow : 2 * 2 ^ d = 2 ^ (d + 1) := by rw [pow_succ_two]; omega
          have hdiv : x / 2 ^ d = 1 := by
            have hge : 1 ≤ x / 2 ^ d := by
              rw [Nat.le_div_iff_mul_le (Nat.two_pow_pos d)]
              simpa using h1
            have hlt : x / 2 ^ d < 2 := by
              rw [Nat.div_lt_iff_lt_mul (Nat.two_pow_pos d), hpow]
              exact h2
            omega
          have hdm := Nat.div_add_mod x (2 ^ d)
          rw [hdiv] at hdm
          omega
        have hos : upper d xs =
            ((upper d xs).map fun x => x % 2 ^ d).map (fun y => 2 ^ d + y) := by
          have hmap : ((upper d xs).map fun x => x % 2 ^ d).map (fun y => 2 ^ d + y) =
              (upper d xs).map (fun x => 2 ^ d + x % 2 ^ d) := by
            rw [List.map_map]
            rfl
          rw [hmap]
          symm
          have h2 := List.map_congr_left (l := upper d xs)
            (f := fun x => 2 ^ d + x % 2 ^ d) (g := fun x => x) (by
              intro x hx
              have h1 : 2 ^ d ≤ x := hbit_up x hx
              have h3 := hmod x hx
              show 2 ^ d + x % 2 ^ d = x
              omega)
          rwa [List.map_id'] at h2
        have hkk : k - (lower d xs).length <
            ((upper d xs).map fun x => x % 2 ^ d).length := by
          rw [List.length_map]
          omega
        have hih := ih ((upper d xs).map fun x => x % 2 ^ d)
          (k - (lower d xs).length)
          (fun y hy => by
            rcases List.mem_map.mp hy with ⟨x, hx, rfl⟩
            exact Nat.mod_lt x (Nat.two_pow_pos d)) hkk
        have hms_up : (upper d xs).mergeSort =
            (((upper d xs).map fun x => x % 2 ^ d).mergeSort).map (fun y => 2 ^ d + y) := by
          rw [hos]
          exact mergeSort_map_add _ (2 ^ d)
        have hkth_eq : kth d (upper d xs) (k - (lower d xs).length) =
            kth d ((upper d xs).map fun x => x % 2 ^ d) (k - (lower d xs).length) :=
          (congrArg (fun l => kth d l (k - (lower d xs).length)) hos).trans
            (kth_map_add_shift _ _ _ _ (by simp) hkk)
        rw [hms_up, List.getElem?_map, hih]
        show (some (2 ^ d + kth d ((upper d xs).map fun x => x % 2 ^ d)
            (k - (lower d xs).length))) =
          some (2 ^ d + kth d (upper d xs) (k - (lower d xs).length))
        congr 1
        exact hkth_eq.symm
