import TinylambOpt.ResearchGoals.Atlas.TextExecution

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Wavelet_Quantile

open TinylambOpt.ResearchGoals.Atlas.TextExecution
open TinylambOpt.ResearchGoals.Atlas.TextExecution.Wavelet

/-! `Wavelet.Quantile` says that the wavelet-tree selection `kth` returns the
k-th element of the sorted document-ID list.  This is true: at each level the
current bit splits the domain into two consecutive halves, and `kth` recurses
into the half containing the requested rank. -/

private def genWords : Nat → List Word
  | 0 => [[]]
  | n + 1 => genWords n ++ (genWords n).flatMap (fun xs => (List.range 8).map (fun x => x :: xs))

private def bruteQuantile : Bool :=
  [0, 1, 2, 3].all (fun d => (genWords 3).all (fun xs =>
    if xs.all (fun x => x < 2 ^ d) then
      (List.range xs.length).all (fun k => decide (xs.mergeSort[k]? = some (kth d xs k)))
    else true))

#guard bruteQuantile

private theorem filter_eq_self_of_all_true {α : Type} (p : α → Bool) (l : List α)
    (h : ∀ x ∈ l, p x = true) : l.filter p = l := by
  induction l with
  | nil => simp
  | cons a as ih =>
      have ha : p a = true := h a (by simp)
      have ht : ∀ x ∈ as, p x = true := by intro x hx; exact h x (by simp [hx])
      simp [ha, ih ht]

private theorem filter_eq_nil_of_all_false {α : Type} (p : α → Bool) (l : List α)
    (h : ∀ x ∈ l, p x = false) : l.filter p = [] := by
  induction l with
  | nil => simp
  | cons a as ih =>
      have ha : p a = false := h a (by simp)
      have ht : ∀ x ∈ as, p x = false := by intro x hx; exact h x (by simp [hx])
      simp [ha, ih ht]

private theorem lt_two_pow_of_testBit_false {d x : Nat} (h : x < 2 ^ (d + 1))
    (hb : x.testBit d = false) : x < 2 ^ d := by
  by_cases hlt : x < 2 ^ d
  · exact hlt
  · have hge : 2 ^ d ≤ x := Nat.le_of_not_gt hlt
    have ht := Nat.testBit_of_two_pow_le_and_two_pow_add_one_gt hge h
    rw [hb] at ht
    cases ht

/-- `kth` only uses the lower bits and stable filters, hence is permutation
invariant. -/
private theorem kth_perm {d : Nat} {xs ys : Word} (hxy : xs.Perm ys) (k : Nat) :
    kth d xs k = kth d ys k := by
  induction d generalizing xs ys k with
  | zero => simp [kth]
  | succ d ih =>
      have hlo : (lower d xs).Perm (lower d ys) := by
        simpa [lower] using hxy.filter (fun x => !x.testBit d)
      have hhi : (upper d xs).Perm (upper d ys) := by
        simpa [upper] using hxy.filter (fun x => x.testBit d)
      have hlen := hlo.length_eq
      simp only [kth, hlen]
      by_cases hk : k < (lower d ys).length
      · rw [ite_eq_left hk, ite_eq_left hk]
        exact ih hlo k
      · rw [ite_eq_right hk, ite_eq_right hk]
        congr 1
        exact ih hhi (k - (lower d ys).length)

private theorem testBit_add_mul_two_pow (d m x : Nat) :
    (m * 2 ^ (d + 1) + x).testBit d = x.testBit d := by
  induction m with
  | zero => simp
  | succ m ih =>
      calc
        ((m + 1) * 2 ^ (d + 1) + x).testBit d =
            (2 ^ (d + 1) + (m * 2 ^ (d + 1) + x)).testBit d := by
              congr 1
              rw [Nat.add_mul]
              simp
              omega
        _ = (m * 2 ^ (d + 1) + x).testBit d :=
              Nat.testBit_two_pow_add_gt (Nat.lt_succ_self d) _
        _ = x.testBit d := ih

/-- Adding a multiple of `2^d` does not change any of the lower `d` bits,
so `kth d` is unaffected. -/
private theorem kth_add_mul_pow (d j m : Nat) (xs : Word) :
    kth d (xs.map (fun x => m * 2 ^ d + x)) j = kth d xs j := by
  induction d generalizing xs j m with
  | zero => simp [kth]
  | succ d ih =>
      have hbit : ∀ x : Nat, (m * 2 ^ (d + 1) + x).testBit d = x.testBit d :=
        testBit_add_mul_two_pow d m
      have hpow : m * 2 ^ (d + 1) = (2 * m) * 2 ^ d := by
        rw [Nat.pow_succ]
        ac_rfl
      have hlow : lower d (xs.map (fun x => m * 2 ^ (d + 1) + x)) =
          (lower d xs).map (fun x => m * 2 ^ (d + 1) + x) := by
        unfold lower
        rw [List.filter_map]
        congr 1
        exact List.filter_congr (by
          intro x hx
          simp only [Function.comp_apply]
          rw [hbit])
      have hup : upper d (xs.map (fun x => m * 2 ^ (d + 1) + x)) =
          (upper d xs).map (fun x => m * 2 ^ (d + 1) + x) := by
        unfold upper
        rw [List.filter_map]
        congr 1
        exact List.filter_congr (by
          intro x hx
          simp only [Function.comp_apply]
          rw [hbit])
      have ihlo : kth d ((lower d xs).map (fun x => m * 2 ^ (d + 1) + x)) j =
          kth d (lower d xs) j := by
        rw [hpow]
        exact ih j (2 * m) (lower d xs)
      have ihhi : kth d ((upper d xs).map (fun x => m * 2 ^ (d + 1) + x))
          (j - (lower d xs).length) = kth d (upper d xs) (j - (lower d xs).length) := by
        rw [hpow]
        exact ih (j - (lower d xs).length) (2 * m) (upper d xs)
      simp only [kth, hlow, hup, List.length_map]
      by_cases hj : j < (lower d xs).length
      · rw [ite_eq_left hj, ite_eq_left hj]
        exact ihlo
      · rw [ite_eq_right hj, ite_eq_right hj]
        congr 1

private theorem map_add_sub_of_ge (c : Nat) (l : List Nat) (h : ∀ x ∈ l, c ≤ x) :
    l.map (fun x => c + (x - c)) = l := by
  induction l with
  | nil => simp
  | cons a as ih =>
      have ha : c + (a - c) = a := by
        have := h a (by simp)
        omega
      have ht : ∀ x ∈ as, c ≤ x := by intro x hx; exact h x (by simp [hx])
      simp [ha, ih ht]

/-- For a sorted list, filtering by the current bit puts the low elements
first and the high elements last. -/
private theorem sorted_eq_lower_append_upper (d : Nat) (ys : Word)
    (hsorted : ys.Pairwise (fun a b : Nat => a ≤ b))
    (hbound : ∀ x ∈ ys, x < 2 ^ (d + 1)) :
    ys = lower d ys ++ upper d ys := by
  induction ys with
  | nil => simp [lower, upper]
  | cons a as ih =>
      have hs_tail : as.Pairwise (fun a b : Nat => a ≤ b) := (List.pairwise_cons.mp hsorted).2
      have hle : ∀ x ∈ as, a ≤ x := (List.pairwise_cons.mp hsorted).1
      have hb_tail : ∀ x ∈ as, x < 2 ^ (d + 1) := by
        intro x hx; exact hbound x (by simp [hx])
      have ihtail := ih hs_tail hb_tail
      by_cases hbit : a.testBit d = true
      · have ht : ∀ x ∈ as, x.testBit d = true := by
          intro x hx
          have hage : 2 ^ d ≤ a := Nat.ge_two_pow_of_testBit hbit
          have hxge : 2 ^ d ≤ x := Nat.le_trans hage (hle x hx)
          exact Nat.testBit_of_two_pow_le_and_two_pow_add_one_gt hxge (hb_tail x hx)
        have hl : lower d as = [] := by
          unfold lower
          exact filter_eq_nil_of_all_false (fun x => !x.testBit d) as (by
            intro x hx
            have h := ht x hx
            simp [h])
        have hu : upper d as = as := by
          unfold upper
          exact filter_eq_self_of_all_true (fun x => x.testBit d) as ht
        have hlow_cons : lower d (a :: as) = [] := by
          calc
            lower d (a :: as) = lower d as := by
              unfold lower
              simp [hbit]
            _ = [] := hl
        have hup_cons : upper d (a :: as) = a :: as := by
          calc
            upper d (a :: as) = a :: upper d as := by
              unfold upper
              simp [hbit]
            _ = a :: as := by rw [hu]
        rw [hlow_cons, hup_cons]
        simp
      · have hbfalse : a.testBit d = false := Bool.eq_false_of_ne_true hbit
        have hlow_cons : lower d (a :: as) = a :: lower d as := by
          simp [lower, hbfalse]
        have hup_cons : upper d (a :: as) = upper d as := by
          simp [upper, hbfalse]
        rw [hlow_cons, hup_cons]
        exact congrArg (fun l => a :: l) ihtail

private theorem pairwise_sub_const {c : Nat} {l : List Nat}
    (h : l.Pairwise (fun a b : Nat => a ≤ b)) (hge : ∀ x ∈ l, c ≤ x) :
    (l.map (fun x => x - c)).Pairwise (fun a b : Nat => a ≤ b) := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.pairwise_cons] at h
      simp only [List.map_cons]
      rw [List.pairwise_cons]
      constructor
      · intro a' ha'
        rcases List.mem_map.mp ha' with ⟨y, hy, rfl⟩
        have hay : a ≤ y := h.1 y hy
        have hage : c ≤ a := hge a (by simp)
        have hyge : c ≤ y := hge y (by simp [hy])
        omega
      · exact ih h.2 (by intro x hx; exact hge x (by simp [hx]))

private theorem getElem?_append_of_eq {α : Type} {l a b : List α} (h : l = a ++ b)
    {k : Nat} (hk : k < a.length) : l[k]? = a[k]? := by
  subst l
  exact List.getElem?_append_left hk

private theorem getElem?_append_of_eq_right {α : Type} {l a b : List α} (h : l = a ++ b)
    {k : Nat} (hk : a.length ≤ k) : l[k]? = b[k - a.length]? := by
  subst l
  exact List.getElem?_append_right hk

/-- The k-th element of a sorted, in-range list is exactly `kth`. -/
private theorem sorted_kth (d : Nat) (ys : Word) (k : Nat)
    (hsorted : ys.Pairwise (fun a b : Nat => a ≤ b))
    (hbound : ∀ x ∈ ys, x < 2 ^ d) (hk : k < ys.length) :
    ys[k]? = some (kth d ys k) := by
  induction d generalizing ys k with
  | zero =>
      have hall0 : ∀ x ∈ ys, x = 0 := by
        intro x hx
        have hxlt : x < 1 := by simpa using hbound x hx
        omega
      rw [List.getElem?_eq_getElem hk]
      congr
      exact hall0 (ys[k]) (List.getElem_mem hk)
  | succ d ih =>
      have hbound' : ∀ x ∈ ys, x < 2 ^ (d + 1) := hbound
      have hys : ys = lower d ys ++ upper d ys :=
        sorted_eq_lower_append_upper d ys hsorted hbound'
      by_cases hklo : k < (lower d ys).length
      · rw [getElem?_append_of_eq hys hklo]
        have hslo : (lower d ys).Pairwise (fun a b : Nat => a ≤ b) := hsorted.filter _
        have hblo : ∀ x ∈ lower d ys, x < 2 ^ d := by
          intro x hx
          have hp := List.mem_filter.mp hx
          exact lt_two_pow_of_testBit_false (hbound' x hp.1) (by
            have h := hp.2
            simpa [lower] using h)
        have ihlo := ih (lower d ys) k hslo hblo hklo
        have hkth : kth (d + 1) ys k = kth d (lower d ys) k := by
          simp [kth, hklo]
        rw [ihlo, hkth]
      · have hkle : (lower d ys).length ≤ k := Nat.le_of_not_gt hklo
        rw [getElem?_append_of_eq_right hys hkle]
        let hi := upper d ys
        let hi' := hi.map (fun x => x - 2 ^ d)
        have hylen0 : ys.length = (lower d ys ++ upper d ys).length := congrArg List.length hys
        have hylen : ys.length = (lower d ys).length + hi.length := by
          simpa [hi] using hylen0
        have hkhi : k - (lower d ys).length < hi.length := by omega
        have hshi : hi.Pairwise (fun a b : Nat => a ≤ b) := by
          simpa [hi, upper] using (hsorted.filter (fun x => x.testBit d))
        have hbhi : ∀ x ∈ hi, 2 ^ d ≤ x ∧ x < 2 ^ (d + 1) := by
          intro x hx
          have hp : x ∈ upper d ys := by simpa [hi, upper] using hx
          have hpf := List.mem_filter.mp hp
          have hxbit : x.testBit d = true := by
            have h := hpf.2
            simpa [upper] using h
          exact ⟨Nat.ge_two_pow_of_testBit hxbit, hbound' x hpf.1⟩
        have hshi' : hi'.Pairwise (fun a b : Nat => a ≤ b) := by
          exact pairwise_sub_const hshi (by intro x hx; exact (hbhi x hx).1)
        have hbhi' : ∀ x ∈ hi', x < 2 ^ d := by
          intro x hx
          rcases List.mem_map.mp (by simpa [hi'] using hx) with ⟨y, hy, rfl⟩
          have hylt' : y < 2 ^ d + 2 ^ d := by
            have h := (hbhi y hy).2
            rw [Nat.pow_succ] at h
            omega
          omega
        have hkhi' : k - (lower d ys).length < hi'.length := by simpa [hi'] using hkhi
        have ihhi := ih hi' (k - (lower d ys).length) hshi' hbhi' hkhi'
        have hi_eq : hi = hi'.map (fun x => 2 ^ d + x) := by
          have h := map_add_sub_of_ge (2 ^ d) hi (by intro x hx; exact (hbhi x hx).1)
          simpa [hi', List.map_map, Function.comp_def] using h.symm
        have hshift : kth d hi (k - (lower d ys).length) =
            kth d hi' (k - (lower d ys).length) := by
          rw [hi_eq]
          simpa using (kth_add_mul_pow d (k - (lower d ys).length) 1 hi')
        have hgetmap : hi'[(k - (lower d ys).length)]? =
            (hi[(k - (lower d ys).length)]?).map (fun x => x - 2 ^ d) := by
          simp [hi', hi]
        have hgetsome : hi[(k - (lower d ys).length)]? =
            some (hi[(k - (lower d ys).length)]) :=
          List.getElem?_eq_getElem hkhi
        have heq : hi[(k - (lower d ys).length)] =
            2 ^ d + kth d hi (k - (lower d ys).length) := by
          rw [hgetmap, hgetsome] at ihhi
          simp at ihhi
          rw [← hshift] at ihhi
          have hge : 2 ^ d ≤ hi[(k - (lower d ys).length)] :=
            (hbhi _ (List.getElem_mem hkhi)).1
          omega
        have hkth : kth (d + 1) ys k =
            2 ^ d + kth d hi (k - (lower d ys).length) := by
          simp [kth, hi, hklo]
        rw [hkth, List.getElem?_eq_getElem hkhi, heq]

theorem sound_Wavelet_Quantile : Wavelet.Quantile.Goal := by
  intro d xs k hxbound hk
  let ys := xs.mergeSort
  have hperm : xs.Perm ys := by
    simpa [ys] using (List.mergeSort_perm xs (fun a b : Nat => a ≤ b)).symm
  have hlenys : ys.length = xs.length := hperm.length_eq.symm
  have hk' : k < ys.length := by omega
  have hysbound : ∀ x ∈ ys, x < 2 ^ d := by
    intro x hx
    have hxmem : x ∈ xs := by simpa [ys] using hx
    exact hxbound x hxmem
  have hsorted : ys.Pairwise (fun a b : Nat => a ≤ b) := by
    simpa [ys] using
      (List.pairwise_mergeSort
        (le := fun a b : Nat => a ≤ b)
        (fun a b c hab hbc => by
          have hab' : a ≤ b := by simpa using hab
          have hbc' : b ≤ c := by simpa using hbc
          simpa using Nat.le_trans hab' hbc')
        (fun a b => by
          by_cases h : a ≤ b
          · simp [h]
          · have h' : b ≤ a := Nat.le_of_lt (Nat.lt_of_not_ge h)
            simp [h, h']) xs)
  have hs := sorted_kth d ys k hsorted hysbound hk'
  have hp := kth_perm (d := d) hperm k
  rw [← hp] at hs
  simpa [ys] using hs

end TinylambOpt.ResearchProofs.Atlas.Goal_Wavelet_Quantile
