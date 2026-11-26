import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Certificates and refutations for the second posting-join shard.

All proofs are self-contained and use only Lean's core tactic vocabulary.
-/

namespace TinylambOpt.ResearchProofs.StorageExecution.PostingJoinsB

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

private def Cut (xs : List Nat) (v i : Nat) : Prop :=
  (∀ j, j < i → xs.getD j 0 < v) ∧
  (∀ j, j < xs.length → i ≤ j → v ≤ xs.getD j 0)

private theorem getD_eq_getElem {xs : List Nat} {i : Nat} (hi : i < xs.length) :
    xs.getD i 0 = xs[i] := by
  simp [List.getD, List.getElem?_eq_getElem hi]

private theorem getD_toArray (xs : List Nat) (i : Nat) :
    xs.toArray[i]?.getD 0 = xs.getD i 0 := by
  by_cases hi : i < xs.length
  · have ha : i < xs.toArray.size := by simpa [Array.length_toList] using hi
    simp [List.getD, Array.getElem?_eq_getElem ha, List.getElem?_eq_getElem hi,
      List.getElem_toArray ha]
  · have ha : xs.toArray.size ≤ i := by simpa [Array.length_toList] using hi
    simp [List.getD, Array.getElem?_eq_none ha, List.getElem?_eq_none (by omega : xs.length ≤ i)]

private theorem cut_of_mid_lt {xs : List Nat} {v lo mid : Nat}
    (hs : xs.Pairwise (· ≤ ·)) (hcut : Cut xs v lo)
    (hmid : mid < xs.length) (hv : xs[mid] < v) : Cut xs v (mid+1) := by
  constructor
  · intro j hj
    by_cases heq : j = mid
    · subst j
      rw [getD_eq_getElem hmid]
      exact hv
    · have hjm : j < mid := by omega
      have hjlen : j < xs.length := by omega
      have hrel : xs.getD j 0 ≤ xs.getD mid 0 := by
        rw [getD_eq_getElem hjlen, getD_eq_getElem hmid]
        exact hs.rel_getElem_of_lt hjlen hmid hjm
      rw [getD_eq_getElem hjlen]
      omega
  · intro j hjlen hge
    exact hcut.2 j hjlen (by omega)

private theorem cut_of_mid_ge {xs : List Nat} {v lo mid : Nat}
    (hs : xs.Pairwise (· ≤ ·)) (hcut : Cut xs v lo)
    (hmid : mid < xs.length) (hv : v ≤ xs[mid]) : Cut xs v mid := by
  constructor
  · intro j hj
    exact hcut.1 j (by omega)
  · intro j hjlen hge
    by_cases heq : j = mid
    · subst j
      rw [getD_eq_getElem hmid]
      exact hv
    · have hmj : mid < j := by omega
      have hrel : xs.getD mid 0 ≤ xs.getD j 0 := by
        rw [getD_eq_getElem hmid, getD_eq_getElem hjlen]
        exact hs.rel_getElem_of_lt hmid hjlen hmj
      rw [getD_eq_getElem hjlen]
      omega

private theorem searchBound_cut {xs : Array Nat} (v fuel lo hi : Nat)
    (hlo : lo ≤ hi) (hhi : hi ≤ xs.size) (hf : hi-lo ≤ fuel)
    (hbelow : ∀ j, j < lo → xs.toList.getD j 0 < v)
    (habove : ∀ j, hi ≤ j → j < xs.toList.length → v ≤ xs.toList.getD j 0)
    (hs : xs.toList.Pairwise (· ≤ ·)) :
    Cut xs.toList v (searchBound xs v fuel lo hi) ∧
      lo ≤ searchBound xs v fuel lo hi ∧ searchBound xs v fuel lo hi ≤ hi := by
  induction fuel generalizing lo hi with
  | zero =>
      have heq : lo = hi := by omega
      subst hi
      simp only [searchBound]
      exact ⟨⟨hbelow, fun j hj hjlen => habove j (Nat.le_refl lo) hjlen⟩,
        Nat.le_refl lo, Nat.le_refl lo⟩
  | succ fuel ih =>
      by_cases hge : lo ≥ hi
      · have heq : lo = hi := by omega
        subst hi
        rw [searchBound]
        simp only [hge, ↓reduceIte]
        exact ⟨⟨hbelow, fun j hj hjlen => habove j (Nat.le_refl lo) hjlen⟩,
          Nat.le_refl lo, Nat.le_refl lo⟩
      · have hlt : lo < hi := by omega
        let mid := (lo+hi)/2
        have hml : lo ≤ mid := by omega
        have hmh : mid < hi := by omega
        have hmid : mid < xs.size := by omega
        have hval : xs[mid]?.getD 0 = xs[mid] := by
          rw [Array.getElem?_eq_getElem hmid, Option.getD_some]
        have hlistval : xs.toList.getD mid 0 = xs[mid] := by
          rw [← getD_toArray xs.toList mid, Array.getElem?_eq_getElem hmid, Option.getD_some]
        rw [searchBound]
        simp only [hge, ↓reduceIte]
        change Cut xs.toList v
            (if xs[mid]?.getD 0 < v then searchBound xs v fuel (mid+1) hi
              else searchBound xs v fuel lo mid) ∧
          lo ≤ (if xs[mid]?.getD 0 < v then searchBound xs v fuel (mid+1) hi
              else searchBound xs v fuel lo mid) ∧
          (if xs[mid]?.getD 0 < v then searchBound xs v fuel (mid+1) hi
              else searchBound xs v fuel lo mid) ≤ hi
        rw [hval]
        by_cases hv : xs[mid] < v
        · rw [if_pos hv]
          have hbelow' : ∀ j, j < mid+1 → xs.toList.getD j 0 < v := by
            intro j hj
            by_cases heq : j = mid
            · subst j
              have hmidlen : mid < xs.toList.length := by
                simpa [Array.length_toList] using hmid
              rw [getD_eq_getElem hmidlen, hlistval]
              exact hv
            · have hjm : j < mid := by omega
              have hjlen : j < xs.toList.length := by
                simpa [Array.length_toList] using (by omega : j < xs.size)
              have hrel : xs.toList.getD j 0 ≤ xs.toList.getD mid 0 := by
                rw [getD_eq_getElem hjlen]
                have hmidlen : mid < xs.toList.length := by
                  simpa [Array.length_toList] using hmid
                rw [getD_eq_getElem hmidlen, hlistval]
                exact hs.rel_getElem_of_lt hjlen hmidlen hjm
              have hbelowj := hbelow j (by omega)
              omega
          have hres := ih (mid+1) hi (by omega) (by omega) (by omega) hbelow' habove hs
          obtain ⟨hc, hlb, hub⟩ := hres
          exact ⟨hc, Nat.le_trans hml hlb, hub⟩
        · rw [if_neg hv]
          have habove' : ∀ j, mid ≤ j → j < xs.toList.length → v ≤ xs.toList.getD j 0 := by
            intro j hj hjlen
            by_cases heq : j = mid
            · subst j
              have hmidlen : mid < xs.toList.length := by
                simpa [Array.length_toList] using hmid
              rw [getD_eq_getElem hmidlen, hlistval]
              omega
            · have hmj : mid < j := by omega
              have hjarr : j < xs.size := by simpa [Array.length_toList] using hjlen
              have hmidlen : mid < xs.toList.length := by
                simpa [Array.length_toList] using hmid
              have hrel : xs.toList.getD mid 0 ≤ xs.toList.getD j 0 := by
                rw [hlistval, getD_eq_getElem hjlen]
                exact hs.rel_getElem_of_lt hmidlen hjlen hmj
              have habovej := habove j (by omega) hjlen
              omega
          have hres := ih lo mid hml (by omega) (by omega) hbelow habove' hs
          obtain ⟨hc, hlb, hub⟩ := hres
          exact ⟨hc, hlb, Nat.le_trans hub hmh⟩

private theorem width_bound (n : Nat) : n ≤ 2 ^ width n := by
  unfold width
  split
  · rename_i hz
    simp only [beq_iff_eq] at hz
    subst n
    decide
  · rename_i hn
    have hne : n ≠ 0 := by
      intro h
      subst n
      simp at hn
    have hlt : n < 2 ^ (n.log2+1) := (Nat.log2_lt hne).mp (by omega)
    exact Nat.le_of_lt hlt

private theorem lower_cut {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    Cut ys v (lower ys v) ∧ lower ys v ≤ ys.length := by
  unfold lower
  have hf : ys.length - 0 ≤ width ys.length + 1 := by
    have hwb := width_bound ys.length
    omega
  have h := searchBound_cut (xs := ys.toArray) v (width ys.length + 1) 0 ys.length
    (Nat.zero_le _) (by simp) hf
    (by intro j hj; omega)
    (by
      intro j hj hjlen
      omega)
    (by simpa using hs)
  simpa using h

private theorem lower_le_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v ≤ ys.length := (lower_cut hs v).2

private theorem lower_lt_of_lt {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) {i : Nat}
    (hi : i < lower ys v) : ys.getD i 0 < v := (lower_cut hs v).1.1 i hi

private theorem lower_le_of_ge {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) {i : Nat}
    (hj : lower ys v ≤ i) (hi : i < ys.length) : v ≤ ys.getD i 0 :=
  (lower_cut hs v).1.2 i hi hj

private theorem lower_mono {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) {a b : Nat}
    (hab : a ≤ b) : lower ys a ≤ lower ys b := by
  apply Nat.le_of_not_gt
  intro h
  have hba : lower ys b < lower ys a := h
  have hb := lower_lt_of_lt hs a h
  have ha := lower_le_of_ge hs b (Nat.le_refl _)
    (Nat.lt_of_lt_of_le h (lower_le_length hs a))
  omega

private theorem filter_prefix {α : Type} (p : α → Bool) (xs : List α) (i : Nat)
    (hi : i < (xs.filter p).length) :
    ∃ h : i < xs.length, p xs[i]'h = true := by
  induction xs generalizing i with
  | nil => simp at hi
  | cons x xs ih =>
      by_cases hp : p x = true
      · simp only [List.filter_cons, hp, ite_true, List.length_cons] at hi
        cases i with
        | zero => exact ⟨Nat.zero_lt_succ _, hp⟩
        | succ i =>
            have htail := ih i (by omega)
            exact ⟨by omega, by simpa using htail.2⟩
      · simp [hp] at hi
        have htail := ih i hi
        exact ⟨by omega, by simpa using htail.2⟩

private theorem lower_eq_filter_lt_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v = (ys.filter (fun x => decide (x < v))).length := by
  apply Nat.le_antisymm
  · apply Nat.le_of_not_gt
    intro h
    have hnl : (ys.filter (fun x => decide (x < v))).length < lower ys v := h
    have hnlen : (ys.filter (fun x => decide (x < v))).length < ys.length :=
      Nat.lt_of_lt_of_le hnl (lower_le_length hs v)
    have hlow := lower_lt_of_lt hs v hnl
    have hp := (filter_prefix (fun x => decide (x < v)) ys
      (ys.filter (fun x => decide (x < v))).length hnl).2
    rw [getD_eq_getElem hnlen] at hlow
    simp only [decide_eq_true_eq] at hp
    omega
  · apply Nat.le_of_not_gt
    intro h
    have hln : lower ys v < (ys.filter (fun x => decide (x < v))).length := h
    have hlen : lower ys v < ys.length :=
      Nat.lt_of_lt_of_le hln (List.length_filter_le _ _)
    have hlow := lower_lt_of_lt hs v hln
    have hp := (filter_prefix (fun x => decide (x < v)) ys (lower ys v) hln).2
    rw [getD_eq_getElem hlen] at hlow
    simp only [decide_eq_true_eq] at hp
    omega

private theorem drop_take_eq_take_drop {α : Type} (xs : List α) (i j : Nat) (hij : i ≤ j) :
    (xs.drop i).take (j-i) = (xs.take j).drop i := by
  induction xs generalizing i j with
  | nil => simp
  | cons x xs ih =>
      cases i with
      | zero => simp
      | succ i =>
          cases j with
          | zero => omega
          | succ j =>
              simp only [List.drop_succ_cons, List.take_succ_cons, Nat.succ_sub_succ]
              exact ih i j (by omega)

private theorem filter_lt_prefix_filter_lt {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b : Nat} (hab : a ≤ b) :
    ys.filter (fun x => decide (x < b)) =
      ys.filter (fun x => decide (x < a)) ++
        ys.filter (fun x => decide (a ≤ x && x < b)) := by
  induction ys with
  | nil => simp
  | cons x xs ih =>
      have hs' := hs.tail
      by_cases hxa : x < a
      · have hxb : x < b := by omega
        simp [hxa, hxb, ih hs']
      · by_cases hxb : x < b
        · have hax : a ≤ x := by omega
          have hnil : xs.filter (fun y => decide (y < a)) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro y hy
            simp only [decide_eq_true_eq]
            have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
            omega
          simp [hxa, hxb, hax, hnil, ih hs']
        · have hnil_a : xs.filter (fun y => decide (y < a)) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro y hy
            simp only [decide_eq_true_eq]
            have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
            omega
          have hnil_ab : xs.filter (fun y => decide (a ≤ y && y < b)) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro y hy
            simp only [decide_eq_true_eq, Bool.and_eq_true]
            intro hay _
            have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
            omega
          simp [hxa, hxb, hnil_a, hnil_ab, ih hs']

private theorem take_filter_prefix {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    ys.take (ys.filter (fun x => decide (x < v))).length =
      ys.filter (fun x => decide (x < v)) := by
  induction ys with
  | nil => simp
  | cons x xs ih =>
      have hs' := hs.tail
      by_cases hx : x < v
      · simp [hx, ih hs']
      · have hnil : xs.filter (fun y => decide (y < v)) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro y hy
          simp only [decide_eq_true_eq]
          have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
          omega
        simp [hx, hnil]

private theorem slice_eq_filter {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (a b : Nat) :
    slice ys a b = ys.filter (fun y => decide (a ≤ y && y < b)) := by
  unfold slice
  by_cases hab : a ≤ b
  · rw [drop_take_eq_take_drop ys (lower ys a) (lower ys b) (lower_mono hs hab),
      lower_eq_filter_lt_length hs a, lower_eq_filter_lt_length hs b,
      take_filter_prefix hs b, filter_lt_prefix_filter_lt hs hab, List.drop_left]
  · have hba : b ≤ a := by omega
    have hmono := lower_mono hs hba
    have hz : lower ys b - lower ys a = 0 := by omega
    rw [hz, List.take_zero]
    symm
    apply List.filter_eq_nil_iff.mpr
    intro y hy
    simp only [decide_eq_true_eq, Bool.and_eq_true]
    intro hay _
    omega

end TinylambOpt.ResearchProofs.StorageExecution.PostingJoinsB
