import Std
import TinylambOpt.ProposedGoals.OrderedSetPercentile

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_OrderedSetPercentile

open TinylambOpt.ProposedGoals.OrderedSetPercentile

-- Exhaustive check (rows over {none, -1, 0, 1, 2} of length <= 3,
-- num, den <= 4) confirms the goal for all small inputs.


private theorem rankOf_le (num den n : Nat) (hden : den > 0) (hnum : num ≤ den) :
    rankOf num den n ≤ n := by
  unfold rankOf
  simp [show den ≠ 0 by omega]
  have hmul : num * n ≤ n * den := by
    simpa [Nat.mul_comm] using (Nat.mul_le_mul_right n hnum)
  have hlt : num * n + den - 1 < (n + 1) * den := by
    rw [Nat.succ_mul]
    omega
  have hdiv : (num * n + den - 1) / den < n + 1 := (Nat.div_lt_iff_lt_mul hden).2 hlt
  omega

private theorem sorted_nonNull (xs : List Int) :
    (xs.mergeSort (fun a b : Int => a <= b)).Pairwise (fun a b => a ≤ b) := by
  have h := List.pairwise_mergeSort (le := fun a b : Int => a <= b)
    (by
      intro a b c hab hbc
      have hab' : a ≤ b := by simpa using hab
      have hbc' : b ≤ c := by simpa using hbc
      simp [show a ≤ c by omega])
    (by
      intro a b
      by_cases h : a ≤ b
      · simp [h]
      · have h' : b ≤ a := by omega
        simp [h, h']) xs
  exact h.imp (by intro a b hab; exact of_decide_eq_true hab)

private theorem countP_filter {α : Type} (p : α → Bool) (xs : List α) :
    xs.countP p = (xs.filter p).length := List.countP_eq_length_filter

private theorem perm_counts {xs ys : List Int} (hperm : ys.Perm xs) (v : Int) :
    countLe ys v = countLe xs v ∧ countLt ys v = countLt xs v := by
  constructor
  · have h := List.Perm.countP_eq (fun x : Int => x <= v) hperm
    rw [List.countP_eq_length_filter, List.countP_eq_length_filter] at h
    simpa [countLe] using h
  · have h := List.Perm.countP_eq (fun x : Int => x < v) hperm
    rw [List.countP_eq_length_filter, List.countP_eq_length_filter] at h
    simpa [countLt] using h

private theorem filter_eq_nil_of_all_false {α : Type} (p : α → Bool) {xs : List α}
    (h : ∀ x ∈ xs, p x = false) : xs.filter p = [] := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      have hx : p x = false := h x (by simp)
      have ht : ∀ z ∈ xs, p z = false := by intro z hz; exact h z (by simp [hz])
      simp [List.filter_cons, hx, ih ht]

private theorem filter_take_drop {α : Type} (p : α → Bool) (xs : List α) (n : Nat) :
    xs.filter p = (xs.take n).filter p ++ (xs.drop n).filter p := by
  induction xs generalizing n with
  | nil => simp
  | cons x xs ih =>
      cases n with
      | zero => simp
      | succ n =>
          by_cases h : p x <;> simpa [h] using ih n

private theorem countLe_ge_rank (ys : List Int) (hs : ys.Pairwise (fun a b => a ≤ b))
    (r : Nat) (hr0 : 0 < r) (hrlen : r ≤ ys.length) : r ≤ countLe ys ys[r - 1] := by
  let v := ys[r - 1]
  have hidx : r - 1 < ys.length := by omega
  have hv : ∀ i (hiL : i < ys.length), i < r → ys[i] ≤ v := by
    intro i hiL hi
    by_cases hir : i = r - 1
    · subst i; simpa [v] using (le_refl (ys[r - 1]))
    · have hltir : i < r - 1 := by omega
      simpa [v] using (hs.rel_getElem_of_lt hiL hidx hltir)
  have htake : (ys.take r).filter (fun x : Int => x <= v) = ys.take r := by
    apply List.filter_eq_self.mpr
    intro x hx
    rw [List.mem_iff_getElem] at hx
    rcases hx with ⟨i, hi, hxi⟩
    have hiLen : (ys.take r).length = min r ys.length := List.length_take
    have hiTake : i < r := by
      simp [hiLen] at hi
      omega
    have hiL : i < ys.length := by
      simp [hiLen] at hi
      omega
    have htakei : (ys.take r)[i] = ys[i] := by
      simpa using (List.getElem_take (xs := ys) (j := r) (i := i))
    rw [htakei] at hxi
    rw [← hxi]
    exact decide_eq_true (hv i hiL hiTake)
  have hsub : ((ys.take r).filter (fun x : Int => x <= v)).Sublist
      (ys.filter (fun x : Int => x <= v)) :=
    List.Sublist.filter _ (List.take_sublist r ys)
  have hlen := hsub.length_le
  rw [htake] at hlen
  have htk : (ys.take r).length = r := by rw [List.length_take]; exact Nat.min_eq_left hrlen
  rw [htk] at hlen
  simpa [countLe, v] using hlen

private theorem countLt_lt_rank (ys : List Int) (hs : ys.Pairwise (fun a b => a ≤ b))
    (r : Nat) (hr0 : 0 < r) (hrlen : r ≤ ys.length) : countLt ys ys[r - 1] < r := by
  let v := ys[r - 1]
  change (ys.filter (fun x : Int => x < v)).length < r
  have hidx : r - 1 < ys.length := by omega
  have hdrop : (ys.drop (r - 1)).filter (fun x : Int => x < v) = [] := by
    apply filter_eq_nil_of_all_false
    intro x hx
    rw [List.mem_iff_getElem] at hx
    rcases hx with ⟨j, hj, hxj⟩
    have hjlen : (ys.drop (r - 1)).length = ys.length - (r - 1) := List.length_drop
    have hjIdx : r - 1 + j < ys.length := by
      simp [hjlen] at hj
      omega
    have hdropj : (ys.drop (r - 1))[j] = ys[r - 1 + j] := by
      simpa using (List.getElem_drop (xs := ys) (i := r - 1) (j := j))
    rw [hdropj] at hxj
    have hval : v ≤ ys[r - 1 + j] := by
      by_cases hj0 : j = 0
      · subst j; simpa [v]
      · have hltidx : r - 1 < r - 1 + j := by omega
        simpa [v] using (hs.rel_getElem_of_lt hidx hjIdx hltidx)
    have hnot : ¬ ys[r - 1 + j] < v := by omega
    rw [← hxj]
    exact decide_eq_false hnot
  have hsplit := filter_take_drop (fun x : Int => x < v) ys (r - 1)
  rw [hsplit, hdrop]
  simp only [List.append_nil]
  have hlen : ((ys.take (r - 1)).filter (fun x : Int => x < v)).length ≤
      (ys.take (r - 1)).length := List.length_filter_le _ _
  have htk : (ys.take (r - 1)).length = r - 1 := by
    rw [List.length_take]; exact Nat.min_eq_left (by omega)
  rw [htk] at hlen
  simpa [countLt] using (show ((ys.take (r - 1)).filter (fun x : Int => x < v)).length < r by omega)


private theorem rank_unique (ys : List Int) (hs : ys.Pairwise (fun a b => a ≤ b))
    (r : Nat) (hr0 : 0 < r) (hrlen : r ≤ ys.length) (v : Int)
    (h1 : countLt ys v < r) (h2 : r ≤ countLe ys v) : v = ys[r - 1] := by
  let w := ys[r - 1]
  have hidx : r - 1 < ys.length := by omega
  by_cases hvw : v < w
  · have hdrop : (ys.drop (r - 1)).filter (fun x : Int => x <= v) = [] := by
      apply filter_eq_nil_of_all_false
      intro x hx
      rw [List.mem_iff_getElem] at hx
      rcases hx with ⟨j, hj, hxj⟩
      have hjlen : (ys.drop (r - 1)).length = ys.length - (r - 1) := List.length_drop
      have hjIdx : r - 1 + j < ys.length := by
        simp [hjlen] at hj
        omega
      have hdropj : (ys.drop (r - 1))[j] = ys[r - 1 + j] := by
        simpa using (List.getElem_drop (xs := ys) (i := r - 1) (j := j))
      rw [hdropj] at hxj
      have hval : w ≤ ys[r - 1 + j] := by
        by_cases hj0 : j = 0
        · subst j; simpa [w]
        · have hltidx : r - 1 < r - 1 + j := by omega
          simpa [w] using (hs.rel_getElem_of_lt hidx hjIdx hltidx)
      have hnot : ¬ ys[r - 1 + j] ≤ v := by omega
      rw [← hxj]
      exact decide_eq_false hnot
    have hsplit := filter_take_drop (fun x : Int => x <= v) ys (r - 1)
    have hlen : (ys.filter (fun x : Int => x <= v)).length ≤ (ys.take (r - 1)).length := by
      rw [hsplit, hdrop]
      simp only [List.append_nil]
      exact List.length_filter_le _ _
    have htk : (ys.take (r - 1)).length = r - 1 := by
      rw [List.length_take]; exact Nat.min_eq_left (by omega)
    rw [htk] at hlen
    have : countLe ys v ≤ r - 1 := by simpa [countLe] using hlen
    omega
  · by_cases hwv : w < v
    · have htake : (ys.take r).filter (fun x : Int => x < v) = ys.take r := by
        apply List.filter_eq_self.mpr
        intro x hx
        rw [List.mem_iff_getElem] at hx
        rcases hx with ⟨i, hi, hxi⟩
        have hiLen : (ys.take r).length = min r ys.length := List.length_take
        have hiTake : i < r := by simp [hiLen] at hi; omega
        have hiL : i < ys.length := by simp [hiLen] at hi; omega
        have hle : ys[i] ≤ w := by
          by_cases hir : i = r - 1
          · subst i; simpa [w] using (le_refl (ys[r - 1]))
          · have hltir : i < r - 1 := by omega
            simpa [w] using (hs.rel_getElem_of_lt hiL hidx hltir)
        have htakei : (ys.take r)[i] = ys[i] := by
          simpa using (List.getElem_take (xs := ys) (j := r) (i := i))
        rw [htakei] at hxi
        rw [← hxi]
        exact decide_eq_true (by omega)
      have hsub : ((ys.take r).filter (fun x : Int => x < v)).Sublist
          (ys.filter (fun x : Int => x < v)) :=
        List.Sublist.filter _ (List.take_sublist r ys)
      have hlen := hsub.length_le
      rw [htake] at hlen
      have htk : (ys.take r).length = r := by
        rw [List.length_take]; exact Nat.min_eq_left hrlen
      rw [htk] at hlen
      have : r ≤ countLt ys v := by simpa [countLt] using hlen
      omega
    · omega

theorem sound_OrderedSetPercentile : TinylambOpt.ProposedGoals.OrderedSetPercentile.Goal := by
  intro rows num den hden hnum
  let xs := nonNull rows
  let ys := xs.mergeSort (fun a b : Int => a <= b)
  let r := rankOf num den xs.length
  have hrle : r ≤ xs.length := by simpa [r] using (rankOf_le num den xs.length hden hnum)
  have hperm : ys.Perm xs := by simpa [ys] using (List.mergeSort_perm xs (fun a b : Int => a <= b))
  have hyslen : ys.length = xs.length := hperm.length_eq
  have hs : ys.Pairwise (fun a b => a ≤ b) := by simpa [ys] using (sorted_nonNull xs)
  have hct : ∀ v : Int, countLe ys v = countLe xs v ∧ countLt ys v = countLt xs v :=
    fun v => perm_counts hperm v
  by_cases hr0 : r = 0
  · have hcond : (r == 0 || xs.length < r) = true := by simp [hr0]
    unfold sortedPick rankPick
    simp [xs, ys, r, hr0, hcond]
  · have hrpos : 0 < r := by omega
    have hrlen : r ≤ ys.length := by omega
    have hidx : r - 1 < ys.length := by omega
    have hwMemYs : ys[r - 1] ∈ ys := List.mem_iff_getElem.mpr ⟨r - 1, hidx, rfl⟩
    have hwMem : ys[r - 1] ∈ xs := hperm.mem_iff.mp hwMemYs
    have hpW : ((fun v : Int => countLt xs v < r && r <= countLe xs v) (ys[r - 1])) = true := by
      have hlt := countLt_lt_rank ys hs r hrpos hrlen
      have hle := countLe_ge_rank ys hs r hrpos hrlen
      rw [(hct (ys[r - 1])).2] at hlt
      rw [(hct (ys[r - 1])).1] at hle
      simp [hlt, hle]
    have huniq : ∀ v ∈ xs, ((fun v : Int => countLt xs v < r && r <= countLe xs v) v) = true →
        v = ys[r - 1] := by
      intro v hv hpv
      have hpv' : countLt xs v < r ∧ r ≤ countLe xs v := by simpa using hpv
      apply rank_unique ys hs r hrpos hrlen v
      · rw [(hct v).2]; exact hpv'.1
      · rw [(hct v).1]; exact hpv'.2
    have hfind : xs.find? (fun v : Int => countLt xs v < r && r <= countLe xs v) = some (ys[r - 1]) := by
      cases hf : xs.find? (fun v : Int => countLt xs v < r && r <= countLe xs v) with
      | none =>
          have hn := List.find?_eq_none.mp hf
          exact False.elim (hn (ys[r - 1]) hwMem hpW)
      | some y =>
          have hyMem : y ∈ xs := by
            rcases List.find?_eq_some_iff_getElem.mp hf with ⟨_, j, hj, hjy, _⟩
            exact List.mem_iff_getElem.mpr ⟨j, hj, hjy⟩
          rcases List.find?_eq_some_iff_getElem.mp hf with ⟨hpy, _, _, _, _⟩
          have hyEq := huniq y hyMem hpy
          simpa [hyEq]
    have hcond : (r == 0 || xs.length < r) = false := by simp [hr0, show ¬ xs.length < r by omega]
    have hget : ys[r - 1]? = some ys[r - 1] := List.getElem?_eq_some_iff.mpr ⟨hidx, rfl⟩
    unfold sortedPick rankPick
    simp [xs, ys, r, hyslen, hcond, hfind, hget]

end TinylambOpt.ResearchProofs.Atlas.Goal_OrderedSetPercentile
