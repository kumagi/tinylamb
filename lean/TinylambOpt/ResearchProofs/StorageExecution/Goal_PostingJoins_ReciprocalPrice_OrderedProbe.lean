import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ReciprocalPrice_OrderedProbe

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ReciprocalPrice
open TinylambOpt.ResearchGoals.Compression.Wire

private theorem getD_eq_getElem {xs : List Nat} {i : Nat} (hi : i < xs.length) :
    xs.getD i 0 = xs[i] := by
  simp [List.getD, List.getElem?_eq_getElem hi]

private theorem getD_toArray (xs : List Nat) (i : Nat) :
    xs.toArray[i]?.getD 0 = xs.getD i 0 := by
  by_cases hi : i < xs.length
  · have ha : i < xs.toArray.size := by simpa using hi
    rw [Array.getElem?_eq_getElem ha, Option.getD_some]
    simp [List.getD, List.getElem?_eq_getElem hi, List.getElem_toArray ha]
  · have ha : xs.toArray.size ≤ i := by simpa using hi
    rw [Array.getElem?_eq_none ha]
    simp [List.getD, List.getElem?_eq_none (by omega : xs.length ≤ i)]

private def Cut (xs : List Nat) (v i : Nat) : Prop :=
  (∀ j, j < i → xs.getD j 0 < v) ∧
  (∀ j, i ≤ j → j < xs.length → v ≤ xs.getD j 0)

private theorem searchBound_cut {xs : Array Nat} (v fuel lo hi : Nat)
    (hlo : lo ≤ hi) (hhi : hi ≤ xs.size) (hf : hi - lo < 2 ^ fuel)
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
      constructor
      · exact ⟨hbelow, fun j hj hjlen => habove j (by omega) hjlen⟩
      · simp
  | succ fuel ih =>
      by_cases hge : lo ≥ hi
      · have heq : lo = hi := by omega
        subst hi
        simp only [searchBound, hge, ↓reduceIte]
        constructor
        · exact ⟨hbelow, fun j hj hjlen => habove j (by omega) hjlen⟩
        · simp
      · have hlt : lo < hi := by omega
        let mid := (lo + hi) / 2
        have hml : lo ≤ mid := by omega
        have hmh : mid < hi := by omega
        have hmid : mid < xs.size := by omega
        have hmidlen : mid < xs.toList.length := by simpa using hmid
        have hval : xs[mid]?.getD 0 = xs[mid] := by
          rw [Array.getElem?_eq_getElem hmid, Option.getD_some]
        have hlistval : xs.toList.getD mid 0 = xs[mid] := by
          rw [← getD_toArray xs.toList mid, Array.getElem?_eq_getElem hmid, Option.getD_some]
        rw [searchBound]
        simp only [hge, ↓reduceIte]
        change Cut xs.toList v
            (if xs[mid]?.getD 0 < v then searchBound xs v fuel (mid + 1) hi
              else searchBound xs v fuel lo mid) ∧
          lo ≤ (if xs[mid]?.getD 0 < v then searchBound xs v fuel (mid + 1) hi
            else searchBound xs v fuel lo mid) ∧
          (if xs[mid]?.getD 0 < v then searchBound xs v fuel (mid + 1) hi
            else searchBound xs v fuel lo mid) ≤ hi
        rw [hval]
        by_cases hv : xs[mid] < v
        · rw [ite_eq_left hv]
          have hbelow' : ∀ j, j < mid + 1 → xs.toList.getD j 0 < v := by
            intro j hj
            by_cases heq : j = mid
            · subst j
              rw [hlistval]
              exact hv
            · have hjm : j < mid := by omega
              have hjlen : j < xs.toList.length := by
                simpa using (by omega : j < xs.size)
              have hrel : xs.toList.getD j 0 ≤ xs.toList.getD mid 0 := by
                rw [getD_eq_getElem hjlen, getD_eq_getElem hmidlen]
                exact hs.rel_getElem_of_lt hjlen hmidlen hjm
              rw [hlistval] at hrel
              by_cases hjlo : j < lo
              · exact hbelow j hjlo
              · omega
          have hp : 0 < 2 ^ fuel := Nat.pow_pos (by decide : 0 < 2)
          have hf' : hi - (mid + 1) < 2 ^ fuel := by
            have hf2 : hi - lo ≤ 2 * 2 ^ fuel - 1 := by
              have hpow := Nat.pow_succ 2 fuel
              omega
            omega
          have hres := ih (mid + 1) hi (by omega) (by omega) hf' hbelow' habove
          obtain ⟨hc, hlb, hub⟩ := hres
          exact ⟨hc, by omega, hub⟩
        · rw [ite_eq_right hv]
          have habove' : ∀ j, mid ≤ j → j < xs.toList.length → v ≤ xs.toList.getD j 0 := by
            intro j hj hjlen
            by_cases heq : j = mid
            · subst j
              rw [hlistval]
              omega
            · have hmj : mid < j := by omega
              have hrel : xs.toList.getD mid 0 ≤ xs.toList.getD j 0 := by
                rw [getD_eq_getElem hmidlen, getD_eq_getElem hjlen]
                exact hs.rel_getElem_of_lt hmidlen hjlen hmj
              rw [hlistval] at hrel
              by_cases hjhi : hi ≤ j
              · exact habove j hjhi hjlen
              · omega
          have hp : 0 < 2 ^ fuel := Nat.pow_pos (by decide : 0 < 2)
          have hf' : mid - lo < 2 ^ fuel := by
            have hf2 : hi - lo ≤ 2 * 2 ^ fuel - 1 := by
              have hpow := Nat.pow_succ 2 fuel
              omega
            omega
          have hres := ih lo mid hml (by omega) hf' hbelow habove'
          obtain ⟨hc, hlb, hub⟩ := hres
          exact ⟨hc, hlb, by omega⟩

private theorem fuel_bound (n : Nat) : n < 2 ^ (width n + 1) := by
  unfold width
  by_cases h : n == 0
  · have hn0 : n = 0 := by simpa using h
    subst n
    simp
  · have hn : n ≠ 0 := by simpa using h
    simp [h]
    have hlt : n < 2 ^ (n.log2 + 1) := (Nat.log2_lt hn).mp (Nat.lt_succ_self _)
    rw [show n.log2 + 1 + 1 = (n.log2 + 1).succ by omega, Nat.pow_succ]
    omega

private theorem lower_cut {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    Cut ys v (lower ys v) ∧ lower ys v ≤ ys.length := by
  unfold lower
  have hf : ys.length - 0 < 2 ^ (width ys.length + 1) := by
    have := fuel_bound ys.length
    omega
  have h := searchBound_cut (xs := ys.toArray) v (width ys.length + 1) 0 ys.length
    (Nat.zero_le _) (by simp) hf
    (by intro j hj; omega)
    (by
      intro j hj hjlen
      have hjlen' : j < ys.length := by simpa using hjlen
      omega)
    (by simpa using hs)
  simpa [getD_toArray] using h

private theorem lower_le_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v ≤ ys.length := (lower_cut hs v).2

private theorem lower_lt_of_lt {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) {i : Nat}
    (hi : i < lower ys v) : ys.getD i 0 < v := (lower_cut hs v).1.1 i hi

private theorem lower_le_of_ge {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) {i : Nat}
    (hj : lower ys v ≤ i) (hi : i < ys.length) : v ≤ ys.getD i 0 :=
  (lower_cut hs v).1.2 i hj hi

private theorem length_filter_lt_ge {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) {v i : Nat}
    (hi : i < ys.length) (hlt : ys[i] < v) :
    i + 1 ≤ (ys.filter (fun x => decide (x < v))).length := by
  induction ys generalizing i with
  | nil => simp at hi
  | cons y ys ih =>
      cases i with
      | zero =>
          have hy : y < v := by simpa using hlt
          simp [hy]
      | succ i =>
          have hi' : i < ys.length := by simpa using hi
          have hlt' : ys[i] < v := by simpa using hlt
          have htail := ih hs.tail hi' hlt'
          by_cases hy : y < v
          · simp [hy]
            omega
          · have hymem : ys[i] ∈ ys := List.getElem_mem hi'
            have hyle : y ≤ ys[i] := List.rel_of_pairwise_cons hs hymem
            exfalso
            omega

private theorem length_filter_lt_le {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) {v i : Nat}
    (hi : i < ys.length) (hge : v ≤ ys[i]) :
    (ys.filter (fun x => decide (x < v))).length ≤ i := by
  induction ys generalizing i with
  | nil => simp at hi
  | cons y ys ih =>
      cases i with
      | zero =>
          have hyge : v ≤ y := by simpa using hge
          have hnil : ys.filter (fun x => decide (x < v)) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro z hz
            simp only [decide_eq_true_eq]
            have hyz : y ≤ z := List.rel_of_pairwise_cons hs hz
            omega
          simp [hyge, hnil]
      | succ i =>
          have hi' : i < ys.length := by simpa using hi
          have hge' : v ≤ ys[i] := by simpa using hge
          have htail := ih hs.tail hi' hge'
          by_cases hy : y < v
          · simp [hy]
            omega
          · simp [hy]
            omega

private theorem lower_eq_filter_lt_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v = (ys.filter (fun x => decide (x < v))).length := by
  apply Nat.le_antisymm
  · apply Nat.le_of_not_gt
    intro h
    have hnlen : (ys.filter (fun x => decide (x < v))).length < ys.length :=
      Nat.lt_of_lt_of_le h (lower_le_length hs v)
    have hlow := lower_lt_of_lt hs v h
    rw [getD_eq_getElem hnlen] at hlow
    have hcnt := length_filter_lt_ge hs hnlen hlow
    omega
  · apply Nat.le_of_not_gt
    intro h
    have hln : lower ys v < (ys.filter (fun x => decide (x < v))).length := h
    have hlen : lower ys v < ys.length :=
      Nat.lt_of_lt_of_le hln (List.length_filter_le _ _)
    have hge := lower_le_of_ge hs v (Nat.le_refl _) hlen
    rw [getD_eq_getElem hlen] at hge
    have hcnt := length_filter_lt_le hs hlen hge
    omega

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

private theorem slice_zero_eq_filter {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (b : Nat) :
    slice ys 0 b = ys.filter (fun x => decide (x < b)) := by
  unfold slice
  rw [lower_eq_filter_lt_length hs 0, lower_eq_filter_lt_length hs b]
  have hz : (ys.filter (fun x => decide (x < 0))).length = 0 := by simp
  rw [hz, Nat.sub_zero, List.drop_zero]
  exact take_filter_prefix hs b

theorem sound_OrderedProbe : ReciprocalPrice.OrderedProbe.Goal := by
  intro u t x ys hv
  have hs := hv.1
  have hyslt : ∀ y ∈ ys, y < u := hv.2
  unfold seek
  dsimp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ReciprocalPrice.search]
  by_cases hx : x = 0
  · subst x
    by_cases hu : u = 0
    · subst u
      have hnil : ys = [] := by
        apply List.eq_nil_iff_forall_not_mem.mpr
        intro y hy
        have := hyslt y hy
        omega
      simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, hnil]
    · have hupos : 0 < u := Nat.pos_of_ne_zero hu
      simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, hupos]
      rw [slice_zero_eq_filter hs]
      have htrue : ys.filter (fun y => true) = ys := by simp
      rw [htrue]
      apply List.filter_eq_self.mpr
      intro y hy
      simp [hyslt y hy]
  · have hxpos : 0 < x := Nat.pos_of_ne_zero hx
    by_cases hu : u = 0
    · subst u
      have hnil : ys = [] := by
        apply List.eq_nil_iff_forall_not_mem.mpr
        intro y hy
        have := hyslt y hy
        omega
      simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, hnil]
    · have hupos : 0 < u := Nat.pos_of_ne_zero hu
      have hbpos : 0 < min u (t / x + 1) := by
        exact Nat.lt_min.mpr ⟨hupos, Nat.zero_lt_succ _⟩
      simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, hx, hbpos]
      rw [slice_zero_eq_filter hs]
      apply List.filter_congr
      intro y hy
      have hyu := hyslt y hy
      by_cases hylt : y < t / x + 1
      · have hyb : y < min u (t / x + 1) := Nat.lt_min.mpr ⟨hyu, hylt⟩
        have hyle : y ≤ t / x := by omega
        have hymul : y * x ≤ t := (Nat.le_div_iff_mul_le hxpos).mp hyle
        have hmul : x * y ≤ t := by simpa [Nat.mul_comm] using hymul
        simp [hyb, hmul]
      · have hnyle : ¬ y ≤ t / x := by omega
        have hynmul : ¬ y * x ≤ t := by
          intro hm
          exact hnyle ((Nat.le_div_iff_mul_le hxpos).mpr hm)
        have hnmul : ¬ x * y ≤ t := by
          simpa [Nat.mul_comm] using hynmul
        have hnyb : ¬ y < min u (t / x + 1) := by omega
        simp [hnyb, hnmul]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ReciprocalPrice_OrderedProbe
