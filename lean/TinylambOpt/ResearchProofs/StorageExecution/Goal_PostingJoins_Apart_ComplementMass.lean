import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `Apart.ComplementMass` — `∀ u t x ys, valid u ys → fan search u t x ys + ys.countP
  (fun y => x≤y+t && y≤x+t) = ys.length`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Apart_ComplementMass

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Apart

private def Cut (xs : List Nat) (v i : Nat) : Prop :=
  (∀ j, j < i → xs.getD j 0 < v) ∧
  (∀ j, i ≤ j → j < xs.length → v ≤ xs.getD j 0)

private theorem getD_eq_getElem {xs : List Nat} {i : Nat} (hi : i < xs.length) :
    xs.getD i 0 = xs[i] := by
  simp [List.getD, hi]

private theorem getD_toArray (xs : List Nat) (i : Nat) :
    xs.toArray[i]?.getD 0 = xs.getD i 0 := by
  by_cases hi : i < xs.length
  · have ha : i < xs.toArray.size := by simpa using hi
    rw [Array.getElem?_eq_getElem ha, Option.getD_some, List.getElem_toArray ha]
    simp [List.getD, hi]
  · have ha : xs.toArray.size ≤ i := by simpa using hi
    rw [Array.getElem?_eq_none ha]
    simp [List.getD, hi]

private theorem optional_some {a b : Nat} (h : a < b) : optional a b = [(a, b)] := by
  simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h]

private theorem optional_none {a b : Nat} (h : ¬ a < b) : optional a b = [] := by
  simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h]

private theorem width_pow_bound (n : Nat) : n < 2 ^ (width n + 1) := by
  unfold width
  by_cases h : n == 0
  · have hn : n = 0 := by simpa using h
    subst hn
    simp
  · have hne : n ≠ 0 := by intro hz; simp [hz] at h
    simp [h]
    have hlt : n < 2 ^ (n.log2 + 1) := (Nat.log2_lt hne).mp (Nat.lt_succ_self _)
    have hpow : (2 : Nat) ^ (n.log2 + 1 + 1) = 2 * 2 ^ (n.log2 + 1) := by
      rw [Nat.pow_succ]
      omega
    rw [hpow]
    omega

private theorem searchBound_cut {xs : Array Nat} (v fuel lo hi : Nat)
    (hlo : lo ≤ hi) (hhi : hi ≤ xs.size) (hf : hi - lo < 2 ^ fuel)
    (hbelow : ∀ j, j < lo → xs.toList.getD j 0 < v)
    (habove : ∀ j, hi ≤ j → j < xs.toList.length → v ≤ xs.toList.getD j 0)
    (hs : xs.toList.Pairwise (· ≤ ·)) :
    Cut xs.toList v (searchBound xs v fuel lo hi) ∧
      lo ≤ searchBound xs v fuel lo hi ∧ searchBound xs v fuel lo hi ≤ hi := by
  induction fuel generalizing lo hi with
  | zero =>
      have heq : lo = hi := by
        have h2 : (2 : Nat) ^ 0 = 1 := by simp
        omega
      subst hi
      simp only [searchBound]
      exact ⟨⟨hbelow, habove⟩, Nat.le_refl lo, Nat.le_refl lo⟩
  | succ fuel ih =>
      by_cases hge : lo ≥ hi
      · have heq : lo = hi := by omega
        subst hi
        simp only [searchBound, hge, ↓reduceIte]
        exact ⟨⟨hbelow, habove⟩, trivial, trivial⟩
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
        have hpow : (2 : Nat) ^ (fuel + 1) = 2 * 2 ^ fuel := by
          simp [Nat.pow_succ, Nat.mul_comm]
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
              by_cases hjl : j < lo
              · exact hbelow j hjl
              · have hrel : xs.toList.getD j 0 ≤ xs.toList.getD mid 0 := by
                  rw [getD_eq_getElem hjlen, getD_eq_getElem hmidlen]
                  exact hs.rel_getElem_of_lt hjlen hmidlen hjm
                rw [hlistval] at hrel
                omega
          have hf' : hi - (mid + 1) < 2 ^ fuel := by
            rw [hpow] at hf
            omega
          obtain ⟨hc, hlb, hub⟩ := ih (mid + 1) hi (by omega) (by omega) hf' hbelow' habove
          exact ⟨hc, Nat.le_trans (by omega : lo ≤ mid + 1) hlb, hub⟩
        · rw [ite_eq_right hv]
          have habove' : ∀ j, mid ≤ j → j < xs.toList.length → v ≤ xs.toList.getD j 0 := by
            intro j hj hjlen
            by_cases heq : j = mid
            · subst j
              rw [hlistval]
              omega
            · have hmj : mid < j := by omega
              by_cases hjh : hi ≤ j
              · exact habove j hjh hjlen
              · have hrel : xs.toList.getD mid 0 ≤ xs.toList.getD j 0 := by
                  rw [hlistval, getD_eq_getElem hjlen]
                  exact hs.rel_getElem_of_lt hmidlen hjlen hmj
                omega
          have hf' : mid - lo < 2 ^ fuel := by
            rw [hpow] at hf
            omega
          obtain ⟨hc, hlb, hub⟩ := ih lo mid hml (by omega) hf' hbelow habove'
          exact ⟨hc, hlb, Nat.le_trans hub (Nat.le_of_lt hmh)⟩

private theorem lower_cut {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    Cut ys v (lower ys v) ∧ lower ys v ≤ ys.length := by
  unfold lower
  have hf : ys.length - 0 < 2 ^ (width ys.length + 1) := by
    have := width_pow_bound ys.length
    omega
  have h := searchBound_cut (xs := ys.toArray) v (width ys.length + 1) 0 ys.length
    (Nat.zero_le _) (by simp) hf
    (by intro j hj; omega)
    (by
      intro j hj hjlen
      have hjl : j < ys.length := by simpa using hjlen
      omega)
    (by simpa using hs)
  simpa using h

private theorem lower_le_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v ≤ ys.length := (lower_cut hs v).2

private theorem lower_lt_of_lt {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) {i : Nat}
    (hi : i < lower ys v) : ys.getD i 0 < v := (lower_cut hs v).1.1 i hi

private theorem lower_le_of_ge {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) {i : Nat}
    (hj : lower ys v ≤ i) (hi : i < ys.length) : v ≤ ys.getD i 0 :=
  (lower_cut hs v).1.2 i hj hi

private theorem lower_eq_filter_lt_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v = (ys.filter (fun x => decide (x < v))).length := by
  have hle : lower ys v ≤ ys.length := lower_le_length hs v
  have htake : (ys.take (lower ys v)).filter (fun x => decide (x < v)) = ys.take (lower ys v) := by
    rw [List.filter_eq_self]
    intro x hx
    rw [List.mem_take_iff_getElem] at hx
    rcases hx with ⟨i, hi, rfl⟩
    simp only [decide_eq_true_eq, List.getElem_take]
    have hi' : i < lower ys v := by omega
    have hiys : i < ys.length := by omega
    have : ys.getD i 0 < v := lower_lt_of_lt hs v hi'
    rwa [getD_eq_getElem hiys] at this
  have hdrop : (ys.drop (lower ys v)).filter (fun x => decide (x < v)) = [] := by
    rw [List.filter_eq_nil_iff]
    intro x hx
    rw [List.mem_iff_getElem] at hx
    rcases hx with ⟨i, hi, rfl⟩
    simp only [List.getElem_drop]
    have hiys : lower ys v + i < ys.length := by
      have hlen : i < (ys.drop (lower ys v)).length := hi
      rw [List.length_drop] at hlen
      omega
    have hge := lower_le_of_ge hs v (Nat.le_add_right _ _) hiys
    rw [getD_eq_getElem hiys] at hge
    by_cases hxlt : ys[lower ys v + i] < v
    · omega
    · simp [hxlt]
  have hsplit := (List.take_append_drop (lower ys v) ys).symm
  calc
    lower ys v = (ys.take (lower ys v)).length := by
      rw [List.length_take, Nat.min_eq_left hle]
    _ = ((ys.take (lower ys v)).filter (fun x => decide (x < v))).length := by rw [htake]
    _ = ((ys.take (lower ys v)).filter (fun x => decide (x < v)) ++
          (ys.drop (lower ys v)).filter (fun x => decide (x < v))).length := by
        rw [hdrop, List.append_nil]
    _ = ((ys.take (lower ys v) ++ ys.drop (lower ys v)).filter (fun x => decide (x < v))).length := by
        rw [List.filter_append]
    _ = (ys.filter (fun x => decide (x < v))).length := by rw [← hsplit]

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
            intro hay
            have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
            omega
          simp [hxa, hxb, hnil_a, hnil_ab, ih hs']

private theorem lower_mono {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) {a b : Nat}
    (hab : a ≤ b) : lower ys a ≤ lower ys b := by
  rw [lower_eq_filter_lt_length hs a, lower_eq_filter_lt_length hs b]
  rw [filter_lt_prefix_filter_lt hs hab]
  simp

private theorem lower_zero {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) : lower ys 0 = 0 := by
  rw [lower_eq_filter_lt_length hs 0]
  simp

private theorem lower_domain {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) {u : Nat}
    (hv : ∀ y ∈ ys, y < u) : lower ys u = ys.length := by
  rw [lower_eq_filter_lt_length hs u]
  have hself : ys.filter (fun x => decide (x < u)) = ys := by
    rw [List.filter_eq_self]
    intro y hy
    simp [hv y hy]
  rw [hself]

private theorem lt_min_iff_of_ltu {u b y : Nat} (hyu : y < u) :
    y < min u b ↔ y < b := by
  constructor
  · intro h
    exact Nat.lt_of_lt_of_le h (Nat.min_le_right u b)
  · intro h
    by_cases hb : b ≤ u
    · rw [Nat.min_eq_right hb]
      exact h
    · rw [Nat.min_eq_left (by omega)]
      exact hyu

private theorem fan_left_part {ys : List Nat} (_hs : ys.Pairwise (· ≤ ·)) {u x : Nat} :
    ((optional 0 (min u x)).map (fun (a, b) => lower ys b - lower ys a)).sum =
      lower ys (min u x) - lower ys 0 := by
  by_cases h : 0 < min u x
  · rw [optional_some h]
    simp
  · have hz : min u x = 0 := by omega
    rw [optional_none h, hz]
    simp

private theorem fan_right_part {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) {u x : Nat} :
    ((optional (x + 1) u).map (fun (a, b) => lower ys b - lower ys a)).sum =
      lower ys u - lower ys (x + 1) := by
  by_cases h : x + 1 < u
  · rw [optional_some h]
    simp
  · rw [optional_none h]
    have hle : lower ys u ≤ lower ys (x + 1) := lower_mono hs (Nat.le_of_not_gt h)
    simp
    omega

private theorem fan_apart {u t x : Nat} {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    (hvu : ∀ y ∈ ys, y < u) :
    fan search u t x ys = lower ys (min u (x - t)) + (ys.length - lower ys (x + t + 1)) := by
  unfold fan
  simp only [search]
  rw [List.map_append, List.sum_append]
  rw [fan_left_part hs, fan_right_part hs]
  rw [lower_zero hs, lower_domain hs hvu]
  simp

private theorem countP_eq_filter_length (p : Nat → Bool) (ys : List Nat) :
    ys.countP p = (ys.filter p).length := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
      by_cases h : p y = true
      · simp [h, ih]
      · have hf : p y = false := by cases hp : p y <;> simp_all
        simp [hf, ih]

private theorem filter_apart_eq_append (t x : Nat) {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) :
    ys.filter (fun y => decide (y + t < x) || decide (x + t < y)) =
      ys.filter (fun y => decide (y + t < x)) ++ ys.filter (fun y => decide (x + t < y)) := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
      have hs' : ys.Pairwise (· ≤ ·) := hs.tail
      have ih' := ih hs'
      by_cases hp : y + t < x
      · have hq : ¬ x + t < y := by omega
        simp [hp, hq, ih']
      · by_cases hq : x + t < y
        · have hnilp : ys.filter (fun z => decide (z + t < x)) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro z hz
            simp only [decide_eq_true_eq]
            have hyz : y ≤ z := List.rel_of_pairwise_cons hs hz
            omega
          simp [hp, hq, hnilp, ih']
        · simp [hp, hq, ih']

private theorem filter_lt_add_filter_ge (ys : List Nat) (v : Nat) :
    (ys.filter (fun y => decide (y < v))).length +
      (ys.filter (fun y => decide (v ≤ y))).length = ys.length := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
      by_cases h : y < v
      · have hge : ¬ v ≤ y := by omega
        simp [h, hge] at ih ⊢
        omega
      · have hge : v ≤ y := by omega
        simp [h, hge] at ih ⊢
        omega

private theorem count_apart {u t x : Nat} {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    (hvu : ∀ y ∈ ys, y < u) :
    ys.countP (fun y => decide (x + t < y) || decide (y + t < x)) =
      lower ys (min u (x - t)) + (ys.length - lower ys (x + t + 1)) := by
  rw [countP_eq_filter_length]
  have hswap : ys.filter (fun y => decide (x + t < y) || decide (y + t < x)) =
      ys.filter (fun y => decide (y + t < x) || decide (x + t < y)) := by
    apply List.filter_congr
    intro y hy
    exact Bool.or_comm _ _
  rw [hswap, filter_apart_eq_append t x hs, List.length_append]
  have hfilter_lt : ys.filter (fun y => decide (y + t < x)) =
      ys.filter (fun y => decide (y < min u (x - t))) := by
    apply List.filter_congr
    intro y hy
    have hyu : y < u := hvu y hy
    by_cases h : y + t < x
    · have hyx : y < x - t := Nat.lt_sub_of_add_lt h
      have hym : y < min u (x - t) := (lt_min_iff_of_ltu hyu).mpr hyx
      simp [h, hym]
    · have hyx : ¬ y < x - t := by
        intro hh
        exact h (Nat.add_lt_of_lt_sub hh)
      have hym : ¬ y < min u (x - t) := by
        intro hh
        exact hyx ((lt_min_iff_of_ltu hyu).mp hh)
      simp [h, hym]
  have hfilter_gt : ys.filter (fun y => decide (x + t < y)) =
      ys.filter (fun y => decide (x + t + 1 ≤ y)) := by
    apply List.filter_congr
    intro y hy
    by_cases h : x + t < y
    · have hge : x + t + 1 ≤ y := by omega
      simp [h, hge]
    · have hge : ¬ x + t + 1 ≤ y := by omega
      simp [h, hge]
  rw [hfilter_lt, hfilter_gt]
  have hge_count : (ys.filter (fun y => decide (x + t + 1 ≤ y))).length =
      ys.length - lower ys (x + t + 1) := by
    have hp := filter_lt_add_filter_ge ys (x + t + 1)
    rw [← lower_eq_filter_lt_length hs (x + t + 1)] at hp
    omega
  rw [← lower_eq_filter_lt_length hs (min u (x - t)), hge_count]

private theorem countP_add_of_complement (p q : Nat → Bool) (ys : List Nat)
    (hcomp : ∀ y, p y = true ↔ q y = false) :
    ys.countP p + ys.countP q = ys.length := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
      by_cases hp : p y = true
      · have hq : q y = false := (hcomp y).mp hp
        simp [hp, hq] at ih ⊢
        omega
      · have hpf : p y = false := by cases hpv : p y <;> simp_all
        have hq : q y = true := by
          cases hqv : q y with
          | false => exact False.elim (hp ((hcomp y).mpr hqv))
          | true => rfl
        simp [hpf, hq] at ih ⊢
        omega

theorem sound_ComplementMass : Apart.ComplementMass.Goal := by
  intro u t x ys hv
  have hs : ys.Pairwise (· ≤ ·) := hv.1
  have hvu : ∀ y ∈ ys, y < u := hv.2
  have hfan : fan search u t x ys = ys.countP (fun y => decide (x + t < y) || decide (y + t < x)) := by
    rw [fan_apart hs hvu, count_apart hs hvu]
  rw [hfan]
  apply countP_add_of_complement
  intro y
  by_cases h1 : x + t < y
  · have hn : ¬ y ≤ x + t := by omega
    simp [h1, hn]
  · by_cases h2 : y + t < x
    · have hn : ¬ x ≤ y + t := by omega
      simp [h1, h2, hn]
    · have hx : x ≤ y + t := by omega
      have hy : y ≤ x + t := by omega
      simp [h1, h2, hx, hy]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Apart_ComplementMass
