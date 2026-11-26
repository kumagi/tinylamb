import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Apart_MaskedJoin

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Apart

/-! Self-contained certificate for `Apart.MaskedJoin`.

The binary lower bound cuts the sorted right-key column exactly at the filter
boundary, so the two-offset fan equals the number of matching keys.  Deleting
right positions by index subtracts exactly the matching deleted positions:
`eraseDups` turns the deletion list into a set, while `zipIdx` assigns every
right position a unique index. -/

private theorem arr_getD (xs : Array Nat) (i : Nat) : xs[i]?.getD 0 = xs.toList.getD i 0 := by
  by_cases h : i < xs.size
  · have h1 : xs[i]? = some xs[i] := Array.getElem?_eq_getElem h
    have h2 : i < xs.toList.length := by simpa using h
    have h3 : xs.toList[i]? = some xs.toList[i] := List.getElem?_eq_getElem h2
    simp only [h1, Option.getD_some]
    simp [List.getD, h3]
  · have hle : xs.size ≤ i := by omega
    have h1 : xs[i]? = none := Array.getElem?_eq_none hle
    have h2 : xs.toList.length ≤ i := by simpa using hle
    have h3 : xs.toList[i]? = none := List.getElem?_eq_none h2
    simp only [h1, Option.getD_none]
    simp [List.getD, h3]

private theorem getD_eq_getElem {xs : List Nat} {i : Nat} (hi : i < xs.length) :
    xs.getD i 0 = xs[i] := by
  simp [List.getD, List.getElem?_eq_getElem hi]

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
      have heq : lo = hi := by simp at hf; omega
      subst hi
      simp only [searchBound]
      exact ⟨⟨hbelow, habove⟩, ⟨Nat.le_refl _, Nat.le_refl _⟩⟩
  | succ fuel ih =>
      by_cases hge : lo ≥ hi
      · have heq : lo = hi := by omega
        subst hi
        rw [searchBound]
        simp only [ite_eq_left (Nat.le_refl lo)]
        exact ⟨⟨hbelow, habove⟩, ⟨Nat.le_refl _, Nat.le_refl _⟩⟩
      · have hlt : lo < hi := by omega
        let mid := (lo + hi) / 2
        have hml : lo ≤ mid := by omega
        have hmh : mid < hi := by omega
        have hmid : mid < xs.size := by omega
        have hmidlen : mid < xs.toList.length := by simpa using hmid
        have hval : xs[mid]?.getD 0 = xs.toList.getD mid 0 := arr_getD xs mid
        have hpow : 2 ^ (fuel + 1) = 2 * 2 ^ fuel := by rw [Nat.pow_succ]; omega
        rw [searchBound]
        simp only [hge, ↓reduceIte]
        rw [hval]
        by_cases hv : xs.toList.getD mid 0 < v
        · rw [ite_eq_left hv]
          have hbelow' : ∀ j, j < mid + 1 → xs.toList.getD j 0 < v := by
            intro j hj
            by_cases hjmid : j = mid
            · subst j; exact hv
            · have hjlt : j < mid := by omega
              have hjlen : j < xs.toList.length := by omega
              by_cases hjlo : j < lo
              · exact hbelow j hjlo
              · have hrel : xs.toList.getD j 0 ≤ xs.toList.getD mid 0 := by
                  have hpair := hs.rel_getElem_of_lt hjlen hmidlen hjlt
                  rw [← getD_eq_getElem hjlen, ← getD_eq_getElem hmidlen] at hpair
                  exact hpair
                omega
          have hf' : hi - (mid + 1) < 2 ^ fuel := by
            rw [hpow] at hf
            omega
          obtain ⟨hc, hlb, hub⟩ := ih (mid + 1) hi (by omega) (by omega) hf' hbelow' habove
          exact ⟨hc, Nat.le_trans (Nat.le_trans hml (Nat.le_succ mid)) hlb, hub⟩
        · rw [ite_eq_right hv]
          have habove' : ∀ j, mid ≤ j → j < xs.toList.length → v ≤ xs.toList.getD j 0 := by
            intro j hj hjlen
            by_cases hjmid : j = mid
            · subst j; omega
            · have hmj : mid < j := by omega
              by_cases hjhi : hi ≤ j
              · exact habove j hjhi hjlen
              · have hrel : xs.toList.getD mid 0 ≤ xs.toList.getD j 0 := by
                  have hpair := hs.rel_getElem_of_lt hmidlen hjlen hmj
                  rw [← getD_eq_getElem hmidlen, ← getD_eq_getElem hjlen] at hpair
                  exact hpair
                omega
          have hf' : mid - lo < 2 ^ fuel := by
            rw [hpow] at hf
            omega
          obtain ⟨hc, hlb, hub⟩ := ih lo mid (by omega) (by omega) hf' hbelow habove'
          exact ⟨hc, hlb, Nat.le_trans hub (Nat.le_of_lt hmh)⟩

private theorem width_pow_bound (n : Nat) : n < 2 ^ (width n + 1) := by
  unfold width
  by_cases h : n == 0
  · have hn : n = 0 := by simpa using h
    subst n
    simp
  · have hne : n ≠ 0 := by intro hz; simp [hz] at h
    simp [h]
    have hlt : n < 2 ^ (n.log2 + 1) := (Nat.log2_lt hne).mp (Nat.lt_succ_self _)
    have hp : 2 ^ (n.log2 + 1 + 1) = 2 * 2 ^ (n.log2 + 1) := by rw [Nat.pow_succ]; omega
    rw [hp]
    omega

private theorem lower_cut {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    Cut ys v (lower ys v) ∧ lower ys v ≤ ys.length := by
  unfold lower
  have hf : ys.length - 0 < 2 ^ (width ys.length + 1) := by
    simpa using width_pow_bound ys.length
  have h := searchBound_cut (xs := ys.toArray) v (width ys.length + 1) 0 ys.length
    (Nat.zero_le _) (by simp) hf
    (by intro j hj; omega)
    (by intro j hj hjlen
        have hjl : j < ys.length := by simpa using hjlen
        omega)
    (by simpa using hs)
  simpa [arr_getD] using h

private theorem lower_eq_filter_lt_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v = (ys.filter (fun x => decide (x < v))).length := by
  let i := lower ys v
  have hcut : Cut ys v i := by simpa [i] using (lower_cut hs v).1
  have hle : i ≤ ys.length := by simpa [i] using (lower_cut hs v).2
  have htake : (ys.take i).filter (fun x => decide (x < v)) = ys.take i := by
    apply List.filter_eq_self.mpr
    intro x hx
    rw [List.mem_take_iff_getElem] at hx
    rcases hx with ⟨j, hj, rfl⟩
    simp only [decide_eq_true_eq]
    have hji : j < i := by omega
    have hjys : j < ys.length := by omega
    have hlt : ys.getD j 0 < v := hcut.1 j hji
    rwa [getD_eq_getElem hjys] at hlt
  have hdrop : (ys.drop i).filter (fun x => decide (x < v)) = [] := by
    apply List.filter_eq_nil_iff.mpr
    intro x hx
    rw [List.mem_iff_getElem] at hx
    rcases hx with ⟨j, hj, rfl⟩
    simp only [decide_eq_true_eq, List.getElem_drop]
    intro hlt
    have hjlen : i + j < ys.length := by
      have ht : j < (ys.drop i).length := hj
      rw [List.length_drop] at ht
      omega
    have hge : v ≤ ys.getD (i + j) 0 := hcut.2 (i + j) (Nat.le_add_right _ _) hjlen
    rw [getD_eq_getElem hjlen] at hge
    omega
  have hsplit := (List.take_append_drop i ys).symm
  calc
    i = (ys.take i).length := by rw [List.length_take]; simp [Nat.min_eq_left hle]
    _ = ((ys.take i).filter (fun x => decide (x < v))).length := by rw [htake]
    _ = ((ys.take i).filter (fun x => decide (x < v)) ++
          (ys.drop i).filter (fun x => decide (x < v))).length := by rw [hdrop, List.append_nil]
    _ = ((ys.take i ++ ys.drop i).filter (fun x => decide (x < v))).length := by rw [List.filter_append]
    _ = (ys.filter (fun x => decide (x < v))).length := by
        exact congrArg (fun l : List Nat => (l.filter (fun x => decide (x < v))).length) hsplit.symm

private theorem filter_lt_prefix_filter_lt {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) {a b : Nat}
    (hab : a ≤ b) :
    ys.filter (fun y => decide (y < b)) =
      ys.filter (fun y => decide (y < a)) ++ ys.filter (fun y => decide (a ≤ y && y < b)) := by
  induction ys with
  | nil => simp
  | cons x xs ih =>
      have hs' := hs.tail
      by_cases hxa : x < a
      · have hxb : x < b := by omega
        simp [hxa, hxb, ih hs']
      · by_cases hxb : x < b
        · have hax : a ≤ x := by omega
          have hnila : xs.filter (fun y => decide (y < a)) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro y hy
            simp only [decide_eq_true_eq]
            have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
            omega
          simp [hxa, hxb, hax, hnila, ih hs']
        · have hnila : xs.filter (fun y => decide (y < a)) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro y hy
            simp only [decide_eq_true_eq]
            have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
            omega
          have hnilab : xs.filter (fun y => decide (a ≤ y && y < b)) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro y hy
            simp only [decide_eq_true_eq, Bool.and_eq_true]
            intro hay
            have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
            omega
          simp [hxa, hxb, hnila, hnilab, ih hs']

private theorem lower_interval {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) {a b : Nat}
    (hab : a ≤ b) :
    lower ys b - lower ys a = (ys.filter (fun y => decide (a ≤ y && y < b))).length := by
  rw [lower_eq_filter_lt_length hs b, lower_eq_filter_lt_length hs a]
  have hsplit := filter_lt_prefix_filter_lt hs (ys := ys) hab
  rw [hsplit, List.length_append]
  omega

private theorem lt_sub_iff_add_lt (y t x : Nat) : y < x - t ↔ y + t < x := by
  omega

private theorem le_sub_iff_not_add_lt (y t x : Nat) : x - t ≤ y ↔ ¬ y + t < x := by
  omega

private theorem test_or_band {t x y : Nat} :
    search.test t x y = true ↔ y < x - t ∨ x + t + 1 ≤ y := by
  simp only [search, Bool.or_eq_true, decide_eq_true_eq]
  rw [lt_sub_iff_add_lt]
  omega

private theorem band_iff_not_test {t x y : Nat} :
    (x - t ≤ y ∧ y < x + t + 1) ↔ search.test t x y = false := by
  have hiff := test_or_band (t := t) (x := x) (y := y)
  constructor
  · intro hb
    have hno : ¬ (y < x - t ∨ x + t + 1 ≤ y) := by
      intro h
      rcases h with hlt | hge
      · exact (Nat.not_le_of_gt hlt) hb.1
      · exact (Nat.not_lt_of_ge hge) hb.2
    cases hbt : search.test t x y
    · rfl
    · exact False.elim (hno (hiff.mp hbt))
  · intro hfalse
    have hno : ¬ (y < x - t ∨ x + t + 1 ≤ y) := by
      intro hor
      have ht : search.test t x y = true := hiff.mpr hor
      rw [ht] at hfalse
      contradiction
    constructor
    · exact Nat.le_of_not_gt (fun hlt => hno (Or.inl hlt))
    · have hnoge : ¬ x + t + 1 ≤ y := fun hge => hno (Or.inr hge)
      exact Nat.lt_of_not_ge hnoge

private theorem filter_complement_length (ys : List Nat) (p : Nat → Bool) :
    (ys.filter p).length + (ys.filter (fun y => !p y)).length = ys.length := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
      by_cases hp : p y <;> simp [hp, ih] <;> omega

private theorem filter_low_min {u a : Nat} {ys : List Nat}
    (hv : ∀ y ∈ ys, y < u) :
    ys.filter (fun y => decide (y < min u a)) = ys.filter (fun y => decide (y < a)) := by
  apply List.filter_congr
  intro y hy
  have hyu := hv y hy
  by_cases h : y < min u a
  · have hya : y < a := by
      rw [Nat.lt_min] at h
      exact h.2
    simp [h, hya]
  · have hya : ¬ y < a := by
      intro hya
      apply h
      rw [Nat.lt_min]
      exact ⟨hyu, hya⟩
    simp [h, hya]

private theorem filter_high_u {u a : Nat} {ys : List Nat}
    (hv : ∀ y ∈ ys, y < u) :
    ys.filter (fun y => decide (a ≤ y && y < u)) = ys.filter (fun y => decide (a ≤ y)) := by
  apply List.filter_congr
  intro y hy
  by_cases hay : a ≤ y
  · have hyu := hv y hy
    simp [hay, hyu]
  · simp [hay]

private theorem low_count {u t x : Nat} {ys : List Nat}
    (hs : ys.Pairwise (· ≤ ·)) (hv : ∀ y ∈ ys, y < u) :
    lower ys (min u (x - t)) - lower ys 0 =
      (ys.filter (fun y => decide (y < x - t))).length := by
  rw [lower_interval hs (Nat.zero_le _)]
  congr 1
  have h1 : ys.filter (fun y => decide (0 ≤ y && y < min u (x - t))) =
      ys.filter (fun y => decide (y < min u (x - t))) := by
    apply List.filter_congr
    intro y hy
    simp
  rw [h1, filter_low_min hv]

private theorem high_count {u t x : Nat} {ys : List Nat}
    (hs : ys.Pairwise (· ≤ ·)) (hv : ∀ y ∈ ys, y < u) (hle : x + t + 1 ≤ u) :
    lower ys u - lower ys (x + t + 1) =
      (ys.filter (fun y => decide (x + t + 1 ≤ y))).length := by
  rw [lower_interval hs hle]
  congr 1
  exact filter_high_u hv

private theorem low_empty {u t x : Nat} {ys : List Nat}
    (hv : ∀ y ∈ ys, y < u) (hnot : ¬ 0 < min u (x - t)) :
    (ys.filter (fun y => decide (y < x - t))).length = 0 := by
  rw [List.length_eq_zero_iff]
  apply List.filter_eq_nil_iff.mpr
  intro y hy
  simp only [decide_eq_true_eq]
  by_cases hle : x - t ≤ u
  · have hmin : min u (x - t) = x - t := Nat.min_eq_right hle
    rw [hmin] at hnot
    have hx0 : x - t = 0 := by omega
    omega
  · have hmin : min u (x - t) = u := Nat.min_eq_left (by omega)
    rw [hmin] at hnot
    have hu0 : u = 0 := by omega
    have hyu := hv y hy
    omega

private theorem high_empty {u t x : Nat} {ys : List Nat}
    (hv : ∀ y ∈ ys, y < u) (hnot : ¬ x + t + 1 < u) :
    (ys.filter (fun y => decide (x + t + 1 ≤ y))).length = 0 := by
  rw [List.length_eq_zero_iff]
  apply List.filter_eq_nil_iff.mpr
  intro y hy
  simp only [decide_eq_true_eq]
  have hyu := hv y hy
  omega

private theorem fan_apart {u t x : Nat} {ys : List Nat} (hv : valid u ys) :
    fan search u t x ys = ys.countP (search.test t x) := by
  have hs : ys.Pairwise (· ≤ ·) := hv.1
  have hvlt : ∀ y ∈ ys, y < u := hv.2
  let lo := (ys.filter (fun y => decide (y < x - t))).length
  let band := (ys.filter (fun y => decide (x - t ≤ y && y < x + t + 1))).length
  let hi := (ys.filter (fun y => decide (x + t + 1 ≤ y))).length
  have hab : x - t ≤ x + t + 1 := by omega
  have hpart1 := filter_lt_prefix_filter_lt hs (ys := ys) hab
  have hlen1 : lo + band = (ys.filter (fun y => decide (y < x + t + 1))).length := by
    dsimp [lo, band]
    rw [hpart1, List.length_append]
  have hpart2len : (ys.filter (fun y => decide (y < x + t + 1))).length + hi = ys.length := by
    by_cases hxu : x + t + 1 ≤ u
    · have hp := filter_lt_prefix_filter_lt hs (ys := ys) hxu
      have hfu : ys.filter (fun y => decide (y < u)) = ys := by
        apply List.filter_eq_self.mpr
        intro y hy
        simp only [decide_eq_true_eq]
        exact hvlt y hy
      have hhi' : ys.filter (fun y => decide (x + t + 1 ≤ y && y < u)) =
          ys.filter (fun y => decide (x + t + 1 ≤ y)) := by
        exact filter_high_u hvlt
      have hplen := congrArg List.length hp
      rw [hfu] at hplen
      simp only [List.length_append] at hplen
      rw [hhi'] at hplen
      omega
    · have hfu : ys.filter (fun y => decide (y < x + t + 1)) = ys := by
        apply List.filter_eq_self.mpr
        intro y hy
        simp only [decide_eq_true_eq]
        have hyu := hvlt y hy
        omega
      have hhi0 : ys.filter (fun y => decide (x + t + 1 ≤ y)) = [] := by
        apply List.filter_eq_nil_iff.mpr
        intro y hy
        simp only [decide_eq_true_eq]
        have hyu := hvlt y hy
        omega
      have hz : hi = 0 := by
        dsimp [hi]
        rw [hhi0]
        simp
      rw [hfu]
      omega
  have htotal : lo + band + hi = ys.length := by omega
  have hcomp : ys.countP (search.test t x) + band = ys.length := by
    dsimp [band]
    rw [List.countP_eq_length_filter]
    have hnilcompl : ys.filter (fun y => !(decide (search.test t x y))) =
        ys.filter (fun y => decide (x - t ≤ y && y < x + t + 1)) := by
      apply List.filter_congr
      intro y hy
      by_cases hb : x - t ≤ y ∧ y < x + t + 1
      · have htfalse : search.test t x y = false :=
          (band_iff_not_test (t := t) (x := x) (y := y)).mp hb
        have hbtrue : decide (x - t ≤ y && y < x + t + 1) = true := by
          simp only [decide_eq_true_eq, Bool.and_eq_true]
          exact hb
        rw [hbtrue]
        simp [htfalse]
      · have httrue : search.test t x y = true := by
          cases hbt : search.test t x y
          · exact False.elim (hb ((band_iff_not_test (t := t) (x := x) (y := y)).mpr hbt))
          · rfl
        have hbfalse : decide (x - t ≤ y && y < x + t + 1) = false := by
          rw [decide_eq_false_iff_not]
          simp only [decide_eq_true_eq, Bool.and_eq_true]
          exact hb
        rw [hbfalse]
        simp [httrue]
    have hsplit := filter_complement_length ys (fun y => decide (search.test t x y))
    rw [← hnilcompl]
    exact hsplit
  have hfan : fan search u t x ys = lo + hi := by
    unfold fan search
    by_cases hA : 0 < min u (x - t)
    · by_cases hB : x + t + 1 < u
      · simp only [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional,
          hA, hB, ↓reduceIte, List.map_append, List.sum_append, List.map_cons,
          List.map_nil, List.sum_cons, List.sum_nil, Nat.add_zero]
        rw [low_count hs hvlt, high_count hs hvlt (Nat.le_of_lt hB)]
      · simp only [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional,
          hA, hB, ↓reduceIte, List.map_append, List.sum_append, List.map_cons,
          List.map_nil, List.sum_cons, List.sum_nil, Nat.add_zero]
        rw [low_count hs hvlt]
        have hz : hi = 0 := by
          dsimp [hi]
          exact high_empty hvlt hB
        omega
    · by_cases hB : x + t + 1 < u
      · simp only [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional,
          hA, hB, ↓reduceIte, List.map_append, List.sum_append, List.map_cons,
          List.map_nil, List.sum_cons, List.sum_nil, Nat.add_zero]
        rw [high_count hs hvlt (Nat.le_of_lt hB)]
        have hz : lo = 0 := by
          dsimp [lo]
          exact low_empty hvlt hA
        omega
      · simp only [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional,
          hA, hB, ↓reduceIte, List.map_append, List.sum_append, List.map_nil, List.sum_nil]
        have hzlo : lo = 0 := by
          dsimp [lo]
          exact low_empty hvlt hA
        have hzhi : hi = 0 := by
          dsimp [hi]
          exact high_empty hvlt hB
        omega
  rw [hfan]
  omega

private theorem eraseDups_nodup (xs : List Nat) : xs.eraseDups.Nodup := by
  let motive : Nat → Prop := fun n => ∀ xs : List Nat, xs.length = n → xs.eraseDups.Nodup
  have H : ∀ n, motive n := by
    intro n
    induction n using Nat.strongRecOn with
    | ind n ih =>
        intro xs hlen
        cases xs with
        | nil => simp
        | cons a xs =>
            rw [List.eraseDups_cons, List.nodup_cons]
            constructor
            · intro ha
              have hf := List.mem_filter.mp (List.mem_eraseDups.mp ha)
              simpa using hf.2
            · have hlen' : xs.length + 1 = n := by simpa using hlen
              have hlt : (xs.filter (fun b => !b == a)).length < n := by
                have hle : (xs.filter (fun b => !b == a)).length ≤ xs.length := List.length_filter_le _ _
                omega
              exact ih _ hlt _ rfl
  exact H xs.length xs rfl

private theorem countP_zipIdx_aux (ys : List Nat) (p : Nat → Bool) (off : Nat) :
    ys.countP p = (ys.zipIdx off).countP (fun q => p q.1) := by
  induction ys generalizing off with
  | nil => simp [List.zipIdx]
  | cons y ys ih =>
      by_cases hp : p y <;> simpa [List.zipIdx_cons, hp] using ih (off + 1)

private theorem countP_partition (xs : List (Nat × Nat)) (p : Nat → Bool) (ds : List Nat) :
    xs.countP (fun q => p q.1) =
      (xs.filter (fun q => !ds.contains q.2)).countP (fun q => p q.1) +
        (xs.filter (fun q => ds.contains q.2)).countP (fun q => p q.1) := by
  induction xs with
  | nil => simp
  | cons q xs ih =>
      by_cases hd : ds.contains q.2 <;> by_cases hp : p q.1 <;>
        simp only [List.countP_cons, List.filter_cons, hd, hp, Bool.not_true, Bool.not_false,
          Bool.true_eq_false, Bool.false_eq_true, ↓reduceIte] <;> omega

private theorem range_filter_shift (n : Nat) (q : Nat → Bool) :
    ((List.range (n + 1)).filter q).length =
      (if q 0 then 1 else 0) + ((List.range n).filter (fun j => q (j + 1))).length := by
  induction n with
  | zero =>
      by_cases h0 : q 0 = true <;> simp [h0]
  | succ n ih =>
      rw [List.range_succ, List.filter_append, List.length_append, ih, List.range_succ,
        List.filter_append, List.length_append]
      by_cases h0 : q 0 = true <;> by_cases hn : q (n + 1) = true <;> simp [h0, hn] <;> omega

private theorem zipIdx_filter_length_aux (ys : List Nat) (ds : List Nat) (p : Nat → Bool) (off : Nat) :
    ((ys.zipIdx off).filter (fun q => ds.contains q.2 && p q.1)).length =
      ((List.range ys.length).filter (fun j => ds.contains (off + j) && p (ys.getD j 0))).length := by
  induction ys generalizing off with
  | nil => simp [List.zipIdx]
  | cons y ys ih =>
      rw [List.zipIdx_cons]
      simp only [List.length_cons]
      rw [range_filter_shift]
      simp only [Nat.add_zero, Nat.zero_add, List.getD_cons_zero]
      have htail :
          ((ys.zipIdx (off + 1)).filter (fun q => ds.contains q.2 && p q.1)).length =
            ((List.range ys.length).filter
              (fun j => ds.contains (off + (j + 1)) && p ((y :: ys).getD (j + 1) 0))).length := by
        rw [ih (off + 1)]
        congr 1
        apply List.filter_congr
        intro j hj
        simp only [List.getD_cons_succ]
        have heq : off + 1 + j = off + (j + 1) := by omega
        rw [heq]
      simp only [List.filter_cons, List.length_cons]
      by_cases h : (ds.contains off && p y) = true
      · rw [h]
        simp only [↓reduceIte, List.length_cons]
        rw [htail]
        omega
      · have hf : (ds.contains off && p y) = false := by
          cases h' : ds.contains off && p y
          · rfl
          · exact False.elim (h h')
        rw [hf]
        simp only [Bool.false_eq_true, ↓reduceIte, Nat.zero_add]
        rw [htail]
private theorem deleted_count_eq (ys ds : List Nat) (p : Nat → Bool) :
    ds.eraseDups.countP (fun i => ys[i]?.any p) =
      (ys.zipIdx.filter (fun q => ds.contains q.2 && p q.1)).length := by
  rw [List.countP_eq_length_filter]
  have hlhs : ds.eraseDups.filter (fun i => ys[i]?.any p) =
      ds.eraseDups.filter (fun i => decide (i < ys.length) && p (ys.getD i 0)) := by
    apply List.filter_congr
    intro i hi
    by_cases h : i < ys.length
    · have hsome : ys[i]? = some ys[i] := List.getElem?_eq_getElem h
      have hget : ys.getD i 0 = ys[i] := getD_eq_getElem h
      simp [h, hsome, hget]
    · have hnone : ys[i]? = none := List.getElem?_eq_none (by omega)
      simp [h, hnone]
  rw [hlhs]
  have hperm :
      (ds.eraseDups.filter (fun i => decide (i < ys.length) && p (ys.getD i 0))).Perm
        ((List.range ys.length).filter (fun i => decide (i ∈ ds.eraseDups) && p (ys.getD i 0))) := by
    apply (List.perm_ext_iff_of_nodup ((eraseDups_nodup ds).filter _) (List.nodup_range.filter _)).mpr
    intro i
    constructor
    · intro hi
      rw [List.mem_filter] at hi
      rw [List.mem_filter, List.mem_range]
      simp only [decide_eq_true_eq, Bool.and_eq_true]
      have hids : i ∈ ds := List.mem_eraseDups.mp hi.1
      have hlt : i < ys.length := by
        have := hi.2
        simp only [decide_eq_true_eq, Bool.and_eq_true] at this
        exact this.1
      have hp' : p (ys.getD i 0) = true := by
        have := hi.2
        simp only [decide_eq_true_eq, Bool.and_eq_true] at this
        exact this.2
      exact ⟨hlt, List.mem_eraseDups.mpr hids, hp'⟩
    · intro hi
      rw [List.mem_filter] at hi
      rw [List.mem_range] at hi
      simp only [decide_eq_true_eq, Bool.and_eq_true] at hi
      rw [List.mem_filter]
      simp only [decide_eq_true_eq, Bool.and_eq_true]
      exact ⟨hi.2.1, hi.1, hi.2.2⟩
  have hright :
      ((List.range ys.length).filter (fun i => decide (i ∈ ds.eraseDups) && p (ys.getD i 0))).length =
        ((List.range ys.length).filter (fun j => ds.contains j && p (ys.getD j 0))).length := by
    congr 1
    apply List.filter_congr
    intro j hj
    simp [List.mem_eraseDups]
  have hz := zipIdx_filter_length_aux ys ds p 0
  simp only [Nat.zero_add] at hz
  have hgoal :
      (ds.eraseDups.filter (fun i => decide (i < ys.length) && p (ys.getD i 0))).length =
        ((List.range ys.length).filter (fun j => ds.contains j && p (ys.getD j 0))).length := by
    rw [← hright]
    exact hperm.length_eq
  rw [hz]
  exact hgoal

private theorem masked_from_fan {u t x : Nat} {ys ds : List Nat} (p : Nat → Bool)
    (hfan : fan search u t x ys = ys.countP p) :
    fan search u t x ys - ds.eraseDups.countP (fun i => ys[i]?.any p) =
      ((ys.zipIdx.filter (fun q => !ds.contains q.2)).map Prod.fst).countP p := by
  rw [hfan]
  have hz : ys.countP p = ys.zipIdx.countP (fun q => p q.1) := by
    simpa using countP_zipIdx_aux ys p 0
  have hpart := countP_partition ys.zipIdx p ds
  have hdel : ds.eraseDups.countP (fun i => ys[i]?.any p) =
      (ys.zipIdx.filter (fun q => ds.contains q.2)).countP (fun q => p q.1) := by
    rw [deleted_count_eq]
    rw [List.countP_eq_length_filter]
    have hff :
        (ys.zipIdx.filter (fun q => ds.contains q.2)).filter (fun q => p q.1) =
          ys.zipIdx.filter (fun q => ds.contains q.2 && p q.1) := by
      rw [List.filter_filter]
      apply List.filter_congr
      intro q hq
      by_cases hp : p q.1 <;> simp [hp]
    rw [← hff]
  have hsurv :
      ((ys.zipIdx.filter (fun q => !ds.contains q.2)).map Prod.fst).countP p =
        (ys.zipIdx.filter (fun q => !ds.contains q.2)).countP (fun q => p q.1) := by
    rw [List.countP_map]
    rfl
  rw [hz, hdel, hsurv, hpart]
  omega

theorem sound_MaskedJoin :
    TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Apart.MaskedJoin.Goal := by
  intro u t x ys ds hv
  exact masked_from_fan (search.test t x) (fan_apart hv)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Apart_MaskedJoin
