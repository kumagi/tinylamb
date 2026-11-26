import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `Following.SlidingOffsets` — `∀ u t x ys, valid u ys → fan search u t (x+1) ys +
  ys.count (x+1) = fan search u t x ys + ys.count (x+t+1)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Following_SlidingOffsets

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Following

/-! ## The binary lower bound on a sorted column -/

private theorem getD_eq_getElem {xs : List Nat} {i : Nat} (hi : i < xs.length) :
    xs.getD i 0 = xs[i] := by
  simp [List.getD, List.getElem?_eq_getElem hi]

private theorem getD_toArray (xs : List Nat) (i : Nat) :
    xs.toArray[i]?.getD 0 = xs.getD i 0 := by
  by_cases hi : i < xs.length
  · have ha : i < xs.toArray.size := by simpa using hi
    rw [Array.getElem?_eq_getElem ha, Option.getD_some, List.getElem_toArray ha]
    simp [List.getD, List.getElem?_eq_getElem hi]
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
      simp [searchBound]
      exact ⟨hbelow, habove⟩
  | succ fuel ih =>
      by_cases hge : lo ≥ hi
      · have heq : lo = hi := by omega
        subst hi
        simp [searchBound, hge]
        exact ⟨hbelow, habove⟩
      · have hlt : lo < hi := by omega
        have hf' : hi - lo ≤ 2 ^ fuel * 2 := by
          simpa [Nat.pow_succ] using (Nat.le_of_lt hf)
        have hhalf : (hi - lo) / 2 ≤ 2 ^ fuel := by omega
        let mid := (lo + hi) / 2
        have hml : lo ≤ mid := by omega
        have hmh : mid < hi := by omega
        have hmid : mid < xs.size := by omega
        have hmidlen : mid < xs.toList.length := by simpa using hmid
        have hval : xs[mid]?.getD 0 = xs[mid] := by
          rw [Array.getElem?_eq_getElem hmid, Option.getD_some]
        have hlistval : xs[mid] = xs.toList.getD mid 0 := by
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
              rw [← hlistval]
              exact hv
            · have hjm : j < mid := by omega
              have hjlen : j < xs.toList.length := by
                simpa using (by omega : j < xs.size)
              by_cases hjl : j < lo
              · exact hbelow j hjl
              · have hrel : xs.toList.getD j 0 ≤ xs.toList.getD mid 0 := by
                  rw [getD_eq_getElem hjlen, getD_eq_getElem hmidlen]
                  exact hs.rel_getElem_of_lt hjlen hmidlen hjm
                rw [← hlistval] at hrel
                omega
          have hres := ih (mid + 1) hi (by omega) (by omega) (by omega) hbelow' habove
          obtain ⟨hc, hlb, hub⟩ := hres
          exact ⟨hc, by omega, hub⟩
        · rw [ite_eq_right hv]
          have habove' : ∀ j, mid ≤ j → j < xs.toList.length → v ≤ xs.toList.getD j 0 := by
            intro j hj hjlen
            by_cases heq : j = mid
            · subst j
              rw [← hlistval]
              omega
            · have hmj : mid < j := by omega
              by_cases hjh : hi ≤ j
              · exact habove j hjh hjlen
              · have hrel : xs.toList.getD mid 0 ≤ xs.toList.getD j 0 := by
                  rw [getD_eq_getElem hmidlen, getD_eq_getElem hjlen]
                  exact hs.rel_getElem_of_lt hmidlen hjlen hmj
                rw [← hlistval] at hrel
                omega
          have hres := ih lo mid hml (by omega) (by omega) hbelow habove'
          obtain ⟨hc, hlb, hub⟩ := hres
          exact ⟨hc, hlb, by omega⟩

private theorem width_fuel (n : Nat) : n < 2 ^ (width n + 1) := by
  unfold width
  split
  · rename_i h
    simp at h
    subst n
    simp
  · rename_i h
    have hne : n ≠ 0 := by simpa using h
    have hlog : n.log2 < n.log2 + 1 := Nat.lt_succ_self _
    have hlt : n < 2 ^ (n.log2 + 1) :=
      (Nat.log2_lt (n := n) (k := n.log2 + 1) hne).mp hlog
    exact Nat.lt_of_lt_of_le hlt
      (Nat.pow_le_pow_right (by decide : 2 > 0) (by omega))

private theorem lower_cut {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    Cut ys v (lower ys v) ∧ lower ys v ≤ ys.length := by
  unfold lower
  have hf : ys.length - 0 < 2 ^ (width ys.length + 1) := by
    have := width_fuel ys.length
    omega
  have h := searchBound_cut (xs := ys.toArray) v (width ys.length + 1) 0 ys.length
    (Nat.zero_le _) (by simp) hf
    (by intro j hj; omega)
    (by
      intro j hj hjlen
      have hj' : j < ys.length := by simpa using hjlen
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

private theorem lower_mono {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) {a b : Nat}
    (hab : a ≤ b) : lower ys a ≤ lower ys b := by
  apply Nat.le_of_not_gt
  intro h
  have hb := lower_lt_of_lt hs a h
  have ha := lower_le_of_ge hs b (Nat.le_refl _)
    (Nat.lt_of_lt_of_le h (lower_le_length hs a))
  omega

private theorem filter_lt_length_ge_of_all_lt (v : Nat) {ys : List Nat} {n : Nat}
    (hnlen : n ≤ ys.length) (hall : ∀ j, j < n → ys.getD j 0 < v) :
    n ≤ (ys.filter (fun x => decide (x < v))).length := by
  induction ys generalizing n with
  | nil =>
      simp at hnlen
      subst n
      simp
  | cons x xs ih =>
      cases n with
      | zero => simp
      | succ n =>
          have hnlen' : n ≤ xs.length := by simpa using hnlen
          have hx : x < v := hall 0 (by omega)
          have hall' : ∀ j, j < n → xs.getD j 0 < v := by
            intro j hj
            have h := hall (j + 1) (by omega)
            simpa [List.getD_cons_succ] using h
          have htail := ih (n := n) hnlen' hall'
          simp [hx]
          omega

private theorem getD_lt_of_lt_filter_lt_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    (v : Nat) {n : Nat} (hn : n < (ys.filter (fun x => decide (x < v))).length) :
    ys.getD n 0 < v := by
  induction ys generalizing n with
  | nil => simp at hn
  | cons x xs ih =>
      cases n with
      | zero =>
          have hx : x < v := by
            by_cases hxv : x < v
            · exact hxv
            · exfalso
              have hnil : xs.filter (fun y => decide (y < v)) = [] := by
                apply List.filter_eq_nil_iff.mpr
                intro y hy
                have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
                by_cases hyv : y < v
                · omega
                · simp [hyv]
              simp [hxv, hnil] at hn
          simpa using hx
      | succ n =>
          by_cases hxv : x < v
          · simp [hxv] at hn
            simp only [List.getD_cons_succ]
            exact ih (n := n) hs.tail hn
          · have hnil : xs.filter (fun y => decide (y < v)) = [] := by
              apply List.filter_eq_nil_iff.mpr
              intro y hy
              have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
              by_cases hyv : y < v
              · omega
              · simp [hyv]
            simp [hxv, hnil] at hn

private theorem lower_eq_filter_lt_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v = (ys.filter (fun x => decide (x < v))).length := by
  apply Nat.le_antisymm
  · exact filter_lt_length_ge_of_all_lt v (lower_le_length hs v)
      (fun j hj => lower_lt_of_lt hs v hj)
  · apply Nat.le_of_not_gt
    intro h
    have hlen : lower ys v < ys.length :=
      Nat.lt_of_lt_of_le h (List.length_filter_le _ _)
    have hlt := getD_lt_of_lt_filter_lt_length hs v h
    have hge := lower_le_of_ge hs v (Nat.le_refl _) hlen
    omega

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
            have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
            by_cases hay : a ≤ y
            · by_cases hyb : y < b
              · omega
              · simp [hay, hyb]
            · simp [hay]
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

/-- The two lower-bound offsets delimit exactly the sorted slice `[a,b)`. -/
private theorem lower_sub_eq_filter {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b : Nat} (hab : a ≤ b) :
    lower ys b - lower ys a = (ys.filter (fun y => decide (a ≤ y && y < b))).length := by
  rw [lower_eq_filter_lt_length hs b, lower_eq_filter_lt_length hs a]
  rw [filter_lt_prefix_filter_lt hs hab, List.length_append]
  omega

private theorem lower_sub_eq_zero_of_ge {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b : Nat} (hba : b ≤ a) :
    lower ys b - lower ys a = 0 := by
  exact Nat.sub_eq_zero_of_le (lower_mono hs hba)

private theorem filter_interval_nil_of_ge (ys : List Nat) {a b : Nat} (hba : b ≤ a) :
    ys.filter (fun y => decide (a ≤ y && y < b)) = [] := by
  apply List.filter_eq_nil_iff.mpr
  intro y hy
  simp only [decide_eq_true_eq, Bool.and_eq_true]
  omega

/-! ## Fan-out of a following search is a filtered length -/

private abbrev opt := TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional

private theorem opt_eq (a b : Nat) : opt a b = if a < b then [(a, b)] else [] := rfl

private theorem filter_interval_eq (ys : List Nat) (a b : Nat) :
    ys.filter (fun y => decide (a ≤ y && y < b)) =
      ys.filter (fun y => decide (a ≤ y) && decide (y < b)) := by
  apply List.filter_congr
  intro y hy
  by_cases h1 : a ≤ y <;> by_cases h2 : y < b <;> simp [h1, h2]

private theorem fan_search_eq_filter (u t x : Nat) {ys : List Nat}
    (hs : ys.Pairwise (· ≤ ·)) :
    fan search u t x ys =
      (ys.filter (fun y => decide (x + 1 ≤ y) && decide (y < min u (x + t + 1)))).length := by
  unfold fan search
  change ((opt (x + 1) (min u (x + t + 1))).map
    (fun p => lower ys p.2 - lower ys p.1)).sum = _
  by_cases h : x + 1 < min u (x + t + 1)
  · have hz := lower_sub_eq_filter (ys := ys) hs
      (a := x + 1) (b := min u (x + t + 1)) (by omega)
    rw [show opt (x + 1) (min u (x + t + 1)) =
        [(x + 1, min u (x + t + 1))] by rw [opt_eq]; simp [h]]
    simp only [List.map_cons, List.sum_cons]
    rw [hz, filter_interval_eq]
    simp
  · have hba : min u (x + t + 1) ≤ x + 1 := by omega
    have hf := filter_interval_nil_of_ge ys hba
    rw [show opt (x + 1) (min u (x + t + 1)) = [] by rw [opt_eq]; simp [h]]
    simp only [List.map_nil, List.sum_nil]
    rw [← filter_interval_eq ys (x + 1) (min u (x + t + 1)), hf]
    simp

private theorem fan_search_eq_countP (u t x : Nat) {ys : List Nat} (hv : valid u ys) :
    fan search u t x ys = ys.countP (search.test t x) := by
  rw [fan_search_eq_filter u t x hv.1, List.countP_eq_length_filter]
  apply congrArg List.length
  apply List.filter_congr
  intro y hy
  have hyu : y < u := hv.2 y hy
  by_cases h1 : x + 1 ≤ y <;> by_cases h2 : y < min u (x + t + 1) <;>
    by_cases h3 : x < y <;> by_cases h4 : y ≤ x + t <;>
      simp [search, h1, h2, h3, h4] <;> omega

/-! ## Counting predicate unions -/

private theorem countP_or_disjoint (ys : List Nat) (p q : Nat → Bool)
    (hd : ∀ y ∈ ys, p y = true → q y = false) :
    ys.countP (fun y => p y || q y) = ys.countP p + ys.countP q := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
      have hd' : ∀ z ∈ ys, p z = true → q z = false :=
        fun z hz => hd z (by simp [hz])
      by_cases hp : p y = true
      · have hq := hd y (by simp) hp
        simp [List.countP_cons, hp, hq, ih hd']
        omega
      · by_cases hq : q y = true
        · simp [List.countP_cons, hp, hq, ih hd']
          omega
        · simp [List.countP_cons, hp, hq, ih hd']

/-! ## The sliding-offset identity -/

private theorem count_eq_countP_beq (ys : List Nat) (a : Nat) :
    ys.count a = ys.countP (fun y => decide (y == a)) := by
  rw [List.count_eq_length_filter, List.countP_eq_length_filter]
  apply congrArg List.length
  apply List.filter_congr
  intro y hy
  by_cases h : y == a <;> simp [h]

theorem sound_SlidingOffsets : SlidingOffsets.Goal := by
  intro u t x ys hv
  rw [fan_search_eq_countP u t (x + 1) hv, fan_search_eq_countP u t x hv]
  change ys.countP (fun y => search.test t (x + 1) y) + ys.count (x + 1) =
    ys.countP (fun y => search.test t x y) + ys.count (x + t + 1)
  have hdleft : ∀ y ∈ ys,
      search.test t (x + 1) y = true → decide (y == x + 1) = false := by
    intro y hy h
    simp [search] at h
    simp [beq_iff_eq]
    omega
  have hsplitleft : ∀ y,
      (search.test t (x + 1) y || decide (y == x + 1)) =
        (decide (x < y) && decide (y ≤ x + 1 + t)) := by
    intro y
    by_cases h1 : y = x + 1 <;> by_cases h2 : x + 1 < y <;>
      by_cases h3 : y ≤ x + 1 + t <;> by_cases h4 : x < y <;>
        simp [search, h1, h2, h3, h4, beq_iff_eq] <;> omega
  have hleft : ys.countP (fun y => search.test t (x + 1) y) + ys.count (x + 1) =
      ys.countP (fun y => decide (x < y) && decide (y ≤ x + 1 + t)) := by
    rw [count_eq_countP_beq ys (x + 1)]
    rw [show ys.countP (fun y => search.test t (x + 1) y) +
        ys.countP (fun y => decide (y == x + 1)) =
        ys.countP (fun y => search.test t (x + 1) y || decide (y == x + 1)) from
      (countP_or_disjoint ys (fun y => search.test t (x + 1) y)
        (fun y => decide (y == x + 1)) hdleft).symm]
    apply List.countP_congr
    intro y hy
    rw [hsplitleft y]
  have hdright : ∀ y ∈ ys,
      search.test t x y = true → decide (y == x + t + 1) = false := by
    intro y hy h
    simp [search] at h
    simp [beq_iff_eq]
    omega
  have hsplitright : ∀ y,
      (search.test t x y || decide (y == x + t + 1)) =
        (decide (x < y) && decide (y ≤ x + 1 + t)) := by
    intro y
    by_cases h1 : y = x + t + 1 <;> by_cases h2 : y ≤ x + t <;>
      by_cases h3 : x < y <;> by_cases h4 : y ≤ x + 1 + t <;>
        simp [search, h1, h2, h3, h4, beq_iff_eq] <;> omega
  have hright : ys.countP (fun y => search.test t x y) + ys.count (x + t + 1) =
      ys.countP (fun y => decide (x < y) && decide (y ≤ x + 1 + t)) := by
    rw [count_eq_countP_beq ys (x + t + 1)]
    rw [show ys.countP (fun y => search.test t x y) +
        ys.countP (fun y => decide (y == x + t + 1)) =
        ys.countP (fun y => search.test t x y || decide (y == x + t + 1)) from
      (countP_or_disjoint ys (fun y => search.test t x y)
        (fun y => decide (y == x + t + 1)) hdright).symm]
    apply List.countP_congr
    intro y hy
    rw [hsplitright y]
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Following_SlidingOffsets
