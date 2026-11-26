import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_AdjacentBucket_OffsetCount

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.AdjacentBucket

private abbrev opt := TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional

private theorem opt_eq (a b : Nat) :
    opt a b = if a < b then [(a, b)] else [] := rfl

private theorem opt_some {a b : Nat} (h : a < b) : opt a b = [(a, b)] := by
  simp [opt_eq, h]

private theorem opt_none {a b : Nat} (h : ¬ a < b) : opt a b = [] := by
  simp [opt_eq, h]

private theorem valid_pairwise {u : Nat} {ys : List Nat} (hv : valid u ys) :
    ys.Pairwise (· ≤ ·) := hv.1

private theorem valid_lt {u : Nat} {ys : List Nat} (hv : valid u ys) {y : Nat}
    (hy : y ∈ ys) : y < u := hv.2 y hy

/-! ## The binary lower bound on a sorted column -/

private def Cut (xs : List Nat) (v i : Nat) : Prop :=
  (∀ j, j < i → xs.getD j 0 < v) ∧
  (∀ j, i ≤ j → j < xs.length → v ≤ xs.getD j 0)

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

private theorem searchBound_cut {xs : Array Nat} (v fuel lo hi : Nat)
    (hlo : lo ≤ hi) (hhi : hi ≤ xs.size) (hf : hi - lo < 2 ^ fuel)
    (hbelow : ∀ j, j < lo → xs.toList.getD j 0 < v)
    (habove : ∀ j, hi ≤ j → j < xs.toList.length → v ≤ xs.toList.getD j 0)
    (hs : xs.toList.Pairwise (· ≤ ·)) :
    Cut xs.toList v (searchBound xs v fuel lo hi) ∧
      lo ≤ searchBound xs v fuel lo hi ∧ searchBound xs v fuel lo hi ≤ hi := by
  induction fuel generalizing lo hi with
  | zero =>
    have h2 : (2 : Nat) ^ 0 = 1 := by simp
    have heq : lo = hi := by omega
    subst hi
    exact ⟨⟨hbelow, habove⟩, ⟨by simp [searchBound], by simp [searchBound]⟩⟩
  | succ fuel ih =>
    by_cases hge : lo ≥ hi
    · have heq : lo = hi := by omega
      subst hi
      simp only [searchBound, hge, ↓reduceIte]
      exact ⟨⟨hbelow, habove⟩, ⟨by simp [searchBound], by simp [searchBound]⟩⟩
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
                have hpair := hs.rel_getElem_of_lt hjlen hmidlen hjm
                rw [← getD_eq_getElem hjlen, ← getD_eq_getElem hmidlen] at hpair
                exact hpair
              rw [hlistval] at hrel
              omega
        have hf' : hi - (mid + 1) < 2 ^ fuel := by
          rw [hpow] at hf
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
            by_cases hjh : hi ≤ j
            · exact habove j hjh hjlen
            · have hrel : xs.toList.getD mid 0 ≤ xs.toList.getD j 0 := by
                have hpair := hs.rel_getElem_of_lt hmidlen hjlen hmj
                rw [← getD_eq_getElem hmidlen, ← getD_eq_getElem hjlen] at hpair
                exact hpair
              rw [hlistval] at hrel
              omega
        have hf' : mid - lo < 2 ^ fuel := by
          rw [hpow] at hf
          omega
        have hres := ih lo mid hml (by omega) hf' hbelow habove'
        obtain ⟨hc, hlb, hub⟩ := hres
        exact ⟨hc, hlb, by omega⟩

private theorem width_pow_bound (n : Nat) : n < 2 ^ (width n + 1) := by
  unfold width
  by_cases h : n == 0
  · have hn : n = 0 := by simpa using h
    subst hn
    simp
  · have hne : n ≠ 0 := by intro hz; simp [hz] at h
    simp [h]
    have hlt : n < 2 ^ (n.log2 + 1) :=
      (Nat.log2_lt hne).mp (Nat.lt_succ_self _)
    have hpow : (2 : Nat) ^ (n.log2 + 1 + 1) = 2 * 2 ^ (n.log2 + 1) := by
      rw [Nat.pow_succ]
      omega
    rw [hpow]
    omega

private theorem lower_cut {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    Cut ys v (lower ys v) ∧ lower ys v ≤ ys.length := by
  unfold lower
  have hf : ys.length - 0 < 2 ^ (width ys.length + 1) := by
    have := width_pow_bound ys.length
    omega
  have h := searchBound_cut (xs := ys.toArray) v (width ys.length + 1) 0 ys.length
    (Nat.zero_le _) (by simp) hf
    (by intro j hj; omega)
    (by intro j hj hjlen
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

private theorem lower_mono {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) {a b : Nat}
    (hab : a ≤ b) : lower ys a ≤ lower ys b := by
  apply Nat.le_of_not_gt
  intro h
  have hb := lower_lt_of_lt hs a h
  have ha := lower_le_of_ge hs b (Nat.le_refl _)
    (Nat.lt_of_lt_of_le h (lower_le_length hs a))
  omega

/-! ## Equality between binary lower bounds and filtered lengths -/

private theorem lower_eq_filter_lt_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v = (ys.filter (fun x => decide (x < v))).length := by
  have hle : lower ys v ≤ ys.length := lower_le_length hs v
  have htake : (ys.take (lower ys v)).filter (fun x => decide (x < v)) =
      ys.take (lower ys v) := by
    rw [List.filter_eq_self]
    intro x hx
    rw [List.mem_take_iff_getElem] at hx
    rcases hx with ⟨i, hi, rfl⟩
    simp only [decide_eq_true_eq, List.getElem_take]
    have hiys : i < ys.length := by omega
    have hlt : ys.getD i 0 < v :=
      lower_lt_of_lt hs v (Nat.lt_of_lt_of_le hi (Nat.min_le_left _ _))
    rwa [getD_eq_getElem hiys] at hlt
  have hdrop : (ys.drop (lower ys v)).filter (fun x => decide (x < v)) = [] := by
    rw [List.filter_eq_nil_iff]
    intro x hx
    rw [List.mem_iff_getElem] at hx
    rcases hx with ⟨i, hi, rfl⟩
    simp only [decide_eq_true_eq, List.getElem_drop]
    have hiys : lower ys v + i < ys.length := by
      have hlen : i < (ys.drop (lower ys v)).length := hi
      rw [List.length_drop] at hlen
      omega
    intro hlt
    have hge := lower_le_of_ge hs v (Nat.le_add_right _ _) hiys
    rw [getD_eq_getElem hiys] at hge
    omega
  have hsplit := (List.take_append_drop (lower ys v) ys).symm
  calc
    lower ys v = (ys.take (lower ys v)).length := by
      rw [List.length_take]
      simp [Nat.min_eq_left hle]
    _ = ((ys.take (lower ys v)).filter (fun x => decide (x < v))).length := by rw [htake]
    _ = ((ys.take (lower ys v)).filter (fun x => decide (x < v)) ++
          (ys.drop (lower ys v)).filter (fun x => decide (x < v))).length := by
        rw [hdrop, List.append_nil]
    _ = ((ys.take (lower ys v) ++ ys.drop (lower ys v)).filter
          (fun x => decide (x < v))).length := by rw [List.filter_append]
    _ = (ys.filter (fun x => decide (x < v))).length := by rw [← hsplit]

private theorem filter_lt_split {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b : Nat} (hab : a ≤ b) :
    (ys.filter (fun y => decide (y < b))).length =
      (ys.filter (fun y => decide (y < a))).length +
      (ys.filter (fun y => decide (a ≤ y && y < b))).length := by
  induction ys with
  | nil => simp
  | cons x xs ih =>
      have hs' := hs.tail
      have ih' := ih hs'
      by_cases hxa : x < a
      · have hxb : x < b := by omega
        simp [hxa, hxb, ih'] <;> omega
      · by_cases hxb : x < b
        · have hax : a ≤ x := by omega
          have hnil : xs.filter (fun y => decide (y < a)) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro y hy
            simp only [decide_eq_true_eq]
            have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
            omega
          simp [hxa, hxb, hax, hnil, ih'] <;> omega
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
            have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
            omega
          simp [hxa, hxb, hnil_a, hnil_ab, ih'] <;> omega

private theorem lower_sub_eq_filter {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b : Nat} (hab : a ≤ b) :
    lower ys b - lower ys a = (ys.filter (fun y => decide (a ≤ y && y < b))).length := by
  rw [lower_eq_filter_lt_length hs b, lower_eq_filter_lt_length hs a]
  rw [filter_lt_split hs hab]
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

private theorem lower_sub_filter_all {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    (lo hi : Nat) :
    lower ys hi - lower ys lo =
      (ys.filter (fun y => decide (lo ≤ y && y < hi))).length := by
  by_cases h : lo ≤ hi
  · exact lower_sub_eq_filter hs h
  · have hge : hi ≤ lo := by omega
    rw [lower_sub_eq_zero_of_ge hs hge]
    rw [show ys.filter (fun y => decide (lo ≤ y && y < hi)) = [] from
      filter_interval_nil_of_ge ys hge]
    simp

/-! ## Counting a disjoint union of predicates -/

private theorem countP_or_disjoint (ys : List Nat) (p q : Nat → Bool)
    (hd : ∀ y ∈ ys, p y = true → q y = false) :
    ys.countP (fun y => p y || q y) = ys.countP p + ys.countP q := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
      have hd' : ∀ z ∈ ys, p z = true → q z = false :=
        fun z hz => hd z (by simp [hz])
      have hih := ih hd'
      by_cases hp : p y = true
      · have hq : q y = false := hd y (by simp) hp
        simp [List.countP_cons, hp, hq, hih, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
      · by_cases hq : q y = true
        · simp [List.countP_cons, hp, hq, hih, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
        · simp [List.countP_cons, hp, hq, hih]

/-! ## Elementary bucket arithmetic -/

private theorem mul_eq_prev_add {k w : Nat} (hk : 1 ≤ k) : k * w = (k - 1) * w + w := by
  have hk' : k = (k - 1) + 1 := by omega
  calc
    k * w = ((k - 1) + 1) * w := congrArg (fun t : Nat => t * w) hk'
    _ = (k - 1) * w + 1 * w := by rw [Nat.add_mul]
    _ = (k - 1) * w + w := by rw [Nat.one_mul]

private theorem mul_sub_w {k w : Nat} (hk : 1 ≤ k) : k * w - w = (k - 1) * w := by
  rw [mul_eq_prev_add hk]
  omega

private theorem mul_add_one {k w : Nat} : k * w + w = (k + 1) * w := by
  rw [Nat.add_mul, Nat.one_mul]

private theorem mul_add_two {k w : Nat} : k * w + 2 * w = ((k + 1) + 1) * w := by
  rw [Nat.add_mul, Nat.one_mul, Nat.add_mul, Nat.one_mul]
  omega

/-! ## The fan-out of `AdjacentBucket.search` as two sorted slices -/

private theorem fan_adjacent_eq_filters (u t x : Nat) {ys : List Nat}
    (hs : ys.Pairwise (· ≤ ·)) :
    fan search u t x ys =
      (ys.filter (fun y => decide (x / (t + 1) * (t + 1) - (t + 1) ≤ y &&
                                  y < min u (x / (t + 1) * (t + 1))))).length +
      (ys.filter (fun y => decide (x / (t + 1) * (t + 1) + (t + 1) ≤ y &&
                                  y < min u (x / (t + 1) * (t + 1) + 2 * (t + 1))))).length := by
  unfold fan search
  dsimp only
  let w := t + 1
  let a := x / w * w
  change ((opt (a - w) (min u a) ++ opt (a + w) (min u (a + 2 * w))).map
      (fun p : Nat × Nat => lower ys p.2 - lower ys p.1)).sum =
    (ys.filter (fun y => decide (a - w ≤ y && y < min u a))).length +
    (ys.filter (fun y => decide (a + w ≤ y && y < min u (a + 2 * w)))).length
  have hz1 : lower ys (min u a) - lower ys (a - w) =
      (ys.filter (fun y => decide (a - w ≤ y && y < min u a))).length :=
    lower_sub_filter_all hs (a - w) (min u a)
  have hz2 : lower ys (min u (a + 2 * w)) - lower ys (a + w) =
      (ys.filter (fun y => decide (a + w ≤ y && y < min u (a + 2 * w)))).length :=
    lower_sub_filter_all hs (a + w) (min u (a + 2 * w))
  by_cases h1 : a - w < min u a
  · by_cases h2 : a + w < min u (a + 2 * w)
    · rw [opt_some h1, opt_some h2]
      simp only [List.map_append, List.sum_append, List.map_cons, List.map_nil,
        List.sum_cons, List.sum_nil, Nat.add_zero, Nat.zero_add]
      rw [hz1, hz2]
    · rw [opt_some h1, opt_none h2]
      simp only [List.map_append, List.sum_append, List.map_cons, List.map_nil,
        List.sum_cons, List.sum_nil, Nat.add_zero, Nat.zero_add, List.append_nil]
      rw [hz1]
      have hz2zero : lower ys (min u (a + 2 * w)) - lower ys (a + w) = 0 :=
        Nat.sub_eq_zero_of_le (lower_mono hs (by omega))
      have hf2zero : (ys.filter (fun y => decide (a + w ≤ y && y < min u (a + 2 * w)))).length = 0 := by
        rw [← hz2]
        exact hz2zero
      rw [hf2zero]
      omega
  · by_cases h2 : a + w < min u (a + 2 * w)
    · rw [opt_none h1, opt_some h2]
      simp only [List.map_append, List.sum_append, List.map_cons, List.map_nil,
        List.sum_cons, List.sum_nil, Nat.add_zero, Nat.zero_add]
      rw [hz2]
      have hz1zero : lower ys (min u a) - lower ys (a - w) = 0 :=
        Nat.sub_eq_zero_of_le (lower_mono hs (by omega))
      have hf1zero : (ys.filter (fun y => decide (a - w ≤ y && y < min u a))).length = 0 := by
        rw [← hz1]
        exact hz1zero
      rw [hf1zero]
      omega
    · rw [opt_none h1, opt_none h2]
      simp only [List.map_append, List.sum_append, List.map_cons, List.map_nil,
        List.sum_cons, List.sum_nil, Nat.add_zero, Nat.zero_add, List.append_nil]
      have hz1zero : lower ys (min u a) - lower ys (a - w) = 0 :=
        Nat.sub_eq_zero_of_le (lower_mono hs (by omega))
      have hz2zero : lower ys (min u (a + 2 * w)) - lower ys (a + w) = 0 :=
        Nat.sub_eq_zero_of_le (lower_mono hs (by omega))
      have hf1zero : (ys.filter (fun y => decide (a - w ≤ y && y < min u a))).length = 0 := by
        rw [← hz1]
        exact hz1zero
      have hf2zero : (ys.filter (fun y => decide (a + w ≤ y && y < min u (a + 2 * w)))).length = 0 := by
        rw [← hz2]
        exact hz2zero
      rw [hf1zero, hf2zero]
      simp

/-! ## The two slices are exactly the neighboring buckets -/

theorem sound_OffsetCount : AdjacentBucket.OffsetCount.Goal := by
  intro u t x ys hv
  have hs := valid_pairwise hv
  rw [fan_adjacent_eq_filters u t x hs]
  let w := t + 1
  let a := x / w * w
  change (ys.filter (fun y => decide (a - w ≤ y && y < min u a))).length +
      (ys.filter (fun y => decide (a + w ≤ y && y < min u (a + 2 * w)))).length =
    ys.countP (search.test t x)
  let p1 : Nat → Bool := fun y => decide (a - w ≤ y && y < min u a)
  let p2 : Nat → Bool := fun y => decide (a + w ≤ y && y < min u (a + 2 * w))
  change (ys.filter p1).length + (ys.filter p2).length =
    ys.countP (search.test t x)
  rw [← List.countP_eq_length_filter (l := ys) (p := p1),
      ← List.countP_eq_length_filter (l := ys) (p := p2)]
  have hdisj : ∀ y ∈ ys, p1 y = true → p2 y = false := by
    intro y _ hp
    have hp' : a - w ≤ y ∧ y < min u a := by simpa [p1] using hp
    cases hp2v : p2 y
    · rfl
    · have hp2' : a + w ≤ y ∧ y < min u (a + 2 * w) := by simpa [p2] using hp2v
      omega
  have hcount := countP_or_disjoint ys p1 p2 hdisj
  have hcoal : ∀ y ∈ ys, (p1 y || p2 y) = search.test t x y := by
    intro y hy
    have hyu : y < u := valid_lt hv hy
    have hw : 0 < w := by omega
    have hp1_iff : p1 y = true ↔ y / w + 1 = x / w := by
      constructor
      · intro hp1
        have hp : a - w ≤ y ∧ y < min u a := by simpa [p1] using hp1
        have hkpos : 1 ≤ x / w := by
          by_cases hk0 : x / w = 0
          · have ha0 : a = 0 := by simp [a, hk0]
            have hy0 : y < 0 := by simpa [ha0] using hp.2
            omega
          · rcases Nat.eq_zero_or_pos (x / w) with hz | hpos
            · exact False.elim (hk0 hz)
            · exact hpos
        have hmul : a = (x / w - 1) * w + w := by
          dsimp [a]
          exact mul_eq_prev_add hkpos
        have hamw : a - w = (x / w - 1) * w := by
          rw [hmul]
          omega
        have hlo : (x / w - 1) * w ≤ y := by simpa [hamw] using hp.1
        have hhi : y < ((x / w - 1) + 1) * w := by
          have hya : y < a := by
            have hmin := Nat.min_le_right u a
            omega
          rw [hmul] at hya
          simpa [Nat.add_mul, Nat.one_mul] using hya
        have hdiv : y / w = x / w - 1 := Nat.div_eq_of_lt_le hlo hhi
        rw [hdiv]
        exact Nat.sub_add_cancel hkpos
      · intro hdiv_eq
        have hkpos : 1 ≤ x / w := by
          calc
            1 ≤ y / w + 1 := Nat.succ_le_succ (Nat.zero_le _)
            _ = x / w := hdiv_eq
        have hdiv : y / w = x / w - 1 := by
          rw [← hdiv_eq]
          simp
        have hlo : (x / w - 1) * w ≤ y := by
          have h := Nat.div_mul_le_self y w
          rw [hdiv] at h
          exact h
        have hhi : y < (x / w - 1) * w + w := by
          have h := Nat.lt_div_mul_add (a := y) hw
          rw [hdiv] at h
          exact h
        have hmul : a = (x / w - 1) * w + w := by
          dsimp [a]
          exact mul_eq_prev_add hkpos
        have hamw : a - w = (x / w - 1) * w := by
          rw [hmul]
          omega
        simp only [p1, decide_eq_true_eq, Bool.and_eq_true]
        constructor
        · simpa [hamw] using hlo
        · exact Nat.lt_min.mpr ⟨hyu, by simpa [hmul] using hhi⟩
    have hp2_iff : p2 y = true ↔ x / w + 1 = y / w := by
      constructor
      · intro hp2
        have hp : a + w ≤ y ∧ y < min u (a + 2 * w) := by simpa [p2] using hp2
        have hlow : (x / w + 1) * w ≤ y := by
          have heq : a + w = (x / w + 1) * w := by
            dsimp [a]
            exact mul_add_one
          simpa [heq] using hp.1
        have hhi : y < ((x / w + 1) + 1) * w := by
          have hya : y < a + 2 * w := by
            have hmin := Nat.min_le_right u (a + 2 * w)
            omega
          have heq : a + 2 * w = ((x / w + 1) + 1) * w := by
            dsimp [a]
            exact mul_add_two
          simpa [heq] using hya
        have hdiv : y / w = x / w + 1 := Nat.div_eq_of_lt_le hlow hhi
        rw [hdiv]
      · intro hdiv_eq
        have hdiv : y / w = x / w + 1 := hdiv_eq.symm
        have hlow : (x / w + 1) * w ≤ y := by
          have h := Nat.div_mul_le_self y w
          rw [hdiv] at h
          exact h
        have hhi : y < ((x / w + 1) + 1) * w := by
          change y < Nat.succ (x / w + 1) * w
          rw [Nat.succ_mul]
          have h := Nat.lt_div_mul_add (a := y) hw
          rw [hdiv] at h
          exact h
        have heq1 : a + w = (x / w + 1) * w := by
          dsimp [a]
          exact mul_add_one
        have heq2 : a + 2 * w = ((x / w + 1) + 1) * w := by
          dsimp [a]
          exact mul_add_two
        simp only [p2, decide_eq_true_eq, Bool.and_eq_true]
        constructor
        · simpa [heq1] using hlow
        · exact Nat.lt_min.mpr ⟨hyu, by simpa [heq2] using hhi⟩
    have hor : (p1 y || p2 y) = true ↔ search.test t x y = true := by
      rw [Bool.or_eq_true, hp1_iff, hp2_iff]
      simp only [search, beq_iff_eq, Bool.or_eq_true, w]
      constructor
      · intro h
        rcases h with h | h
        · exact Or.inr h
        · exact Or.inl h
      · intro h
        rcases h with h | h
        · exact Or.inr h
        · exact Or.inl h
    cases h1 : p1 y || p2 y <;> cases h2 : search.test t x y <;> simp_all
  have hmain : ys.countP (search.test t x) = ys.countP (fun y => p1 y || p2 y) := by
    apply List.countP_congr
    intro y hy
    rw [hcoal y hy]
  have hmain2 : ys.countP (search.test t x) = ys.countP p1 + ys.countP p2 := by
    rw [hmain, hcount]
  exact hmain2.symm

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_AdjacentBucket_OffsetCount
