import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `Apart.OffsetCount` — Two lower-bound offsets per interval compute the exact join
  multiplicity.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Apart_OffsetCount

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Apart

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

/-! ## The fan-out of `Apart.search` as two sorted slices -/

private theorem opt_map_sum (a b : Nat) {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) :
    ((opt a b).map (fun p : Nat × Nat => lower ys p.2 - lower ys p.1)).sum =
      lower ys b - lower ys a := by
  by_cases h : a < b
  · rw [opt_some h]
    simp
  · rw [opt_none h]
    have hba : b ≤ a := by omega
    rw [lower_sub_eq_zero_of_ge hs hba]
    simp

theorem sound_OffsetCount : Apart.OffsetCount.Goal := by
  intro u t x ys hv
  have hs := valid_pairwise hv
  let pt : Nat → Bool := fun y => x + t < y || y + t < x
  unfold fan
  dsimp only [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Apart.search]
  change (((opt 0 (min u (x - t)) ++ opt (x + t + 1) u).map
      (fun p : Nat × Nat => lower ys p.2 - lower ys p.1)).sum = ys.countP pt)
  rw [List.map_append, List.sum_append]
  rw [opt_map_sum 0 (min u (x - t)) hs, opt_map_sum (x + t + 1) u hs]
  have h1 : lower ys (min u (x - t)) - lower ys 0 =
      (ys.filter (fun y => decide (y < min u (x - t)))).length := by
    simpa using (lower_sub_filter_all hs 0 (min u (x - t)))
  have h2 : lower ys u - lower ys (x + t + 1) =
      (ys.filter (fun y => decide (x + t + 1 ≤ y && y < u))).length := by
    simpa using (lower_sub_filter_all hs (x + t + 1) u)
  rw [h1, h2]
  let p1 : Nat → Bool := fun y => decide (y < min u (x - t))
  let p2 : Nat → Bool := fun y => decide (x + t + 1 ≤ y && y < u)
  change (ys.filter p1).length + (ys.filter p2).length = ys.countP pt
  rw [← List.countP_eq_length_filter (l := ys) (p := p1),
      ← List.countP_eq_length_filter (l := ys) (p := p2)]
  have hdisj : ∀ y ∈ ys, p1 y = true → p2 y = false := by
    intro y _ hp
    have hylt : y < x - t := by
      have hymin : y < min u (x - t) := by simpa [p1] using hp
      exact Nat.lt_of_lt_of_le hymin (Nat.min_le_right u (x - t))
    cases hp2v : p2 y with
    | false => rfl
    | true =>
        have hle : x + t + 1 ≤ y := by
          by_cases hle : x + t + 1 ≤ y
          · exact hle
          · simp [p2, hle] at hp2v
        omega
  have hcount := countP_or_disjoint ys p1 p2 hdisj
  have hcoal : ∀ y ∈ ys, (p1 y || p2 y) = pt y := by
    intro y hy
    have hyu : y < u := valid_lt hv hy
    have hp1 : p1 y = true ↔ y + t < x := by
      constructor
      · intro h
        have hymin : y < min u (x - t) := by simpa [p1] using h
        have hx : y < x - t := Nat.lt_of_lt_of_le hymin (Nat.min_le_right u (x - t))
        omega
      · intro h
        have hylt : y < x - t := by omega
        have hymin : y < min u (x - t) := Nat.lt_min.mpr ⟨hyu, hylt⟩
        simp [p1, hymin]
    have hp2 : p2 y = true ↔ x + t < y := by
      constructor
      · intro h
        have hle : x + t + 1 ≤ y := by
          by_cases hle : x + t + 1 ≤ y
          · exact hle
          · simp [p2, hle] at h
        omega
      · intro h
        have hle : x + t + 1 ≤ y := by omega
        simp [p2, hle, hyu]
    have hor : (p1 y || p2 y) = true ↔ pt y = true := by
      rw [Bool.or_eq_true, hp1, hp2]
      simp only [pt, Bool.or_eq_true, decide_eq_true_eq]
      constructor
      · intro h
        rcases h with h | h <;> simp [h]
      · intro h
        rcases h with h | h <;> simp [h]
    cases hL : p1 y || p2 y <;> cases hR : pt y <;> simp_all
  have hmain : ys.countP pt = ys.countP (fun y => p1 y || p2 y) := by
    apply List.countP_congr
    intro y hy
    rw [hcoal y hy]
  rw [hmain, hcount]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Apart_OffsetCount
