import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.PeakCount.Remaining

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PeakCount

/-! A peak of a series is a position `i` with `0 < i`, `i + 1 < length`,
`x[i-1] < x[i]` and `x[i+1] < x[i]`.  The following predicates and lemmas
restate `PeakCount.measure` as a count over indices, which makes locality
arguments (concatenation, order-preserving maps, single-point edits) direct. -/

private theorem zipIdx_eq_range (x : Series) :
    x.zipIdx = (List.range x.length).map (fun i => (x[i]?.getD 0, i)) := by
  apply List.ext_getElem?
  intro i
  rw [List.getElem?_zipIdx, List.getElem?_map]
  by_cases hi : i < x.length
  · have hr : (List.range x.length)[i]? = some i := by simp [hi]
    rw [hr]
    simp [hi, List.getElem?_eq_getElem hi]
  · have hr : (List.range x.length)[i]? = none := by simp [hi]
    rw [hr]
    simp [List.getElem?_eq_none (by omega : x.length ≤ i)]

private def pk (x : Series) (i : Nat) : Bool :=
  decide (0 < i) && x[i - 1]?.any (fun a => a < x[i]?.getD 0) &&
    x[i + 1]?.any (fun b => b < x[i]?.getD 0)

private theorem measure_eq_count_pk (x : Series) :
    PeakCount.measure x = ((List.range x.length).countP (pk x) : Int) := by
  unfold PeakCount.measure
  rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
  congr 1
  rw [zipIdx_eq_range]
  rw [List.filter_map]
  simp only [List.length_map]
  congr 1

private theorem pk_eq_of_bounds (x : Series) (i : Nat) (hi0 : 0 < i)
    (hi1 : i + 1 < x.length) :
    pk x i = true ↔ x[i - 1] < x[i] ∧ x[i + 1] < x[i] := by
  have hi : i < x.length := by omega
  have him : i - 1 < x.length := by omega
  dsimp [pk]
  rw [List.getElem?_eq_getElem hi, List.getElem?_eq_getElem him,
    List.getElem?_eq_getElem hi1]
  simp [hi0]

private theorem pk_false_zero (x : Series) : pk x 0 = false := by
  simp [pk]

private theorem pk_false_last (x : Series) : pk x (x.length - 1) = false := by
  by_cases h : x.length = 0
  · simp [h, pk]
  · have hnext : x.length - 1 + 1 = x.length := by omega
    dsimp [pk]
    rw [hnext, List.getElem?_eq_none (by omega : x.length ≤ x.length)]
    simp

private theorem pk_true_bounds (x : Series) {i : Nat} (h : pk x i = true) :
    0 < i ∧ i + 1 < x.length := by
  constructor
  · by_cases hi : 0 < i
    · exact hi
    · dsimp [pk] at h
      have hf : (decide (0 < i) : Bool) = false := by simp [hi]
      rw [hf] at h
      simp at h
  · by_cases hi : i + 1 < x.length
    · exact hi
    · have hge : x.length ≤ i + 1 := by omega
      dsimp [pk] at h
      rw [List.getElem?_eq_none hge] at h
      simp at h

private theorem pk_no_adjacent (x : Series) (i : Nat) :
    ¬ (pk x i = true ∧ pk x (i + 1) = true) := by
  intro h
  have hb1 := pk_true_bounds x h.1
  have hb2 := pk_true_bounds x h.2
  have h1 := (pk_eq_of_bounds x i hb1.1 (by omega)).mp h.1
  have h2 := (pk_eq_of_bounds x (i + 1) hb2.1 hb2.2).mp h.2
  have h2left : x[i] < x[i + 1] := by simpa using h2.1
  omega

private theorem countP_congr {α : Type} (p q : α → Bool) (l : List α)
    (h : ∀ a ∈ l, p a = q a) : l.countP p = l.countP q := by
  rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
  congr 1
  apply List.filter_congr
  intro a ha
  exact h a ha

private theorem countP_range_succ (p : Nat → Bool) (n : Nat) :
    (List.range (n + 1)).countP p = (List.range n).countP p + (if p n then 1 else 0) := by
  rw [List.range_succ, List.countP_append]
  simp

private theorem measure_eq_count_prefix (x : Series) :
    PeakCount.measure x = ((List.range (x.length - 1)).countP (pk x) : Int) := by
  rw [measure_eq_count_pk]
  by_cases h : x.length = 0
  · simp [h]
  · have hs : x.length = (x.length - 1) + 1 := by omega
    rw [hs, countP_range_succ]
    have hf : pk x (x.length - 1) = false := pk_false_last x
    simp [hf]

private theorem range_add (m n : Nat) :
    List.range (m + n) = List.range m ++ (List.range n).map (fun j => m + j) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [Nat.add_succ, List.range_succ, ih]
      simp [List.range_succ, Nat.add_assoc]

private theorem measure_nil : PeakCount.measure ([] : Series) = 0 := by
  unfold PeakCount.measure
  simp

private theorem measure_singleton (a : Int) : PeakCount.measure [a] = 0 := by
  unfold PeakCount.measure
  simp

private theorem measure_two (a b : Int) : PeakCount.measure [a, b] = 0 := by
  unfold PeakCount.measure
  simp

private theorem measure_three (a b c : Int) :
    PeakCount.measure [a, b, c] = (if a < b ∧ c < b then 1 else 0) := by
  by_cases h : a < b ∧ c < b <;>
    unfold PeakCount.measure <;> simp [List.countP_cons, h] <;> omega

private theorem measure_four (a b c d : Int) :
    PeakCount.measure [a, b, c, d] =
      (if a < b ∧ c < b then 1 else 0) + (if b < c ∧ d < c then 1 else 0) := by
  by_cases h1 : a < b ∧ c < b <;> by_cases h2 : b < c ∧ d < c <;>
    unfold PeakCount.measure <;> simp [List.countP_cons, h1, h2] <;> omega

private theorem measure_of_length_le_two (z : Series) (h : z.length ≤ 2) :
    PeakCount.measure z = 0 := by
  rw [measure_eq_count_prefix]
  by_cases h0 : z.length = 0
  · simp [h0]
  · by_cases h1 : z.length = 1
    · simp [h1]
    · have h2 : z.length = 2 := by omega
      rw [h2]
      simp [pk_false_zero]

private theorem range_succ_from_zero (n : Nat) :
    List.range (n + 1) = 0 :: (List.range n).map (fun j => j + 1) := by
  rw [List.range_succ_eq_map]

private theorem measure_eq_count_pk_shift (x : Series) :
    PeakCount.measure x =
      ((List.range (x.length - 2)).countP (fun j => pk x (j + 1)) : Int) := by
  rw [measure_eq_count_pk]
  by_cases h0 : x.length = 0
  · simp [h0]
  · by_cases h1 : x.length = 1
    · simp [h1, pk_false_zero]
    · have hs : x.length = (x.length - 1) + 1 := by omega
      rw [hs, range_succ_from_zero]
      simp only [List.countP_append, List.countP_cons, List.countP_nil, List.map_map,
        Function.comp_def, Nat.zero_add]
      rw [List.countP_map]
      have hz : pk x 0 = false := pk_false_zero x
      rw [hz]
      change ((List.range (x.length - 1)).countP (fun j => pk x (j + 1)) : Int) =
        ((List.range (x.length - 2)).countP (fun j => pk x (j + 1)) : Int)
      congr 1
      have hs2 : x.length - 1 = (x.length - 2) + 1 := by omega
      rw [hs2, List.range_succ, List.countP_append]
      simp only [List.countP_cons, List.countP_nil, Nat.add_zero]
      have hlast : pk x (x.length - 2 + 1) = false := by
        have : x.length - 2 + 1 = x.length - 1 := by omega
        rw [this]
        exact pk_false_last x
      rw [hlast]
      simp

private theorem range_concat_split (m n : Nat) (hm : 0 < m) (hn : 0 < n) :
    List.range (m + n - 1) =
      List.range (m - 1) ++ ([m - 1] ++ (List.range (n - 1)).map (fun j => m + j)) := by
  have hsum : m + n - 1 = (m - 1) + n := by omega
  rw [hsum, range_add]
  congr 1
  have hn' : n = (n - 1) + 1 := by omega
  rw [hn', range_succ_from_zero]
  simp only [List.map_append, List.map_cons, List.map_nil, List.append_nil, List.map_map,
    Function.comp_def, Nat.add_zero]
  congr 1
  apply List.map_congr_left
  intro j hj
  omega

private theorem getElem?_append_of_lt (x y : Series) {i : Nat} (h : i < x.length) :
    (x ++ y)[i]? = x[i]? := by
  rw [List.getElem?_append]
  split
  · rfl
  · omega

private theorem getElem?_append_of_ge (x y : Series) {i : Nat} (h : x.length ≤ i) :
    (x ++ y)[i]? = y[i - x.length]? := by
  rw [List.getElem?_append]
  split
  · omega
  · rfl

private theorem pk_false_of_ge (x : Series) (i : Nat) (h : x.length ≤ i) :
    pk x i = false := by
  dsimp [pk]
  have h1 : x.length ≤ i + 1 := by omega
  rw [List.getElem?_eq_none h1]
  simp

private theorem pk_append_left (x y : Series) (i : Nat) (hi : i + 1 < x.length) :
    pk (x ++ y) i = pk x i := by
  have h0 : i < x.length := by omega
  have hm : i - 1 < x.length := by omega
  dsimp [pk]
  rw [getElem?_append_of_lt x y h0, getElem?_append_of_lt x y hm,
    getElem?_append_of_lt x y hi]

private theorem pk_append_right (x y : Series) (j : Nat) (hj : 0 < j) :
    pk (x ++ y) (x.length + j) = pk y j := by
  have h1 : x.length ≤ x.length + j - 1 := by omega
  have h2 : x.length ≤ x.length + j := by omega
  have h3 : x.length ≤ x.length + j + 1 := by omega
  dsimp [pk]
  rw [getElem?_append_of_ge x y h1, getElem?_append_of_ge x y h2,
    getElem?_append_of_ge x y h3]
  have e1 : x.length + j - 1 - x.length = j - 1 := by omega
  have e2 : x.length + j - x.length = j := by omega
  have e3 : x.length + j + 1 - x.length = j + 1 := by omega
  rw [e1, e2, e3]
  have hi0 : 0 < x.length + j := by omega
  simp [hj, hi0]

private theorem count_pk_shifted (x y : Series) :
    ((List.range (y.length - 1)).map (fun j => x.length + j)).countP (pk (x ++ y)) =
      (if pk (x ++ y) x.length then 1 else 0) +
        ((List.range (y.length - 2)).countP (fun j => pk y (j + 1)) : Nat) := by
  by_cases hy : y.length = 0
  · have hyl : y = [] := (List.length_eq_zero_iff).mp hy
    subst y
    have hf : pk x x.length = false := pk_false_of_ge x x.length (by omega)
    simp [hf]
  · by_cases hy1 : y.length = 1
    · have hf : pk (x ++ y) x.length = false := by
        dsimp [pk]
        have hge : (x ++ y).length ≤ x.length + 1 := by simp [hy1]
        rw [List.getElem?_eq_none hge]
        simp
      simp [hf, hy1]
    · have hs : y.length - 1 = (y.length - 2) + 1 := by omega
      rw [hs, range_succ_from_zero]
      simp only [List.map_append, List.map_cons, List.map_nil, List.append_nil,
        List.countP_append, List.countP_cons, List.countP_nil, Nat.add_zero]
      simp only [List.map_map, Function.comp_def]
      rw [List.countP_map]
      change ((List.range (y.length - 2)).countP (fun j => pk (x ++ y) (x.length + (j + 1))) +
          (if pk (x ++ y) x.length then 1 else 0)) =
        ((if pk (x ++ y) x.length then 1 else 0) +
          (List.range (y.length - 2)).countP (fun j => pk y (j + 1)))
      have hcnt : (List.range (y.length - 2)).countP (fun j => pk (x ++ y) (x.length + (j + 1))) =
          (List.range (y.length - 2)).countP (fun j => pk y (j + 1)) := by
        apply countP_congr
        intro j hj
        have h := pk_append_right x y (j + 1) (by omega)
        simpa using h
      omega

private theorem measure_append_split (x y : Series) :
    PeakCount.measure (x ++ y) = PeakCount.measure x + PeakCount.measure y +
      (if pk (x ++ y) (x.length - 1) then 1 else 0) +
      (if pk (x ++ y) x.length then 1 else 0) := by
  by_cases hx : x.length = 0
  · have hxl : x = [] := (List.length_eq_zero_iff).mp hx
    subst x
    simp [pk_false_zero, measure_nil]
  · by_cases hy : y.length = 0
    · have hyl : y = [] := (List.length_eq_zero_iff).mp hy
      subst y
      simp [pk_false_last, pk_false_of_ge, measure_nil]
    · rw [measure_eq_count_prefix (x ++ y)]
      have hlen : (x ++ y).length = x.length + y.length := by simp
      rw [hlen, range_concat_split x.length y.length (by omega) (by omega)]
      rw [List.countP_append, List.countP_append]
      simp only [List.countP_cons, List.countP_nil, Nat.add_zero]
      have hleft : (List.range (x.length - 1)).countP (pk (x ++ y)) =
          (List.range (x.length - 1)).countP (pk x) := by
        apply countP_congr
        intro i hi
        apply pk_append_left
        have hi' : i < x.length - 1 := by simpa using hi
        omega
      rw [hleft]
      rw [count_pk_shifted]
      simp only [Int.natCast_add, Int.natCast_zero, Nat.zero_add]
      rw [← measure_eq_count_prefix x, ← measure_eq_count_pk_shift y]
      by_cases hA : pk (x ++ y) (x.length - 1) = true <;>
        by_cases hB : pk (x ++ y) x.length = true <;>
          simp [hA, hB] <;> ac_rfl

private theorem measure_cons (a : Int) (y : Series) :
    PeakCount.measure (a :: y) = PeakCount.measure y + (if pk (a :: y) 1 then 1 else 0) := by
  have h := measure_append_split [a] y
  simpa [measure_singleton, pk_false_zero] using h

private theorem pk_map_true_iff (x : Series) (f : Int → Int)
    (hf : ∀ a ∈ x, ∀ b ∈ x, (a < b ↔ f a < f b)) (i : Nat) :
    pk (x.map f) i = true ↔ pk x i = true := by
  constructor
  · intro h
    have hb := pk_true_bounds (x.map f) h
    have hi0 : 0 < i := hb.1
    have hi1 : i + 1 < x.length := by simpa using hb.2
    have hi : i < x.length := by omega
    have him : i - 1 < x.length := by omega
    have hbmap : i + 1 < (x.map f).length := by simpa using hi1
    have hlt := (pk_eq_of_bounds (x.map f) i hb.1 hbmap).mp h
    have hleft : f (x[i - 1]) < f (x[i]) := by
      simpa [List.getElem?_map, List.getElem?_eq_getElem him,
        List.getElem?_eq_getElem hi] using hlt.1
    have hright : f (x[i + 1]) < f (x[i]) := by
      simpa [List.getElem?_map, List.getElem?_eq_getElem hi1,
        List.getElem?_eq_getElem hi] using hlt.2
    have hm0 : x[i - 1] ∈ x := List.getElem_mem him
    have hmi : x[i] ∈ x := List.getElem_mem hi
    have hm1 : x[i + 1] ∈ x := List.getElem_mem hi1
    exact (pk_eq_of_bounds x i hi0 hi1).mpr
      ⟨(hf (x[i - 1]) hm0 (x[i]) hmi).mpr hleft,
       (hf (x[i + 1]) hm1 (x[i]) hmi).mpr hright⟩
  · intro h
    have hb := pk_true_bounds x h
    have hi0 : 0 < i := hb.1
    have hi1 : i + 1 < x.length := hb.2
    have hbmap : i + 1 < (x.map f).length := by simpa using hi1
    have hlt := (pk_eq_of_bounds x i hi0 hi1).mp h
    have hi : i < x.length := by omega
    have him : i - 1 < x.length := by omega
    have hm0 : x[i - 1] ∈ x := List.getElem_mem him
    have hmi : x[i] ∈ x := List.getElem_mem hi
    have hm1 : x[i + 1] ∈ x := List.getElem_mem hi1
    have hleft : f (x[i - 1]) < f (x[i]) := (hf (x[i - 1]) hm0 (x[i]) hmi).mp hlt.1
    have hright : f (x[i + 1]) < f (x[i]) := (hf (x[i + 1]) hm1 (x[i]) hmi).mp hlt.2
    apply (pk_eq_of_bounds (x.map f) i hb.1 hbmap).mpr
    constructor
    · simpa [List.getElem?_map, List.getElem?_eq_getElem him,
        List.getElem?_eq_getElem hi] using hleft
    · simpa [List.getElem?_map, List.getElem?_eq_getElem hi1,
        List.getElem?_eq_getElem hi] using hright

private theorem pk_map (x : Series) (f : Int → Int)
    (hf : ∀ a ∈ x, ∀ b ∈ x, (a < b ↔ f a < f b)) (i : Nat) :
    pk (x.map f) i = pk x i := by
  by_cases h : pk x i = true
  · have h' : pk (x.map f) i = true := (pk_map_true_iff x f hf i).mpr h
    rw [h, h']
  · have hz : pk x i = false := by
      cases p : pk x i <;> simp_all
    have h' : pk (x.map f) i = false := by
      have hn : pk (x.map f) i ≠ true := by
        intro hh
        exact h ((pk_map_true_iff x f hf i).mp hh)
      cases p : pk (x.map f) i <;> simp_all
    rw [hz, h']

private theorem last_two_cons_of_length (a : Int) (xs : Series) (h : 2 ≤ xs.length) :
    ((a :: xs).reverse.take 2).reverse = (xs.reverse.take 2).reverse := by
  rw [List.reverse_cons]
  have hle : 2 ≤ xs.reverse.length := by simpa using h
  rw [List.take_append_of_le_length (l₁ := xs.reverse) (l₂ := [a]) hle]

private theorem boundary_two (a b : Int) (y : Series) :
    PeakCount.measure ([a, b] ++ y) = PeakCount.measure [a, b] + PeakCount.measure y +
      PeakCount.measure (([a, b].reverse.take 2).reverse ++ y.take 2) := by
  induction y with
  | nil =>
      simp [measure_nil, measure_two]
  | cons c ys =>
      cases ys with
      | nil =>
          have hbd : (([a, b].reverse.take 2).reverse ++ [c].take 2) = [a, b, c] := by simp
          rw [hbd]
          simp [measure_three, measure_two, measure_singleton, measure_nil]
      | cons d zs =>
          have hbd : (([a, b].reverse.take 2).reverse ++ (c :: d :: zs).take 2) = [a, b, c, d] := by simp
          have hpk1 : pk (a :: b :: c :: d :: zs) 1 = true ↔ a < b ∧ c < b := by
            apply pk_eq_of_bounds <;> simp
          have hpk2 : pk (b :: c :: d :: zs) 1 = true ↔ b < c ∧ d < c := by
            apply pk_eq_of_bounds <;> simp
          rw [hbd]
          change PeakCount.measure (a :: b :: c :: d :: zs) = _
          rw [measure_cons a (b :: c :: d :: zs), measure_cons b (c :: d :: zs),
            measure_four, measure_two]
          by_cases h1 : a < b ∧ c < b <;> by_cases h2 : b < c ∧ d < c <;>
            simp [h1, h2, hpk1, hpk2] <;> omega

private theorem boundary_peaks_all (x : Series) : ∀ y,
    PeakCount.measure (x ++ y) = PeakCount.measure x + PeakCount.measure y +
      PeakCount.measure ((x.reverse.take 2).reverse ++ y.take 2) := by
  induction x with
  | nil =>
      intro y
      have htake : PeakCount.measure (y.take 2) = 0 :=
        measure_of_length_le_two (y.take 2) (by simp [Nat.min_le_left])
      simp [measure_nil, pk_false_zero, htake]
  | cons a xs ih =>
      intro y
      cases xs with
      | nil =>
          cases y with
          | nil =>
              simp [measure_singleton, measure_nil]
          | cons b ys =>
              cases ys with
              | nil =>
                  have hbd : (([a].reverse.take 2).reverse ++ [b].take 2) = [a, b] := by simp
                  rw [hbd]
                  simp [measure_two, measure_singleton, measure_nil]
              | cons c zs =>
                  have hbd : (([a].reverse.take 2).reverse ++ (b :: c :: zs).take 2) = [a, b, c] := by simp
                  have hpk : pk (a :: b :: c :: zs) 1 = true ↔ a < b ∧ c < b := by
                    apply pk_eq_of_bounds <;> simp
                  rw [hbd]
                  change PeakCount.measure (a :: b :: c :: zs) = _
                  rw [measure_cons a (b :: c :: zs), measure_three, measure_singleton]
                  by_cases h : a < b ∧ c < b <;> simp [h, hpk] <;> omega
      | cons b xs2 =>
          cases xs2 with
          | nil =>
              exact boundary_two a b y
          | cons c zs =>
              have hxs2 : 2 ≤ (b :: c :: zs).length := by simp
              have hlast := last_two_cons_of_length a (b :: c :: zs) hxs2
              have ihy := ih y
              rw [← hlast] at ihy
              have hbit : pk (a :: ((b :: c :: zs) ++ y)) 1 = pk (a :: b :: c :: zs) 1 := by
                change pk ((a :: b :: c :: zs) ++ y) 1 = pk (a :: b :: c :: zs) 1
                apply pk_append_left
                simp
              change PeakCount.measure (a :: ((b :: c :: zs) ++ y)) = _
              rw [measure_cons a ((b :: c :: zs) ++ y), ihy, measure_cons a (b :: c :: zs)]
              rw [hbit]
              ac_rfl

theorem sound_BoundaryPeaks :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PeakCount.BoundaryPeaks.Goal := by
  exact boundary_peaks_all

theorem sound_OrderDictionary :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PeakCount.OrderDictionary.Goal := by
  intro x f hf
  rw [measure_eq_count_pk, measure_eq_count_pk]
  simp only [List.length_map]
  congr 1
  apply countP_congr
  intro i hi
  exact pk_map x f hf i

private theorem adjacent_cap_pair (x : Series) :
    PeakCount.measure x ≤ (((x.length - 1) / 2 : Nat) : Int) ∧
      (pk x 1 = false → PeakCount.measure x ≤ (((x.length - 2) / 2 : Nat) : Int)) := by
  induction x with
  | nil =>
      constructor <;> simp [measure_nil, pk]
  | cons a xs ih =>
      cases xs with
      | nil =>
          constructor <;> simp [measure_singleton, pk]
      | cons b ys =>
          rcases ih with ⟨ihP, ihQ⟩
          constructor
          · by_cases h1 : pk (a :: b :: ys) 1 = true
            · have hrel : pk (a :: b :: ys) 2 = pk (b :: ys) 1 := by
                simpa using pk_append_right [a] (b :: ys) 1 (by omega)
              have hn2not : ¬ pk (b :: ys) 1 = true := by
                intro hb
                apply pk_no_adjacent (a :: b :: ys) 1
                constructor
                · exact h1
                · rw [hrel]
                  exact hb
              have hn2 : pk (b :: ys) 1 = false := by
                cases hbb : pk (b :: ys) 1 <;> simp_all
              rw [measure_cons a (b :: ys), if_pos h1]
              have hq := ihQ hn2
              have hnlen : 1 ≤ ys.length := by
                have hb := pk_true_bounds (a :: b :: ys) h1
                simp at hb
                omega
              have hcapq : ((b :: ys).length - 2) / 2 = (ys.length - 1) / 2 := by simp
              have hcapx : ((a :: b :: ys).length - 1) / 2 = (ys.length + 1) / 2 := by simp
              rw [hcapq] at hq
              rw [hcapx]
              have harith : (ys.length - 1) / 2 + 1 ≤ (ys.length + 1) / 2 := by omega
              calc
                PeakCount.measure (b :: ys) + 1 ≤
                    ((((ys.length - 1) / 2 : Nat) : Int) + 1) := by omega
                _ = (((ys.length - 1) / 2 + 1 : Nat) : Int) := by
                  rw [Int.natCast_add, Int.natCast_one]
                _ ≤ (((ys.length + 1) / 2 : Nat) : Int) := (Int.ofNat_le).mpr harith
            · rw [measure_cons a (b :: ys), if_neg h1]
              have hcapx : ((a :: b :: ys).length - 1) / 2 = (ys.length + 1) / 2 := by simp
              have hacap : ((b :: ys).length - 1) / 2 = ys.length / 2 := by simp
              rw [hcapx]
              rw [hacap] at ihP
              have hcastle := (Int.ofNat_le).mpr (by omega : ys.length / 2 ≤ (ys.length + 1) / 2)
              omega
          · intro hfalse
            have hfnot : ¬ pk (a :: b :: ys) 1 = true := by simpa [hfalse]
            rw [measure_cons a (b :: ys), if_neg hfnot]
            change PeakCount.measure (b :: ys) ≤ ((((a :: b :: ys).length - 2) / 2 : Nat) : Int)
            have hcapx : ((a :: b :: ys).length - 2) / 2 = ys.length / 2 := by simp
            have hacap : ((b :: ys).length - 1) / 2 = ys.length / 2 := by simp
            rw [hcapx]
            rw [hacap] at ihP
            exact ihP

theorem sound_AdjacentCap :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PeakCount.AdjacentCap.Goal := by
  intro x
  exact (adjacent_cap_pair x).1

theorem disproof_DuplicateLoss :
    ¬ TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PeakCount.DuplicateLoss.Goal := by
  intro h
  have hh := h [0, 1, 0]
  have hneq : ¬ (PeakCount.measure ([0, 1, 0].flatMap (repeated 2)) = PeakCount.measure [0, 1, 0]) := by
    native_decide
  exact hneq hh

private theorem measure_edit (x : Series) (a : Int) (y : Series) :
    PeakCount.measure (x ++ a :: y) = PeakCount.measure x + PeakCount.measure y +
      (if pk (x ++ a :: y) (x.length - 1) then 1 else 0) +
      (if pk (x ++ a :: y) x.length then 1 else 0) +
      (if pk (a :: y) 1 then 1 else 0) := by
  rw [measure_append_split x (a :: y)]
  rw [measure_cons a y]
  omega

private theorem edit_bits_le_two (x : Series) (a : Int) (y : Series) :
    (if pk (x ++ a :: y) (x.length - 1) then 1 else 0) +
      (if pk (x ++ a :: y) x.length then 1 else 0) +
      (if pk (a :: y) 1 then 1 else 0) ≤ (2 : Int) := by
  by_cases h0 : pk (x ++ a :: y) (x.length - 1) = true <;>
    by_cases h1 : pk (x ++ a :: y) x.length = true <;>
    by_cases h2 : pk (a :: y) 1 = true
  · have hb := pk_true_bounds (x ++ a :: y) h0
    have heq : x.length - 1 + 1 = x.length := by omega
    exact False.elim (pk_no_adjacent (x ++ a :: y) (x.length - 1)
      ⟨h0, by simpa [heq] using h1⟩)
  · simp [h0, h1, h2]
  · simp [h0, h1, h2]
  · simp [h0, h1, h2]
  · have hrel : pk (x ++ a :: y) (x.length + 1) = pk (a :: y) 1 := by
      simpa using pk_append_right x (a :: y) 1 (by omega)
    have h2z : pk (x ++ a :: y) (x.length + 1) = true := by simpa [hrel] using h2
    exact False.elim (pk_no_adjacent (x ++ a :: y) x.length ⟨h1, h2z⟩)
  · simp [h0, h1, h2]
  · simp [h0, h1, h2]
  · simp [h0, h1, h2]

private theorem natAbs_le_of_bounds {d : Int} (hlo : -2 ≤ d) (hhi : d ≤ 2) : d.natAbs ≤ 2 := by
  apply (Int.ofNat_le).mp
  by_cases h0 : 0 ≤ d
  · rw [Int.natAbs_of_nonneg h0]
    exact hhi
  · have hd : d ≤ 0 := by omega
    have hn : 0 ≤ -d := by omega
    have hcast : (↑(d.natAbs) : Int) = (↑((-d).natAbs) : Int) := by
      rw [Int.natAbs_neg]
    rw [hcast, Int.natAbs_of_nonneg hn]
    omega

theorem sound_BoundedRepair :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PeakCount.BoundedRepair.Goal := by
  intro x y a b
  rw [measure_edit x a y, measure_edit x b y]
  have hA := edit_bits_le_two x a y
  have hB := edit_bits_le_two x b y
  have hA0 : 0 ≤ (if pk (x ++ a :: y) (x.length - 1) then 1 else 0) +
        (if pk (x ++ a :: y) x.length then 1 else 0) + (if pk (a :: y) 1 then 1 else 0) := by
    by_cases h0 : pk (x ++ a :: y) (x.length - 1) = true <;>
      by_cases h1 : pk (x ++ a :: y) x.length = true <;>
      by_cases h2 : pk (a :: y) 1 = true <;> simp [h0, h1, h2]
  have hB0 : 0 ≤ (if pk (x ++ b :: y) (x.length - 1) then 1 else 0) +
        (if pk (x ++ b :: y) x.length then 1 else 0) + (if pk (b :: y) 1 then 1 else 0) := by
    by_cases h0 : pk (x ++ b :: y) (x.length - 1) = true <;>
      by_cases h1 : pk (x ++ b :: y) x.length = true <;>
      by_cases h2 : pk (b :: y) 1 = true <;> simp [h0, h1, h2]
  have hle :
      (((PeakCount.measure x + PeakCount.measure y +
        (if pk (x ++ a :: y) (x.length - 1) then 1 else 0)) +
        (if pk (x ++ a :: y) x.length then 1 else 0)) +
        (if pk (a :: y) 1 then 1 else 0)) -
      (((PeakCount.measure x + PeakCount.measure y +
        (if pk (x ++ b :: y) (x.length - 1) then 1 else 0)) +
        (if pk (x ++ b :: y) x.length then 1 else 0)) +
        (if pk (b :: y) 1 then 1 else 0)) ≤ 2 := by omega
  have hge2 : -2 ≤
      (((PeakCount.measure x + PeakCount.measure y +
        (if pk (x ++ a :: y) (x.length - 1) then 1 else 0)) +
        (if pk (x ++ a :: y) x.length then 1 else 0)) +
        (if pk (a :: y) 1 then 1 else 0)) -
      (((PeakCount.measure x + PeakCount.measure y +
        (if pk (x ++ b :: y) (x.length - 1) then 1 else 0)) +
        (if pk (x ++ b :: y) x.length then 1 else 0)) +
        (if pk (b :: y) 1 then 1 else 0)) := by omega
  exact natAbs_le_of_bounds hge2 hle

end TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.PeakCount.Remaining
