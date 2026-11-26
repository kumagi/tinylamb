import TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `RestartedDelta.PredicateRank` — `∀ c lo hi, ((rows layout c).countP (fun r => lo ≤
  r.2 && r.2 < hi)) = ((layout.lanes c).map (fun a => band a lo hi)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages.RestartedDeltaPredicateRank

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.RestartedDelta

/-! ## Generic list helpers -/

private theorem filterMap_some_eq_map {α β : Type} (l : List α) (f : α → β) :
    l.filterMap (fun x => some (f x)) = l.map f := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem filterMap_congr' {α β : Type} {l : List α} {f g : α → Option β}
    (h : ∀ x ∈ l, f x = g x) : l.filterMap f = l.filterMap g := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.filterMap_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem range_filterMap_all {α : Type} (n : Nat) (f : Nat → Option α) (g : Nat → α)
    (h : ∀ i, i < n → f i = some (g i)) :
    (List.range n).filterMap f = (List.range n).map g := by
  rw [← filterMap_some_eq_map (List.range n) g]
  apply filterMap_congr'
  intro i hi
  exact h i (List.mem_range.mp hi)

private theorem rows_range_map (lay : Layout) (c : Config) (f : Nat → Int)
    (h : ∀ i, i < c.n → lay.read c i = some (f i)) :
    rows lay c = (List.range c.n).map (fun i => (i, f i)) := by
  unfold rows
  exact range_filterMap_all c.n (fun i => (lay.read c i).map (fun v => (i, v)))
    (fun i => (i, f i)) (by intro i hi; simp [h i hi])

private theorem decide_eq_of_iff {a b : Prop} [Decidable a] [Decidable b] (h : a ↔ b) :
    decide a = decide b := by
  by_cases ha : a
  · have hb : b := h.mp ha
    simp [ha, hb]
  · have hb : ¬ b := fun hb => ha (h.mpr hb)
    simp [ha, hb]

private theorem filter_congr_bool {α : Type} (l : List α) (p q : α → Bool)
    (h : ∀ x ∈ l, p x = q x) : l.filter p = l.filter q := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.filter_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem map_congr {α β : Type} (l : List α) (f g : α → β)
    (h : ∀ x ∈ l, f x = g x) : l.map f = l.map g := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem countP_eq_sum {α : Type} (l : List α) (f : α → Bool) :
    l.countP f = ((l.map (fun x => if f x then (1 : Nat) else 0)).sum) := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : f x <;> simp [h, ih] <;> omega

private theorem sum_map_zero_nat (l : List Nat) : (l.map (fun _ => (0 : Nat))).sum = 0 := by
  induction l <;> simp [*]

private theorem sum_range_zero_tail (k m : Nat) (f : Nat → Nat) (hkm : k ≤ m)
    (hz : ∀ r, k ≤ r → r < m → f r = 0) :
    ((List.range m).map f).sum = ((List.range k).map f).sum := by
  induction m generalizing k with
  | zero =>
      have hk0 : k = 0 := by omega
      subst k
      simp
  | succ m ih =>
      by_cases hk : k = m + 1
      · subst k
        rfl
      · have hk' : k ≤ m := by omega
        rw [show m + 1 = Nat.succ m by omega, List.range_succ, List.map_append, List.sum_append]
        rw [ih k hk' (fun r hrk hrm => hz r hrk (by omega))]
        simp [hz m hk' (by omega)]

private theorem range_count_lt (n t : Nat) :
    ((List.range n).filter (fun i => i < t)).length = min n t := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.filter_append]
      by_cases h : n < t <;> simp [h, ih] <;> omega

private theorem length_filter_not {α : Type} (l : List α) (p : α → Bool) :
    (l.filter (fun x => !p x)).length = l.length - (l.filter p).length := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x = true
      · simp [h, ih]
      · have hf : p x = false := by cases h' : p x <;> simp_all
        simp [hf, ih]
        have hle : (List.filter p xs).length ≤ xs.length := List.length_filter_le p xs
        omega

private theorem range_count_ge (n t : Nat) :
    ((List.range n).filter (fun i => t ≤ i)).length = n - min n t := by
  have hnot : (List.range n).filter (fun i => t ≤ i) =
      (List.range n).filter (fun i => !(i < t)) := by
    apply filter_congr_bool
    intro i _
    by_cases h : i < t
    · simp [h]
    · simp [h]
      omega
  rw [hnot, length_filter_not, range_count_lt, List.length_range]

private theorem filter_subset_length {α : Type} (l : List α) (p q : α → Bool)
    (h : ∀ x ∈ l, p x = true → q x = true) :
    (l.filter p).length ≤ (l.filter q).length := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases hx : p x = true
      · have hqx := h x (by simp) hx
        have hi := ih (fun y hy => h y (by simp [hy]))
        have hp : (x :: xs).filter p = x :: (xs.filter p) := by simp [hx]
        have hq : (x :: xs).filter q = x :: (xs.filter q) := by simp [hqx]
        rw [hp, hq, List.length_cons, List.length_cons]
        omega
      · have hx0 : p x = false := by
          cases hpx : p x
          · rfl
          · exfalso
            exact hx (by simp [hpx])
        by_cases hqx : q x = true
        · have hi := ih (fun y hy => h y (by simp [hy]))
          have hp : (x :: xs).filter p = xs.filter p := by simp [hx0]
          have hq : (x :: xs).filter q = x :: (xs.filter q) := by simp [hqx]
          rw [hp, hq, List.length_cons]
          omega
        · have hqx0 : q x = false := by
            cases hqx' : q x
            · rfl
            · exfalso
              exact hqx (by simp [hqx'])
          have hi := ih (fun y hy => h y (by simp [hy]))
          have hp : (x :: xs).filter p = xs.filter p := by simp [hx0]
          have hq : (x :: xs).filter q = xs.filter q := by simp [hqx0]
          rw [hp, hq]
          omega

/-! ## `beforeVal` counts arithmetic-progression elements below a cut -/

private theorem lt_ceil_iff (d : Int) (hd : 0 < d) (i : Nat) (x : Int) :
    d * (i : Int) < x ↔ i < ((x + d - 1) / d).toNat := by
  let q := (x + d - 1) / d
  constructor
  · intro h
    have hceil : d * ((i : Int) + 1) ≤ x + d - 1 := by
      rw [Int.mul_add]
      omega
    have hq : (i : Int) + 1 ≤ q :=
      (Int.le_ediv_iff_mul_le hd).mpr (by simpa [Int.mul_comm] using hceil)
    change i < q.toNat
    exact (Int.toNat_lt_toNat (n := (i : Int)) (by omega : 0 < q)).mpr (by omega)
  · intro h
    have hiq : (i : Int) < q := Int.lt_of_toNat_lt h
    have hceil0 : ((i : Int) + 1) * d ≤ x + d - 1 :=
      (Int.le_ediv_iff_mul_le hd).mp (by omega : (i : Int) + 1 ≤ q)
    have hceil : d * ((i : Int) + 1) ≤ x + d - 1 := by
      simpa [Int.mul_comm] using hceil0
    rw [Int.mul_add] at hceil
    omega

private theorem gt_floor_iff (d : Int) (hd : 0 < d) (i : Nat) (x : Int) :
    d * (i : Int) > x ↔ ((x / d) + 1).toNat ≤ i := by
  let q := x / d
  constructor
  · intro h
    have h1 : d * q ≤ x := by
      simpa [Int.mul_comm] using Int.ediv_mul_le x (by omega : d ≠ 0)
    have h2 : d * q < d * (i : Int) := by omega
    have h3 : q < (i : Int) := Int.lt_of_mul_lt_mul_left h2 (by omega : 0 ≤ d)
    have hqi : q + 1 ≤ (i : Int) := by omega
    change (q + 1).toNat ≤ i
    rw [Int.toNat_le]
    exact hqi
  · intro h
    have hiq : q + 1 ≤ (i : Int) := (Int.toNat_le).mp h
    have hmul : d * (q + 1) ≤ d * (i : Int) :=
      Int.mul_le_mul_of_nonneg_left hiq (by omega : 0 ≤ d)
    have hlt : x < d * (q + 1) := by
      simpa [Int.mul_comm] using Int.lt_ediv_add_one_mul_self x hd
    omega

private theorem beforeVal_eq_count (a : Lane) (cut : Int) :
    beforeVal a cut = ((List.range a.n).filter
      (fun i => a.base + a.step * ((i : Nat) : Int) < cut)).length := by
  by_cases hz : a.step = 0
  · by_cases h : a.base < cut
    · unfold beforeVal
      rw [hz]
      simp [h]
      have hf : (List.range a.n).filter (fun i => true) = List.range a.n := by
        apply List.filter_eq_self.mpr
        intro i _
        rfl
      rw [hf]
      simp [List.length_range]
    · unfold beforeVal
      rw [hz]
      simp [h]
      have hf : (List.range a.n).filter (fun i => false) = [] := by
        apply List.filter_eq_nil_iff.mpr
        intro i _
        simp
      rw [hf]
      simp [List.length_range]
  · by_cases hp : 0 < a.step
    · unfold beforeVal
      rw [if_neg (by simp [hz]), if_pos hp]
      have hf : (List.range a.n).filter (fun i => a.base + a.step * ((i : Nat) : Int) < cut) =
          (List.range a.n).filter (fun i => i <
            ((cut - a.base + a.step - 1) / a.step).toNat) := by
        apply filter_congr_bool
        intro i _
        apply decide_eq_of_iff
        have hh := lt_ceil_iff a.step hp i (cut - a.base)
        change a.step * (i : Int) < cut - a.base ↔ _ at hh
        constructor <;> intro h <;> omega
      rw [hf, range_count_lt]
    · have hn : a.step < 0 := by omega
      unfold beforeVal
      rw [if_neg (by simp [hz]), if_neg hp]
      have hd : 0 < -a.step := by omega
      have hf : (List.range a.n).filter (fun i => a.base + a.step * ((i : Nat) : Int) < cut) =
          (List.range a.n).filter (fun i => ((a.base - cut) / (-a.step) + 1).toNat ≤ i) := by
        apply filter_congr_bool
        intro i _
        apply decide_eq_of_iff
        have hh := gt_floor_iff (-a.step) hd i (a.base - cut)
        change (-a.step) * (i : Int) > a.base - cut ↔ _ at hh
        rw [Int.neg_mul] at hh
        omega
      rw [hf, range_count_ge]

private theorem count_lt_partition (l : List Nat) (f : Nat → Int) (lo hi : Int) (hle : lo ≤ hi) :
    (l.filter (fun i => f i < hi)).length =
    (l.filter (fun i => f i < lo)).length +
    (l.filter (fun i => lo ≤ f i && f i < hi)).length := by
  induction l with
  | nil => simp
  | cons i is ih =>
      simp only [List.filter_cons]
      by_cases hhi : f i < hi
      · by_cases hlo : f i < lo
        · have hge : ¬ (lo ≤ f i) := by omega
          have hge_bool : decide (lo ≤ f i) = false := by simp [hge]
          have hnot : (lo ≤ f i && f i < hi) = false := by simp [hge]
          simp [hhi, hlo, hnot, hge_bool, ih]
          omega
        · have hge : lo ≤ f i := by omega
          have hge_bool : decide (lo ≤ f i) = true := by simp [hge]
          have hint : (lo ≤ f i && f i < hi) = true := by simp [hge, hhi]
          have hlofalse : (f i < lo) = false := by
            have : ¬ (f i < lo) := hlo
            simp [this]
          simp [hhi, hlofalse, hint, hge_bool, ih]
          omega
      · have hhi_false : (f i < hi) = false := by
          have : ¬ (f i < hi) := hhi
          simp [this]
        have hlo_le : lo ≤ f i := Int.le_trans hle (Int.not_lt.mp hhi)
        have hlo_false : (f i < lo) = false := by
          have : ¬ (f i < lo) := Int.not_lt.mpr hlo_le
          simp [this]
        have hint_false : (lo ≤ f i && f i < hi) = false := by simp [hhi]
        simp [hhi_false, hlo_false, hint_false, ih]

private def lanePoints (a : Lane) : List (Nat × Int) :=
  (List.range a.n).map (fun i : Nat => (a.start + i * a.stride, a.base + a.step * (i : Int)))

private theorem lane_band (a : Lane) (lo hi : Int) :
    band a lo hi = (lanePoints a).countP (fun r => lo ≤ r.2 && r.2 < hi) := by
  rw [band, beforeVal_eq_count a hi, beforeVal_eq_count a lo]
  rw [lanePoints]
  rw [List.countP_map]
  have hcong : ∀ i ∈ List.range a.n,
      ((fun r => lo ≤ r.2 && r.2 < hi) ∘
        (fun i => (a.start + i * a.stride, a.base + a.step * (i : Int)))) i ↔
      (lo ≤ a.base + a.step * ((i : Nat) : Int) &&
       a.base + a.step * ((i : Nat) : Int) < hi) := by
    intro i _
    simp only [Function.comp_apply, Prod.snd]
  rw [List.countP_congr hcong]
  rw [List.countP_eq_length_filter]
  by_cases hle : lo ≤ hi
  · have hpart := count_lt_partition (List.range a.n)
      (fun i => a.base + a.step * ((i : Nat) : Int)) lo hi hle
    omega
  · have hb : (List.range a.n).filter
        (fun i => lo ≤ a.base + a.step * ((i : Nat) : Int) &&
          a.base + a.step * ((i : Nat) : Int) < hi) = [] := by
      apply List.filter_eq_nil_iff.mpr
      intro i _
      have hlt : hi < lo := by omega
      have hfalse : (lo ≤ a.base + a.step * ((i : Nat) : Int) &&
          a.base + a.step * ((i : Nat) : Int) < hi) = false := by
        by_cases hv : a.base + a.step * ((i : Nat) : Int) < hi
        · have hvlo : a.base + a.step * ((i : Nat) : Int) < lo := by omega
          have hge : ¬ (lo ≤ a.base + a.step * ((i : Nat) : Int)) := by omega
          simp [hv, hge]
        · simp [hv]
      simp [hfalse]
    have hmono :
        ((List.range a.n).filter (fun i => a.base + a.step * ((i : Nat) : Int) < hi)).length ≤
        ((List.range a.n).filter (fun i => a.base + a.step * ((i : Nat) : Int) < lo)).length := by
      apply filter_subset_length
      intro i _ hh
      have hhv : a.base + a.step * (i : Int) < hi := of_decide_eq_true hh
      have hlt : hi < lo := by omega
      have hlti : a.base + a.step * (i : Int) < lo := by omega
      simp [hlti]
    rw [hb]
    simp
    omega

private theorem band_eq_count (a : Lane) (lo hi : Int) :
    band a lo hi =
      ((List.range a.n).map (fun i : Nat =>
        if lo ≤ a.base + a.step * (i : Int) &&
           a.base + a.step * (i : Int) < hi then (1 : Nat) else 0)).sum := by
  rw [lane_band a lo hi, countP_eq_sum]
  rw [lanePoints]
  simp only [List.map_map]
  apply congrArg List.sum
  apply map_congr
  intro i _
  simp


private theorem filterMap_range_split {α : Type} (f : Nat → Option α) (m k : Nat) :
    (List.range (m+k)).filterMap f =
      (List.range m).filterMap f ++ (List.range k).filterMap (fun j => f (m+j)) := by
  rw [List.range_add, List.filterMap_append, List.filterMap_map]
  rfl

private theorem rows_lanes (c : Config) :
    rows layout c = (layout.lanes c).flatMap lanePoints := by
  simp only [layout, List.flatMap_cons, List.flatMap_nil, List.append_nil]
  unfold rows lanePoints span lane
  dsimp only
  have hn1 : (min c.n c.split - 0 + 1 - 1) / 1 = min c.n c.split := by omega
  have hn2 : (min c.n c.n - c.split + 1 - 1) / 1 = c.n - c.split := by omega
  rw [hn1, hn2]
  have hnk : min c.n c.split + (c.n - min c.n c.split) = c.n := by omega
  have key := filterMap_range_split
    (fun i => Option.map (fun x => (i, x))
      (if i < c.split then some (c.base + c.step * (i : Int))
        else some (c.bias + c.step * ((i - c.split : Nat) : Int))))
    (min c.n c.split) (c.n - min c.n c.split)
  rw [hnk] at key
  rw [key]
  congr 1
  · rw [range_filterMap_all _ _ (fun i : Nat => (i, c.base + c.step * (i : Int))) (by
      intro i hi
      have : i < c.split := by omega
      simp [this])]
    apply List.map_congr_left
    intro i _
    simp
  · by_cases hk : c.n - min c.n c.split = 0
    · rw [hk]
      have : c.n - c.split = 0 := by omega
      rw [this]; rfl
    · have hms : min c.n c.split = c.split := by omega
      rw [hms]
      rw [range_filterMap_all _ _ (fun j : Nat => (c.split + j, c.bias + c.step * (j : Int))) (by
        intro j hj
        have h1 : ¬ (c.split + j < c.split) := by omega
        have h2 : c.split + j - c.split = j := by omega
        simp [h1, h2])]
      apply List.map_congr_left
      intro i _
      simp

private theorem countP_flatMap_sum {α β : Type} (l : List α) (f : α → List β) (p : β → Bool) :
    (l.flatMap f).countP p = (l.map (fun a => (f a).countP p)).sum := by
  induction l with
  | nil => simp
  | cons a as ih => simp [List.countP_append, ih]

theorem sound_RestartedDelta_PredicateRank : RestartedDelta.PredicateRank.Goal := by
  intro c lo hi
  rw [rows_lanes c, countP_flatMap_sum]
  apply congrArg List.sum
  apply List.map_congr_left
  intro a _
  exact (lane_band a lo hi).symm

end TinylambOpt.ResearchProofs.Atlas.EncodedPages.RestartedDeltaPredicateRank

-- C++ references to this model or its proof.
-- executor/detail/execution_strategies.hpp
