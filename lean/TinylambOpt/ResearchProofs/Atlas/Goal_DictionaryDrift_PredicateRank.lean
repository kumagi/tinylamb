import TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `DictionaryDrift.PredicateRank` — `∀ c lo hi, ((rows layout c).countP (fun r => lo ≤
  r.2 && r.2 < hi)) = ((layout.lanes c).map (fun a => band a lo hi)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.Atlas.Goal_DictionaryDrift_PredicateRank

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.DictionaryDrift

/-! Brute-force sanity check: no counterexample for small pages, dictionaries,
    signed steps and a grid of cut points. -/
#eval do
  let mut bad : Array (Nat × List Int × Int × Int × Int) := #[]
  for n in List.range 18 do
    for dict in [[], [0], [3], [2, -1], [0, 4, 1], [5, 5, -2], [1, 2, 3, 4]] do
      for s in [-3, -1, 0, 2, 5] do
        for lo in [-6, -1, 0, 2, 7] do
          for hi in [-5, 0, 1, 4, 12] do
            let c : Config := ⟨n, 0, 0, 0, s, 0, dict⟩
            let lhs := (rows layout c).countP
              (fun (r : Nat × Int) => lo ≤ r.2 && r.2 < hi)
            let rhs := ((layout.lanes c).map (fun a => band a lo hi)).sum
            if lhs != rhs then bad := bad.push (n, dict, s, lo, hi)
  pure (bad.toList.take 20, bad.size)

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

/-! ## Modular residues and striped sums -/

private theorem div_eq_one_of_bounds (t p : Nat) (hp : 0 < p)
    (hge : p ≤ t) (hlt : t < 2 * p) : t / p = 1 := by
  have hlt2 : t / p < 2 := (Nat.div_lt_iff_lt_mul hp).mpr hlt
  have hnle : ¬ t / p ≤ 0 := by
    intro h
    have hh := (Nat.div_le_iff_le_mul hp).mp h
    omega
  omega

private theorem mod_step_count (n p r : Nat) (hp : 0 < p) (hr : r < p) :
    (n - r + p - 1) / p = n / p + if r < n % p then 1 else 0 := by
  have hd := Nat.div_add_mod n p
  have hs := Nat.mod_lt n hp
  by_cases hnr : n < r
  · have hdiv : n / p = 0 := Nat.div_eq_of_lt (by omega)
    have hm : n - r + p - 1 = p - 1 := by grind
    have hnot : ¬ r < n % p := by omega
    have hpdiv : (p - 1) / p = 0 := Nat.div_eq_of_lt (Nat.sub_lt hp (by omega : 0 < 1))
    simp [hm, hdiv, hnot, hpdiv]
  · have hrn : r ≤ n := by omega
    have hsub : n - r + p - 1 = n / p * p + (n % p + p - 1 - r) := by grind
    rw [hsub, Nat.mul_comm (n / p) p]
    rw [Nat.mul_add_div hp (n / p) (n % p + p - 1 - r)]
    by_cases hlt : r < n % p
    · have hge : p ≤ n % p + p - 1 - r := by omega
      have hlt2 : n % p + p - 1 - r < 2 * p := by omega
      have hdiv2 : (n % p + p - 1 - r) / p = 1 := div_eq_one_of_bounds _ _ hp hge hlt2
      simp [hlt, hdiv2]
    · have hle : n % p ≤ r := by omega
      have hlt2 : n % p + p - 1 - r < p := by omega
      have hdiv2 : (n % p + p - 1 - r) / p = 0 := Nat.div_eq_of_lt hlt2
      simp [hlt, hdiv2]

private theorem succ_mod_step (n p : Nat) (hp : 0 < p) :
    (n + 1) % p = if n % p + 1 < p then n % p + 1 else 0 := by
  by_cases hp1 : p = 1
  · subst p
    have hm : n % 1 = 0 := by
      have := Nat.mod_lt n (by omega : 0 < 1)
      omega
    simp [hm]
  · have hp' : 1 < p := by omega
    have h1 : (n + 1) % p = (n % p + 1 % p) % p := Nat.add_mod n 1 p
    have h1p : 1 % p = 1 := Nat.mod_eq_of_lt hp'
    rw [h1, h1p]
    by_cases h : n % p + 1 < p
    · rw [Nat.mod_eq_of_lt h]
      simp [h]
    · have heq : n % p + 1 = p := by
        have hs := Nat.mod_lt n hp
        omega
      rw [heq, Nat.mod_self]
      simp [h]

private theorem succ_div_step (n p : Nat) (hp : 0 < p) :
    (n + 1) / p = n / p + if n % p + 1 < p then 0 else 1 := by
  have hd := Nat.div_add_mod n p
  have hs := Nat.mod_lt n hp
  by_cases h : n % p + 1 < p
  · have hEq : n + 1 = p * (n / p) + (n % p + 1) := by grind
    rw [hEq, Nat.mul_add_div hp (n / p) (n % p + 1), Nat.div_eq_of_lt h]
    simp [h]
  · have heq : n % p + 1 = p := by omega
    have hEq : n + 1 = p * (n / p + 1) := by grind
    rw [hEq, Nat.mul_comm p (n / p + 1), Nat.mul_div_left _ hp]
    simp [h]

private theorem residue_count_eq_zero_of_ge (N r p : Nat) (hr : N ≤ r) (hp : 0 < p) :
    (N - r + p - 1) / p = 0 := by
  have hsub : N - r = 0 := by omega
  rw [hsub, Nat.zero_add]
  exact Nat.div_eq_of_lt (Nat.sub_lt hp (by omega : 0 < 1))

private theorem range_filter_mod_eq (n p r : Nat) (hp : 0 < p) (hr : r < p) :
    (List.range n).filter (fun i => i % p == r) =
      (List.range ((n - r + p - 1) / p)).map (fun q => r + q * p) := by
  induction n with
  | zero =>
      have hm : (0 - r + p - 1) / p = 0 := by
        have : 0 - r + p - 1 = p - 1 := by omega
        rw [this, Nat.div_eq_of_lt (Nat.sub_lt hp (by omega : 0 < 1))]
      have hpdiv : (p - 1) / p = 0 := Nat.div_eq_of_lt (Nat.sub_lt hp (by omega : 0 < 1))
      simp [hm, hpdiv]
  | succ n ih =>
      rw [List.range_succ, List.filter_append, ih]
      rw [mod_step_count n p r hp hr, mod_step_count (n + 1) p r hp hr]
      rw [succ_mod_step n p hp, succ_div_step n p hp]
      by_cases hn : n % p == r
      · have hnp : n % p = r := Nat.eq_of_beq_eq_true hn
        have hltold : ¬ r < n % p := by omega
        have htail : List.map (fun q => r + q * p) (List.range (n / p)) ++ [n] =
            List.map (fun q => r + q * p) (List.range (n / p + 1)) := by
          rw [show n / p + 1 = Nat.succ (n / p) by omega, List.range_succ, List.map_append]
          simp
          have hd := Nat.div_add_mod n p
          rw [Nat.mul_comm p (n / p)] at hd
          have hval : r + n / p * p = n := by omega
          rw [hval]
        by_cases hs : r + 1 < p
        · have hltnew : r < r + 1 := by omega
          simp [hn, hltold, hnp, hs, hltnew]
          exact htail
        · have hltnew : ¬ r < 0 := by omega
          simp [hn, hltold, hnp, hs, hltnew]
          exact htail
      · have hne0 : n % p ≠ r := by intro he; exact hn (by rw [he]; simp)
        have hltold : r < n % p ∨ n % p < r := by
          by_cases hrlt : r < n % p
          · exact Or.inl hrlt
          · right
            omega
        by_cases h : n % p + 1 < p
        · rcases hltold with hlt | hgt
          · have hltnew : r < n % p + 1 := by omega
            simp [hn, h, hlt, hltnew]
          · have hltnew : ¬ r < n % p + 1 := by omega
            have hnlt : ¬ r < n % p := by omega
            simp [hn, h, hnlt, hltnew]
        · have hne : n % p ≠ r := by intro he; exact hn (by rw [he]; simp)
          have heq : n % p + 1 = p := by
            have hs' := Nat.mod_lt n hp
            omega
          have hlt : r < n % p := by omega
          have hltnew : ¬ r < 0 := by omega
          simp [hn, h, hlt, hltnew]

private theorem sum_update_nat (p r0 : Nat) (hr0 : r0 < p) (s : Nat → Nat) (v : Nat) :
    ((List.range p).map (fun r => if r == r0 then v + s r else s r)).sum =
      v + ((List.range p).map s).sum := by
  induction p generalizing r0 s v with
  | zero => omega
  | succ p ih =>
      rw [show p + 1 = Nat.succ p by omega, List.range_succ, List.map_append, List.sum_append,
        List.map_append, List.sum_append]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
      by_cases heq : p = r0
      · have hb : p == r0 := by rw [heq]; simp
        rw [if_pos hb]
        have hpref : (List.range p).map (fun r => if r == r0 then v + s r else s r) =
            (List.range p).map s := by
          apply List.map_congr_left
          intro r hr
          have hrlt : r < p := List.mem_range.mp hr
          have hne : ¬ r == r0 := by rw [beq_iff_eq]; omega
          simp [hne]
        rw [hpref]
        omega
      · have hb : ¬ (p == r0) := by rw [beq_iff_eq]; exact heq
        rw [if_neg hb]
        have hlt : r0 < p := by omega
        rw [ih r0 hlt s v]
        omega

private theorem striped_sum_partition_nat (p : Nat) (hp : 0 < p) (F : Nat → Nat) :
    ∀ xs : List Nat, (xs.map F).sum =
      ((List.range p).map (fun r => ((xs.filter (fun i => i % p == r)).map F).sum)).sum := by
  intro xs
  induction xs with
  | nil =>
      rw [List.map_nil, List.sum_nil]
      exact (sum_map_zero_nat (List.range p)).symm
  | cons x xs ih =>
      rw [List.map_cons, List.sum_cons, ih]
      rw [← sum_update_nat p (x % p) (Nat.mod_lt x hp)]
      apply congrArg List.sum
      apply List.map_congr_left
      intro r hr
      by_cases h : x % p == r
      · have h' : x % p = r := Nat.eq_of_beq_eq_true h
        simp [h, h']
      · have h' : x % p ≠ r := by intro e; exact h (by rw [e]; simp)
        simp [h']
        intro hrx
        exact (h' hrx.symm).elim

private def stripedValue (p : Nat) (make : Nat → Int × Int) (i : Nat) : Int :=
  let (z, d) := make (i % p)
  z + d * ((i / p : Nat) : Int)

private theorem stripedValue_residue (p : Nat) (hp : 0 < p) (make : Nat → Int × Int)
    (r q : Nat) (hr : r < p) :
    stripedValue p make (r + q * p) =
      (make r).1 + (make r).2 * (q : Int) := by
  unfold stripedValue
  have hmod : (r + q * p) % p = r := by
    rw [Nat.add_mul_mod_self_right]
    exact Nat.mod_eq_of_lt hr
  have hdiv : (r + q * p) / p = q := by
    rw [Nat.mul_comm q p, Nat.add_mul_div_left r q hp, Nat.div_eq_of_lt hr]
    simp
  rw [hmod, hdiv]

private theorem striped_sum_nat (p : Nat) (hp : 0 < p) (n : Nat) (make : Nat → Int × Int)
    (g : Nat → Int → Nat) :
    ((List.range n).map (fun i => g i (stripedValue p make i))).sum =
      ((List.range p).map (fun r =>
        let (z, d) := make r
        ((List.range ((n - r + p - 1) / p)).map
          (fun q => g (r + q * p) (z + d * (q : Int)))).sum)).sum := by
  rw [striped_sum_partition_nat p hp (fun i => g i (stripedValue p make i)) (List.range n)]
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  have hrp : r < p := List.mem_range.mp hr
  rw [range_filter_mod_eq n p r hp hrp]
  simp only [List.map_map, Function.comp_apply]
  apply congrArg List.sum
  apply List.map_congr_left
  intro q hq
  change g (r + q * p) (stripedValue p make (r + q * p)) =
    g (r + q * p) ((make r).1 + (make r).2 * (q : Int))
  rw [stripedValue_residue p hp make r q hrp]

/-! ## Main theorem -/

theorem sound_DictionaryDrift_PredicateRank : DictionaryDrift.PredicateRank.Goal := by
  intro c lo hi
  by_cases hne : c.dict = []
  · have hrows : rows layout c = [] := by
      unfold rows
      apply List.filterMap_eq_nil_iff.mpr
      intro i _
      simp [layout, hne]
    have hlanes : layout.lanes c = [] := by simp [layout, hne]
    rw [hrows, hlanes]
    simp
  · let p := c.dict.length
    have hp : 0 < p := List.length_pos_iff.mpr hne
    let make : Nat → Int × Int := fun r => (c.dict[r]?.getD 0, c.step)
    let g : Nat → Int → Nat := fun _ v => if lo ≤ v && v < hi then 1 else 0
    have hval : ∀ i, i < c.n → layout.read c i = some (stripedValue p make i) := by
      intro i _
      have hlt : i % c.dict.length < c.dict.length := Nat.mod_lt i hp
      have hget : c.dict[i % c.dict.length]? = some (c.dict[i % c.dict.length]) := by
        rw [List.getElem?_eq_some_iff]
        exact ⟨hlt, rfl⟩
      simp [layout, stripedValue, make, p, hget, Nat.mod_mod]
    have hrows : rows layout c = (List.range c.n).map (fun i => (i, stripedValue p make i)) := by
      exact rows_range_map layout c (fun i => stripedValue p make i) hval
    rw [hrows, countP_eq_sum]
    simp only [List.map_map, Function.comp_apply, Prod.snd]
    change ((List.range c.n).map (fun i => g i (stripedValue p make i))).sum = _
    rw [striped_sum_nat p hp c.n make g]
    have hband : ∀ r : Nat, r < p →
        band (lane c.n r p (c.dict[r]?.getD 0) c.step) lo hi =
          ((List.range ((c.n - r + p - 1) / p)).map
            (fun q => g (r + q * p) ((make r).1 + (make r).2 * (q : Int)))).sum := by
      intro r hr
      change band (lane c.n r p (make r).1 (make r).2) lo hi = _
      rw [band_eq_count]
      apply congrArg List.sum
      apply map_congr
      intro q _
      change (if lo ≤ (make r).1 + (make r).2 * (q : Int) &&
                 (make r).1 + (make r).2 * (q : Int) < hi then (1 : Nat) else 0) =
             g (r + q * p) ((make r).1 + (make r).2 * (q : Int))
      simp [g]
    have hzero : ∀ r : Nat, min p c.n ≤ r → r < p →
        ((List.range ((c.n - r + p - 1) / p)).map
          (fun q => g (r + q * p) ((make r).1 + (make r).2 * (q : Int)))).sum = 0 := by
      intro r hge _
      have hnle : c.n ≤ r := by
        by_cases hpn : p ≤ c.n
        · have hmin : min p c.n = p := Nat.min_eq_left hpn
          omega
        · have hmin : min p c.n = c.n := Nat.min_eq_right (by omega)
          omega
      have hz : (c.n - r + p - 1) / p = 0 :=
        residue_count_eq_zero_of_ge c.n r p hnle hp
      simp [hz]
    rw [sum_range_zero_tail (min p c.n) p
      (fun r => ((List.range ((c.n - r + p - 1) / p)).map
        (fun q => g (r + q * p) ((make r).1 + (make r).2 * (q : Int)))).sum)
      (Nat.min_le_left _ _) hzero]
    have hlanes : layout.lanes c = striped c p make := by
      rfl
    rw [hlanes]
    simp only [striped, List.map_map, Function.comp_apply]
    apply congrArg List.sum
    apply List.map_congr_left
    intro r hr
    have hrp : r < p := by
      have : r < min p c.n := List.mem_range.mp hr
      omega
    change ((List.range ((c.n - r + p - 1) / p)).map
      (fun q => g (r + q * p) ((make r).1 + (make r).2 * (q : Int)))).sum =
      band (lane c.n r p (make r).1 (make r).2) lo hi
    exact (hband r hrp).symm

end TinylambOpt.ResearchProofs.Atlas.Goal_DictionaryDrift_PredicateRank
