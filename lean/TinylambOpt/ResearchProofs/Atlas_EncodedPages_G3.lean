import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.Atlas_EncodedPages_G3

open TinylambOpt.ResearchGoals.Atlas.EncodedPages

-- =====================================================================
-- Generic list helpers
-- =====================================================================

private theorem list_sum_append_int (l₁ l₂ : List Int) :
    (l₁ ++ l₂).sum = l₁.sum + l₂.sum := by
  induction l₁ with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem list_map_append {α β : Type} (f : α → β) (l₁ l₂ : List α) :
    (l₁ ++ l₂).map f = l₁.map f ++ l₂.map f := by
  induction l₁ <;> simp [*]

private theorem range_succ_map_sum (f : Nat → Int) (n : Nat) :
    ((List.range (n+1)).map f).sum = ((List.range n).map f).sum + f n := by
  rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
  simp

private theorem sum_map_le {α : Type} (l : List α) (f g : α → Nat)
    (h : ∀ x ∈ l, f x ≤ g x) : (l.map f).sum ≤ (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      exact Nat.add_le_add (h x (by simp)) (ih (fun y hy => h y (by simp [hy])))

private theorem sum_map_sub_nat {α : Type} (l : List α) (f g : α → Nat)
    (h : ∀ x ∈ l, g x ≤ f x) :
    ((l.map fun x => f x - g x).sum) = (l.map f).sum - (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hle : (xs.map g).sum ≤ (xs.map f).sum := sum_map_le xs g f (fun y hy => h y (by simp [hy]))
      have hx : g x ≤ f x := h x (by simp)
      rw [List.map_cons, List.map_cons, List.sum_cons, List.sum_cons]
      rw [ih (fun y hy => h y (by simp [hy]))]
      let a := f x
      let b := g x
      let cc := (List.map f xs).sum
      let d := (List.map g xs).sum
      change (a - b) + (cc - d) = (a + cc) - (b + d)
      omega

private theorem sum_map_sub {α : Type} (l : List α) (f g : α → Int) :
    ((l.map fun x => f x - g x).sum) = (l.map f).sum - (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem sum_map_mul_int (l : List Int) (c : Int) :
    (l.map (fun x => c * x)).sum = c * l.sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Int.mul_add]

private theorem filterMap_some_eq_map {α β : Type} (l : List α) (f : α → β) :
    l.filterMap (fun x => some (f x)) = l.map f := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem filterMap_ite_eq_filter_map {α β : Type} (l : List α) (p : α → Bool) (g : α → β) :
    l.filterMap (fun x => if p x then some (g x) else none) = (l.filter p).map g := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x = true <;> simp [h, ih]

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

private theorem map_pair_filter {α β : Type} (xs : List α) (f : α → Nat) (g : α → β)
    (p : Nat → Bool) :
    (xs.map (fun x => (f x, g x))).filter (fun r => p r.1) =
      (xs.filter (fun x => p (f x))).map (fun x => (f x, g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih => by_cases h : p (f x) <;> simp [h, ih]

-- =====================================================================
-- Arithmetic helpers: tri, partialSum, lane slices
-- =====================================================================

private theorem choose2_succ (k : Nat) :
    TinylambOpt.ResearchGoals.PairAggregates.choose2 (k + 1) =
      TinylambOpt.ResearchGoals.PairAggregates.choose2 k + k := by
  unfold TinylambOpt.ResearchGoals.PairAggregates.choose2
  by_cases hk : k = 0
  · simp [hk]
  · have hkm1 : k + 1 - 1 = k := by omega
    rw [hkm1]
    have hmul1 : (k + 1) * k = k * k + k := by
      rw [Nat.add_mul, Nat.one_mul]
    have hmul2 : k * (k - 1) = k * k - k := by
      rw [Nat.mul_sub_left_distrib, Nat.mul_one]
    have hle : k ≤ k * k := by
      have := Nat.mul_le_mul_left k (show 1 ≤ k by omega)
      simpa using this
    have hcan : k * k - k + k = k * k := Nat.sub_add_cancel hle
    have hEq : (k + 1) * k = k * (k - 1) + 2 * k := by
      rw [hmul1, hmul2]
      omega
    rw [hEq]
    exact Nat.add_mul_div_left (k * (k - 1)) k (by decide : 0 < 2)

private theorem tri_succ (k : Nat) : tri (k + 1) = tri k + (k : Int) := by
  dsimp [tri]
  rw [choose2_succ]
  simp [Int.natCast_add]

private theorem partialSum_succ (a : Lane) (k : Nat) :
    partialSum a (k + 1) = partialSum a k + (a.base + (k : Int) * a.step) := by
  rw [partialSum, partialSum, tri_succ]
  simp [Int.natCast_add, Int.add_mul]
  omega

private theorem partialSum_eq_sum (a : Lane) (k : Nat) :
    partialSum a k = ((List.range k).map (fun i : Nat => a.base + (i : Int) * a.step)).sum := by
  induction k with
  | zero => simp [partialSum, tri, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  | succ k ih =>
      rw [partialSum_succ, ih]
      rw [List.range_succ, List.map_append, List.sum_append]
      simp

private theorem range_count_lt (n t : Nat) :
    ((List.range n).filter (fun i => i < t)).length = min n t := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.filter_append]
      by_cases h : n < t <;> simp [h, ih] <;> omega

private theorem range_count_between (n tlo thi : Nat) (hle : tlo ≤ thi) :
    ((List.range n).filter (fun i => tlo ≤ i && i < thi)).length = min n thi - min n tlo := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.filter_append]
      by_cases h1 : tlo ≤ n <;> by_cases h2 : n < thi <;> simp [h1, h2, ih] <;> omega

private theorem range_sum_between (n tlo thi : Nat) (f : Nat → Int) (hle : tlo ≤ thi) :
    (((List.range n).filter (fun i => tlo ≤ i && i < thi)).map f).sum =
      ((List.range (min n thi)).map f).sum - ((List.range (min n tlo)).map f).sum := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.filter_append]
      by_cases h1 : tlo ≤ n <;> by_cases h2 : n < thi
      · simp [h1, h2]
        rw [ih]
        have hmthi : min n thi = n := Nat.min_eq_left (by omega)
        have hmtlo : min n tlo = tlo := Nat.min_eq_right (by omega)
        have hmnthi : min (n + 1) thi = n + 1 := Nat.min_eq_left (by omega)
        have hmntlo : min (n + 1) tlo = tlo := Nat.min_eq_right (by omega)
        rw [hmthi, hmtlo, hmnthi, hmntlo]
        simp [List.range_succ]
        omega
      · simp [h1, h2]
        rw [ih]
        have hnthi : min (n + 1) thi = min n thi := by omega
        have hntlo : min (n + 1) tlo = min n tlo := by omega
        rw [hnthi, hntlo]
      · simp [h1, h2]
        rw [ih]
        have hmthi : min n thi = n := Nat.min_eq_left (by omega)
        have hmtlo : min n tlo = n := Nat.min_eq_left (by omega)
        have hmnthi : min (n + 1) thi = n + 1 := Nat.min_eq_left (by omega)
        have hmntlo : min (n + 1) tlo = n + 1 := Nat.min_eq_left (by omega)
        rw [hmthi, hmtlo, hmnthi, hmntlo]
        simp [List.range_succ]
      · simp [h1, h2]
        omega

private theorem ceilDiv_eq (d s : Nat) (hs : 0 < s) : d + s - 1 - (s - 1) = d := by omega

private theorem pos_lt_iff (start stride i p : Nat) (hs : 0 < stride) :
    start + i * stride < p ↔ i < (p - start + stride - 1) / stride := by
  by_cases hps : p ≤ start
  · have hp0 : p - start = 0 := by omega
    have hzero : (p - start + stride - 1) / stride = 0 := by
      rw [hp0, Nat.zero_add]
      exact Nat.div_eq_of_lt (by omega)
    rw [hzero]
    omega
  · have hps' : start < p := by omega
    rw [Nat.lt_div_iff_mul_lt hs]
    have hrewrite : p - start + stride - 1 - (stride - 1) = p - start :=
      ceilDiv_eq (p - start) stride hs
    rw [hrewrite]
    omega

private theorem beforePos_eq_min (a : Lane) (p : Nat) (hs : 0 < a.stride) :
    beforePos a p = ((List.range a.n).filter (fun i => a.start + i * a.stride < p)).length := by
  unfold beforePos
  have hiff : ∀ i : Nat, (a.start + i * a.stride < p) ↔ (i < (p - a.start + a.stride - 1) / a.stride) :=
    fun i => pos_lt_iff a.start a.stride i p hs
  have hf : (List.range a.n).filter (fun i => a.start + i * a.stride < p) =
      (List.range a.n).filter (fun i => i < (p - a.start + a.stride - 1) / a.stride) :=
    filter_congr_bool _ _ _ (fun i _ => decide_eq_of_iff (hiff i))
  rw [hf, range_count_lt]

private def lanePoints (a : Lane) : List (Nat × Int) :=
  (List.range a.n).map (fun i : Nat => (a.start + i * a.stride, a.base + (i : Int) * a.step))

private theorem lanePoints_filter_eq (a : Lane) (lo hi : Nat) (hs : 0 < a.stride) :
    (lanePoints a).filter (fun r => lo ≤ r.1 && r.1 < hi) =
      ((List.range a.n).filter (fun i : Nat => lo ≤ a.start + i * a.stride ∧ a.start + i * a.stride < hi)).map
        (fun i : Nat => (a.start + i * a.stride, a.base + (i : Int) * a.step)) := by
  unfold lanePoints
  simpa using (map_pair_filter (List.range a.n) (fun i : Nat => a.start + i * a.stride)
    (fun i : Nat => a.base + (i : Int) * a.step) (fun p : Nat => lo ≤ p && p < hi))

private theorem lane_slice_count (a : Lane) (lo hi : Nat) (hle : lo ≤ hi) (hs : 0 < a.stride) :
    ((lanePoints a).filter (fun r => lo ≤ r.1 && r.1 < hi)).length = beforePos a hi - beforePos a lo := by
  rw [lanePoints_filter_eq a lo hi hs]
  simp only [List.length_map]
  let thi := (hi - a.start + a.stride - 1) / a.stride
  let tlo := (lo - a.start + a.stride - 1) / a.stride
  change ((List.range a.n).filter (fun i : Nat => lo ≤ a.start + i * a.stride ∧ a.start + i * a.stride < hi)).length =
    min a.n thi - min a.n tlo
  have hiff_hi : ∀ i, a.start + i * a.stride < hi ↔ i < thi :=
    fun i => pos_lt_iff a.start a.stride i hi hs
  have hiff_lo : ∀ i, a.start + i * a.stride < lo ↔ i < tlo :=
    fun i => pos_lt_iff a.start a.stride i lo hs
  have hfilter : (List.range a.n).filter (fun i : Nat => lo ≤ a.start + i * a.stride ∧ a.start + i * a.stride < hi) =
      (List.range a.n).filter (fun i : Nat => tlo ≤ i && i < thi) := by
    apply filter_congr_bool
    intro i hi_mem
    change decide (lo ≤ a.start + i * a.stride ∧ a.start + i * a.stride < hi) =
      decide (tlo ≤ i ∧ i < thi)
    apply decide_eq_of_iff
    constructor
    · intro h
      constructor
      · by_cases hn : tlo ≤ i
        · exact hn
        · have hitlo : i < tlo := by omega
          have hlt : a.start + i * a.stride < lo := hiff_lo i |>.mpr hitlo
          omega
      · exact hiff_hi i |>.mp h.2
    · intro h
      constructor
      · by_cases hlt : a.start + i * a.stride < lo
        · have hitlo : i < tlo := hiff_lo i |>.mp hlt
          omega
        · omega
      · exact hiff_hi i |>.mpr h.2
  have htle : tlo ≤ thi := by
    dsimp [tlo, thi]
    apply Nat.div_le_div_right
    omega
  rw [hfilter, range_count_between a.n tlo thi htle]

private theorem lane_slice_sum (a : Lane) (lo hi : Nat) (hle : lo ≤ hi) (hs : 0 < a.stride) :
    (((lanePoints a).filter (fun r => lo ≤ r.1 && r.1 < hi)).map Prod.snd).sum =
      partialSum a (beforePos a hi) - partialSum a (beforePos a lo) := by
  rw [lanePoints_filter_eq a lo hi hs]
  simp only [List.map_map]
  let f : Nat → Int := fun i => a.base + (i : Int) * a.step
  let thi := (hi - a.start + a.stride - 1) / a.stride
  let tlo := (lo - a.start + a.stride - 1) / a.stride
  have hiff_hi : ∀ i, a.start + i * a.stride < hi ↔ i < thi :=
    fun i => pos_lt_iff a.start a.stride i hi hs
  have hiff_lo : ∀ i, a.start + i * a.stride < lo ↔ i < tlo :=
    fun i => pos_lt_iff a.start a.stride i lo hs
  have hfilter : (List.range a.n).filter (fun i : Nat => lo ≤ a.start + i * a.stride ∧ a.start + i * a.stride < hi) =
      (List.range a.n).filter (fun i : Nat => tlo ≤ i && i < thi) := by
    apply filter_congr_bool
    intro i hi_mem
    change decide (lo ≤ a.start + i * a.stride ∧ a.start + i * a.stride < hi) =
      decide (tlo ≤ i ∧ i < thi)
    apply decide_eq_of_iff
    constructor
    · intro h
      constructor
      · by_cases hn : tlo ≤ i
        · exact hn
        · have hitlo : i < tlo := by omega
          have hlt : a.start + i * a.stride < lo := hiff_lo i |>.mpr hitlo
          omega
      · exact hiff_hi i |>.mp h.2
    · intro h
      constructor
      · by_cases hlt : a.start + i * a.stride < lo
        · have hitlo : i < tlo := hiff_lo i |>.mp hlt
          omega
        · omega
      · exact hiff_hi i |>.mpr h.2
  have htle : tlo ≤ thi := by
    dsimp [tlo, thi]
    apply Nat.div_le_div_right
    omega
  rw [hfilter]
  change (((List.range a.n).filter (fun i : Nat => tlo ≤ i && i < thi)).map f).sum =
      partialSum a (beforePos a hi) - partialSum a (beforePos a lo)
  rw [range_sum_between a.n tlo thi f htle]
  rw [← partialSum_eq_sum, ← partialSum_eq_sum]
  have bh : beforePos a hi = min a.n thi := by
    rw [beforePos_eq_min a hi hs]
    rw [filter_congr_bool _ _ _ (fun i _ => decide_eq_of_iff (hiff_hi i))]
    rw [range_count_lt]
  have bl : beforePos a lo = min a.n tlo := by
    rw [beforePos_eq_min a lo hs]
    rw [filter_congr_bool _ _ _ (fun i _ => decide_eq_of_iff (hiff_lo i))]
    rw [range_count_lt]
  rw [bh, bl]

-- =====================================================================
-- Generic slice-state derivation from rows = flatMap lanePoints
-- =====================================================================

private theorem slice_flatMap (ls : List Lane)
    (hspos : ∀ a ∈ ls, 0 < a.stride) (lo hi : Nat) (hle : lo ≤ hi) :
    let selected := (ls.flatMap lanePoints).filter (fun r => lo ≤ r.1 && r.1 < hi)
    (selected.length,(selected.map Prod.snd).sum) =
      ((ls.map (fun a => beforePos a hi - beforePos a lo)).sum,
       (ls.map (fun a => partialSum a (beforePos a hi)-partialSum a (beforePos a lo))).sum) := by
  dsimp
  induction ls with
  | nil => simp
  | cons a ls ih =>
      simp only [List.flatMap_cons, List.filter_append, List.map_append, List.length_append,
        List.sum_append, List.map_cons, List.sum_cons, List.length_cons, List.mem_cons]
      have ha := hspos a (by simp)
      have hlen := lane_slice_count a lo hi hle ha
      have hsum := lane_slice_sum a lo hi hle ha
      have ih' := ih (fun b hb => hspos b (by simp [hb]))
      have ih1 := congrArg Prod.fst ih'
      have ih2 := congrArg Prod.snd ih'
      simp only at ih1 ih2
      apply Prod.ext <;> simp only [Prod.fst, Prod.snd]
      · rw [ih1]
        simp [hlen]
      · rw [ih2]
        simp [hsum]

-- =====================================================================
-- Modular residue partition of a range
-- =====================================================================

private theorem div_eq_one_of_bounds (t p : Nat) (hp : 0 < p)
    (hge : p ≤ t) (hlt : t < 2*p) : t / p = 1 := by
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

private theorem residue_count_succ (n p r : Nat) (hp : 0 < p) (hr : r < p) :
    (n + 1 - r + p - 1) / p = (n - r + p - 1) / p + if n % p == r then 1 else 0 := by
  rw [mod_step_count (n+1) p r hp hr, mod_step_count n p r hp hr]
  rw [succ_mod_step n p hp, succ_div_step n p hp]
  by_cases hn : n % p == r
  · have hnp : n % p = r := Nat.eq_of_beq_eq_true hn
    have hltold : ¬ r < n % p := by omega
    by_cases hs : r + 1 < p
    · have hltnew : r < r + 1 := by omega
      simp [hnp, hs, hltnew, hltold]
    · have hltnew : ¬ r < 0 := by omega
      simp [hnp, hs, hltnew, hltold]
  · have hne0 : n % p ≠ r := by intro he; exact hn (by rw [he]; simp)
    have hltold : r < n % p ∨ n % p < r := by
      by_cases hrlt : r < n % p
      · exact Or.inl hrlt
      · right; omega
    by_cases h : n % p + 1 < p
    · rcases hltold with hlt | hgt
      · have hltnew : r < n % p + 1 := by omega
        simp [hn, h, hlt, hltnew]
      · have hltnew : ¬ r < n % p + 1 := by omega
        have hnlt : ¬ r < n % p := by omega
        simp [hn, h, hnlt, hltnew]
    · have heq : n % p + 1 = p := by
        have hs' := Nat.mod_lt n hp
        omega
      have hlt : r < n % p := by omega
      have hltnew : ¬ r < 0 := by omega
      simp [hn, h, hlt, hltnew]

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
      rw [mod_step_count (n+1) p r hp hr, mod_step_count n p r hp hr]
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
          · right; omega
        by_cases h : n % p + 1 < p
        · rcases hltold with hlt | hgt
          · have hltnew : r < n % p + 1 := by omega
            simp [hn, h, hlt, hltnew]
          · have hltnew : ¬ r < n % p + 1 := by omega
            have hnlt : ¬ r < n % p := by omega
            simp [hn, h, hnlt, hltnew]
        · have heq : n % p + 1 = p := by
            have hs' := Nat.mod_lt n hp
            omega
          have hlt : r < n % p := by omega
          have hltnew : ¬ r < 0 := by omega
          simp [hn, h, hlt, hltnew]

private theorem sum_map_add_int {α : Type} (l : List α) (f g : α → Int) :
    ((l.map fun x => f x + g x).sum) = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem sum_ite_filter {α : Type} (l : List α) (p : α → Bool) (v : Int) :
    ((l.map (fun x => if p x then v else 0)).sum) =
      (((l.filter p).map (fun _ => v)).sum) := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

private theorem range_filter_eq_out (p a : Nat) (h : p ≤ a) :
    (List.range p).filter (fun r => r == a) = [] := by
  induction p with
  | zero => simp
  | succ p ih =>
      rw [List.range_succ, List.filter_append]
      have hne : (p == a) = false := beq_eq_false_iff_ne.mpr (by omega)
      rw [ih (by omega)]
      simp [hne]

private theorem range_filter_eq_singleton (p a : Nat) (ha : a < p) :
    (List.range p).filter (fun r => r == a) = [a] := by
  induction p with
  | zero => omega
  | succ p ih =>
      rw [List.range_succ, List.filter_append]
      by_cases hap : a < p
      · have htail : (List.filter (fun r => r == a) [p]) = [] := by
          have hne : (p == a) = false := beq_eq_false_iff_ne.mpr (by omega)
          simp [hne]
        rw [ih hap, htail]
        simp
      · have haeq : a = p := by omega
        subst a
        rw [range_filter_eq_out p p (by omega)]
        simp

private theorem range_filter_mod_singleton (n p : Nat) (hp : 0 < p) :
    (List.range p).filter (fun r => n % p == r) = [n % p] := by
  have hlt : n % p < p := Nat.mod_lt n hp
  have hf : (List.range p).filter (fun r => n % p == r) =
      (List.range p).filter (fun r => r == n % p) := by
    apply filter_congr_bool
    intro r hr
    by_cases h : n % p == r
    · have h' : r == n % p := by
        have heq : n % p = r := Nat.eq_of_beq_eq_true h
        rw [heq]
        simp
      simp [h, h']
    · have hne : n % p ≠ r := by intro he; exact h (by rw [he]; simp)
      have h' : ¬ r == n % p := by
        intro hh
        have heq : r = n % p := Nat.eq_of_beq_eq_true hh
        exact hne heq.symm
      simp [h, h']
  rw [hf, range_filter_eq_singleton p (n % p) hlt]

private theorem map_congr {α β : Type} (l : List α) (f g : α → β)
    (h : ∀ x ∈ l, f x = g x) : l.map f = l.map g := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem sum_congr_int {α : Type} (l : List α) (f g : α → Int)
    (h : ∀ x ∈ l, f x = g x) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem residue_partition_sum (n p : Nat) (hp : 0 < p) (f : Nat → Int) :
  ((List.range (min p n)).map (fun r =>
    ((List.range ((n - r + p - 1) / p)).map (fun q => f (r + q*p))).sum)).sum =
  ((List.range n).map f).sum := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ← ih]
      simp
      by_cases hnp : n < p
      · have hminn : min p n = n := Nat.min_eq_right (by omega)
        have hmins : min p (n+1) = n+1 := Nat.min_eq_right (by omega)
        rw [hminn, hmins]
        rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
        have hsum : ((List.range n).map (fun r =>
            ((List.range ((n+1-r+p-1)/p)).map (fun q => f (r+q*p))).sum)).sum =
            ((List.range n).map (fun r =>
            ((List.range ((n-r+p-1)/p)).map (fun q => f (r+q*p))).sum)).sum := by
          apply sum_congr_int
          intro r hr
          have hrp : r < p := by
            have := List.mem_range.mp hr
            omega
          have hnmod : n % p = n := Nat.mod_eq_of_lt hnp
          have hne : ¬ n % p == r := by
            intro hh
            have heq : n % p = r := Nat.eq_of_beq_eq_true hh
            have hmem := List.mem_range.mp hr
            omega
          rw [residue_count_succ n p r hp hrp]
          simp [hne]
        rw [hsum]
        have hlast : ((List.range ((n+1-n+p-1)/p)).map (fun q => f (n+q*p))).sum = f n := by
          have hcnt : (n+1-n+p-1)/p = 1 := by
            have hn1 : n + 1 - n = 1 := by omega
            rw [hn1]
            have hp1 : 1 + p - 1 = p := by omega
            rw [hp1, Nat.div_self hp]
          rw [hcnt]
          simp
        simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
        rw [hlast]
        omega
      · have hminn : min p n = p := Nat.min_eq_left (by omega)
        have hmins : min p (n+1) = p := Nat.min_eq_left (by omega)
        rw [hminn, hmins]
        have hsum : ((List.range p).map (fun r =>
            ((List.range ((n+1-r+p-1)/p)).map (fun q => f (r+q*p))).sum)).sum =
            ((List.range p).map (fun r =>
              ((List.range ((n-r+p-1)/p)).map (fun q => f (r+q*p))).sum +
                if n % p == r then f n else 0)).sum := by
          apply sum_congr_int
          intro r hr
          have hrp : r < p := List.mem_range.mp hr
          rw [residue_count_succ n p r hp hrp]
          by_cases hnr : n % p == r
          · rw [if_pos hnr, range_succ_map_sum]
            have hcnt : (n - r + p - 1) / p = n / p := by
              rw [mod_step_count n p r hp hrp]
              have hlt : ¬ r < n % p := by
                have heq : n % p = r := Nat.eq_of_beq_eq_true hnr
                omega
              simp [hlt]
            rw [hcnt]
            have hval : r + (n / p) * p = n := by
              have hd := Nat.div_add_mod n p
              rw [Nat.mul_comm p (n / p)] at hd
              have heq : n % p = r := Nat.eq_of_beq_eq_true hnr
              omega
            rw [hval]
            simp [hnr]
          · simp [hnr]
        rw [hsum, sum_map_add_int]
        have hif : ((List.range p).map (fun r => if n % p == r then f n else 0)).sum = f n := by
          rw [sum_ite_filter, range_filter_mod_singleton n p hp]
          simp
        rw [hif]

-- =====================================================================
-- Striped lanes: partition sums and generic slice state
-- =====================================================================

private theorem residue_count_eq_zero_of_ge (N r p : Nat) (hr : N ≤ r) (hp : 0 < p) :
    (N - r + p - 1) / p = 0 := by
  have hsub : N - r = 0 := by omega
  rw [hsub, Nat.zero_add]
  exact Nat.div_eq_of_lt (Nat.sub_lt hp (by omega : 0 < 1))

private theorem residue_partition_sum_range (n p k : Nat) (hp : 0 < p)
    (hkmin : min p n ≤ k) (hkp : k ≤ p) (f : Nat → Int) :
    ((List.range k).map (fun r =>
      ((List.range ((n - r + p - 1) / p)).map (fun q => f (r + q*p))).sum)).sum =
    ((List.range n).map f).sum := by
  induction k with
  | zero =>
      have hn0 : n = 0 := by omega
      subst n
      simp
  | succ k ih =>
      by_cases hk : min p n ≤ k
      · rw [List.range_succ, List.map_append, List.sum_append, ih hk (by omega)]
        have hp_le_n_false : ¬ p ≤ n := by
          intro hpn
          have hmin : min p n = p := Nat.min_eq_left hpn
          omega
        have hmin : min p n = n := Nat.min_eq_right (by omega)
        have hkge : n ≤ k := by omega
        have hz : (n - k + p - 1) / p = 0 :=
          residue_count_eq_zero_of_ge n k p hkge hp
        simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
        rw [hz]
        simp
      · have hkeq : k + 1 = min p n := by omega
        rw [show k + 1 = min p n by omega]
        exact residue_partition_sum n p hp f

private theorem sum_const_one_range (k : Nat) : ((List.range k).map (fun _ => (1 : Int))).sum = (k : Int) := by
  induction k with
  | zero => simp
  | succ k ih => rw [List.range_succ, List.map_append, List.sum_append, ih]; simp

private theorem sum_natCast_eq' {α : Type} (l : List α) (g : α → Nat) :
    ((l.map (fun x => (g x : Int))).sum) = (((l.map g).sum : Nat) : Int) := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Int.natCast_add]

private theorem residue_partition_count_range (n p k : Nat) (hp : 0 < p)
    (hkmin : min p n ≤ k) (hkp : k ≤ p) :
    ((List.range k).map (fun r => (n - r + p - 1) / p)).sum = n := by
  have h := residue_partition_sum_range n p k hp hkmin hkp (fun _ => (1 : Int))
  simp only [sum_const_one_range] at h
  rw [sum_natCast_eq' (List.range k) (fun r => (n - r + p - 1) / p)] at h
  exact_mod_cast h

private theorem beforePos_lane_eq_count (c : Config) (p r t : Nat) (z d : Int)
    (hp : 0 < p) (hr : r < p) :
    beforePos (lane c.n r p z d) t = (min c.n t - r + p - 1) / p := by
  unfold beforePos lane
  dsimp
  by_cases htc : c.n ≤ t
  · have hmin : min c.n t = c.n := Nat.min_eq_left htc
    rw [hmin]
    have hle : (c.n - r + p - 1) / p ≤ (t - r + p - 1) / p := by
      apply Nat.div_le_div_right
      omega
    rw [Nat.min_eq_left hle]
  · have ht : t < c.n := by omega
    have hmin : min c.n t = t := Nat.min_eq_right (by omega)
    rw [hmin]
    have hle : (t - r + p - 1) / p ≤ (c.n - r + p - 1) / p := by
      apply Nat.div_le_div_right
      omega
    rw [Nat.min_eq_right hle]

private theorem lane_value_of_striped (p : Nat) (hp : 0 < p) (r q : Nat) (hr : r < p)
    (f : Nat → Int) (make : Nat → Int × Int)
    (hval : ∀ i, f i = (make (i % p)).1 + (make (i % p)).2 * ((i / p : Nat) : Int)) :
    f (r + q*p) = (make r).1 + (make r).2 * (q : Int) := by
  have hmod : (r + q*p) % p = r := by
    rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hr]
  have hdiv : (r + q*p) / p = q := by
    rw [Nat.add_mul_div_right r q hp, Nat.div_eq_of_lt hr]
    simp
  rw [hval, hmod, hdiv]

private theorem filter_map_pair_range (n lo hi : Nat) (f : Nat → Int) :
    ((List.range n).map (fun i => (i, f i))).filter (fun r => lo ≤ r.1 && r.1 < hi) =
      ((List.range n).filter (fun i => lo ≤ i && i < hi)).map (fun i => (i, f i)) := by
  simpa using (map_pair_filter (List.range n) (fun i : Nat => i) f (fun x : Nat => lo ≤ x && x < hi))

private theorem residue_lt_of_count (N p r q : Nat) (hp : 0 < p) (hr : r < p)
    (hq : q < (N - r + p - 1) / p) : r + q * p < N := by
  by_cases hle : r ≤ N
  · have hmul : q * p < N - r + p - 1 - (p - 1) := (Nat.lt_div_iff_mul_lt hp).mp hq
    have hdiff : N - r + p - 1 - (p - 1) = N - r := by omega
    rw [hdiff] at hmul
    omega
  · have hz : (N - r + p - 1) / p = 0 := residue_count_eq_zero_of_ge N r p (by omega) hp
    omega

private theorem stripedSlice (c : Config) (p : Nat) (hp : 0 < p)
    (make : Nat → Int × Int) (f : Nat → Int)
    (hval : ∀ i, f i = (make (i % p)).1 + (make (i % p)).2 * ((i / p : Nat) : Int))
    (lo hi : Nat) (hle : lo ≤ hi) :
    let selected := ((List.range c.n).map (fun i => (i, f i))).filter (fun r => lo ≤ r.1 && r.1 < hi)
    (selected.length,(selected.map Prod.snd).sum) =
      (((striped c p make).map (fun a => beforePos a hi - beforePos a lo)).sum,
       ((striped c p make).map (fun a => partialSum a (beforePos a hi)-partialSum a (beforePos a lo))).sum) := by
  dsimp
  let Nhi := min c.n hi
  let Nlo := min c.n lo
  let k := min p c.n
  have hk_le_p : k ≤ p := Nat.min_le_left _ _
  have hNhi_le : Nhi ≤ c.n := Nat.min_le_left _ _
  have hNlo_le : Nlo ≤ c.n := Nat.min_le_left _ _
  have min_mono_right (a b c : Nat) (h : a ≤ b) : min c a ≤ min c b := by
    simp only [Nat.min_def]
    split <;> split <;> omega
  have min_mono_left (a b c : Nat) (h : a ≤ b) : min a c ≤ min b c := by
    simp only [Nat.min_def]
    split <;> split <;> omega
  have hkhi : min p Nhi ≤ k := by
    dsimp [k]
    exact min_mono_right Nhi c.n p hNhi_le
  have hklo : min p Nlo ≤ k := by
    dsimp [k]
    exact min_mono_right Nlo c.n p hNlo_le
  have hLenL : ((((List.range c.n).map (fun i => (i, f i))).filter (fun r => lo ≤ r.1 && r.1 < hi)).length) =
      Nhi - Nlo := by
    rw [filter_map_pair_range]
    simp only [List.length_map]
    rw [range_count_between c.n lo hi hle]
  have hLenR : (((striped c p make).map (fun a => beforePos a hi - beforePos a lo)).sum) = Nhi - Nlo := by
    unfold striped
    simp only [List.map_map, Function.comp_apply]
    change ((List.range (min p c.n)).map (fun r =>
      beforePos (lane c.n r p (make r).1 (make r).2) hi -
      beforePos (lane c.n r p (make r).1 (make r).2) lo)).sum = Nhi - Nlo
    rw [show (List.range (min p c.n)).map (fun r =>
      beforePos (lane c.n r p (make r).1 (make r).2) hi -
      beforePos (lane c.n r p (make r).1 (make r).2) lo) =
      (List.range k).map (fun r => (Nhi - r + p - 1)/p - (Nlo - r + p - 1)/p) by
        dsimp [k]
        apply map_congr
        intro r hr
        have hrp : r < p := by
          have := List.mem_range.mp hr
          omega
        rw [beforePos_lane_eq_count c p r hi (make r).1 (make r).2 hp hrp]
        rw [beforePos_lane_eq_count c p r lo (make r).1 (make r).2 hp hrp]]
    have hpoint : ∀ r ∈ List.range k, (Nlo - r + p - 1) / p ≤ (Nhi - r + p - 1) / p := by
      intro r hr
      apply Nat.div_le_div_right
      dsimp [Nhi, Nlo]
      have hlohi : min c.n lo ≤ min c.n hi := by
        rw [Nat.min_comm c.n lo, Nat.min_comm c.n hi]
        exact min_mono_left lo hi c.n hle
      omega
    rw [sum_map_sub_nat (h := hpoint)]
    rw [residue_partition_count_range Nhi p k hp hkhi hk_le_p]
    rw [residue_partition_count_range Nlo p k hp hklo hk_le_p]
  have hSumL : ((((List.range c.n).map (fun i => (i, f i))).filter (fun r => lo ≤ r.1 && r.1 < hi)).map Prod.snd).sum =
      ((List.range Nhi).map f).sum - ((List.range Nlo).map f).sum := by
    rw [filter_map_pair_range]
    simp only [List.map_map, Function.comp_apply]
    change (((List.range c.n).filter (fun i => lo ≤ i && i < hi)).map f).sum =
      ((List.range Nhi).map f).sum - ((List.range Nlo).map f).sum
    rw [range_sum_between c.n lo hi f hle]
  have hSumR : (((striped c p make).map (fun a => partialSum a (beforePos a hi) - partialSum a (beforePos a lo))).sum) =
      ((List.range Nhi).map f).sum - ((List.range Nlo).map f).sum := by
    unfold striped
    simp only [List.map_map, Function.comp_apply]
    change ((List.range (min p c.n)).map (fun r =>
      partialSum (lane c.n r p (make r).1 (make r).2)
        (beforePos (lane c.n r p (make r).1 (make r).2) hi) -
      partialSum (lane c.n r p (make r).1 (make r).2)
        (beforePos (lane c.n r p (make r).1 (make r).2) lo))).sum =
      ((List.range Nhi).map f).sum - ((List.range Nlo).map f).sum
    rw [show (List.range (min p c.n)).map (fun r =>
      partialSum (lane c.n r p (make r).1 (make r).2)
        (beforePos (lane c.n r p (make r).1 (make r).2) hi) -
      partialSum (lane c.n r p (make r).1 (make r).2)
        (beforePos (lane c.n r p (make r).1 (make r).2) lo)) =
      (List.range k).map (fun r =>
        ((List.range ((Nhi - r + p - 1)/p)).map (fun q => f (r+q*p))).sum -
        ((List.range ((Nlo - r + p - 1)/p)).map (fun q => f (r+q*p))).sum) by
        dsimp [k]
        apply map_congr
        intro r hr
        have hrp : r < p := by
          have := List.mem_range.mp hr
          omega
        rw [beforePos_lane_eq_count c p r hi (make r).1 (make r).2 hp hrp]
        rw [beforePos_lane_eq_count c p r lo (make r).1 (make r).2 hp hrp]
        rw [partialSum_eq_sum, partialSum_eq_sum]
        unfold lane
        dsimp
        congr 1
        · apply sum_congr_int
          intro q hq
          have hq' : q < (Nhi - r + p - 1)/p := List.mem_range.mp hq
          have hrq : r + q*p < Nhi := residue_lt_of_count Nhi p r q hp hrp hq'
          have hpos : r + q*p < c.n := by omega
          rw [lane_value_of_striped p hp r q hrp f make hval]
          simp [Int.mul_comm]
        · apply sum_congr_int
          intro q hq
          have hq' : q < (Nlo - r + p - 1)/p := List.mem_range.mp hq
          have hrq : r + q*p < Nlo := residue_lt_of_count Nlo p r q hp hrp hq'
          have hpos : r + q*p < c.n := by omega
          rw [lane_value_of_striped p hp r q hrp f make hval]
          simp [Int.mul_comm]]
    rw [sum_map_sub]
    rw [residue_partition_sum_range Nhi p k hp hkhi hk_le_p f]
    rw [residue_partition_sum_range Nlo p k hp hklo hk_le_p f]
  apply Prod.ext <;> simp only [Prod.fst, Prod.snd]
  · exact hLenL.trans hLenR.symm
  · exact hSumL.trans hSumR.symm



-- =====================================================================
-- Counting residues in a range
-- =====================================================================

private theorem count_residue_mod (n p r : Nat) (hp : 0 < p) (hr : r < p) :
    ((List.range n).filter (fun i => i % p == r)).length = (n - r + p - 1) / p := by
  induction n with
  | zero =>
      rw [residue_count_eq_zero_of_ge 0 r p (by omega) hp]
      simp
  | succ n ih =>
      rw [List.range_succ, List.filter_append, List.length_append, ih]
      rw [residue_count_succ n p r hp hr]
      by_cases h : n % p == r <;> simp [h]

private theorem length_filterMap_isSome {α β : Type} (l : List α) (f : α → Option β) :
    (l.filterMap f).length = (l.filter (fun x => (f x).isSome)).length := by
  induction l with
  | nil => simp
  | cons x xs ih => cases hfx : f x <;> simp [ih, hfx]

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


private theorem sum_filter_const {α : Type} (l : List α) (p : α → Bool) (v : Int) :
    (((l.filter p).map (fun _ => v)).sum) = v * ((l.filter p).length : Int) := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih, Int.mul_add] <;> omega

-- =====================================================================
-- LaneMajor
-- =====================================================================

namespace LaneMajor

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.LaneMajor

theorem SliceState_sound : SliceState.Goal := by
  intro c lo hi hle
  dsimp
  have hw : 0 < width c := by dsimp [width]; omega
  have hval : ∀ i : Nat,
      c.base + c.step * (i / width c : Nat) + c.bias * (i % width c : Nat) =
        (c.base + c.bias * (i % width c : Nat)) + c.step * (i / width c : Nat) := by
    intro i
    omega
  have hrows : rows layout c = (List.range c.n).map (fun i =>
      (i, c.base + c.step*(i/width c:Nat) + c.bias*(i%width c:Nat))) := by
    unfold rows
    apply range_filterMap_all
    intro i hi
    simp [layout]
  rw [hrows]
  exact stripedSlice c (width c) hw (fun r => (c.base + c.bias*r, c.step))
    (fun i => c.base + c.step*(i/width c:Nat) + c.bias*(i%width c:Nat))
    (fun i => by simpa using hval i) lo hi hle

-- TODO(FrequencySeparation)
-- TODO(ResidueJoin)
-- TODO(DeleteProgression)
-- TODO(AlignedProduct)

end LaneMajor

-- =====================================================================
-- RaggedStripe
-- =====================================================================


namespace RaggedStripe

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.RaggedStripe

-- TODO(SliceState)

theorem NullCount_sound : NullCount.Goal := by
  intro c
  let w := width c
  have hw : 0 < w := by dsimp [w, width]; omega
  have hlen : (rows layout c).length = ((List.range c.n).filter (fun i => !(i % w == w-1))).length := by
    unfold rows
    rw [length_filterMap_isSome]
    apply congrArg List.length
    apply filter_congr_bool
    intro i hi
    dsimp [w]
    by_cases h : i % width c == width c - 1
    · have heq : i % width c = width c - 1 := Nat.eq_of_beq_eq_true h
      simp [layout, h, heq]
    · have hne : i % width c ≠ width c - 1 := by intro he; exact h (by rw [he]; simp)
      simp [layout, h, hne]
  rw [hlen, length_filter_not]
  have hnone := count_residue_mod c.n w (w-1) hw (by omega : w-1 < w)
  have hwcount : (c.n - (w-1) + w - 1) / w = c.n / w := by
    by_cases hge : w-1 ≤ c.n
    · have hsub : c.n - (w-1) + w - 1 = c.n := by omega
      rw [hsub]
    · have hsub : c.n - (w-1) = 0 := by omega
      rw [hsub, Nat.zero_add]
      have hlt : w - 1 < w := by omega
      rw [Nat.div_eq_of_lt hlt]
      have hnlt : c.n < w := by omega
      rw [Nat.div_eq_of_lt hnlt]
  rw [hnone, hwcount]
  dsimp [w]
  simp

-- TODO(ResidueJoin)
-- TODO(DeleteProgression)
-- TODO(AlignedProduct)

end RaggedStripe

-- =====================================================================
-- Boustrophedon
-- =====================================================================

namespace Boustrophedon

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.Boustrophedon

theorem SliceState_sound : SliceState.Goal := by
  intro c lo hi hle
  dsimp
  let w := width c
  let per := 2 * w
  have hw : 0 < w := by dsimp [w, width]; omega
  have hper : 0 < per := by dsimp [per]; omega
  let make : Nat → Int × Int := fun r =>
    (c.base + c.step*(if r < w then r else 2*w-1-(r-w):Nat), c.step*per)
  let f : Nat → Int := fun i =>
    let k := i / w
    let r := i % w
    c.base + c.step*(k*w + (if k%2 == 0 then r else w-1-r):Nat)
  have hval : ∀ i : Nat, f i = (make (i % per)).1 + (make (i % per)).2 * ((i / per : Nat) : Int) := by
    intro i
    let s := i % per
    let q := i / per
    have hs_lt : s < per := by dsimp [s]; exact Nat.mod_lt i hper
    have hdecomp0 : i = per * q + s := by
      dsimp [s, q]
      exact (Nat.div_add_mod i per).symm
    have hmul : per * q = (2*q) * w := by
      dsimp [per]
      simp [Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm]
    have hdecomp : i = s + (2*q) * w := by omega
    by_cases hsw : s < w
    · have hdivw : i / w = 2 * q := by
        rw [hdecomp, Nat.add_mul_div_right s (2*q) hw]
        have hsw0 : s / w = 0 := Nat.div_eq_of_lt hsw
        rw [hsw0]
        omega
      have hmodw : i % w = s := by
        rw [hdecomp, Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hsw]
      have hpar : (i / w) % 2 = 0 := by
        rw [hdivw]
        exact Nat.mul_mod_right 2 q
      have hmake : (make s).1 = c.base + c.step*(s:Int) := by
        dsimp [make]
        rw [if_pos hsw]
      have hgoal : f i = (make s).1 + (make s).2 * (q : Int) := by
        have hinside : (i / w * w + (if (i / w) % 2 == 0 then i % w else w - 1 - (i % w) : Nat)) =
            (2*q) * w + s := by
          rw [hdivw, hmodw]
          simp [hpar]
        change c.base + c.step * ((i / w * w + (if (i / w) % 2 == 0 then i % w else w - 1 - (i % w)) : Nat)) =
          (make (i % per)).1 + (make (i % per)).2 * (i / per : Int)
        rw [hinside]
        have hmodper : (i % per) = s := rfl
        rw [hmodper]
        have hdivper : (i / per) = q := rfl
        rw [Int.ofNat_ediv_ofNat]
        rw [hdivper]
        dsimp [make]
        rw [if_pos hsw]
        dsimp [per]
        simp [Int.natCast_add, Int.mul_add, Int.mul_assoc, Int.mul_comm, Int.mul_left_comm]
        omega
      simpa [s, q] using hgoal
    · have hswge : w ≤ s := by omega
      let t := s - w
      have htlt : t < w := by dsimp [t]; dsimp [per] at hs_lt; omega
      have hs_eq : s = w + t := by dsimp [t]; omega
      have hi : i = t + (2*q+1) * w := by
        rw [hdecomp, hs_eq]
        simp [Nat.mul_add, Nat.add_mul, Nat.mul_assoc, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
      have hdivw : i / w = 2*q + 1 := by
        rw [hi, Nat.add_mul_div_right t (2*q+1) hw]
        have htw0 : t / w = 0 := Nat.div_eq_of_lt htlt
        rw [htw0]
        omega
      have hmodw : i % w = t := by
        rw [hi, Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt htlt]
      have hpar : (i / w) % 2 = 1 := by
        rw [hdivw]
        omega
      have hgoal : f i = (make s).1 + (make s).2 * (q : Int) := by
        have hinside : (i / w * w + (if (i / w) % 2 == 0 then i % w else w - 1 - (i % w) : Nat)) =
            (2*q+1) * w + (w-1-t) := by
          rw [hdivw, hmodw]
          simp [hpar]
        change c.base + c.step * ((i / w * w + (if (i / w) % 2 == 0 then i % w else w - 1 - (i % w)) : Nat)) =
          (make (i % per)).1 + (make (i % per)).2 * (i / per : Int)
        rw [hinside]
        have hmodper : (i % per) = s := rfl
        rw [hmodper]
        have hdivper : (i / per) = q := rfl
        rw [Int.ofNat_ediv_ofNat]
        rw [hdivper]
        dsimp [make]
        rw [if_neg hsw]
        rw [hs_eq]
        have hsub : 2*w - 1 - (w+t-w) = w + (w-1-t) := by omega
        rw [hsub]
        dsimp [per]
        simp [Int.natCast_add, Int.mul_add, Int.mul_assoc, Int.mul_comm, Int.mul_left_comm]
        omega
      simpa [s, q] using hgoal
  have hrows : rows layout c = (List.range c.n).map (fun i => (i, f i)) := by
    unfold rows
    apply range_filterMap_all
    intro i hi
    simp [layout, f, w]
  rw [hrows]
  have hlanes : layout.lanes c = striped c per make := by
    dsimp [layout, striped, make, per, w]
  rw [hlanes]
  exact stripedSlice c per hper make f hval lo hi hle

-- TODO(FullTiles)
-- TODO(ResidueJoin)
-- TODO(DeleteProgression)
-- TODO(AlignedProduct)

end Boustrophedon

-- =====================================================================
-- PeriodicPatch
-- =====================================================================

namespace PeriodicPatch

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.PeriodicPatch

theorem SliceState_sound : SliceState.Goal := by
  intro c lo hi hle
  dsimp
  let w := width c
  have hw : 0 < w := by dsimp [w, width]; omega
  let make : Nat → Int × Int := fun r =>
    (c.base + c.step*r + (if r == 0 then c.bias else 0), c.step*w)
  let f : Nat → Int := fun i =>
    c.base + c.step*(i:Int) + (if i % w == 0 then c.bias else 0)
  have hval : ∀ i : Nat, f i = (make (i % w)).1 + (make (i % w)).2 * ((i / w : Nat) : Int) := by
    intro i
    let r := i % w
    let q := i / w
    have hrlt : r < w := by dsimp [r]; exact Nat.mod_lt i hw
    have hi : i = r + q*w := by
      dsimp [r, q]
      have hd := Nat.div_add_mod i w
      rw [Nat.mul_comm w (i / w)] at hd
      omega
    have hcast : (i : Int) = (r : Int) + (q : Int) * (w : Int) := by exact_mod_cast hi
    have hq' : (r + q*w) / w = q := by
      rw [Nat.add_mul_div_right r q hw]
      simp [Nat.div_eq_of_lt hrlt]
    have hnum : ((r + q*w : Nat) : Int) = (r : Int) + (q : Int) * (w : Int) := by
      simp [Int.natCast_add, Int.natCast_mul]
    have hr' : (r + q*w) % w = r := by
      rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hrlt]
    by_cases hr0 : r == 0
    · have hr0' : r = 0 := Nat.eq_of_beq_eq_true hr0
      have himod : i % w == 0 := by simp [hi, hr0', Nat.mod_eq_of_lt hw]
      dsimp [f, make]
      rw [hcast]
      rw [← hnum]
      rw [Int.ofNat_mod_ofNat, Int.ofNat_ediv_ofNat, hq', hr']
      simp [himod, Int.mul_add, Int.mul_comm, Int.mul_assoc, Int.natCast_add]
      omega
    · have hr0' : r ≠ 0 := by intro he; exact hr0 (by rw [he]; simp)
      have himod : ¬ i % w == 0 := by
        rw [show i % w = r by simp [hi, Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hrlt]]
        exact hr0
      dsimp [f, make]
      rw [hcast]
      rw [← hnum]
      rw [Int.ofNat_mod_ofNat, Int.ofNat_ediv_ofNat, hq', hr']
      simp [himod, Int.mul_add, Int.mul_comm, Int.mul_assoc, Int.natCast_add]
      omega
  have hrows : rows layout c = (List.range c.n).map (fun i => (i, f i)) := by
    unfold rows
    apply range_filterMap_all
    intro i hi
    simp [layout, f, w]
  rw [hrows]
  have hlanes : layout.lanes c = striped c w make := by
    dsimp [layout, striped, make, w]
  rw [hlanes]
  exact stripedSlice c w hw make f hval lo hi hle


theorem PatchBudget_sound : PatchBudget.Goal := by
  intro c
  dsimp
  let w := width c
  have hw : 0 < w := by dsimp [w, width]; omega
  let f : Nat → Int := fun i => c.base + c.step*(i:Int) + (if i%w == 0 then c.bias else 0)
  have hrowsall : rows layout c = (List.range c.n).map (fun i : Nat => (i, f i)) := by
    unfold rows
    exact range_filterMap_all c.n
      (fun i => (layout.read c i).map (fun v => (i,v)))
      (fun i : Nat => (i, f i)) (by intro i hi; simp [layout, w, f])
  have hrows : (rows layout c).map Prod.snd = (List.range c.n).map f := by
    rw [hrowsall]
    simp
  rw [hrows, sum_map_add_int]
  let g : Nat → Int := fun i => c.base + c.step*(i:Int)
  have hlinall : rows TinylambOpt.ResearchGoals.Atlas.EncodedPages.LinearOrdinal.layout c =
      (List.range c.n).map (fun i : Nat => (i, g i)) := by
    unfold rows
    exact range_filterMap_all c.n
      (fun i => (TinylambOpt.ResearchGoals.Atlas.EncodedPages.LinearOrdinal.layout.read c i).map (fun v => (i,v)))
      (fun i : Nat => (i, g i)) (by intro i hi; simp [TinylambOpt.ResearchGoals.Atlas.EncodedPages.LinearOrdinal.layout, g])
  have hbase : ((List.range c.n).map g).sum =
      ((rows TinylambOpt.ResearchGoals.Atlas.EncodedPages.LinearOrdinal.layout c).map Prod.snd).sum := by
    rw [hlinall]
    simp only [List.map_map, Function.comp_apply]
    rfl
  rw [hbase]
  have hite : ((List.range c.n).map (fun i : Nat => if i % w == 0 then c.bias else 0)).sum =
      c.bias * (((c.n+w-1)/w : Nat) : Int) := by
    rw [sum_ite_filter, sum_filter_const, count_residue_mod c.n w 0 hw (by omega)]
    simp
  rw [hite]
  dsimp [w]
-- TODO(ResidueJoin)
-- TODO(DeleteProgression)
-- TODO(AlignedProduct)

end PeriodicPatch

-- =====================================================================
-- SparseSample
-- =====================================================================

namespace SparseSample

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.SparseSample

theorem SliceState_sound : SliceState.Goal := by
  intro c lo hi hle
  dsimp
  let w := width c
  let r := c.split % w
  have hw : 0 < w := by dsimp [w, width]; omega
  have hr : r < w := by dsimp [r]; exact Nat.mod_lt _ hw
  let a : Lane := lane c.n r w c.base c.step
  have ha_stride : 0 < a.stride := by dsimp [a, lane]; exact hw
  have hrows : rows layout c = lanePoints a := by
    unfold rows
    have hf : (List.range c.n).filterMap (fun i => (layout.read c i).map (fun v => (i,v))) =
        (List.range c.n).filterMap (fun i =>
          if i % w == r then some (i, c.base + c.step*(i/w:Nat)) else none) := by
      apply filterMap_congr'
      intro i hi
      simp [layout, w, r]
    rw [hf]
    have hf2 : (List.range c.n).filterMap (fun i : Nat =>
          if i % w == r then some (i, c.base + c.step*(i/w:Nat)) else none) =
        ((List.range c.n).filter (fun i => i % w == r)).map
          (fun i : Nat => (i, c.base + c.step*(i/w:Nat))) :=
      filterMap_ite_eq_filter_map (List.range c.n) (fun i : Nat => i % w == r)
        (fun i : Nat => (i, c.base + c.step*(i/w:Nat)))
    rw [hf2, range_filter_mod_eq c.n w r hw hr]
    unfold lanePoints a lane
    simp only [List.map_map, Function.comp_apply]
    apply map_congr
    intro q hq
    have hdiv : (r + q*w) / w = q := by
      rw [Nat.add_mul_div_right r q hw, Nat.div_eq_of_lt hr]
      simp
    simp only [Function.comp_apply]
    rw [hdiv]
    simp [Int.mul_comm]
  rw [hrows]
  have hflat : [a].flatMap lanePoints = lanePoints a := by simp
  rw [← hflat]
  have hpos : ∀ b ∈ [a], 0 < b.stride := by intro b hb; simp at hb; subst b; exact ha_stride
  exact slice_flatMap [a] hpos lo hi hle

-- TODO(PredicateRank)
-- TODO(ResidueJoin)
-- TODO(DeleteProgression)
-- TODO(AlignedProduct)

end SparseSample

end TinylambOpt.ResearchProofs.Atlas_EncodedPages_G3
