import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E7

open TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-! # Generic list and lane algebra -/

private theorem list_sum_perm {l₁ l₂ : List Int} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem perm_length {α : Type} {l₁ l₂ : List α} (p : l₁.Perm l₂) :
    l₁.length = l₂.length := p.length_eq

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

private theorem sum_map_add_int {α : Type} (l : List α) (f g : α → Int) :
    ((l.map fun x => f x + g x).sum) = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem sum_map_sub {α : Type} (l : List α) (f g : α → Int) :
    ((l.map fun x => f x - g x).sum) = (l.map f).sum - (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem sum_map_add_nat {α : Type} (l : List α) (f g : α → Nat) :
    ((l.map fun x => f x + g x).sum) = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem sum_map_le_nat {α : Type} (l : List α) (f g : α → Nat)
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
      have hle : (xs.map g).sum ≤ (xs.map f).sum :=
        sum_map_le_nat xs g f (fun y hy => h y (by simp [hy]))
      have hx : g x ≤ f x := h x (by simp)
      simp only [List.map_cons, List.sum_cons]
      rw [ih (fun y hy => h y (by simp [hy]))]
      omega

private theorem countP_eq_sum {α : Type} (l : List α) (p : α → Bool) :
    l.countP p = (l.map (fun x => if p x then 1 else 0)).sum := by
  induction l with
  | nil => rfl
  | cons x xs ih => by_cases h : p x <;> simp [h, ih] <;> omega

private theorem countP_between_eq_sub {α : Type} (l : List α) (p q : α → Bool)
    (h : ∀ x ∈ l, q x = true → p x = true) :
    l.countP (fun x => p x && !q x) = l.countP p - l.countP q := by
  have hqlen : l.countP q = (l.filter q).length := List.countP_eq_length_filter
  have hqa : l.countP q = (l.filter q).countP p := by
    rw [hqlen]
    exact (List.countP_eq_length.mpr (fun x hx =>
      h x (List.mem_filter.mp hx).1 (List.mem_filter.mp hx).2)).symm
  have hleft : l.countP (fun x => p x && !q x) = (l.filter fun a => !q a).countP p := by
    rw [← List.countP_filter (p := p) (q := fun a => !q a)]
  have hpart := List.countP_eq_countP_filter_add l p q
  rw [hleft, hpart, hqa]
  exact (Nat.add_sub_cancel_left _ _).symm

private theorem sum_map_ite_zero {α : Type} (l : List α) (p : α → Bool) (f : α → Int) :
    ((l.filter p).map f).sum = (l.map (fun x => if p x then f x else 0)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

private theorem sum_map_const_int {α : Type} (l : List α) (v : Int) :
    (l.map (fun _ => v)).sum = (l.length : Int) * v := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Int.natCast_add, Int.add_mul]; omega

private theorem sum_range_succ (f : Nat → Int) (n : Nat) :
    ((List.range (n + 1)).map f).sum = ((List.range n).map f).sum + f n := by
  rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
  simp

private theorem sum_natCast_eq {α : Type} (l : List α) (g : α → Nat) :
    ((l.map (fun x => (g x : Int))).sum) = (((l.map g).sum : Nat) : Int) := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Int.natCast_add]

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

private theorem squares_succ (k : Nat) :
    squares (k + 1) = squares k + (k : Nat) * k := by
  unfold squares
  have hkm1 : k + 1 - 1 = k := by omega
  rw [hkm1]
  have h2 : 2 * (k + 1) - 1 = 2 * k + 1 := by omega
  rw [h2]
  have hp : (k + 1) * k * (2 * k + 1) = k * (k - 1) * (2 * k - 1) + 6 * (k * k) := by
    by_cases hk : k = 0
    · simp [hk]
    · obtain ⟨m, rfl⟩ := Nat.exists_eq_succ_of_ne_zero hk
      have hm : m + 1 - 1 = m := by omega
      have h2m : 2 * (m + 1) - 1 = 2 * m + 1 := by omega
      rw [hm, h2m]
      change (m + 1 + 1) * (m + 1) * (2 * (m + 1) + 1) =
        (m + 1) * m * (2 * m + 1) + 6 * ((m + 1) * (m + 1))
      grind
  rw [hp, Nat.add_mul_div_left _ _ (by decide : 0 < 6)]
  simp [Int.natCast_add, Int.natCast_mul]

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
    beforePos a p = min a.n ((p - a.start + a.stride - 1) / a.stride) := by
  unfold beforePos
  rfl

private def lanePoints (a : Lane) : List (Nat × Int) :=
  (List.range a.n).map (fun i : Nat => (a.start + i * a.stride, a.base + (i : Int) * a.step))

private theorem lanePoints_filter_eq (a : Lane) (lo hi : Nat) (hs : 0 < a.stride) :
    (lanePoints a).filter (fun r => lo ≤ r.1 && r.1 < hi) =
      ((List.range a.n).filter (fun i : Nat => lo ≤ a.start + i * a.stride ∧ a.start + i * a.stride < hi)).map
        (fun i : Nat => (a.start + i * a.stride, a.base + (i : Int) * a.step)) := by
  unfold lanePoints
  simpa using (map_pair_filter (List.range a.n) (fun i : Nat => a.start + i * a.stride)
    (fun i : Nat => a.base + (i : Int) * a.step) (fun p : Nat => lo ≤ p && p < hi))

private theorem lane_filter_indices_eq (a : Lane) (lo hi : Nat) (hs : 0 < a.stride) :
    (List.range a.n).filter (fun i : Nat => lo ≤ a.start + i * a.stride ∧ a.start + i * a.stride < hi) =
      (List.range a.n).filter (fun i : Nat =>
        (lo - a.start + a.stride - 1) / a.stride ≤ i && i < (hi - a.start + a.stride - 1) / a.stride) := by
  let thi := (hi - a.start + a.stride - 1) / a.stride
  let tlo := (lo - a.start + a.stride - 1) / a.stride
  have hiff_hi : ∀ i, a.start + i * a.stride < hi ↔ i < thi :=
    fun i => pos_lt_iff a.start a.stride i hi hs
  have hiff_lo : ∀ i, a.start + i * a.stride < lo ↔ i < tlo :=
    fun i => pos_lt_iff a.start a.stride i lo hs
  apply filter_congr_bool
  intro i _
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

private theorem lane_slice_count (a : Lane) (lo hi : Nat) (hle : lo ≤ hi) (hs : 0 < a.stride) :
    ((lanePoints a).filter (fun r => lo ≤ r.1 && r.1 < hi)).length = beforePos a hi - beforePos a lo := by
  rw [lanePoints_filter_eq a lo hi hs]
  simp only [List.length_map]
  rw [lane_filter_indices_eq a lo hi hs]
  apply range_count_between
  apply Nat.div_le_div_right
  omega

private theorem lane_slice_sum (a : Lane) (lo hi : Nat) (hle : lo ≤ hi) (hs : 0 < a.stride) :
    (((lanePoints a).filter (fun r => lo ≤ r.1 && r.1 < hi)).map Prod.snd).sum =
      partialSum a (beforePos a hi) - partialSum a (beforePos a lo) := by
  rw [lanePoints_filter_eq a lo hi hs]
  rw [List.map_map]
  change (((List.range a.n).filter (fun i : Nat => lo ≤ a.start + i * a.stride ∧ a.start + i * a.stride < hi)).map
    (fun i : Nat => a.base + (i : Int) * a.step)).sum =
      partialSum a (beforePos a hi) - partialSum a (beforePos a lo)
  let thi := (hi - a.start + a.stride - 1) / a.stride
  let tlo := (lo - a.start + a.stride - 1) / a.stride
  have htle : tlo ≤ thi := by
    dsimp [tlo, thi]
    apply Nat.div_le_div_right
    omega
  rw [lane_filter_indices_eq a lo hi hs]
  change (((List.range a.n).filter (fun i : Nat => tlo ≤ i && i < thi)).map
    (fun i : Nat => a.base + (i : Int) * a.step)).sum =
      partialSum a (beforePos a hi) - partialSum a (beforePos a lo)
  rw [range_sum_between a.n tlo thi (fun i : Nat => a.base + (i : Int) * a.step) htle]
  rw [← partialSum_eq_sum a (min a.n thi), ← partialSum_eq_sum a (min a.n tlo)]
  unfold beforePos
  rfl

private theorem flatMap_filter_length (ls : List Lane) (p : Nat × Int → Bool) :
    ((ls.flatMap lanePoints).filter p).length =
      (ls.map (fun a => ((lanePoints a).filter p).length)).sum := by
  induction ls with
  | nil => simp [lanePoints]
  | cons a as ih => simp [List.filter_append, ih]

private theorem flatMap_filter_snd_sum (ls : List Lane) (p : Nat × Int → Bool) :
    (((ls.flatMap lanePoints).filter p).map Prod.snd).sum =
      (ls.map (fun a => (((lanePoints a).filter p).map Prod.snd).sum)).sum := by
  induction ls with
  | nil => simp [lanePoints]
  | cons a as ih => simp [List.filter_append, List.map_append, List.sum_append, ih]

private theorem slice_from_perm (lay : Layout) (c : Config) (lo hi : Nat) (hle : lo ≤ hi)
    (hperm : (rows lay c).Perm ((lay.lanes c).flatMap lanePoints))
    (hs : ∀ a ∈ lay.lanes c, 0 < a.stride) :
    let selected := (rows lay c).filter (fun r => lo ≤ r.1 && r.1 < hi)
    let ls := lay.lanes c
    (selected.length,(selected.map Prod.snd).sum) =
      ((ls.map (fun a => beforePos a hi - beforePos a lo)).sum,
       (ls.map (fun a => partialSum a (beforePos a hi)-partialSum a (beforePos a lo))).sum) := by
  dsimp only
  let p : Nat × Int → Bool := fun r => lo ≤ r.1 && r.1 < hi
  have hf : ((rows lay c).filter p).Perm (((lay.lanes c).flatMap lanePoints).filter p) := hperm.filter p
  apply Prod.ext
  · rw [perm_length hf, flatMap_filter_length]
    apply congrArg List.sum
    apply List.map_congr_left
    intro a ha
    exact lane_slice_count a lo hi hle (hs a ha)
  · rw [list_sum_perm (hf.map Prod.snd), flatMap_filter_snd_sum]
    apply congrArg List.sum
    apply List.map_congr_left
    intro a ha
    exact lane_slice_sum a lo hi hle (hs a ha)

/-! # Arithmetic progressions and their intersection -/

private def progVal (a : Progression) (k : Nat) : Int := a.lo + (k : Int) * a.step

private def progMem (a : Progression) (v : Int) : Prop :=
  ∃ k, k < a.n ∧ v = progVal a k

private theorem contains_iff_mem (a : Progression) (v : Int) :
    contains a v = true ↔ progMem a v := by
  unfold contains lastVal progMem progVal
  constructor
  · intro h
    have h' : 0 < a.n ∧ a.lo ≤ v ∧ v ≤ a.lo + ((a.n - 1 : Nat) : Int) * a.step ∧
        (a.step == 0 || (v - a.lo) % (a.step : Int) == 0) = true := by
      simpa only [Bool.and_eq_true, decide_eq_true_eq, and_assoc] using h
    rcases h' with ⟨hn, hlo, hhi, hmod⟩
    by_cases hs : a.step == 0
    · have hs' : a.step = 0 := Nat.eq_of_beq_eq_true hs
      refine ⟨0, hn, ?_⟩
      simp only [hs', Int.mul_zero, Int.add_zero] at hhi hlo ⊢
      omega
    · have hs' : a.step ≠ 0 := by simpa using hs
      have hpos : (0 : Int) < a.step := by exact_mod_cast Nat.pos_of_ne_zero hs'
      have hmod' : (v - a.lo) % (a.step : Int) = 0 := by
        simpa [hs] using hmod
      let q := (v - a.lo) / a.step
      have hqmul : a.step * q = v - a.lo := by
        simpa only [Int.mul_comm] using Int.ediv_mul_cancel_of_emod_eq_zero hmod'
      have hqmul' : q * a.step = v - a.lo := by simpa only [Int.mul_comm] using hqmul
      have hqnon : 0 ≤ q := Int.nonneg_of_mul_nonneg_right (by omega) hpos
      have hqle : q ≤ (a.n - 1 : Nat) := by
        apply Int.le_of_mul_le_mul_left (b := q) (c := (a.n - 1 : Nat)) _ hpos
        change a.step * ((v - a.lo) / a.step) ≤ a.step * (a.n - 1 : Nat)
        have hh : a.step * ((v - a.lo) / a.step) = v - a.lo := by
          simpa only [Int.mul_comm] using Int.ediv_mul_cancel_of_emod_eq_zero hmod'
        rw [hh]
        have hh2 := Int.sub_le_sub_right hhi a.lo
        have hcancel : a.lo + ((a.n - 1 : Nat) : Int) * a.step - a.lo =
            ((a.n - 1 : Nat) : Int) * a.step := by omega
        rw [hcancel] at hh2
        simpa only [Int.mul_comm] using hh2
      have hqnat : q.toNat < a.n := by
        have hle : q.toNat ≤ a.n - 1 := (Int.toNat_le (m := q) (n := a.n - 1)).mpr hqle
        omega
      refine ⟨q.toNat, hqnat, ?_⟩
      rw [Int.toNat_of_nonneg hqnon]
      symm
      calc
        a.lo + q * a.step = a.lo + (v - a.lo) := congrArg (fun x : Int => a.lo + x) hqmul'
        _ = v := by omega
  · rintro ⟨k, hk, rfl⟩
    change (decide (0 < a.n) && decide (a.lo ≤ a.lo + (k : Int) * a.step) &&
      decide (a.lo + (k : Int) * a.step ≤ a.lo + ((a.n - 1 : Nat) : Int) * a.step) &&
      (a.step == 0 || (a.lo + (k : Int) * a.step - a.lo) % (a.step : Int) == 0)) = true
    have hsub : a.lo + (k : Int) * a.step - a.lo = (k : Int) * a.step := by omega
    rw [hsub]
    by_cases hs : a.step = 0
    · simp [hs]
      omega
    · have hpos : (0 : Int) < a.step := by exact_mod_cast Nat.pos_of_ne_zero hs
      have hkint : (k : Int) ≤ (a.n - 1 : Nat) := by exact_mod_cast (by omega)
      have hmul : a.step * (k : Int) ≤ a.step * (a.n - 1 : Nat) := by
        simpa only [Int.mul_comm] using Int.mul_le_mul_of_nonneg_left hkint (Int.le_of_lt hpos)
      have hmul' : (k : Int) * a.step ≤ ((a.n - 1 : Nat) : Int) * a.step := by
        simpa only [Int.mul_comm] using hmul
      simp only [Bool.and_eq_true, decide_eq_true_eq, and_assoc]
      refine ⟨by omega, ?_, ?_, ?_⟩
      · exact Int.le_add_of_nonneg_right (Int.mul_nonneg (by omega) (Int.le_of_lt hpos))
      · exact Int.add_le_add_left hmul' a.lo
      · rw [Bool.or_eq_true]
        right
        change (((k : Int) * a.step) % a.step == 0) = true
        rw [beq_iff_eq]
        apply Int.emod_eq_zero_of_dvd
        exact ⟨(k : Int), Int.mul_comm _ _⟩

private theorem bezout_spec (a b : Nat) :
    (a.gcd b : Int) = (a : Int) * (bezout a b).1 + (b : Int) * (bezout a b).2 := by
  induction b using Nat.strongRecOn generalizing a with
  | ind b ih =>
      by_cases hb : b = 0
      · subst b
        simp [bezout]
      · have hmod : a % b < b := Nat.mod_lt a (Nat.pos_of_ne_zero hb)
        have hrec := ih (a % b) hmod b
        have hfst : (bezout a b).1 = (bezout b (a % b)).2 := by
          rw [bezout.eq_1]
          simp [hb]
        have hsnd : (bezout a b).2 =
            (bezout b (a % b)).1 - ((a / b : Nat) : Int) * (bezout b (a % b)).2 := by
          rw [bezout.eq_1]
          simp [hb]
        rw [hfst, hsnd]
        rcases hbez : bezout b (a % b) with ⟨x, y⟩
        simp only [hbez]
        have hgcd : a.gcd b = b.gcd (a % b) := by
          rw [Nat.gcd_comm a b, Nat.gcd_rec b a, Nat.gcd_comm]
        rw [hgcd, hrec]
        have hd : (a : Int) = (b : Int) * ((a / b : Nat) : Int) + ((a % b : Nat) : Int) := by
          exact_mod_cast (Nat.div_add_mod a b).symm
        rw [hd]
        grind

private theorem reduced_coprime (a b : Nat) (ha : 0 < a) (hb : 0 < b) :
    (a / a.gcd b).gcd (b / a.gcd b) = 1 := by
  exact Nat.gcd_div_gcd_div_gcd_of_pos_left ha

private theorem bezout_reduced_inverse (a b : Nat) (ha : 0 < a) (hb : 0 < b) :
    ((((a / a.gcd b : Nat) : Int) * (bezout (a / a.gcd b) (b / a.gcd b)).1) - 1) %
        (b / a.gcd b : Nat) = 0 := by
  let g := a.gcd b
  let A := a / g
  let B := b / g
  have hg : 0 < g := Nat.gcd_pos_of_pos_left b ha
  have hA : 0 < A := Nat.div_gcd_pos_of_pos_left b ha
  have hB : 0 < B := Nat.div_gcd_pos_of_pos_right a hb
  have hcop : A.gcd B = 1 := by
    simpa [A, B, g] using reduced_coprime a b ha hb
  have hspec := bezout_spec A B
  rw [hcop] at hspec
  simp only [Int.ofNat_one] at hspec
  apply Int.emod_eq_zero_of_dvd
  refine ⟨-(bezout A B).2, ?_⟩
  change (A : Int) * (bezout A B).1 - 1 = (B : Int) * -(bezout A B).2
  rw [hspec]
  grind

private theorem mem_of_bounds_dvd (a : Progression) (v : Int) (hn : 0 < a.n) (hs : 0 < a.step)
    (hlo : a.lo ≤ v) (hhi : v ≤ lastVal a) (hdvd : (a.step : Int) ∣ v - a.lo) :
    contains a v = true := by
  obtain ⟨q, hq⟩ := hdvd
  have hpos : (0 : Int) < a.step := by exact_mod_cast hs
  have hqnon : 0 ≤ q := Int.nonneg_of_mul_nonneg_right (by omega) hpos
  have hqle : q ≤ (a.n - 1 : Nat) := by
    apply Int.le_of_mul_le_mul_left (b := q) (c := (a.n - 1 : Nat)) _ hpos
    rw [← hq]
    unfold lastVal at hhi
    rw [Int.mul_comm ((a.n - 1 : Nat) : Int) a.step] at hhi
    omega
  apply (contains_iff_mem a v).mpr
  refine ⟨q.toNat, ?_, ?_⟩
  · have hle : q.toNat ≤ a.n - 1 := (Int.toNat_le (m := q) (n := a.n - 1)).mpr hqle
    omega
  · unfold progVal
    rw [Int.toNat_of_nonneg hqnon]
    change v = a.lo + q * a.step
    rw [Int.mul_comm a.step q] at hq
    omega

private theorem span_contains_iff (first step high x : Int) (hs : 0 < step) :
    let n := if first ≤ high then ((high - first) / step).toNat + 1 else 0
    contains ⟨first, step.toNat, n⟩ x = true ↔
      first ≤ x ∧ x ≤ high ∧ (step : Int) ∣ x - first := by
  dsimp only
  constructor
  · intro hx
    have hm := (contains_iff_mem ⟨first, step.toNat, if first ≤ high then ((high - first) / step).toNat + 1 else 0⟩ x).mp hx
    obtain ⟨k, hk, hval⟩ := hm
    unfold progVal at hval
    have hstepcast : (step.toNat : Int) = step := Int.toNat_of_nonneg (by omega)
    simp only [hstepcast] at hval
    by_cases hfh : first ≤ high
    · change k < (if first ≤ high then ((high - first) / step).toNat + 1 else 0) at hk
      rw [if_pos hfh] at hk
      have ht : 0 ≤ (high - first) / step :=
        Int.ediv_nonneg (by omega) (Int.le_of_lt hs)
      have htcast : (((high - first) / step).toNat : Int) = (high - first) / step :=
        Int.toNat_of_nonneg ht
      have hkq : (k : Int) ≤ (high - first) / step :=
        (Int.le_ediv_iff_mul_le hs).mpr (by
          have hkNat : k ≤ ((high - first) / step).toNat := by omega
          have hkInt : (k : Int) ≤ (((high - first) / step).toNat : Int) := by exact_mod_cast hkNat
          calc
            (k : Int) * step ≤ (((high - first) / step).toNat : Int) * step :=
              Int.mul_le_mul_of_nonneg_right hkInt (Int.le_of_lt hs)
            _ = ((high - first) / step) * step := by rw [htcast]
            _ ≤ high - first := Int.ediv_mul_le _ (by omega))
      have hkle : (k : Int) * step ≤ high - first :=
        (Int.le_ediv_iff_mul_le hs).mp hkq
      refine ⟨?_, ?_, ?_⟩
      · rw [hval]
        exact Int.le_add_of_nonneg_right (Int.mul_nonneg (by omega) (Int.le_of_lt hs))
      · calc
          x = first + (k : Int) * step := hval
          _ ≤ first + (high - first) := Int.add_le_add_left hkle first
          _ = high := by omega
      · refine ⟨(k : Int), ?_⟩
        calc
          x - first = (k : Int) * step := by omega
          _ = step * (k : Int) := Int.mul_comm _ _
    · simp [hfh] at hk
  · rintro ⟨hlo, hhi, hdvd⟩
    have hfh : first ≤ high := by omega
    rw [if_pos hfh]
    obtain ⟨q, hq⟩ := hdvd
    have hqnon : 0 ≤ q := Int.nonneg_of_mul_nonneg_right (by omega) hs
    have htcast : (((high - first) / step).toNat : Int) = (high - first) / step :=
      Int.toNat_of_nonneg (Int.ediv_nonneg (by omega) (Int.le_of_lt hs))
    have hqle : q ≤ (high - first) / step := by
      apply (Int.le_ediv_iff_mul_le hs).mpr
      calc
        q * step = x - first := by rw [Int.mul_comm q step]; exact hq.symm
        _ ≤ high - first := Int.sub_le_sub_right hhi first
    apply (contains_iff_mem ⟨first, step.toNat, ((high - first) / step).toNat + 1⟩ x).mpr
    refine ⟨q.toNat, ?_, ?_⟩
    · have hle : q.toNat ≤ ((high - first) / step).toNat := by
        apply (Int.toNat_le (m := q) (n := ((high - first) / step).toNat)).mpr
        rw [htcast]
        exact hqle
      exact Nat.lt_succ_of_le hle
    · unfold progVal
      rw [Int.toNat_of_nonneg hqnon]
      change x = first + q * (step.toNat : Int)
      rw [Int.toNat_of_nonneg (by omega : 0 ≤ step)]
      rw [Int.mul_comm step q] at hq
      omega

private theorem root_dvd (as bs : Nat) (ha : 0 < as) (hb : 0 < bs) (alo blo gap : Int)
    (hgapdef : gap = blo - alo) (hgap : (as.gcd bs : Int) ∣ gap) :
    let g := as.gcd bs
    let A := as / g
    let B := bs / g
    let root := alo + (as : Int) * ((gap / (g : Int) * (bezout A B).1) % (B : Int))
    (as : Int) ∣ root - alo ∧ (bs : Int) ∣ root - blo := by
  dsimp only
  let g := as.gcd bs
  let A := as / g
  let B := bs / g
  let t := (gap / (g : Int) * (bezout A B).1) % (B : Int)
  have hg : 0 < g := Nat.gcd_pos_of_pos_left bs ha
  have hA : 0 < A := Nat.div_gcd_pos_of_pos_left bs ha
  have hB : 0 < B := Nat.div_gcd_pos_of_pos_right as hb
  have hasg : (as : Int) = (g : Int) * (A : Int) := by
    exact_mod_cast (Nat.mul_div_cancel' (Nat.gcd_dvd_left as bs)).symm
  have hbsg : (bs : Int) = (g : Int) * (B : Int) := by
    exact_mod_cast (Nat.mul_div_cancel' (Nat.gcd_dvd_right as bs)).symm
  obtain ⟨z, hz⟩ := hgap
  have hzdiv : gap / (g : Int) = z := Int.ediv_eq_of_eq_mul_right (by omega) hz
  have hinv := bezout_reduced_inverse as bs ha hb
  have hinv' : (B : Int) ∣ (A : Int) * (bezout A B).1 - 1 :=
    Int.dvd_of_emod_eq_zero (by simpa [A, B, g] using hinv)
  have hmod := Int.mul_ediv_add_emod (z * (bezout A B).1) (B : Int)
  have hrem : z * (bezout A B).1 - t =
      (B : Int) * ((z * (bezout A B).1) / (B : Int)) := by
    dsimp [t]
    rw [hzdiv]
    omega
  have hdiff : (B : Int) ∣ z * ((A : Int) * (bezout A B).1 - 1) :=
    Int.dvd_mul_of_dvd_right hinv'
  have hmult : (B : Int) ∣ (A : Int) * (z * (bezout A B).1 - t) :=
    Int.dvd_mul_of_dvd_right (Int.dvd_of_emod_eq_zero (by
      rw [hrem]
      exact Int.emod_eq_zero_of_dvd ⟨_, rfl⟩))
  have hcomb : (B : Int) ∣ (A : Int) * t - z := by
    have heq : z * ((A : Int) * (bezout A B).1 - 1) -
        (A : Int) * (z * (bezout A B).1 - t) = (A : Int) * t - z := by grind
    rw [← heq]
    exact Int.dvd_sub hdiff hmult
  have hbs : (bs : Int) ∣ (as : Int) * t - gap := by
    rw [hbsg, hasg, hz]
    have hh := Int.mul_dvd_mul_left (g : Int) hcomb
    simpa only [Int.mul_sub, Int.mul_assoc] using hh
  constructor
  · have hroot : alo + (as : Int) * ((gap / (g : Int) * (bezout A B).1) % (B : Int)) - alo =
        (as : Int) * t := by dsimp [t]; omega
    rw [hroot]
    exact ⟨t, rfl⟩
  · have hroot : alo + (as : Int) * ((gap / (g : Int) * (bezout A B).1) % (B : Int)) - blo =
        (as : Int) * t - gap := by dsimp [t]; omega
    rw [hroot]
    exact hbs

private theorem first_root_dvd (low root first period : Int) (hp : 0 < period)
    (hfirst : first = low + (root - low) % period) : period ∣ first - root := by
  have hm := Int.mul_ediv_add_emod (root - low) period
  refine ⟨-((root - low) / period), ?_⟩
  grind

private theorem first_le_of_congruence (low root first x period : Int) (hp : 0 < period)
    (hfirst : first = low + (root - low) % period) (hx : low ≤ x)
    (hxdvd : period ∣ x - root) : first ≤ x := by
  have hremnon : 0 ≤ (root - low) % period := Int.emod_nonneg _ (by omega)
  have hremlt : (root - low) % period < period := Int.emod_lt_of_pos _ hp
  have hflow : low ≤ first := by omega
  have hfhigh : first < low + period := by omega
  have hfroot := first_root_dvd low root first period hp hfirst
  by_cases hle : first ≤ x
  · exact hle
  · exfalso
    have hlt : x < first := by omega
    have hpos : 0 < first - x := by omega
    have hltp : first - x < period := by omega
    have hdiff : period ∣ first - x := by
      have heq : (first - root) - (x - root) = first - x := by omega
      rw [← heq]
      exact Int.dvd_sub hfroot hxdvd
    obtain ⟨k, hk⟩ := hdiff
    have hkpos : 0 < k := Int.pos_of_mul_pos_right (by omega) hp
    have hkge : 1 ≤ k := by omega
    have hmul := Int.mul_le_mul_of_nonneg_left hkge (Int.le_of_lt hp)
    omega

private theorem contains_const_iff (lo : Int) (n : Nat) (v : Int) :
    contains ⟨lo, 0, n⟩ v = true ↔ 0 < n ∧ v = lo := by
  unfold contains lastVal
  simp
  omega

private theorem contains_unit01_iff (lo : Int) (P : Prop) [Decidable P] (v : Int) :
    contains ⟨lo, 1, if P then 1 else 0⟩ v = true ↔ P ∧ v = lo := by
  rw [contains_iff_mem]
  constructor
  · rintro ⟨k, hk, hval⟩
    unfold progVal at hval
    by_cases hp : P
    · simp [hp] at hk
      have hk0 : k = 0 := by omega
      subst k
      exact ⟨hp, by simpa [progVal] using hval⟩
    · simp [hp] at hk
  · rintro ⟨hp, rfl⟩
    refine ⟨0, by simp [hp], ?_⟩
    simp [progVal]

private theorem contains_bounds (a : Progression) (v : Int) (h : contains a v = true) :
    a.lo ≤ v ∧ v ≤ lastVal a := by
  have hm := (contains_iff_mem a v).mp h
  obtain ⟨k, hk, hval⟩ := hm
  unfold progVal at hval
  have hsnon : (0 : Int) ≤ (a.step : Int) := by exact_mod_cast (Nat.zero_le a.step)
  have hkint : (k : Int) ≤ ((a.n - 1 : Nat) : Int) := by exact_mod_cast (by omega)
  have hmul' : (k : Int) * (a.step : Int) ≤ ((a.n - 1 : Nat) : Int) * (a.step : Int) :=
    Int.mul_le_mul_of_nonneg_right hkint hsnon
  unfold lastVal
  constructor
  · rw [hval]
    exact Int.le_add_of_nonneg_right (Int.mul_nonneg (by exact_mod_cast Nat.zero_le k) hsnon)
  · rw [hval]
    exact Int.add_le_add_left hmul' a.lo

private theorem contains_dvd (a : Progression) (v : Int) (h : contains a v = true) :
    (a.step : Int) ∣ v - a.lo := by
  obtain ⟨k, _, hval⟩ := (contains_iff_mem a v).mp h
  unfold progVal at hval
  exact ⟨(k : Int), by rw [Int.mul_comm]; omega⟩

private theorem common_gap_dvd (a b : Progression) (v : Int)
    (ha : contains a v = true) (hb : contains b v = true) :
    (a.step.gcd b.step : Int) ∣ b.lo - a.lo := by
  obtain ⟨i, _, hi⟩ := (contains_iff_mem a v).mp ha
  obtain ⟨j, _, hj⟩ := (contains_iff_mem b v).mp hb
  unfold progVal at hi hj
  have hg1 : ((a.step.gcd b.step : Nat) : Int) ∣ (b.step : Int) := by
    exact_mod_cast (Nat.gcd_dvd_right a.step b.step)
  have hg2 : ((a.step.gcd b.step : Nat) : Int) ∣ (a.step : Int) := by
    exact_mod_cast (Nat.gcd_dvd_left a.step b.step)
  have h1' : ((a.step.gcd b.step : Nat) : Int) ∣ (j : Int) * (b.step : Int) :=
    Int.dvd_mul_of_dvd_right hg1
  have h1 : ((a.step.gcd b.step : Nat) : Int) ∣ (b.step : Int) * (j : Int) := by
    simpa [Int.mul_comm] using h1'
  have h2' : ((a.step.gcd b.step : Nat) : Int) ∣ (i : Int) * (a.step : Int) :=
    Int.dvd_mul_of_dvd_right hg2
  have h2 : ((a.step.gcd b.step : Nat) : Int) ∣ (a.step : Int) * (i : Int) := by
    simpa [Int.mul_comm] using h2'
  have heq : (a.step : Int) * (i : Int) - (b.step : Int) * (j : Int) = b.lo - a.lo := by
    have hi' : (a.step : Int) * (i : Int) = (i : Int) * (a.step : Int) := Int.mul_comm _ _
    have hj' : (b.step : Int) * (j : Int) = (j : Int) * (b.step : Int) := Int.mul_comm _ _
    omega
  rw [← heq]
  exact Int.dvd_sub h2 h1

private theorem contains_step_one_ite (lo : Int) (c : Bool) (v : Int) :
    contains ⟨lo, 1, if c then 1 else 0⟩ v = true ↔ c = true ∧ v = lo := by
  cases c <;> simp [contains, lastVal] <;> omega

private theorem intersection_step0_left (a b : Progression) (han : a.n ≠ 0) (hbn : b.n ≠ 0)
    (has : a.step = 0) :
    intersection a b = ⟨a.lo, 1, if contains b a.lo then 1 else 0⟩ := by
  unfold intersection
  simp [han, hbn, has]

private theorem intersection_step0_right (a b : Progression) (han : a.n ≠ 0) (hbn : b.n ≠ 0)
    (has : a.step ≠ 0) (hbs : b.step = 0) :
    intersection a b = ⟨b.lo, 1, if contains a b.lo then 1 else 0⟩ := by
  unfold intersection
  simp [han, hbn, has, hbs]

private theorem intersection_main_form (a b : Progression) (han : a.n ≠ 0) (hbn : b.n ≠ 0)
    (has : a.step ≠ 0) (hbs : b.step ≠ 0) :
    let g := a.step.gcd b.step
    let gap := b.lo - a.lo
    let period := (a.step / g) * b.step
    let unit := (bezout (a.step / g) (b.step / g)).1
    let root := a.lo + (a.step : Int) * ((gap / (g : Int) * unit) % (b.step / g : Nat))
    let low := max a.lo b.lo
    let first := low + (root - low) % (period : Int)
    let high := min (lastVal a) (lastVal b)
    intersection a b = if gap % (g : Int) != 0 then ⟨0, 1, 0⟩ else
      ⟨first, period, if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ := by
  dsimp only
  unfold intersection
  simp [han, hbn, has, hbs]
  by_cases hgap : (b.lo - a.lo) % (a.step.gcd b.step : Int) = 0 <;> simp [hgap]

private theorem intersection_contains (a b : Progression) (v : Int) :
    contains (intersection a b) v = true ↔ contains a v = true ∧ contains b v = true := by
  by_cases han : a.n = 0
  · have hca : contains a v = false := by simp [contains, han]
    simp [hca, intersection, han, contains]
  · by_cases hbn : b.n = 0
    · have hcb : contains b v = false := by simp [contains, hbn]
      simp [hcb, intersection, han, hbn, contains]
    · have hanp : 0 < a.n := by omega
      have hbnp : 0 < b.n := by omega
      by_cases has : a.step = 0
      · rw [intersection_step0_left a b han hbn has]
        rw [contains_step_one_ite]
        have haconst : contains a v = true ↔ 0 < a.n ∧ v = a.lo := by
          rw [contains_iff_mem]
          constructor
          · rintro ⟨k, hk, hval⟩
            unfold progVal at hval
            simp [has] at hval
            exact ⟨by omega, hval⟩
          · rintro ⟨hn, rfl⟩
            exact ⟨0, hn, by simp [progVal, has]⟩
        constructor
        · intro h
          constructor
          · exact haconst.mpr ⟨hanp, h.2⟩
          · rw [h.2]
            by_cases hba : contains b a.lo
            · exact hba
            · have hz : 0 < (0 : Nat) := by simpa [hba] using h.1
              omega
        · rintro ⟨ha, hb⟩
          have hvn : v = a.lo := (haconst.mp ha).2
          have hba : contains b a.lo = true := by rw [← hvn]; exact hb
          exact ⟨by simp [hba], hvn⟩
      · by_cases hbs : b.step = 0
        · rw [intersection_step0_right a b han hbn has hbs]
          rw [contains_step_one_ite]
          have hbconst : contains b v = true ↔ 0 < b.n ∧ v = b.lo := by
            rw [contains_iff_mem]
            constructor
            · rintro ⟨k, hk, hval⟩
              unfold progVal at hval
              simp [hbs] at hval
              exact ⟨by omega, hval⟩
            · rintro ⟨hn, rfl⟩
              exact ⟨0, hn, by simp [progVal, hbs]⟩
          constructor
          · intro h
            constructor
            · have hva : v = b.lo := h.2
              rw [hva]
              by_cases hab : contains a b.lo
              · exact hab
              · have hz : 0 < (0 : Nat) := by simpa [hab] using h.1
                omega
            · exact hbconst.mpr ⟨hbnp, h.2⟩
          · rintro ⟨ha, hb⟩
            have hvn : v = b.lo := (hbconst.mp hb).2
            have hab : contains a b.lo = true := by rw [← hvn]; exact ha
            exact ⟨by simp [hab], hvn⟩
        · have hasp : 0 < a.step := Nat.pos_of_ne_zero has
          have hbsp : 0 < b.step := Nat.pos_of_ne_zero hbs
          let g := a.step.gcd b.step
          let gap := b.lo - a.lo
          let period := (a.step / g) * b.step
          let unit := (bezout (a.step / g) (b.step / g)).1
          let root := a.lo + (a.step : Int) * ((gap / (g : Int) * unit) % ((b.step / g : Nat) : Int))
          let low := max a.lo b.lo
          let first := low + (root - low) % (period : Int)
          let high := min (lastVal a) (lastVal b)
          have hg : 0 < g := by
            dsimp [g]
            exact Nat.gcd_pos_of_pos_left b.step hasp
          have hdiv : 0 < a.step / g := by
            apply Nat.div_pos
            · exact Nat.le_of_dvd hasp (by dsimp [g]; exact Nat.gcd_dvd_left a.step b.step)
            · exact hg
          have hp : (0 : Int) < period := by
            have hpnat : 0 < period := by
              dsimp [period]
              exact Nat.mul_pos hdiv hbsp
            exact_mod_cast hpnat
          have hgapdef : gap = b.lo - a.lo := rfl
          by_cases hgapzero : gap % (g : Int) = 0
          · have hgap : (g : Int) ∣ gap := Int.dvd_of_emod_eq_zero hgapzero
            have hroot := root_dvd a.step b.step hasp hbsp a.lo b.lo gap hgapdef hgap
            have hfirst : first = low + (root - low) % (period : Int) := rfl
            have hfroot := first_root_dvd low root first period hp hfirst
            have hremnon : 0 ≤ (root - low) % (period : Int) := Int.emod_nonneg _ (by omega)
            have hflow : low ≤ first := by dsimp [first]; omega
            have hga : g * (a.step / g) = a.step :=
              Nat.mul_div_cancel' (Nat.gcd_dvd_left a.step b.step)
            have hgb : b.step / g * g = b.step :=
              Nat.div_mul_cancel (Nat.gcd_dvd_right a.step b.step)
            have hper_eq : period = a.step * (b.step / g) := by
              dsimp [period]
              calc
                a.step / g * b.step = a.step / g * (b.step / g * g) := by rw [hgb]
                _ = (a.step / g * (b.step / g)) * g := by ac_rfl
                _ = (g * (a.step / g)) * (b.step / g) := by ac_rfl
                _ = a.step * (b.step / g) := by rw [hga]
            have hper_eq2 : period = b.step * (a.step / g) := by
              dsimp [period]
              rw [Nat.mul_comm]
            have hpa : (a.step : Int) ∣ (period : Int) := by
              refine ⟨((b.step / g : Nat) : Int), ?_⟩
              exact_mod_cast hper_eq
            have hpb : (b.step : Int) ∣ (period : Int) := by
              refine ⟨((a.step / g : Nat) : Int), ?_⟩
              exact_mod_cast hper_eq2
            rw [intersection_main_form a b han hbn has hbs]
            rw [if_neg (by simpa [gap, g] using hgapzero)]
            change contains ⟨first, period, if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ v = true ↔ _
            have hspan := span_contains_iff first (period : Int) high v hp
            dsimp only at hspan
            rw [show ((period : Int).toNat) = period by simp] at hspan
            rw [hspan]
            constructor
            · rintro ⟨hfirstx, hxhigh, hxfirst⟩
              have hxroot : (period : Int) ∣ v - root := by
                have heq : v - root = (v - first) + (first - root) := by omega
                rw [heq]
                exact Int.dvd_add hxfirst hfroot
              have hastep_root : (a.step : Int) ∣ v - root := Int.dvd_trans hpa hxroot
              have hbstep_root : (b.step : Int) ∣ v - root := Int.dvd_trans hpb hxroot
              have hsuma : (v - root) + (root - a.lo) = v - a.lo := by omega
              have hsumb : (v - root) + (root - b.lo) = v - b.lo := by omega
              have hxalo : (a.step : Int) ∣ v - a.lo := by
                rw [← hsuma]
                exact Int.dvd_add hastep_root hroot.1
              have hxblo : (b.step : Int) ∣ v - b.lo := by
                rw [← hsumb]
                exact Int.dvd_add hbstep_root hroot.2
              constructor
              · exact mem_of_bounds_dvd a v hanp hasp (by omega) (by omega) hxalo
              · exact mem_of_bounds_dvd b v hbnp hbsp (by omega) (by omega) hxblo
            · rintro ⟨ha, hb⟩
              have hab := contains_bounds a v ha
              have hbb := contains_bounds b v hb
              have hxblo := contains_dvd b v hb
              have hastep_root : (a.step : Int) ∣ v - root := by
                have heq : v - root = (v - a.lo) - (root - a.lo) := by omega
                rw [heq]
                exact Int.dvd_sub (contains_dvd a v ha) hroot.1
              have hbstep_root : (b.step : Int) ∣ v - root := by
                have heq : v - root = (v - b.lo) - (root - b.lo) := by omega
                rw [heq]
                exact Int.dvd_sub hxblo hroot.2
              have hxroot : (period : Int) ∣ v - root := by
                have hlcm : (Int.lcm (a.step : Int) (b.step : Int) : Int) ∣ v - root :=
                  Int.coe_lcm_dvd hastep_root hbstep_root
                have hperiodlcm : period = Int.lcm (a.step : Int) (b.step : Int) := by
                  calc
                    period = a.step * (b.step / g) := hper_eq
                    _ = a.step * b.step / g :=
                      (Nat.mul_div_assoc a.step (Nat.gcd_dvd_right a.step b.step)).symm
                    _ = a.step.lcm b.step := by rw [← Nat.lcm_eq_mul_div]
                    _ = Int.lcm (a.step : Int) (b.step : Int) := rfl
                rw [hperiodlcm]
                exact hlcm
              have hxfirst : (period : Int) ∣ v - first := by
                have heq : v - first = (v - root) - (first - root) := by omega
                rw [heq]
                exact Int.dvd_sub hxroot hfroot
              have hlowv : low ≤ v := by
                dsimp [low]
                exact Int.max_le.mpr ⟨hab.1, hbb.1⟩
              have hvhigh : v ≤ high := by
                dsimp [high]
                exact Int.le_min.mpr ⟨hab.2, hbb.2⟩
              exact ⟨first_le_of_congruence low root first v period hp hfirst hlowv hxroot,
                hvhigh, hxfirst⟩
          · have hgapne : gap % (g : Int) ≠ 0 := hgapzero
            rw [intersection_main_form a b han hbn has hbs]
            rw [if_pos (by simpa [gap, g] using hgapne)]
            change contains ⟨0, 1, 0⟩ v = true ↔ contains a v = true ∧ contains b v = true
            constructor
            · intro h
              simp [contains] at h
            · rintro ⟨ha, hb⟩
              have hg' := common_gap_dvd a b v ha hb
              exfalso
              exact hgapne (Int.emod_eq_zero_of_dvd hg')


/--! ## Enumerating progression values and counting matches -/

private def progVals (p : Progression) : List Int := (List.range p.n).map (progVal p)

private theorem mem_progVals (p : Progression) (v : Int) :
    v ∈ progVals p ↔ contains p v = true := by
  unfold progVals
  rw [List.mem_map, contains_iff_mem]
  constructor
  · rintro ⟨k, hk, hkv⟩
    exact ⟨k, List.mem_range.mp hk, hkv.symm⟩
  · rintro ⟨k, hk, hkv⟩
    exact ⟨k, List.mem_range.mpr hk, hkv.symm⟩

private theorem progVal_inj_of_pos (p : Progression) (hp : 0 < p.step) {i j : Nat}
    (h : progVal p i = progVal p j) : i = j := by
  unfold progVal at h
  have hdiff : ((i : Int) - (j : Int)) * (p.step : Int) = 0 := by grind
  have hsne : (p.step : Int) ≠ 0 := by exact_mod_cast (Nat.ne_of_gt hp)
  have hij : (i : Int) = (j : Int) := by
    rcases Int.mul_eq_zero.mp hdiff with hz | hz
    · omega
    · exact False.elim (hsne hz)
  exact_mod_cast hij

private theorem progVals_nodup (p : Progression) (hp : 0 < p.step) : (progVals p).Nodup := by
  unfold progVals
  exact List.Pairwise.map (progVal p)
    (fun _ _ hne h => hne (progVal_inj_of_pos p hp h)) List.nodup_range

private theorem count_progVals_eq (p : Progression) (hp : 0 < p.step) (v : Int) :
    (progVals p).countP (fun x => x == v) = if contains p v then 1 else 0 := by
  rw [List.countP_eq_length_filter]
  by_cases hv : contains p v = true
  · rw [if_pos hv]
    have hperm : List.Perm ((progVals p).filter (fun x => x == v)) [v] := by
      refine (List.perm_ext_iff_of_nodup ((progVals_nodup p hp).filter _) (by simp)).mpr ?_
      intro x
      rw [List.mem_filter, mem_progVals]
      simp only [List.mem_singleton]
      constructor
      · rintro ⟨_, hxvB⟩
        exact beq_iff_eq.mp hxvB
      · intro hxv
        subst x
        exact ⟨hv, by simp⟩
    rw [hperm.length_eq]
    simp
  · rw [if_neg hv]
    have hnil : (progVals p).filter (fun x => x == v) = [] := by
      apply List.filter_eq_nil_iff.mpr
      intro x hx hxv
      have heq : x = v := of_decide_eq_true hxv
      rw [heq] at hx
      exact hv ((mem_progVals p v).mp hx)
    rw [hnil]
    simp

private theorem filter_progVals_perm (p r : Progression) (hp : 0 < p.step) (hr : 0 < r.step)
    (hsub : ∀ v, contains r v = true → contains p v = true) :
    List.Perm ((progVals p).filter (fun v => contains r v)) (progVals r) := by
  refine (List.perm_ext_iff_of_nodup ((progVals_nodup p hp).filter _) (progVals_nodup r hr)).mpr ?_
  intro v
  rw [List.mem_filter, mem_progVals, mem_progVals]
  constructor
  · rintro ⟨hpv, hrv⟩
    exact hrv
  · intro hrv
    exact ⟨hsub v hrv, hrv⟩

private theorem contains_n_zero (lo : Int) (s : Nat) (v : Int) :
    contains ⟨lo, s, 0⟩ v = false := by
  simp [contains]

private theorem intersection_step_pos (p q : Progression) : 0 < (intersection p q).step := by
  by_cases h0 : (p.n == 0 || q.n == 0) = true
  · simp [intersection, h0]
  · by_cases hp0 : p.step = 0
    · simp [intersection, h0, hp0]
    · by_cases hq0 : q.step = 0
      · simp [intersection, h0, hp0, hq0]
      · by_cases hgap : ((q.lo - p.lo) % (p.step.gcd q.step : Int) != 0) = true
        · simp [intersection, h0, hp0, hq0, hgap]
        · have hpp : 0 < p.step := Nat.pos_of_ne_zero hp0
          have hqq : 0 < q.step := Nat.pos_of_ne_zero hq0
          simp [intersection, h0, hp0, hq0, hgap]
          exact Nat.mul_pos (Nat.div_pos (Nat.le_of_dvd hpp (Nat.gcd_dvd_left p.step q.step))
            (Nat.gcd_pos_of_pos_left q.step hpp)) hqq

private theorem count_common_prog (p q : Progression) (hp : 0 < p.step) (hq : 0 < q.step) :
    ((progVals p).filter (fun v => contains q v)).length = (intersection p q).n := by
  let r := intersection p q
  have hr : 0 < r.step := by dsimp [r]; exact intersection_step_pos p q
  have hsub : ∀ v, contains r v = true → contains p v = true := by
    intro v hv
    exact (intersection_contains p q v).mp hv |>.1
  have hperm : List.Perm ((progVals p).filter (fun v => contains r v)) (progVals r) :=
    filter_progVals_perm p r hp hr hsub
  have hfilter : (progVals p).filter (fun v => contains q v) =
      (progVals p).filter (fun v => contains r v) := by
    apply filter_congr_bool
    intro v hv
    change decide (contains q v = true) = decide (contains r v = true)
    apply decide_eq_of_iff
    constructor
    · intro hq'
      dsimp [r]
      exact (intersection_contains p q v).mpr ⟨(mem_progVals p v).mp hv, hq'⟩
    · intro hr'
      exact (intersection_contains p q v).mp hr' |>.2
  rw [hfilter, hperm.length_eq]
  change (progVals r).length = r.n
  unfold progVals
  simp


/--! ## Lane values and join multiplicity -/

private def laneVal (a : Lane) (i : Nat) : Int := a.base + (i : Int) * a.step
private def laneVals (a : Lane) : List Int := (List.range a.n).map (laneVal a)
private def lanePos (a : Lane) (i : Nat) : Nat := a.start + i * a.stride

private theorem progVal_values_pos (a : Lane) (h : 0 ≤ a.step) (k : Nat) :
    progVal (values a) k = laneVal a k := by
  simp [progVal, values, laneVal, show ¬ a.step < 0 by omega, Int.natAbs_of_nonneg h]

private theorem progVal_values_neg (a : Lane) (h : a.step < 0) (k : Nat) (hk : k < a.n) :
    progVal (values a) k = laneVal a (a.n - 1 - k) := by
  have hnat : (a.step.natAbs : Int) = -a.step := by
    have hnn : 0 ≤ -a.step := by omega
    rw [← Int.natAbs_neg]
    exact Int.natAbs_of_nonneg hnn
  simp [progVal, values, laneVal, h, hnat]
  have hk' : k ≤ a.n - 1 := by omega
  rw [Int.ofNat_sub hk']
  have hcast : ((a.n - 1 : Nat) : Int) = (a.n : Int) - 1 := by omega
  rw [hcast]
  grind

private theorem contains_values_of_lane (a : Lane) (v : Int) :
    contains (values a) v = true ↔ ∃ i, i < a.n ∧ laneVal a i = v := by
  rw [contains_iff_mem]
  constructor
  · rintro ⟨k, hk0, hval⟩
    change k < a.n at hk0
    have hk : k < a.n := hk0
    by_cases h : 0 ≤ a.step
    · have hp := progVal_values_pos a h k
      rw [hp] at hval
      exact ⟨k, hk, hval.symm⟩
    · have hneg : a.step < 0 := by omega
      have hp := progVal_values_neg a hneg k hk
      rw [hp] at hval
      exact ⟨a.n - 1 - k, by omega, hval.symm⟩
  · rintro ⟨i, hi, hival⟩
    change i < a.n at hi
    by_cases h : 0 ≤ a.step
    · refine ⟨i, hi, ?_⟩
      rw [progVal_values_pos a h i, hival]
    · have hneg : a.step < 0 := by omega
      let k := a.n - 1 - i
      refine ⟨k, by change k < a.n; omega, ?_⟩
      rw [progVal_values_neg a hneg k (by omega)]
      have hidx : a.n - 1 - k = i := by dsimp [k]; omega
      rw [hidx, hival]

private theorem values_step_pos (a : Lane) (ha : a.step ≠ 0) : 0 < (values a).step := by
  unfold values
  exact Nat.pos_of_ne_zero (by simpa using (Int.natAbs_ne_zero.mpr ha))

private theorem laneVal_inj (a : Lane) (ha : a.step ≠ 0) {i j : Nat}
    (h : laneVal a i = laneVal a j) : i = j := by
  unfold laneVal at h
  have hdiff : ((i : Int) - (j : Int)) * a.step = 0 := by grind
  have hsne : a.step ≠ 0 := ha
  have hij : (i : Int) = (j : Int) := by
    rcases Int.mul_eq_zero.mp hdiff with hz | hz
    · omega
    · exact False.elim (hsne hz)
  exact_mod_cast hij

private theorem laneVals_nodup (a : Lane) (ha : a.step ≠ 0) : (laneVals a).Nodup := by
  unfold laneVals
  exact List.Pairwise.map (laneVal a)
    (fun _ _ hne h => hne (laneVal_inj a ha h)) List.nodup_range

private theorem laneVals_perm_progVals_values (a : Lane) (ha : a.step ≠ 0) :
    List.Perm (laneVals a) (progVals (values a)) := by
  refine (List.perm_ext_iff_of_nodup (laneVals_nodup a ha)
    (progVals_nodup (values a) (values_step_pos a ha))).mpr ?_
  intro v
  rw [mem_progVals, contains_values_of_lane]
  rw [laneVals, List.mem_map]
  constructor
  · rintro ⟨i, hi, hiv⟩
    exact ⟨i, List.mem_range.mp hi, hiv⟩
  · rintro ⟨i, hi, hiv⟩
    exact ⟨i, List.mem_range.mpr hi, hiv⟩

private theorem countP_perm {α : Type} {l₁ l₂ : List α} (p : List.Perm l₁ l₂) (f : α → Bool) :
    l₁.countP f = l₂.countP f := by
  rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
  exact (p.filter f).length_eq

private theorem count_laneVals_eq (a : Lane) (ha : a.step ≠ 0) (v : Int) :
    (laneVals a).countP (fun x => x == v) = if contains (values a) v then 1 else 0 := by
  rw [countP_perm (laneVals_perm_progVals_values a ha) (fun x => x == v)]
  exact count_progVals_eq (values a) (values_step_pos a ha) v

private theorem laneVals_const (a : Lane) (ha : a.step = 0) :
    laneVals a = (List.range a.n).map (fun _ => a.base) := by
  unfold laneVals laneVal
  simp [ha]

private theorem countP_swap (l : List Int) (v : Int) :
    l.countP (fun x => v == x) = l.countP (fun x => x == v) := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hb : (v == x) = (x == v) := by
        by_cases h : v = x
        · subst v
          rfl
        · have h' : ¬ x = v := by intro hh; exact h hh.symm
          rw [beq_eq_false_iff_ne.mpr h, beq_eq_false_iff_ne.mpr h']
      cases hvx : v == x <;> cases hxv : x == v <;> simp [hvx, hxv, ih] at hb ⊢

private theorem sum_map_const_nat {α : Type} (l : List α) (v : Nat) :
    (l.map (fun _ => v)).sum = l.length * v := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Nat.succ_mul]; omega

private theorem sum_map_mul_nat {α : Type} (l : List α) (f : α → Nat) (c : Nat) :
    (l.map (fun x => f x * c)).sum = (l.map f).sum * c := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Nat.add_mul]

private theorem countP_const_eq (v w : Int) (n : Nat) :
    ((List.range n).map (fun _ => w)).countP (fun x => v == x) = if v == w then n else 0 := by
  rw [List.countP_eq_length_filter]
  by_cases h : v == w
  · have hvw : v = w := beq_iff_eq.mp h
    have hf : (List.filter (fun x => v == x) ((List.range n).map (fun _ => w))) =
        (List.range n).map (fun _ => w) := by
      apply List.filter_eq_self.mpr
      intro x hx
      rw [List.mem_map] at hx
      obtain ⟨i, hi, rfl⟩ := hx
      simpa using h
    rw [hf]
    simp [h, hvw]
  · have hvw : v ≠ w := by intro h'; exact h (by subst v; simp)
    have hf : (List.filter (fun x => v == x) ((List.range n).map (fun _ => w))) = [] := by
      apply List.filter_eq_nil_iff.mpr
      intro x hx hpred
      rw [List.mem_map] at hx
      obtain ⟨i, hi, rfl⟩ := hx
      exact h hpred
    rw [hf]
    simp [h, hvw]

private theorem joinCount_lane (a b : Lane) :
    ((laneVals a).map (fun v => (laneVals b).countP (fun w => v == w))).sum = joinCount a b := by
  unfold joinCount
  by_cases ha : a.step = 0
  · have haB : (a.step == 0) = true := by simp [ha]
    by_cases hb : b.step = 0
    · have hbB : (b.step == 0) = true := by simp [hb]
      have hla := laneVals_const a ha
      have hlb := laneVals_const b hb
      rw [hla, hlb]
      have hva : values a = ⟨a.base, 0, a.n⟩ := by
        rcases a with ⟨st, sr, nn, bs, sp⟩
        simp at ha ⊢
        simp [values, ha]
      have hvb : values b = ⟨b.base, 0, b.n⟩ := by
        rcases b with ⟨st, sr, nn, bs, sp⟩
        simp at hb ⊢
        simp [values, hb]
      rw [hva, hvb]
      simp [intersection, haB, hbB, contains]
      by_cases hEq : a.base = b.base
      · have hbEq : (a.base == b.base) = true := by simp [hEq]
        by_cases han : a.n = 0
        · simp [han, hEq, hbEq, contains, intersection, lastVal, haB, hbB,
            List.map_map, sum_map_const_nat, Function.comp_def]
        · by_cases hbn : b.n = 0
          · have hc : contains (values b) a.base = false := by
              cases hbool : contains (values b) a.base
              · rfl
              · have hm := (contains_iff_mem (values b) a.base).mp hbool
                obtain ⟨k, hk, _⟩ := hm
                change k < b.n at hk
                simp [hbn] at hk
            simp [han, hbn, hc, hEq, hbEq, contains, intersection, lastVal, haB, hbB,
              List.map_map, sum_map_const_nat, Function.comp_def]
          · have hbpos : 0 < b.n := by omega
            simp [han, hbn, hbpos, hEq, hbEq, contains, intersection, lastVal, haB, hbB,
              List.map_map, sum_map_const_nat, Function.comp_def]
      · have hbEq : (a.base == b.base) = false := by simp [hEq]
        have hle : ¬ (b.base ≤ a.base ∧ a.base ≤ b.base) := by omega
        by_cases han : a.n = 0
        · simp [han, hEq, hbEq, hle, contains, intersection, lastVal, haB, hbB,
            List.map_map, sum_map_const_nat, Function.comp_def]
        · by_cases hbn : b.n = 0
          · simp [han, hbn, hEq, hbEq, hle, contains, intersection, lastVal, haB, hbB,
              List.map_map, sum_map_const_nat, Function.comp_def]
          · have hbpos : 0 < b.n := by omega
            simp [han, hbn, hbpos, hEq, hbEq, hle, contains, intersection, lastVal, haB, hbB,
              List.map_map, sum_map_const_nat, Function.comp_def]
    · have hbBf : (b.step == 0) = false := by simp [hb]
      have hcnt : ∀ v : Int, (laneVals b).countP (fun w => v == w) =
          if contains (values b) v then 1 else 0 := by
        intro v
        rw [countP_swap]
        exact count_laneVals_eq b hb v
      have hmap : (laneVals a).map (fun v => (laneVals b).countP (fun w => v == w)) =
          (laneVals a).map (fun v : Int => if contains (values b) v then 1 else 0) := by
        apply List.map_congr_left
        intro v hv
        exact hcnt v
      rw [hmap]
      have hsum : ((laneVals a).map (fun v : Int => if contains (values b) v then 1 else 0)).sum =
          a.n * (if contains (values b) a.base then 1 else 0) := by
        rw [laneVals_const a ha]
        by_cases h : contains (values b) a.base
        · simp [h, List.map_map, sum_map_const_nat, Function.comp_def]
        · simp [h, List.map_map, sum_map_const_nat, Function.comp_def]
      rw [hsum]
      have hva : values a = ⟨a.base, 0, a.n⟩ := by
        rcases a with ⟨st, sr, nn, bs, sp⟩
        simp at ha ⊢
        simp [values, ha]
      rw [hva]
      by_cases han : a.n = 0
      · simp [han, intersection, lastVal, haB, hbBf]
      · by_cases hvn : (values b).n = 0
        · have hc : contains (values b) a.base = false := by
            cases hbool : contains (values b) a.base
            · rfl
            · have hm := (contains_iff_mem (values b) a.base).mp hbool
              obtain ⟨k, hk, _⟩ := hm
              simp [hvn] at hk
          simp [han, hvn, hc, intersection, lastVal, haB, hbBf]
        · by_cases hc : contains (values b) a.base = true
          · simp [han, hvn, hc, intersection, lastVal, haB, hbBf]
          · simp [han, hvn, hc, intersection, lastVal, haB, hbBf]
  · have haBf : (a.step == 0) = false := by simp [ha]
    by_cases hb : b.step = 0
    · have hbB : (b.step == 0) = true := by simp [hb]
      have hcnt : ∀ v : Int, (laneVals b).countP (fun w => v == w) =
          if v == b.base then b.n else 0 := by
        intro v
        rw [laneVals_const b hb, countP_const_eq]
      have hmap : (laneVals a).map (fun v => (laneVals b).countP (fun w => v == w)) =
          (laneVals a).map (fun v : Int => if v == b.base then b.n else 0) := by
        apply List.map_congr_left
        intro v hv
        exact hcnt v
      rw [hmap]
      have hsum : ((laneVals a).map (fun v : Int => if v == b.base then b.n else 0)).sum =
          b.n * (laneVals a).countP (fun v => v == b.base) := by
        rw [Nat.mul_comm]
        rw [countP_eq_sum]
        rw [← sum_map_mul_nat]
        apply congrArg List.sum
        apply List.map_congr_left
        intro v hv
        by_cases h : v == b.base <;> simp [h]
      rw [hsum, count_laneVals_eq a ha]
      have hvb : values b = ⟨b.base, 0, b.n⟩ := by
        rcases b with ⟨st, sr, nn, bs, sp⟩
        simp at hb ⊢
        simp [values, hb]
      rw [hvb]
      have hvs0 : (values a).step ≠ 0 := by
        exact Nat.ne_of_gt (values_step_pos a ha)
      by_cases han : a.n = 0
      · simp_all [intersection, values, lastVal, contains_n_zero]
      · by_cases hvn : (values a).n = 0
        · have hc : contains (values a) b.base = false := by
            cases hbool : contains (values a) b.base
            · rfl
            · have hm := (contains_iff_mem (values a) b.base).mp hbool
              obtain ⟨k, hk, _⟩ := hm
              simp [hvn] at hk
          simp_all [intersection, values, lastVal, contains_n_zero]
        · by_cases hc : contains (values a) b.base = true
          · by_cases hbn : b.n = 0
            · simp_all [intersection, values, lastVal, contains_n_zero]
            · simp_all [intersection, values, lastVal, contains_n_zero]
          · by_cases hbn : b.n = 0
            · simp_all [intersection, values, lastVal, contains_n_zero]
            · simp_all [intersection, values, lastVal, contains_n_zero]
    · have hbBf : (b.step == 0) = false := by simp [hb]
      have hperm : List.Perm (laneVals a) (progVals (values a)) :=
        laneVals_perm_progVals_values a ha
      have hcnt : ∀ v ∈ laneVals a, (laneVals b).countP (fun w => v == w) =
          if contains (values b) v then 1 else 0 := by
        intro v hv
        rw [countP_swap]
        exact count_laneVals_eq b hb v
      have hsum1 : ((laneVals a).map (fun v => (laneVals b).countP (fun w => v == w))).sum =
          ((laneVals a).map (fun v => if contains (values b) v then 1 else 0)).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro v hv
        exact hcnt v hv
      rw [hsum1]
      have hfilter : ((laneVals a).map (fun v => if contains (values b) v then 1 else 0)).sum =
          ((laneVals a).filter (fun v => contains (values b) v)).length := by
        rw [← List.countP_eq_length_filter, countP_eq_sum]
      rw [hfilter]
      have hlen := (hperm.filter (fun v => contains (values b) v)).length_eq
      rw [hlen]
      rw [count_common_prog (values a) (values b) (values_step_pos a ha) (values_step_pos b hb)]
      simp [haBf, hbBf]


/--! ## Generic goals from a lane decomposition -/

private theorem rows_range_map (lay : Layout) (c : Config) (f : Nat → Int)
    (h : ∀ i, i < c.n → lay.read c i = some (f i)) :
    rows lay c = (List.range c.n).map (fun i => (i, f i)) := by
  unfold rows
  rw [← filterMap_some_eq_map (List.range c.n) (fun i => (i, f i))]
  apply filterMap_congr'
  intro i hi
  have hfi := h i (List.mem_range.mp hi)
  rw [hfi]
  rfl

private theorem list_sum_perm_nat {l₁ l₂ : List Nat} (p : List.Perm l₁ l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem countP_map_eq {α : Type} (l : List α) (g : α → Int) (v : Int) :
    (l.map g).countP (fun x => v == x) = l.countP (fun x => v == g x) := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : v == g x <;> simp [h, ih]

private theorem lanePoints_snd (a : Lane) : (lanePoints a).map Prod.snd = laneVals a := by
  unfold lanePoints laneVals laneVal
  rw [List.map_map]
  apply List.map_congr_left
  intro i hi
  rfl

private theorem flatMap_sum_map (ls : List Lane) (f : Int → Nat) :
    ((ls.flatMap lanePoints).map (fun r => f r.2)).sum =
      (ls.map (fun a => ((laneVals a).map f).sum)).sum := by
  induction ls with
  | nil => simp
  | cons a as ih =>
      simp only [List.flatMap_cons, List.map_append, List.sum_append, List.map_cons, List.sum_cons]
      have hmm : (lanePoints a).map (fun r => f r.2) = (laneVals a).map f := by
        rw [show (lanePoints a).map (fun r => f r.2) = ((lanePoints a).map Prod.snd).map f by
          rw [List.map_map]
          apply List.map_congr_left
          intro x hx
          rfl]
        rw [lanePoints_snd]
      rw [hmm, ih]

private theorem residueJoin_of_perm (lay : Layout) (c : Config) (b : Lane)
    (hperm : (rows lay c).Perm ((lay.lanes c).flatMap lanePoints)) :
    ((rows lay c).map (fun r => (List.range b.n).countP
      (fun (i : Nat) => r.2 == b.base + b.step * (i : Int)))).sum =
      ((lay.lanes c).map (joinCount · b)).sum := by
  let f : Int → Nat := fun v => (List.range b.n).countP
    (fun (i : Nat) => v == b.base + b.step * (i : Int))
  have hp : List.Perm ((rows lay c).map (fun r => f r.2))
      (((lay.lanes c).flatMap lanePoints).map (fun r => f r.2)) := hperm.map _
  rw [list_sum_perm_nat hp, flatMap_sum_map]
  apply congrArg List.sum
  apply List.map_congr_left
  intro a ha
  have hterm : ((laneVals a).map f).sum = joinCount a b := by
    unfold f
    have hmap : (laneVals a).map (fun v => (List.range b.n).countP
        (fun i : Nat => v == b.base + b.step * (i : Int))) =
        (laneVals a).map (fun v => (laneVals b).countP (fun w => v == w)) := by
      apply List.map_congr_left
      intro v hv
      unfold laneVals laneVal
      rw [countP_map_eq]
      simp [Int.mul_comm]
    rw [hmap]
    exact joinCount_lane a b
  exact hterm

namespace RD

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.RestartedDelta

private theorem rows_map_rd (c : Config) :
    rows layout c = (List.range c.n).map (fun i =>
      (i, if i < c.split then c.base + c.step * (i : Int)
        else c.bias + c.step * ((i - c.split : Nat) : Int))) := by
  apply rows_range_map
  intro i _
  by_cases h : i < c.split <;> simp [layout, h]

private theorem rows_lanes_rd (c : Config) :
    rows layout c = (layout.lanes c).flatMap lanePoints := by
  rw [rows_map_rd]
  simp only [layout, List.flatMap_cons, List.flatMap_nil, List.append_nil]
  by_cases h : c.split ≤ c.n
  · have h1 : span c 0 c.split c.base c.step = ⟨0, 1, c.split, c.base, c.step⟩ := by
      unfold span lane
      simp [Nat.min_eq_right h]
    have h2 : span c c.split c.n c.bias c.step = ⟨c.split, 1, c.n - c.split, c.bias, c.step⟩ := by
      unfold span lane
      simp
    rw [h1, h2]
    unfold lanePoints
    simp only
    have hn : c.split + (c.n - c.split) = c.n := Nat.add_sub_of_le h
    have hrange : List.range c.n =
        List.range c.split ++ List.map (fun x => c.split + x) (List.range (c.n - c.split)) := by
      calc
        List.range c.n = List.range (c.split + (c.n - c.split)) := congrArg List.range hn.symm
        _ = _ := List.range_add
    rw [hrange, List.map_append]
    simp only [List.map_map, Function.comp_def]
    have hfirst : (List.range c.split).map (fun i =>
        (i, if i < c.split then c.base + c.step * (i : Int)
          else c.bias + c.step * ((i - c.split : Nat) : Int))) =
        (List.range c.split).map (fun i : Nat => (0 + i * 1, c.base + (i : Int) * c.step)) := by
      apply List.map_congr_left
      intro i hi
      have hi' : i < c.split := List.mem_range.mp hi
      simp [hi', Int.mul_comm]
    have hsecond : (List.range (c.n - c.split)).map (fun i =>
        (c.split + i, if c.split + i < c.split then c.base + c.step * ((c.split + i : Nat) : Int)
          else c.bias + c.step * (((c.split + i - c.split : Nat) : Int)))) =
        (List.range (c.n - c.split)).map (fun i : Nat =>
          (c.split + i * 1, c.bias + (i : Int) * c.step)) := by
      apply List.map_congr_left
      intro i hi
      have hnot : ¬ c.split + i < c.split := by omega
      have hsub : c.split + i - c.split = i := by omega
      simp [hnot, hsub, Int.mul_comm]
    rw [hfirst, hsecond]
  · have h1 : span c 0 c.split c.base c.step = ⟨0, 1, c.n, c.base, c.step⟩ := by
      unfold span lane
      simp [Nat.min_eq_left (by omega : c.n ≤ c.split)]
    have h2 : span c c.split c.n c.bias c.step =
        ⟨c.split, 1, 0, c.bias, c.step⟩ := by
      unfold span lane
      simp [Nat.sub_eq_zero_of_le (by omega : c.n ≤ c.split)]
    rw [h1, h2]
    unfold lanePoints
    simp
    intro i hi
    have his : i < c.split := by omega
    simp [his, Int.mul_comm]

theorem RD_ResidueJoin : TinylambOpt.ResearchGoals.Atlas.EncodedPages.RestartedDelta.ResidueJoin.Goal := by
  intro c b
  apply residueJoin_of_perm layout c b
  rw [rows_lanes_rd c]

end RD


/--! ## Counting even tiles -/

private theorem filterMap_if_eq_filter {α : Type} (l : List Nat) (P : Nat → Bool) (f : Nat → α) :
    l.filterMap (fun i => if P i then some (f i) else none) = (l.filter P).map f := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases h : P x <;> simp [h, ih]

private theorem rows_filtered (lay : Layout) (c : Config) (P : Nat → Bool) (f : Nat → Int)
    (h : ∀ i, i < c.n → lay.read c i = if P i then some (f i) else none) :
    rows lay c = ((List.range c.n).filter P).map (fun i => (i, f i)) := by
  unfold rows
  rw [show (List.range c.n).filterMap (fun i => (lay.read c i).map (fun v => (i, v))) =
      (List.range c.n).filterMap (fun i => if P i then some (i, f i) else none) by
    apply filterMap_congr'
    intro i hi
    rw [h i (List.mem_range.mp hi)]
    by_cases hp : P i <;> simp [hp]]
  rw [filterMap_if_eq_filter]

private theorem fullChunks_eq_range (k w : Nat) :
    (List.range k).flatMap (fun j => (List.range w).map (fun i => j * w + i)) =
      List.range (k * w) := by
  induction k with
  | zero => simp
  | succ k ih =>
      rw [List.range_succ, List.flatMap_append, ih, List.flatMap_cons, List.flatMap_nil]
      rw [Nat.succ_mul, List.range_add, List.append_nil]

private theorem div_two_succ_of_even (q : Nat) (hq : q % 2 = 0) : (q + 1) / 2 = q / 2 := by
  have hdec : q = 2 * (q / 2) := by
    have h := Nat.div_add_mod q 2
    omega
  exact Nat.div_eq_of_lt_le (by omega) (by omega)

private theorem div_two_succ_of_odd (q : Nat) (hq : q % 2 = 1) : (q + 1) / 2 = q / 2 + 1 := by
  have hdec : q = 2 * (q / 2) + 1 := by
    have h := Nat.div_add_mod q 2
    omega
  exact Nat.div_eq_of_lt_le (by omega) (by omega)

private theorem even_blocks_sum (q w : Nat) :
    ((List.range q).map (fun k => if k % 2 == 0 then w else 0)).sum = ((q + 1) / 2) * w := by
  induction q with
  | zero => simp
  | succ q ih =>
      rw [List.range_succ, List.map_append, List.sum_append]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
      rw [ih]
      by_cases h : q % 2 == 0
      · have hq : q % 2 = 0 := by simpa using h
        have hodd : (q + 1) % 2 = 1 := by omega
        have hd : (q + 1 + 1) / 2 = (q + 1) / 2 + 1 := div_two_succ_of_odd (q + 1) hodd
        rw [hd]
        simp [h, Nat.add_mul]
      · have hq : q % 2 = 1 := by
          have hm := Nat.mod_two_eq_zero_or_one q
          rcases hm with h0 | h1
          · exact False.elim (h (by simp [h0]))
          · exact h1
        have hev : (q + 1) % 2 = 0 := by omega
        have hd : (q + 1 + 1) / 2 = (q + 1) / 2 := div_two_succ_of_even (q + 1) hev
        rw [hd]
        simp [h]

private theorem block_filter_len (k w t : Nat) (hw : 0 < w) (ht : t ≤ w) :
    ((List.range t).filter (fun j => ((k * w + j) / w) % 2 == 0)).length =
      if k % 2 == 0 then t else 0 := by
  have hdiv : ∀ j ∈ List.range t, (k * w + j) / w = k := by
    intro j hj
    have hj' : j < w := by have := List.mem_range.mp hj; omega
    rw [Nat.add_comm (k * w) j, Nat.mul_comm k w, Nat.add_mul_div_left j k hw]
    rw [Nat.div_eq_of_lt hj']
    omega
  have hf : (List.range t).filter (fun j => ((k * w + j) / w) % 2 == 0) =
      (List.range t).filter (fun _ => k % 2 == 0) := by
    apply filter_congr_bool
    intro j hj
    rw [hdiv j hj]
  rw [hf]
  by_cases h : k % 2 == 0
  · have hk0 : k % 2 = 0 := by simpa using h
    rw [show (List.range t).filter (fun _ : Nat => k % 2 == 0) = List.range t by
      apply List.filter_eq_self.mpr
      intro x hx
      change decide (k % 2 = 0) = true
      simp [hk0]]
    rw [if_pos h]
    simp
  · have hk1 : k % 2 = 1 := by
      have hm := Nat.mod_two_eq_zero_or_one k
      rcases hm with h0 | h1
      · exact False.elim (h (by simp [h0]))
      · exact h1
    rw [show (List.range t).filter (fun _ : Nat => k % 2 == 0) = [] by
      apply List.filter_eq_nil_iff.mpr
      intro x hx hp
      simpa [hk1] using hp]
    rw [if_neg h]
    simp

private theorem filter_flatMap_len {α β : Type} (bs : List β) (f : β → List α) (P : α → Bool) :
    ((bs.flatMap f).filter P).length = (bs.map (fun b => ((f b).filter P).length)).sum := by
  induction bs with
  | nil => simp
  | cons b bs ih =>
      simp only [List.flatMap_cons]
      rw [List.filter_append, List.length_append, ih]
      simp

namespace PT

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.PagedTombstones

private theorem rows_filtered_pt (c : Config) :
    rows layout c = ((List.range c.n).filter (fun i => (i / width c) % 2 == 0)).map
      (fun i : Nat => (i, c.base + c.step * (i : Int))) := by
  exact rows_filtered layout c (fun i => (i / width c) % 2 == 0)
    (fun i : Nat => c.base + c.step * (i : Int)) (by
      intro i hi
      by_cases h : (i / width c) % 2 == 0
      · have h0 : (i / width c) % 2 = 0 := by simpa using h
        simp [layout, h0]
      · have h1 : (i / width c) % 2 = 1 := by
          have hm := Nat.mod_two_eq_zero_or_one (i / width c)
          rcases hm with hz | ho
          · exact False.elim (h (by simp [hz]))
          · exact ho
        simp [layout, h1])

theorem PT_SkipEpochs :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.PagedTombstones.SkipEpochs.Goal := by
  intro c
  let w := width c
  have hw : 0 < w := by dsimp [w, width]; omega
  let q := c.n / w
  let t := c.n % w
  have ht : t ≤ w := by dsimp [t, w]; exact Nat.le_of_lt (Nat.mod_lt c.n hw)
  have hn : c.n = q * w + t := by
    have h := Nat.div_add_mod c.n w
    rw [Nat.mul_comm] at h
    exact h.symm
  rw [rows_filtered_pt c]
  simp only [List.length_map]
  rw [hn, List.range_add, ← fullChunks_eq_range q w]
  rw [List.filter_append]
  simp only [List.length_append]
  rw [filter_flatMap_len]
  have hblocks : ((List.range q).map (fun k =>
      (((List.range w).map (fun j => k * w + j)).filter
        (fun i => (i / w) % 2 == 0)).length)).sum =
      ((List.range q).map (fun k => if k % 2 == 0 then w else 0)).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro k hk
    have hlen : ((List.map (fun j => k * w + j) (List.range w)).filter
        (fun i => (i / w) % 2 == 0)).length =
        ((List.range w).filter (fun j => ((k * w + j) / w) % 2 == 0)).length := by
      rw [List.filter_map, List.length_map]
      rfl
    rw [hlen]
    exact block_filter_len k w w hw (by omega)
  rw [hblocks, even_blocks_sum]
  have htail : (((List.range t).map (fun j => q * w + j)).filter
      (fun i => (i / w) % 2 == 0)).length = if q % 2 == 0 then t else 0 := by
    have hlen : ((List.map (fun j => q * w + j) (List.range t)).filter
        (fun i => (i / w) % 2 == 0)).length =
        ((List.range t).filter (fun j => ((q * w + j) / w) % 2 == 0)).length := by
      rw [List.filter_map, List.length_map]
      rfl
    rw [hlen]
    exact block_filter_len q w t hw ht
  rw [htail]
  have hdecomp : c.n = (q / 2) * (2 * w) + ((q % 2) * w + t) := by
    dsimp [q, t]
    have hq := Nat.div_add_mod c.n w
    have hq2 := Nat.div_add_mod (c.n / w) 2
    grind
  have hsmall : (q % 2) * w + t < 2 * w := by
    have ht' : t < w := by dsimp [t]; exact Nat.mod_lt c.n hw
    rcases Nat.mod_two_eq_zero_or_one q with hq0 | hq1
    · rw [hq0]
      omega
    · rw [hq1]
      omega
  have hcw : 0 < 2 * w := by omega
  have hdiv : c.n / (2 * w) = q / 2 := by
    rw [hdecomp]
    rw [Nat.add_comm ((q / 2) * (2 * w)) ((q % 2) * w + t)]
    rw [Nat.mul_comm (q / 2) (2 * w)]
    rw [Nat.add_mul_div_left ((q % 2) * w + t) (q / 2) hcw]
    rw [Nat.div_eq_of_lt hsmall]
    omega
  have hmod : c.n % (2 * w) = (q % 2) * w + t := by
    rw [hdecomp]
    rw [Nat.add_comm ((q / 2) * (2 * w)) ((q % 2) * w + t)]
    rw [Nat.mul_comm (q / 2) (2 * w)]
    rw [Nat.add_mul_mod_self_left]
    exact Nat.mod_eq_of_lt hsmall
  rw [← hn]
  change ((q + 1) / 2 * w + if q % 2 == 0 then t else 0) =
    c.n / (2 * w) * w + min w (c.n % (2 * w))
  rw [hdiv, hmod]
  by_cases hq : q % 2 == 0
  · have hq0 : q % 2 = 0 := by simpa using hq
    have hd : (q + 1) / 2 = q / 2 := div_two_succ_of_even q hq0
    rw [hd]
    simp [hq0, Nat.min_eq_right ht]
  · have hq1 : q % 2 = 1 := by
      have hm := Nat.mod_two_eq_zero_or_one q
      rcases hm with h0 | h1
      · exact False.elim (hq (by simp [h0]))
      · exact h1
    have hd : (q + 1) / 2 = q / 2 + 1 := div_two_succ_of_odd q hq1
    rw [hd]
    simp [hq, hq1, Nat.add_mul]

end PT



private theorem deletedSum_lane_stride_one (a : Lane) (has : a.stride = 1) (mask : Progression) :
    (((List.range a.n).filter (fun i : Nat => contains mask (a.start + (i : Int)))).map
      (fun i => laneVal a i)).sum = deletedSum a mask := by
  let p := positions a
  let r := intersection p mask
  have hp : 0 < p.step := by dsimp [p, positions, has]; omega
  have hr : 0 < r.step := by dsimp [r]; exact intersection_step_pos p mask
  have hsub : ∀ v, contains r v = true → contains p v = true := by
    intro v hv
    exact (intersection_contains p mask v).mp hv |>.1
  have hperm : List.Perm ((progVals p).filter (fun v => contains r v)) (progVals r) :=
    filter_progVals_perm p r hp hr hsub
  let valAt : Int → Int := fun x => a.base + (x - (a.start : Int)) * a.step
  have hvalIdx : ∀ i, i < a.n → laneVal a i = valAt (a.start + (i : Int)) := by
    intro i hi
    unfold laneVal valAt
    simp [has]
    grind
  have hvalIdx' : ∀ i, laneVal a i = valAt (a.start + (i : Int)) := by
    intro i
    unfold laneVal valAt
    simp [has]
    grind
  have hleft : (((List.range a.n).filter (fun i : Nat => contains mask (a.start + (i : Int)))).map
      (fun i => laneVal a i)).sum =
      (((progVals p).filter (fun x => contains mask x)).map valAt).sum := by
    simp [progVals, p, positions, has, hvalIdx', Function.comp_def]
    rw [List.filter_map, List.map_map]
    simp [Function.comp_def, progVal]
  have hfr : (progVals p).filter (fun x => contains mask x) =
      (progVals p).filter (fun x => contains r x) := by
    apply filter_congr_bool
    intro x hx
    change decide (contains mask x = true) = decide (contains r x = true)
    apply decide_eq_of_iff
    constructor
    · intro hm
      dsimp [r, p]
      exact (intersection_contains (positions a) mask x).mpr ⟨(mem_progVals p x).mp hx, hm⟩
    · intro hrx
      exact (intersection_contains p mask x).mp hrx |>.2
  rw [hleft, hfr]
  have hsumperm : (((progVals p).filter (fun x => contains r x)).map valAt).sum =
      ((progVals r).map valAt).sum := by
    exact list_sum_perm (hperm.map valAt)
  rw [hsumperm]
  unfold deletedSum
  change ((progVals r).map valAt).sum = (r.n : Int) * firstAt a r + tri r.n * slopeAt a r
  have hvalProg : ∀ t, t < r.n → valAt (progVal r t) =
      firstAt a r + (t : Int) * slopeAt a r := by
    intro t ht
    unfold valAt progVal firstAt slopeAt
    dsimp [p, positions] at hsub ⊢
    rw [has]
    simp
    grind
  have hsumvals : ((progVals r).map valAt).sum =
      ((List.range r.n).map (fun t : Nat => firstAt a r + (t : Int) * slopeAt a r)).sum := by
    unfold progVals
    rw [List.map_map]
    apply congrArg List.sum
    apply List.map_congr_left
    intro t ht
    exact hvalProg t (List.mem_range.mp ht)
  rw [hsumvals]
  rw [← partialSum_eq_sum (⟨0, 1, r.n, firstAt a r, slopeAt a r⟩ : Lane) r.n]
  simp [partialSum]

/--! ## Delete-progression from a lane decomposition -/

private theorem flatMap_snd_sum (ls : List Lane) :
    ((ls.flatMap lanePoints).map Prod.snd).sum =
      (ls.map (fun a => ((lanePoints a).map Prod.snd).sum)).sum := by
  induction ls with
  | nil => simp
  | cons a as ih => simp [List.map_append, List.sum_append, ih]

private theorem sum_map_filter_compl_int {α : Type} (l : List α) (p : α → Bool) (f : α → Int) :
    ((l.filter (fun x => !p x)).map f).sum = (l.map f).sum - ((l.filter p).map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x
      · simp [h, ih]
        omega
      · have hf : (!p x) = true := by simp [h]
        simp [h, hf, ih]
        omega

private theorem lanePoints_filter_snd (a : Lane) (mask : Progression) :
    (((lanePoints a).filter (fun r => contains mask r.1)).map Prod.snd).sum =
      (((List.range a.n).filter (fun i => contains mask (lanePos a i))).map
        (fun i => laneVal a i)).sum := by
  unfold lanePoints lanePos laneVal
  rw [List.filter_map, List.map_map]
  simp [Function.comp_def]

private theorem deleteProgression_of_perm (lay : Layout) (c : Config) (mask : Progression)
    (hdecomp : rows lay c = (lay.lanes c).flatMap lanePoints)
    (hlane : ∀ a ∈ lay.lanes c, a.stride = 1) :
    (((rows lay c).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
      ((lay.lanes c).map (fun a => partialSum a a.n - deletedSum a mask)).sum := by
  rw [hdecomp]
  rw [sum_map_filter_compl_int]
  rw [flatMap_filter_snd_sum, flatMap_snd_sum]
  have hps : (lay.lanes c).map (fun a => ((lanePoints a).map Prod.snd).sum) =
      (lay.lanes c).map (fun a => partialSum a a.n) := by
    apply List.map_congr_left
    intro a ha
    rw [lanePoints_snd]
    unfold laneVals laneVal
    rw [partialSum_eq_sum]
  rw [hps]
  rw [← sum_map_sub]
  apply congrArg List.sum
  apply List.map_congr_left
  intro a ha
  rw [lanePoints_filter_snd]
  have hlanePos : (List.range a.n).filter (fun i => contains mask (lanePos a i)) =
      (List.range a.n).filter (fun i : Nat => contains mask (a.start + (i : Int))) := by
    apply filter_congr_bool
    intro i hi
    unfold lanePos
    rw [hlane a ha]
    simp
  rw [hlanePos]
  rw [deletedSum_lane_stride_one a (hlane a ha) mask]

namespace RD

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.RestartedDelta

theorem RD_DeleteProgression :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.RestartedDelta.DeleteProgression.Goal := by
  intro c mask
  apply deleteProgression_of_perm layout c mask (rows_lanes_rd c)
  intro a ha
  simp [layout] at ha
  rcases ha with rfl | rfl <;> simp [span, lane]

end RD

end TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E7
