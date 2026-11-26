import TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `BucketRuns.DeleteProgression` — `∀ c mask, (((rows layout c).filter (fun r =>
  !contains mask r.1)).map Prod.snd).sum = ((layout.lanes c).map (fun a => partialSum
  a a.n - deletedSum a mask)).sum`
- `BucketRuns.ResidueJoin` — `∀ c (b : Lane), ((rows layout c).map (fun r =>
  (List.range b.n).countP (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum =
  ((layout.lanes c).map (joinCount · b)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages.BucketRunsDeleteProgressionAndResidueJoin

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
  constructor
  · intro h
    have h' : (0 < n ∧ lo ≤ v) ∧ v ≤ lo := by
      simpa [contains, lastVal] using h
    exact ⟨h'.1.1, by omega⟩
  · rintro ⟨hn, rfl⟩
    simp [contains, lastVal, hn]

private theorem contains_bounds (a : Progression) (v : Int) (h : contains a v = true) :
    a.lo ≤ v ∧ v ≤ lastVal a := by
  have hm := (contains_iff_mem a v).mp h
  obtain ⟨k, hk, hval⟩ := hm
  unfold progVal at hval
  have hkint : (k : Int) ≤ ((a.n - 1 : Nat) : Int) := by
    exact_mod_cast (Nat.le_pred_of_lt hk)
  have hmul := Int.mul_le_mul_of_nonneg_left hkint (by omega : 0 ≤ (a.step : Int))
  have hmul' : (k : Int) * a.step ≤ ((a.n - 1 : Nat) : Int) * a.step := by
    simpa only [Int.mul_comm] using hmul
  unfold lastVal
  have hknon : (0 : Int) ≤ k := by omega
  have hstepnon : (0 : Int) ≤ (a.step : Int) := Int.natCast_nonneg _
  have hkmulnon : (0 : Int) ≤ (k : Int) * a.step := Int.mul_nonneg hknon hstepnon
  constructor
  · rw [hval]
    omega
  · rw [hval]
    exact Int.add_le_add_left hmul' a.lo

private theorem contains_dvd (a : Progression) (v : Int) (h : contains a v = true) :
    (a.step : Int) ∣ v - a.lo := by
  obtain ⟨k, _, hval⟩ := (contains_iff_mem a v).mp h
  unfold progVal at hval
  refine ⟨(k : Int), ?_⟩
  rw [hval]
  rw [Int.mul_comm (k : Int) (a.step : Int)]
  omega

private theorem common_gap_dvd (a b : Progression) (v : Int)
    (ha : contains a v = true) (hb : contains b v = true) :
    (a.step.gcd b.step : Int) ∣ b.lo - a.lo := by
  obtain ⟨i, _, hi⟩ := (contains_iff_mem a v).mp ha
  obtain ⟨j, _, hj⟩ := (contains_iff_mem b v).mp hb
  unfold progVal at hi hj
  have hgd : ((a.step.gcd b.step : Nat) : Int) ∣ (b.step : Int) := by
    exact_mod_cast (Nat.gcd_dvd_right a.step b.step)
  have hgl : ((a.step.gcd b.step : Nat) : Int) ∣ (a.step : Int) := by
    exact_mod_cast (Nat.gcd_dvd_left a.step b.step)
  obtain ⟨tg, htg⟩ := hgd
  obtain ⟨tl, htl⟩ := hgl
  have h1 : (a.step.gcd b.step : Int) ∣ (b.step : Int) * (j : Int) := by
    refine ⟨tg * (j : Int), ?_⟩
    rw [htg, Int.mul_assoc]
  have h2 : (a.step.gcd b.step : Int) ∣ (a.step : Int) * (i : Int) := by
    refine ⟨tl * (i : Int), ?_⟩
    rw [htl, Int.mul_assoc]
  have hi' : (i : Int) * a.step = v - a.lo := by
    rw [hi]
    omega
  have hj' : (j : Int) * b.step = v - b.lo := by
    rw [hj]
    omega
  have heq : (b.step : Int) * (j : Int) - (a.step : Int) * (i : Int) = a.lo - b.lo := by
    calc
      (b.step : Int) * (j : Int) - (a.step : Int) * (i : Int)
          = (j : Int) * b.step - (i : Int) * a.step := by
              rw [Int.mul_comm (b.step : Int) (j : Int), Int.mul_comm (a.step : Int) (i : Int)]
      _ = (v - b.lo) - (v - a.lo) := by rw [← hj', ← hi']
      _ = a.lo - b.lo := by omega
  have hd : (a.step.gcd b.step : Int) ∣ a.lo - b.lo := by
    rw [← heq]
    exact Int.dvd_sub h1 h2
  have hneg : -(a.lo - b.lo) = b.lo - a.lo := by omega
  rw [← hneg]
  exact Int.dvd_neg.mpr hd

private theorem period_dvd_delta (s t : Nat) (hs : 0 < s) (ht : 0 < t) (delta : Int)
    (hds : (s : Int) ∣ delta) (hdt : (t : Int) ∣ delta) :
    (((s / s.gcd t) * t : Nat) : Int) ∣ delta := by
  let g := s.gcd t
  let sa := s / g
  let tb := t / g
  have hg : 0 < g := Nat.gcd_pos_of_pos_left t hs
  have hsEq : (s : Int) = (g : Int) * (sa : Int) := by
    exact_mod_cast (Nat.mul_div_cancel' (Nat.gcd_dvd_left s t) : g * sa = s).symm
  have htEq : (t : Int) = (g : Int) * (tb : Int) := by
    exact_mod_cast (Nat.mul_div_cancel' (Nat.gcd_dvd_right s t) : g * tb = t).symm
  have hcop : sa.gcd tb = 1 := by
    simpa [sa, tb, g] using reduced_coprime s t hs ht
  rcases hbez : bezout sa tb with ⟨u, v⟩
  have hspec := bezout_spec sa tb
  rw [hbez] at hspec
  change (sa.gcd tb : Int) = (sa : Int) * u + (tb : Int) * v at hspec
  rw [hcop] at hspec
  simp only [Int.ofNat_one] at hspec
  obtain ⟨k1, hk1⟩ := hds
  obtain ⟨k2, hk2⟩ := hdt
  refine ⟨u * k2 + v * k1, ?_⟩
  push_cast
  push_cast
  grind

private theorem contains_unit_one (lo : Int) (v : Int) :
    contains ⟨lo, 1, 1⟩ v = true ↔ v = lo := by
  rw [contains_iff_mem]
  constructor
  · rintro ⟨k, hk, hv⟩
    have hk1 : k < 1 := by simpa using hk
    have hk0 : k = 0 := by omega
    subst k
    unfold progVal at hv
    simpa using hv
  · intro hv
    subst v
    refine ⟨0, ?_, ?_⟩
    · change 0 < (1 : Nat)
      decide
    · unfold progVal
      simp

private theorem contains_unit_zero (lo : Int) (v : Int) :
    contains ⟨lo, 1, 0⟩ v = true ↔ False := by
  rw [contains_iff_mem]
  constructor
  · rintro ⟨k, hk, _⟩
    have hk0 : k < 0 := by simpa using hk
    omega
  · intro h
    exact h.elim

private theorem intersection_contains (a b : Progression) (v : Int) :
    contains (intersection a b) v = true ↔ contains a v = true ∧ contains b v = true := by
  by_cases han : a.n = 0
  · cases a with
    | mk alo astep an =>
        simp at han
        subst an
        simp [intersection, contains]
  · by_cases hbn : b.n = 0
    · cases b with
      | mk blo bstep bn =>
          simp at hbn
          subst bn
          simp [intersection, contains]
    · have hanp : 0 < a.n := by omega
      have hbnp : 0 < b.n := by omega
      by_cases has : a.step = 0
      · cases a with
        | mk alo astep an =>
          simp at has han hanp
          subst astep
          have hInt : intersection ⟨alo, 0, an⟩ b =
              ⟨alo, 1, if contains b alo then 1 else 0⟩ := by
            simp [intersection, han, hbn]
          rw [hInt]
          by_cases hb : contains b alo = true
          · rw [if_pos hb]
            constructor
            · intro h
              have hvalo : v = alo := (contains_unit_one alo v).mp h
              subst v
              exact ⟨(contains_const_iff alo an alo).mpr ⟨hanp, rfl⟩, hb⟩
            · rintro ⟨ha, hbv⟩
              have hva := (contains_const_iff alo an v).mp ha
              have hvalo : v = alo := hva.2
              subst v
              exact (contains_unit_one alo alo).mpr rfl
          · rw [if_neg hb]
            constructor
            · intro h
              exact (contains_unit_zero alo v).mp h |>.elim
            · rintro ⟨ha, hbv⟩
              have hva := (contains_const_iff alo an v).mp ha
              have hvalo : v = alo := hva.2
              subst v
              exact (hb hbv).elim
      · by_cases hbs : b.step = 0
        · cases b with
          | mk blo bstep bn =>
            simp at hbs hbn hbnp
            subst bstep
            have hInt : intersection a ⟨blo, 0, bn⟩ =
                ⟨blo, 1, if contains a blo then 1 else 0⟩ := by
              simp [intersection, han, hbn, has]
            rw [hInt]
            by_cases ha' : contains a blo = true
            · rw [if_pos ha']
              constructor
              · intro h
                have hvlo : v = blo := (contains_unit_one blo v).mp h
                subst v
                exact ⟨ha', (contains_const_iff blo bn blo).mpr ⟨hbnp, rfl⟩⟩
              · rintro ⟨ha, hbv⟩
                have hvb := (contains_const_iff blo bn v).mp hbv
                have hvlo : v = blo := hvb.2
                subst v
                exact (contains_unit_one blo blo).mpr rfl
            · rw [if_neg ha']
              constructor
              · intro h
                exact (contains_unit_zero blo v).mp h |>.elim
              · rintro ⟨ha, hbv⟩
                have hvb := (contains_const_iff blo bn v).mp hbv
                have hvlo : v = blo := hvb.2
                subst v
                exact (ha' ha).elim
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
          have hgpos : 0 < a.step.gcd b.step := Nat.gcd_pos_of_pos_left b.step hasp
          have hApos : 0 < a.step / a.step.gcd b.step := Nat.div_gcd_pos_of_pos_left b.step hasp
          have hpNat : 0 < period := by
            dsimp [period]
            exact Nat.mul_pos hApos hbsp
          have hp : (0 : Int) < period := by exact_mod_cast hpNat
          have hgapdef : gap = b.lo - a.lo := rfl
          by_cases hgapmod : gap % (g : Int) = 0
          · have hgap : (g : Int) ∣ gap := Int.dvd_of_emod_eq_zero hgapmod
            have hroot := root_dvd a.step b.step hasp hbsp a.lo b.lo gap hgapdef hgap
            have hfirst : first = low + (root - low) % (period : Int) := rfl
            have hfroot := first_root_dvd low root first period hp hfirst
            have hremnon : 0 ≤ (root - low) % (period : Int) := Int.emod_nonneg _ (by omega)
            have hflow : low ≤ first := by dsimp [first]; omega
            have hpaEq : period = a.step * (b.step / g) := by
              dsimp [period]
              have hbEq : b.step = g * (b.step / g) :=
                (Nat.mul_div_cancel' (Nat.gcd_dvd_right a.step b.step)).symm
              calc
                a.step / g * b.step = a.step / g * (g * (b.step / g)) :=
                  congrArg (fun x : Nat => a.step / g * x) hbEq
                _ = (a.step / g * g) * (b.step / g) := by rw [Nat.mul_assoc]
                _ = a.step * (b.step / g) := by
                    rw [Nat.div_mul_cancel (Nat.gcd_dvd_left a.step b.step)]
            have hpbEq : period = b.step * (a.step / g) := by
              dsimp [period]
              exact Nat.mul_comm (a.step / g) b.step
            have hpa : (a.step : Int) ∣ (period : Int) := by
              refine ⟨((b.step / g : Nat) : Int), ?_⟩
              exact_mod_cast hpaEq
            have hpb : (b.step : Int) ∣ (period : Int) := by
              refine ⟨((a.step / g : Nat) : Int), ?_⟩
              exact_mod_cast hpbEq
            have hInt : intersection a b =
                ⟨first, (period : Int).toNat,
                  if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ := by
              dsimp [intersection]
              simp [han, hbn, has, hbs, hgapmod, first, low, high, gap, period, unit, root, g]
            rw [hInt]
            refine (span_contains_iff first (period : Int) high v hp).trans ?_
            constructor
            · rintro ⟨hfirstx, hxhigh, hxfirst⟩
              have hxroot : (period : Int) ∣ v - root := by
                have heq : v - root = (v - first) + (first - root) := by omega
                rw [heq]
                exact Int.dvd_add hxfirst hfroot
              have hx1 : (a.step : Int) ∣ v - root := Int.dvd_trans hpa hxroot
              have hx2 : (b.step : Int) ∣ v - root := Int.dvd_trans hpb hxroot
              have hxalo : (a.step : Int) ∣ v - a.lo := by
                have hsum : (v - root) + (root - a.lo) = v - a.lo := by omega
                rw [← hsum]
                exact Int.dvd_add hx1 hroot.1
              have hxblo : (b.step : Int) ∣ v - b.lo := by
                have hsum : (v - root) + (root - b.lo) = v - b.lo := by omega
                rw [← hsum]
                exact Int.dvd_add hx2 hroot.2
              have hxlow : low ≤ v := by omega
              have hab := contains_bounds a v
              have hbb := contains_bounds b v
              constructor
              · exact mem_of_bounds_dvd a v hanp hasp (by omega) (by omega) hxalo
              · exact mem_of_bounds_dvd b v hbnp hbsp (by omega) (by omega) hxblo
            · rintro ⟨ha, hb⟩
              have hab := contains_bounds a v ha
              have hbb := contains_bounds b v hb
              have hxblo := contains_dvd b v hb
              have hxroot : (period : Int) ∣ v - root :=
                by
                  have hxalo' := contains_dvd a v ha
                  have hxroot_a : (a.step : Int) ∣ v - root := by
                    have hd : (a.step : Int) ∣ (v - a.lo) - (root - a.lo) :=
                      Int.dvd_sub hxalo' hroot.1
                    have heq : (v - a.lo) - (root - a.lo) = v - root := by omega
                    rw [heq] at hd
                    exact hd
                  have hxroot_b : (b.step : Int) ∣ v - root := by
                    have hd : (b.step : Int) ∣ (v - b.lo) - (root - b.lo) :=
                      Int.dvd_sub hxblo hroot.2
                    have heq : (v - b.lo) - (root - b.lo) = v - root := by omega
                    rw [heq] at hd
                    exact hd
                  simpa [period, g] using
                    period_dvd_delta a.step b.step hasp hbsp (v - root) hxroot_a hxroot_b
              have hxfirst : (period : Int) ∣ v - first := by
                have heq : v - first = (v - root) - (first - root) := by omega
                rw [heq]
                exact Int.dvd_sub hxroot hfroot
              have hlowle : low ≤ v := by
                dsimp [low]
                by_cases hba : b.lo ≤ a.lo
                · rw [Int.max_eq_left hba]
                  exact hab.1
                · rw [Int.max_eq_right (by omega)]
                  exact hbb.1
              have hfirstle : first ≤ v :=
                first_le_of_congruence low root first v (period : Int) hp rfl hlowle hxroot
              have hvhigh : v ≤ high := by
                dsimp [high]
                rw [Int.le_min]
                exact ⟨hab.2, hbb.2⟩
              exact ⟨hfirstle, hvhigh, hxfirst⟩
          · have hgapne : gap % (g : Int) ≠ 0 := by simpa using hgapmod
            have hInt : intersection a b = ⟨0, 1, 0⟩ := by
              dsimp [intersection]
              simp [han, hbn, has, hbs, hgapmod, g, gap]
            rw [hInt]
            have hc0 : contains (⟨0, 1, 0⟩ : Progression) v = true ↔ False := by
              simp [contains]
            rw [hc0]
            constructor
            · intro h
              exact h.elim
            · rintro ⟨ha, hb⟩
              have hg := common_gap_dvd a b v ha hb
              exact hgapne (Int.emod_eq_zero_of_dvd hg)

/-! ## BucketRuns: rows as a flat map of lane points -/

private theorem rows_range_map (lay : Layout) (c : Config) (f : Nat → Int)
    (h : ∀ i, i < c.n → lay.read c i = some (f i)) :
    rows lay c = (List.range c.n).map (fun i => (i, f i)) := by
  unfold rows
  exact range_filterMap_all c.n (fun i => (lay.read c i).map (fun v => (i, v)))
    (fun i => (i, f i)) (by intro i hi; simp [h i hi])

private theorem flatMap_congr_left {α β : Type} (l : List α) (f g : α → List β)
    (h : ∀ a ∈ l, f a = g a) : l.flatMap f = l.flatMap g := by
  induction l with
  | nil => rfl
  | cons a as ih => simp [h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private theorem flatMap_sum_nat (ls : List Lane) (f : Nat × Int → Nat) :
    ((ls.flatMap lanePoints).map f).sum = (ls.map (fun a => ((lanePoints a).map f).sum)).sum := by
  induction ls with
  | nil => simp
  | cons a as ih => simp [ih, List.sum_append]

private theorem flatMap_sum_int (ls : List Lane) (f : Nat × Int → Int) :
    ((ls.flatMap lanePoints).map f).sum = (ls.map (fun a => ((lanePoints a).map f).sum)).sum := by
  induction ls with
  | nil => simp
  | cons a as ih => simp [ih, List.sum_append]

private theorem sum_range_const_nat (m c : Nat) :
    ((List.range m).map (fun _ => c)).sum = m * c := by
  induction m with
  | zero => simp
  | succ m ih => rw [show m + 1 = Nat.succ m by omega, List.range_succ,
      List.map_append, List.sum_append, ih]; simp [Nat.succ_mul]

private theorem lanePoints_const_sum (a : Lane) (c : Nat) :
    ((lanePoints a).map (fun _ => c)).sum = a.n * c := by
  simp [lanePoints, List.map_map, Function.comp_def, sum_range_const_nat]

private theorem countP_const (n : Nat) (p : Prop) [Decidable p] :
    (List.range n).countP (fun _ => decide p) = if p then n else 0 := by
  induction n with
  | zero => simp
  | succ n ih => by_cases h : p <;> simp [h, ih]

private theorem countP_eq_zero_of_all_false {α : Type} (l : List α) (p : α → Bool)
    (h : ∀ x ∈ l, p x = false) : l.countP p = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem countP_eq_one_of_unique {α : Type} (l : List α) (p : α → Bool)
    (hnd : l.Nodup)
    (huniq : ∀ x ∈ l, p x = true → ∀ y ∈ l, p y = true → x = y) :
    l.countP p = if ∃ x ∈ l, p x = true then 1 else 0 := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hxnot : x ∉ xs := (List.nodup_cons.mp hnd).1
      have hndxs : xs.Nodup := (List.nodup_cons.mp hnd).2
      by_cases hx : p x = true
      · have hnone : ∀ y ∈ xs, p y = false := by
          intro y hy
          cases hpy : p y with
          | false => rfl
          | true =>
              have heq : y = x := huniq y (by simp [hy]) hpy x (by simp) hx
              subst y
              exact (hxnot hy).elim
        have hxcount : xs.countP p = 0 := countP_eq_zero_of_all_false xs p hnone
        have hex : ∃ y, y ∈ x :: xs ∧ p y = true := ⟨x, by simp, hx⟩
        simp [hx, hxcount, hex]
      · have ih' := ih hndxs (fun a ha hap b hb hbp => huniq a (by simp [ha]) hap b (by simp [hb]) hbp)
        by_cases hex : ∃ y ∈ xs, p y = true
        · have hex' : ∃ y ∈ x :: xs, p y = true := by
            obtain ⟨y, hy, hpy⟩ := hex
            exact ⟨y, by simp [hy], hpy⟩
          simp [hx, ih', hex, hex']
        · have hex' : ¬ ∃ y ∈ x :: xs, p y = true := by
            rintro ⟨y, hy, hpy⟩
            rcases List.mem_cons.mp hy with rfl | hy
            · exact hx hpy
            · exact hex ⟨y, hy, hpy⟩
          simp [hx, ih', hex, hex']

private theorem val_eq_of_step_ne_zero (b : Lane) (i j : Nat)
    (hb : b.step ≠ 0) (h : b.base + b.step * (i : Int) = b.base + b.step * (j : Int)) : i = j := by
  have hm : b.step * (i : Int) = b.step * (j : Int) := Int.add_left_cancel h
  have hc : (i : Int) = (j : Int) := (Int.mul_eq_mul_left_iff hb).mp hm
  exact_mod_cast hc

private theorem contains_values_lane (b : Lane) (v : Int) :
    contains (values b) v = true ↔ ∃ k, k < b.n ∧ b.base + b.step * (k : Int) = v := by
  rw [contains_iff_mem]
  constructor
  · rintro ⟨k, hk, hv⟩
    unfold progVal at hv
    change k < b.n at hk
    by_cases hneg : b.step < 0
    · have hstep : (((values b).step : Nat) : Int) = -b.step := by
        change ((b.step.natAbs : Nat) : Int) = -b.step
        rw [← Int.natAbs_neg b.step]
        exact Int.natAbs_of_nonneg (by omega)
      have hlo : (values b).lo = b.base + ((b.n - 1 : Nat) : Int) * b.step := by
        simp [values, hneg]
      rw [hstep, hlo] at hv
      have hkle : k ≤ b.n - 1 := by omega
      have hcast : (((b.n - 1 - k : Nat)) : Int) = ((b.n - 1 : Nat) : Int) - (k : Int) := by
        rw [Int.natCast_sub hkle]
      refine ⟨b.n - 1 - k, by omega, ?_⟩
      rw [hv, hcast, Int.mul_sub]
      grind
    · have hstep : (((values b).step : Nat) : Int) = b.step := by
        change ((b.step.natAbs : Nat) : Int) = b.step
        exact Int.natAbs_of_nonneg (by omega)
      have hlo : (values b).lo = b.base := by
        simp [values, hneg]
      rw [hstep, hlo] at hv
      refine ⟨k, hk, ?_⟩
      rw [hv, Int.mul_comm]
  · rintro ⟨k, hk, hv⟩
    by_cases hneg : b.step < 0
    · have hkle : k ≤ b.n - 1 := by omega
      have hcast : (((b.n - 1 - k : Nat)) : Int) = ((b.n - 1 : Nat) : Int) - (k : Int) := by
        rw [Int.natCast_sub hkle]
      refine ⟨b.n - 1 - k, ?_, ?_⟩
      · have htmp : b.n - 1 - k = b.n - (k + 1) := by omega
        rw [htmp]
        exact Nat.sub_lt (by omega) (by omega)
      unfold progVal
      have hstep : (((values b).step : Nat) : Int) = -b.step := by
        change ((b.step.natAbs : Nat) : Int) = -b.step
        rw [← Int.natAbs_neg b.step]
        exact Int.natAbs_of_nonneg (by omega)
      have hlo : (values b).lo = b.base + ((b.n - 1 : Nat) : Int) * b.step := by
        simp [values, hneg]
      rw [hstep, hlo, hcast]
      rw [← hv, Int.sub_mul]
      grind
    · refine ⟨k, hk, ?_⟩
      unfold progVal
      have hstep : (((values b).step : Nat) : Int) = b.step := by
        change ((b.step.natAbs : Nat) : Int) = b.step
        exact Int.natAbs_of_nonneg (by omega)
      have hlo : (values b).lo = b.base := by
        simp [values, hneg]
      rw [hstep, hlo]
      rw [← hv, Int.mul_comm]

private theorem lane_residue (a : Lane) (ha0 : a.step = 0) (b : Lane) :
    ((lanePoints a).map (fun r =>
      (List.range b.n).countP (fun i : Nat => r.2 == b.base + b.step * (i : Int)))).sum =
      joinCount a b := by
  let cnt := (List.range b.n).countP (fun i : Nat => a.base == b.base + b.step * (i : Int))
  have hL : ((lanePoints a).map (fun r =>
      (List.range b.n).countP (fun i : Nat => r.2 == b.base + b.step * (i : Int)))).sum =
      ((lanePoints a).map (fun _ => cnt)).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro r hr
    rcases List.mem_map.mp hr with ⟨i, hi, rfl⟩
    simp [lanePoints, ha0, cnt]
  rw [hL]
  rw [lanePoints_const_sum]
  by_cases han : a.n = 0
  · simp [joinCount, han, ha0, values]
  · by_cases hbn : b.n = 0
    · simp [joinCount, intersection, hbn, ha0, cnt, values]
    · have hanp : 0 < a.n := by omega
      have hcn : contains (values b) a.base = true ↔
          ∃ k, k < b.n ∧ b.base + b.step * (k : Int) = a.base := contains_values_lane b a.base
      by_cases hc : contains (values b) a.base = true
      · have hex : ∃ k, k < b.n ∧ b.base + b.step * (k : Int) = a.base := hcn.mp hc
        have hcnt : cnt = if b.step = 0 then b.n else 1 := by
          by_cases hb0 : b.step = 0
          · have hv : a.base = b.base := by
              obtain ⟨k, hk, hkval⟩ := hex
              simpa [hb0] using hkval.symm
            simp [cnt, hb0, hv, countP_const]
          · have huniq : ∀ x ∈ List.range b.n,
                (a.base == b.base + b.step * (x : Int)) = true →
                ∀ y ∈ List.range b.n, (a.base == b.base + b.step * (y : Int)) = true → x = y := by
              intro x hx hpx y hy hpy
              have hx' : b.base + b.step * (x : Int) = a.base :=
                (beq_iff_eq.mp hpx).symm
              have hy' : b.base + b.step * (y : Int) = a.base :=
                (beq_iff_eq.mp hpy).symm
              exact val_eq_of_step_ne_zero b x y hb0 (hx'.trans hy'.symm)
            have hex' : ∃ x ∈ List.range b.n,
                (a.base == b.base + b.step * (x : Int)) = true := by
              obtain ⟨k, hk, hkv⟩ := hex
              refine ⟨k, List.mem_range.mpr hk, ?_⟩
              simpa [hkv]
            change (List.range b.n).countP
                (fun x : Nat => a.base == b.base + b.step * (x : Int)) =
              (if b.step = 0 then b.n else 1)
            have hexNat : ∃ x, x < b.n ∧ a.base = b.base + b.step * (x : Int) := by
              obtain ⟨x, hxmem, hxeq⟩ := hex'
              exact ⟨x, List.mem_range.mp hxmem, beq_iff_eq.mp hxeq⟩
            rw [countP_eq_one_of_unique _ _ List.nodup_range huniq]
            simp [hex', hexNat, hb0]
        have hinter : (intersection (values a) (values b)).n = 1 := by
          have hi : (intersection (values a) (values b)).n =
              (if contains (values b) a.base then 1 else 0) := by
            simp [intersection, han, hbn, ha0, values]
            rfl
          rw [hi]
          simp [hc]
        simp [joinCount, han, hbn, ha0, hinter, hcnt]
      · have hcnt : cnt = 0 := by
          dsimp [cnt]
          change (List.range b.n).countP
              (fun i : Nat => a.base == b.base + b.step * (i : Int)) = 0
          have hfalse : ∀ i ∈ List.range b.n,
              (a.base == b.base + b.step * (i : Int)) = false := by
            intro i hi
            apply Bool.eq_false_of_ne_true
            intro heq
            have hval : b.base + b.step * (i : Int) = a.base := (beq_iff_eq.mp heq).symm
            exact hc (hcn.mpr ⟨i, List.mem_range.mp hi, hval⟩)
          exact countP_eq_zero_of_all_false (List.range b.n)
            (fun i : Nat => a.base == b.base + b.step * (i : Int)) hfalse
        have hinter : (intersection (values a) (values b)).n = 0 := by
          have hi : (intersection (values a) (values b)).n =
              (if contains (values b) a.base then 1 else 0) := by
            simp [intersection, han, hbn, ha0, values]
            rfl
          rw [hi]
          simp [hc]
        simp [joinCount, han, hbn, ha0, hinter, hcnt]

/-! ## BucketRuns rows and ResidueJoin -/

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.BucketRuns

private theorem width_pos (c : Config) : 0 < width c := by
  dsimp [width]
  omega

private theorem fullChunks_eq_range (k w : Nat) :
    (List.range k).flatMap (fun j => (List.range w).map (fun i => j * w + i)) =
      List.range (k * w) := by
  induction k with
  | zero => simp
  | succ k ih =>
      rw [List.range_succ, List.flatMap_append, ih, List.flatMap_cons, List.flatMap_nil]
      rw [Nat.succ_mul, List.range_add, List.append_nil]

private theorem bucket_lanePoints (c : Config) (j : Nat) :
    lanePoints (span c (j * width c) ((j + 1) * width c)
      (c.base + c.step * (j : Int)) 0) =
    (List.range (min c.n ((j + 1) * width c) - j * width c)).map
      (fun i => (j * width c + i, c.base + c.step * (j : Int))) := by
  unfold lanePoints span lane
  simp

private theorem rows_lanes (c : Config) :
    rows layout c = (layout.lanes c).flatMap lanePoints := by
  have hp : 0 < width c := width_pos c
  rw [rows_range_map layout c (fun i => c.base + c.step * ((i / width c : Nat) : Int)) (by
    intro i _
    rfl)]
  simp only [layout, List.flatMap_map, bucket_lanePoints]
  let w := width c
  let q := c.n / w
  let r := c.n % w
  have hnr : q * w + r = c.n := by
    have hd := Nat.div_add_mod c.n w
    change w * (c.n / w) + c.n % w = c.n at hd
    rw [Nat.mul_comm] at hd
    simpa [q, r] using hd
  have hceil : (c.n + w - 1) / w = q + if r = 0 then 0 else 1 := by
    have hr : r < w := Nat.mod_lt c.n hp
    by_cases hz : r = 0
    · rw [if_pos hz]
      have hn : c.n = q * w := by omega
      rw [hn, Nat.div_eq_of_lt_le (k := q) (n := w) (m := q * w + w - 1)
        (by omega) (by rw [Nat.add_mul, Nat.one_mul]; omega)]
      rfl
    · rw [if_neg hz]
      have hrew : c.n + w - 1 = q * w + (r + w - 1) := by omega
      have hone : (r + w - 1) / w = 1 :=
        Nat.div_eq_of_lt_le (by omega) (by omega)
      rw [hrew, Nat.mul_comm q w, Nat.mul_add_div hp q (r + w - 1), hone]
  have hchunks : (List.range (q + if r = 0 then 0 else 1)).flatMap
      (fun j => (List.range (min c.n ((j + 1) * w) - j * w)).map (fun i => j * w + i)) =
      List.range c.n := by
    by_cases hz : r = 0
    · rw [if_pos hz]
      simp only [Nat.add_zero]
      have hn : c.n = q * w := by omega
      rw [hn]
      have hprefix : (List.range q).flatMap
          (fun j => (List.range (min (q * w) ((j + 1) * w) - j * w)).map (fun i => j * w + i)) =
          (List.range q).flatMap (fun j => (List.range w).map (fun i => j * w + i)) := by
        apply flatMap_congr_left
        intro j hj
        apply congrArg (fun m => (List.range m).map (fun i => j * w + i))
        have hjq : j < q := List.mem_range.mp hj
        have hmin : min (q * w) ((j + 1) * w) = (j + 1) * w := by
          apply Nat.min_eq_right
          simpa [Nat.mul_comm] using Nat.mul_le_mul_right w (show j + 1 ≤ q by omega)
        rw [hmin, Nat.add_mul, Nat.one_mul]
        exact Nat.add_sub_cancel_left (j * w) w
      rw [hprefix]
      exact fullChunks_eq_range q w
    · rw [if_neg hz]
      have hrw : r < w := by dsimp [r]; exact Nat.mod_lt c.n hp
      rw [show q + 1 = Nat.succ q by omega, List.range_succ, List.flatMap_append,
        List.flatMap_cons, List.flatMap_nil]
      have hprefix : (List.range q).flatMap
          (fun j => (List.range (min c.n ((j + 1) * w) - j * w)).map (fun i => j * w + i)) =
          (List.range q).flatMap (fun j => (List.range w).map (fun i => j * w + i)) := by
        apply flatMap_congr_left
        intro j hj
        apply congrArg (fun m => (List.range m).map (fun i => j * w + i))
        have hjq : j < q := List.mem_range.mp hj
        have hmin : min c.n ((j + 1) * w) = (j + 1) * w := by
          apply Nat.min_eq_right
          have hmul := Nat.mul_le_mul_right w (show j + 1 ≤ q by omega)
          omega
        rw [hmin, Nat.add_mul, Nat.one_mul]
        exact Nat.add_sub_cancel_left (j * w) w
      rw [hprefix, fullChunks_eq_range]
      have hlast : min c.n ((q + 1) * w) - q * w = r := by
        rw [show c.n = q * w + r from hnr.symm]
        have hle : q * w + r ≤ (q + 1) * w := by
          calc
            q * w + r ≤ q * w + w := Nat.add_le_add_left (Nat.le_of_lt hrw) (q * w)
            _ = (q + 1) * w := by rw [Nat.add_mul, Nat.one_mul]
        rw [Nat.min_eq_left hle]
        exact Nat.add_sub_cancel_left (q * w) r
      rw [hlast, ← hnr, List.range_add, List.append_nil]
  change (List.range c.n).map (fun i => (i, c.base + c.step * ((i / w : Nat) : Int))) =
    (List.range ((c.n + w - 1) / w)).flatMap
      (fun j => (List.range (min c.n ((j + 1) * w) - j * w)).map
        (fun i => (j * w + i, c.base + c.step * (j : Int))))
  have hpair :
      (List.range ((c.n + w - 1) / w)).flatMap
        (fun j => (List.range (min c.n ((j + 1) * w) - j * w)).map
          (fun i => (j * w + i, c.base + c.step * (j : Int)))) =
      ((List.range ((c.n + w - 1) / w)).flatMap
        (fun j => (List.range (min c.n ((j + 1) * w) - j * w)).map (fun i => j * w + i))).map
          (fun p => (p, c.base + c.step * ((p / w : Nat) : Int))) := by
    rw [List.map_flatMap]
    apply flatMap_congr_left
    intro j hj
    rw [List.map_map]
    apply List.map_congr_left
    intro i hi
    have hilen : i < min c.n ((j + 1) * w) - j * w := List.mem_range.mp hi
    have hminle : min c.n ((j + 1) * w) ≤ (j + 1) * w := Nat.min_le_right _ _
    have hsub : min c.n ((j + 1) * w) - j * w ≤ (j + 1) * w - j * w :=
      Nat.sub_le_sub_right hminle _
    have heq : (j + 1) * w - j * w = w := by
      rw [Nat.add_mul, Nat.one_mul]
      exact Nat.add_sub_cancel_left (j * w) w
    have hiw : i < w := by omega
    have hdiv : (j * w + i) / w = j := by
      rw [Nat.add_comm (j * w) i, Nat.mul_comm j w, Nat.add_mul_div_left i j hp,
        Nat.div_eq_of_lt hiw, Nat.zero_add]
    change (j * w + i, c.base + c.step * (j : Int)) =
      (j * w + i, c.base + c.step * (((j * w + i) / w : Nat) : Int))
    rw [hdiv]
  rw [hpair, hceil, hchunks]

theorem sound_ResidueJoin : BucketRuns.ResidueJoin.Goal := by
  intro c b
  rw [rows_lanes c]
  rw [flatMap_sum_nat]
  simp only [layout]
  apply congrArg List.sum
  apply List.map_congr_left
  intro a ha
  have ha0 : a.step = 0 := by
    simp [layout] at ha
    rcases ha with ⟨j, hj, rfl⟩
    simp [span, lane]
  exact lane_residue a ha0 b

/-! ## Counting an intersection of a unit progression -/

private theorem nodup_filter_bool {α : Type} (l : List α) (p : α → Bool) (h : l.Nodup) :
    (l.filter p).Nodup := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp at h ⊢
      by_cases hp : p x = true <;> simp [hp, h.1, ih h.2]

private theorem nodup_map_inj {α β : Type} (l : List α) (f : α → β) (h : l.Nodup)
    (hinj : ∀ x ∈ l, ∀ y ∈ l, f x = f y → x = y) : (l.map f).Nodup := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hxnot : x ∉ xs := (List.nodup_cons.mp h).1
      have hndxs : xs.Nodup := (List.nodup_cons.mp h).2
      have ih' := ih hndxs (fun y hy z hz heq => hinj y (by simp [hy]) z (by simp [hz]) heq)
      constructor
      · intro y hy heq
        rcases List.mem_map.mp hy with ⟨z, hz, hzf⟩
        have hzx : z = x := hinj z (by simp [hz]) x (by simp) (hzf.trans heq.symm)
        exact hxnot (by simpa [hzx] using hz)
      · exact ih'

private theorem intersection_count_unit (s n : Nat) (b : Progression) :
    let a : Progression := ⟨(s : Int), 1, n⟩
    (List.range n).countP (fun i : Nat => contains b ((s : Int) + (i : Int))) =
      (intersection a b).n := by
  dsimp only
  let a : Progression := ⟨(s : Int), 1, n⟩
  let r := intersection a b
  rw [List.countP_eq_length_filter]
  by_cases hn : n = 0
  · subst n
    simp [a, intersection, contains]
  · by_cases hbn : b.n = 0
    · simp [a, intersection, hbn, contains]
    · have hnp : 0 < n := by omega
      have hbnp : 0 < b.n := by omega
      by_cases hb0 : b.step = 0
      · have hInt : r = ⟨b.lo, 1, if contains a b.lo then 1 else 0⟩ := by
          dsimp [r, a, intersection]
          simp [hn, hbn, hb0]
        change ((List.range n).filter (fun i : Nat => contains b ((s : Int) + (i : Int)))).length = r.n
        rw [hInt]
        simp only [Progression.n]
        have hfilterEq : (List.range n).filter (fun i : Nat => contains b ((s : Int) + (i : Int))) =
            (List.range n).filter (fun i : Nat => (b.lo == (s : Int) + (i : Int))) := by
          apply filter_congr_bool
          intro i hi
          cases hb : b with
          | mk blo bst bn =>
            have hbst : bst = 0 := by simpa [hb] using hb0
            subst bst
            have hbnpos : 0 < bn := by simpa [hb] using hbnp
            simp [hb, contains, lastVal, hbnpos]
            by_cases hle : blo ≤ (s : Int) + (i : Int)
            · by_cases hge : (s : Int) + (i : Int) ≤ blo
              · have heq : blo = (s : Int) + (i : Int) := by omega
                simp [hle, hge, heq, hbnpos]
              · have hne : blo ≠ (s : Int) + (i : Int) := by omega
                simp [hle, hge, beq_eq_false_iff_ne.mpr hne]
            · have hne : blo ≠ (s : Int) + (i : Int) := by omega
              simp [hle, beq_eq_false_iff_ne.mpr hne]
        rw [hfilterEq]
        by_cases hc : contains a b.lo = true
        · rw [if_pos hc]
          have huniq : ∀ x ∈ List.range n,
                (b.lo == (s : Int) + (x : Int)) = true →
                ∀ y ∈ List.range n, (b.lo == (s : Int) + (y : Int)) = true → x = y := by
              intro x hx hpx y hy hpy
              exact_mod_cast (Int.add_left_cancel ((beq_iff_eq.mp hpx).symm.trans (beq_iff_eq.mp hpy)))
          have hex : ∃ x, x ∈ List.range n ∧ (b.lo == (s : Int) + (x : Int)) = true := by
            obtain ⟨k, hk, hkv⟩ := (contains_iff_mem a b.lo).mp hc
            unfold progVal at hkv
            dsimp [a] at hkv
            have hkv' : b.lo = (s : Int) + (k : Int) := by simpa using hkv
            refine ⟨k, List.mem_range.mpr hk, ?_⟩
            exact beq_iff_eq.mpr hkv'
          rw [show ((List.range n).filter (fun i : Nat => b.lo == (s : Int) + (i : Int))).length =
              (List.range n).countP (fun i : Nat => b.lo == (s : Int) + (i : Int)) by
            exact (List.countP_eq_length_filter (p := fun i : Nat => b.lo == (s : Int) + (i : Int))
              (l := List.range n)).symm]
          have hexNat : ∃ x, x < n ∧ b.lo = (s : Int) + (x : Int) := by
            obtain ⟨x, hx, hxeq⟩ := hex
            exact ⟨x, List.mem_range.mp hx, beq_iff_eq.mp hxeq⟩
          rw [countP_eq_one_of_unique _ _ List.nodup_range huniq]
          simp [hex, hexNat]
        · rw [if_neg hc]
          have hfalse : ∀ i ∈ List.range n, (b.lo == (s : Int) + (i : Int)) = false := by
            intro i hi
            apply Bool.eq_false_of_ne_true
            intro heq
            exact hc ((contains_iff_mem a b.lo).mpr ⟨i, List.mem_range.mp hi, by
              unfold progVal
              dsimp [a]
              simpa using (beq_iff_eq.mp heq)⟩)
          rw [show ((List.range n).filter (fun i : Nat => b.lo == (s : Int) + (i : Int))).length =
              (List.range n).countP (fun i : Nat => b.lo == (s : Int) + (i : Int)) by
            exact (List.countP_eq_length_filter (p := fun i : Nat => b.lo == (s : Int) + (i : Int))
              (l := List.range n)).symm]
          exact congrArg id (countP_eq_zero_of_all_false (List.range n)
            (fun i : Nat => b.lo == (s : Int) + (i : Int)) hfalse)
      · have hbsp : 0 < b.step := Nat.pos_of_ne_zero hb0
        have hrstep_eq : r.step = b.step := by
          dsimp [r, a, intersection]
          simp [hn, hbn, hb0]
        have hrstep : 0 < r.step := by
          rw [hrstep_eq]
          exact hbsp
        let idx : Nat → Nat := fun k => ((r.lo + (k : Int) * (r.step : Int) - (s : Int))).toNat
        have hidx_nonneg : ∀ k < r.n, 0 ≤ r.lo + (k : Int) * (r.step : Int) - (s : Int) := by
          intro k hk
          have hv : contains r (r.lo + (k : Int) * (r.step : Int)) := by
            apply (contains_iff_mem r _).mpr
            exact ⟨k, hk, rfl⟩
          have hva := (intersection_contains a b _).mp hv |>.1
          have hbnd := contains_bounds a _ hva
          dsimp [a] at hbnd
          omega
        have hidx_eq : ∀ k < r.n, (idx k : Int) = r.lo + (k : Int) * (r.step : Int) - (s : Int) := by
          intro k hk
          dsimp [idx]
          exact Int.toNat_of_nonneg (hidx_nonneg k hk)
        have hidx_lt : ∀ k < r.n, idx k < n := by
          intro k hk
          have hv : contains r (r.lo + (k : Int) * (r.step : Int)) := by
            apply (contains_iff_mem r _).mpr
            exact ⟨k, hk, rfl⟩
          have hva := (intersection_contains a b _).mp hv |>.1
          obtain ⟨i, hi, hival⟩ := (contains_iff_mem a _).mp hva
          unfold progVal at hival
          dsimp [a] at hival
          have hil : (i : Int) = (idx k : Int) := by
            rw [hidx_eq k hk]
            omega
          have hilNat : i = idx k := by exact_mod_cast hil
          rw [← hilNat]
          exact hi
        have hidx_inj : ∀ k1 < r.n, ∀ k2 < r.n, idx k1 = idx k2 → k1 = k2 := by
          intro k1 hk1 k2 hk2 heq
          have h1 := hidx_eq k1 hk1
          have h2 := hidx_eq k2 hk2
          have hmul : (k1 : Int) * (r.step : Int) = (k2 : Int) * (r.step : Int) := by omega
          have hrne : (r.step : Int) ≠ 0 := by omega
          have hc : (k1 : Int) = (k2 : Int) := (Int.mul_eq_mul_right_iff hrne).mp hmul
          exact_mod_cast hc
        have hmapnd : ((List.range r.n).map idx).Nodup := by
          exact nodup_map_inj (List.range r.n) idx List.nodup_range
            (fun x hx y hy heq => hidx_inj x (List.mem_range.mp hx) y (List.mem_range.mp hy) heq)
        have hfilternd : ((List.range n).filter (fun i : Nat => contains b ((s : Int) + (i : Int)))).Nodup :=
          nodup_filter_bool (List.range n) (fun i : Nat => contains b ((s : Int) + (i : Int))) List.nodup_range
        have hmem : ∀ i : Nat,
            i ∈ (List.range n).filter (fun i : Nat => contains b ((s : Int) + (i : Int))) ↔
            i ∈ (List.range r.n).map idx := by
          intro i
          constructor
          · intro hi
            have hi' := List.mem_filter.mp hi
            have hia : contains a ((s : Int) + (i : Int)) := by
              apply (contains_iff_mem a _).mpr
              exact ⟨i, List.mem_range.mp hi'.1, by unfold progVal; dsimp [a]; simp⟩
            have hir : contains r ((s : Int) + (i : Int)) :=
              (intersection_contains a b _).mpr ⟨hia, hi'.2⟩
            obtain ⟨k, hk, hkv⟩ := (contains_iff_mem r _).mp hir
            unfold progVal at hkv
            have hieqInt : (i : Int) = (idx k : Int) := by
              rw [hidx_eq k hk]
              omega
            have hieq : i = idx k := by exact_mod_cast hieqInt
            rw [List.mem_map]
            exact ⟨k, List.mem_range.mpr hk, hieq.symm⟩
          · intro hi
            obtain ⟨k, hk, hik⟩ := List.mem_map.mp hi
            have hklt : k < r.n := List.mem_range.mp hk
            have hv : contains r (r.lo + (k : Int) * (r.step : Int)) := by
              apply (contains_iff_mem r _).mpr
              exact ⟨k, hklt, rfl⟩
            have hbnd := (intersection_contains a b _).mp hv |>.2
            rw [List.mem_filter]
            refine ⟨List.mem_range.mpr (by rw [← hik]; exact hidx_lt k hklt), ?_⟩
            have hikInt : (idx k : Int) = (i : Int) := by exact_mod_cast hik
            have hival : (s : Int) + (i : Int) = r.lo + (k : Int) * (r.step : Int) := by
              rw [← hikInt]
              rw [hidx_eq k hklt]
              omega
            rwa [hival]
        have hperm : ((List.range n).filter (fun i : Nat => contains b ((s : Int) + (i : Int)))).Perm
            ((List.range r.n).map idx) :=
          (List.perm_ext_iff_of_nodup hfilternd hmapnd).mpr hmem
        rw [hperm.length_eq]
        simp only [List.length_map, List.length_range]
        rfl

/-! ## DeleteProgression -/

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

private theorem filter_map_const_snd (l : List (Nat × Int)) (v : Int) (p : Nat × Int → Bool)
    (h : ∀ r ∈ l, r.2 = v) :
    (((l.filter p).map Prod.snd).sum) = (((l.filter p).length : Nat) : Int) * v := by
  induction l with
  | nil => simp
  | cons r rs ih =>
      by_cases hp : p r = true
      · simp [hp, h r (by simp), ih (fun x hx => h x (by simp [hx]))]
        push_cast
        rw [Int.add_mul, Int.one_mul]
        omega
      · simp [hp, ih (fun x hx => h x (by simp [hx]))]

private theorem lane_filter_snd_sum (a : Lane) (ha0 : a.step = 0) (p : Nat × Int → Bool) :
    (((lanePoints a).filter p).map Prod.snd).sum =
      (((lanePoints a).filter p).length : Int) * a.base := by
  apply filter_map_const_snd
  intro r hr
  rcases List.mem_map.mp hr with ⟨i, hi, rfl⟩
  simp [lanePoints, ha0]

private theorem lane_filter_count (a : Lane) (hastride : a.stride = 1) (mask : Progression) :
    ((lanePoints a).filter (fun r => contains mask r.1)).length =
      (intersection (positions a) mask).n := by
  unfold lanePoints
  change (((List.range a.n).map (fun i => (a.start + i * a.stride, a.base + (i : Int) * a.step))).filter
      (fun r => (fun x : Nat => contains mask x) r.1)).length = _
  rw [show ((List.range a.n).map (fun i => (a.start + i * a.stride, a.base + (i : Int) * a.step))).filter
        (fun r => (fun x : Nat => contains mask x) r.1) =
      ((List.range a.n).filter (fun i : Nat => contains mask (a.start + i * a.stride))).map
        (fun i => (a.start + i * a.stride, a.base + (i : Int) * a.step)) by
    exact map_pair_filter (List.range a.n) (fun i => a.start + i * a.stride)
      (fun i => a.base + (i : Int) * a.step) (fun x : Nat => contains mask x)]
  simp only [List.length_map]
  rw [← List.countP_eq_length_filter]
  rw [hastride]
  have hp : positions a = ⟨(a.start : Int), 1, a.n⟩ := by
    unfold positions
    rw [hastride]
  rw [hp]
  simpa using (intersection_count_unit a.start a.n mask)

private theorem lane_delete (a : Lane) (ha0 : a.step = 0) (hastride : a.stride = 1)
    (mask : Progression) :
    (((lanePoints a).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
      partialSum a a.n - deletedSum a mask := by
  rw [lane_filter_snd_sum a ha0]
  rw [length_filter_not]
  have hlp : (lanePoints a).length = a.n := by simp [lanePoints]
  rw [hlp]
  let ov := (intersection (positions a) mask).n
  have hov : ((lanePoints a).filter (fun r => contains mask r.1)).length = ov :=
    lane_filter_count a hastride mask
  rw [hov]
  have hovle : ov ≤ a.n := by
    change ov ≤ a.n
    rw [← hov]
    exact Nat.le_trans (List.length_filter_le (fun r : Nat × Int => contains mask r.1) (lanePoints a))
      (by rw [hlp]; omega)
  have hcast : (((a.n - ov : Nat) : Int)) = (a.n : Int) - (ov : Int) := by
    omega
  rw [hcast]
  have hpartial : partialSum a a.n = (a.n : Int) * a.base := by
    simp [partialSum, ha0]
  have hdel : deletedSum a mask = (ov : Int) * a.base := by
    dsimp [ov]
    simp [deletedSum, firstAt, slopeAt, ha0]
  rw [hpartial, hdel]
  rw [Int.sub_mul]

theorem sound_DeleteProgression : BucketRuns.DeleteProgression.Goal := by
  intro c mask
  rw [rows_lanes c]
  rw [flatMap_filter_snd_sum]
  apply congrArg List.sum
  apply List.map_congr_left
  intro a ha
  have ha0 : a.step = 0 := by
    simp [layout] at ha
    rcases ha with ⟨j, hj, rfl⟩
    simp [span, lane]
  have hastride : a.stride = 1 := by
    simp [layout] at ha
    rcases ha with ⟨j, hj, rfl⟩
    simp [span, lane]
  exact lane_delete a ha0 hastride mask

/-! ## AlignedProduct -/

private theorem intersection_index_perm (s n : Nat) (b : Progression) (hbpos : 0 < b.step) :
  let a : Progression := ⟨(s : Int), 1, n⟩
  let r := intersection a b
  let idx := fun k : Nat => (r.lo + (k : Int) * (r.step : Int) - (s : Int)).toNat
  ((List.range n).filter (fun i : Nat => contains b ((s : Int) + (i : Int)))).Perm
    ((List.range r.n).map idx) := by
  dsimp only
  let a : Progression := ⟨(s : Int), 1, n⟩
  let r := intersection a b
  let idx : Nat → Nat := fun k => (r.lo + (k : Int) * (r.step : Int) - (s : Int)).toNat
  by_cases hn : n = 0
  · subst n
    simp [a, r, idx, intersection, contains]
  · by_cases hbn : b.n = 0
    · simp [a, r, idx, intersection, hbn, contains]
    · have hnp : 0 < n := by omega
      have hbnp : 0 < b.n := by omega
      have hb0 : ¬ b.step = 0 := by omega
      have hbsp : 0 < b.step := hbpos
      have hrstep_eq : r.step = b.step := by
        dsimp [r, a, intersection]
        simp [hn, hbn, hb0]
      have hrstep : 0 < r.step := by
        rw [hrstep_eq]
        exact hbsp
      let idx : Nat → Nat := fun k => ((r.lo + (k : Int) * (r.step : Int) - (s : Int))).toNat
      have hidx_nonneg : ∀ k < r.n, 0 ≤ r.lo + (k : Int) * (r.step : Int) - (s : Int) := by
        intro k hk
        have hv : contains r (r.lo + (k : Int) * (r.step : Int)) := by
          apply (contains_iff_mem r _).mpr
          exact ⟨k, hk, rfl⟩
        have hva := (intersection_contains a b _).mp hv |>.1
        have hbnd := contains_bounds a _ hva
        dsimp [a] at hbnd
        omega
      have hidx_eq : ∀ k < r.n, (idx k : Int) = r.lo + (k : Int) * (r.step : Int) - (s : Int) := by
        intro k hk
        dsimp [idx]
        exact Int.toNat_of_nonneg (hidx_nonneg k hk)
      have hidx_lt : ∀ k < r.n, idx k < n := by
        intro k hk
        have hv : contains r (r.lo + (k : Int) * (r.step : Int)) := by
          apply (contains_iff_mem r _).mpr
          exact ⟨k, hk, rfl⟩
        have hva := (intersection_contains a b _).mp hv |>.1
        obtain ⟨i, hi, hival⟩ := (contains_iff_mem a _).mp hva
        unfold progVal at hival
        dsimp [a] at hival
        have hil : (i : Int) = (idx k : Int) := by
          rw [hidx_eq k hk]
          omega
        have hilNat : i = idx k := by exact_mod_cast hil
        rw [← hilNat]
        exact hi
      have hidx_inj : ∀ k1 < r.n, ∀ k2 < r.n, idx k1 = idx k2 → k1 = k2 := by
        intro k1 hk1 k2 hk2 heq
        have h1 := hidx_eq k1 hk1
        have h2 := hidx_eq k2 hk2
        have hmul : (k1 : Int) * (r.step : Int) = (k2 : Int) * (r.step : Int) := by omega
        have hrne : (r.step : Int) ≠ 0 := by omega
        have hc : (k1 : Int) = (k2 : Int) := (Int.mul_eq_mul_right_iff hrne).mp hmul
        exact_mod_cast hc
      have hmapnd : ((List.range r.n).map idx).Nodup := by
        exact nodup_map_inj (List.range r.n) idx List.nodup_range
          (fun x hx y hy heq => hidx_inj x (List.mem_range.mp hx) y (List.mem_range.mp hy) heq)
      have hfilternd : ((List.range n).filter (fun i : Nat => contains b ((s : Int) + (i : Int)))).Nodup :=
        nodup_filter_bool (List.range n) (fun i : Nat => contains b ((s : Int) + (i : Int))) List.nodup_range
      have hmem : ∀ i : Nat,
          i ∈ (List.range n).filter (fun i : Nat => contains b ((s : Int) + (i : Int))) ↔
          i ∈ (List.range r.n).map idx := by
        intro i
        constructor
        · intro hi
          have hi' := List.mem_filter.mp hi
          have hia : contains a ((s : Int) + (i : Int)) := by
            apply (contains_iff_mem a _).mpr
            exact ⟨i, List.mem_range.mp hi'.1, by unfold progVal; dsimp [a]; simp⟩
          have hir : contains r ((s : Int) + (i : Int)) :=
            (intersection_contains a b _).mpr ⟨hia, hi'.2⟩
          obtain ⟨k, hk, hkv⟩ := (contains_iff_mem r _).mp hir
          unfold progVal at hkv
          have hieqInt : (i : Int) = (idx k : Int) := by
            rw [hidx_eq k hk]
            omega
          have hieq : i = idx k := by exact_mod_cast hieqInt
          rw [List.mem_map]
          exact ⟨k, List.mem_range.mpr hk, hieq.symm⟩
        · intro hi
          obtain ⟨k, hk, hik⟩ := List.mem_map.mp hi
          have hklt : k < r.n := List.mem_range.mp hk
          have hv : contains r (r.lo + (k : Int) * (r.step : Int)) := by
            apply (contains_iff_mem r _).mpr
            exact ⟨k, hklt, rfl⟩
          have hbnd := (intersection_contains a b _).mp hv |>.2
          rw [List.mem_filter]
          refine ⟨List.mem_range.mpr (by rw [← hik]; exact hidx_lt k hklt), ?_⟩
          have hikInt : (idx k : Int) = (i : Int) := by exact_mod_cast hik
          have hival : (s : Int) + (i : Int) = r.lo + (k : Int) * (r.step : Int) := by
            rw [← hikInt]
            rw [hidx_eq k hklt]
            omega
          rwa [hival]
      have hperm : ((List.range n).filter (fun i : Nat => contains b ((s : Int) + (i : Int)))).Perm
          ((List.range r.n).map idx) :=
        (List.perm_ext_iff_of_nodup hfilternd hmapnd).mpr hmem
      exact hperm

private theorem sum_map_eq_zero {α : Type} (l : List α) (f : α → Int)
    (h : ∀ x ∈ l, f x = 0) : (l.map f).sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem sum_eq_single_nat (n i0 : Nat) (hi0 : i0 < n) (f : Nat → Int)
    (hzero : ∀ i < n, i ≠ i0 → f i = 0) :
    ((List.range n).map f).sum = f i0 := by
  induction n with
  | zero => omega
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.sum_append]
      by_cases heq : n = i0
      · subst n
        rw [show ((List.range i0).map f).sum = 0 from
          sum_map_eq_zero (List.range i0) f (by
            intro i hi
            have hilt : i < i0 := List.mem_range.mp hi
            exact hzero i (by omega) (by omega))]
        simp
      · have hi0n : i0 < n := by omega
        rw [ih hi0n (fun i hi hne => hzero i (by omega) hne)]
        simp [heq, hzero n (by omega) heq]

private theorem aligned_intersection_facts (a b : Lane) (hastride : a.stride = 1)
    (hbspos : 0 < b.stride) (hrn : (intersection (positions a) (positions b)).n ≠ 0) :
    let r := intersection (positions a) (positions b)
    r.step = b.stride ∧ (b.stride : Int) ∣ r.lo - (b.start : Int) := by
  dsimp only
  let r := intersection (positions a) (positions b)
  have han : (positions a).n ≠ 0 := by
    intro h
    have : r.n = 0 := by simp [r, intersection, h]
    exact hrn this
  have hbn : (positions b).n ≠ 0 := by
    intro h
    have : r.n = 0 := by simp [r, intersection, h]
    exact hrn this
  have hrstep : r.step = b.stride := by
    dsimp [positions] at han hbn
    dsimp [r, positions, intersection]
    have hg : (1 : Nat).gcd b.stride = 1 := Nat.gcd_one_left b.stride
    have hbne : b.stride ≠ 0 := by omega
    simp_all [han, hbn, hastride, hbspos, hbne, hg]
  have hrpos : 0 < r.n := Nat.pos_of_ne_zero hrn
  have hcontains : contains r r.lo = true := by
    apply (contains_iff_mem r r.lo).mpr
    refine ⟨0, hrpos, ?_⟩
    unfold progVal
    simp
  have hbcont : contains (positions b) r.lo = true :=
    (intersection_contains (positions a) (positions b) r.lo).mp hcontains |>.2
  have hdiv := contains_dvd (positions b) r.lo hbcont
  dsimp [positions] at hdiv
  exact ⟨hrstep, hdiv⟩
