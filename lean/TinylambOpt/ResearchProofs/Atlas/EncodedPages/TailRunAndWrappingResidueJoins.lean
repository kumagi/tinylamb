import TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `BucketRuns.ResidueJoin` — `∀ c (b : Lane), ((rows layout c).map (fun r =>
  (List.range b.n).countP (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum =
  ((layout.lanes c).map (joinCount · b)).sum`
- `MirroredTail.ResidueJoin` — `∀ c (b : Lane), ((rows layout c).map (fun r =>
  (List.range b.n).countP (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum =
  ((layout.lanes c).map (joinCount · b)).sum`
- `WrappingSequence.ResidueJoin` — `∀ c (b : Lane), ((rows layout c).map (fun r =>
  (List.range b.n).countP (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum =
  ((layout.lanes c).map (joinCount · b)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages.TailRunAndWrappingResidueJoins

open TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-! ## Generic list and lane algebra -/

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

/-! ## Arithmetic progressions and their intersection -/

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

private theorem contains_const_prog (a : Progression) (has : a.step = 0) (v : Int) :
    contains a v = true ↔ 0 < a.n ∧ v = a.lo := by
  cases a with
  | mk lo st n =>
      simp at has
      subst st
      exact contains_const_iff lo n v

private theorem contains_step_one_small (lo : Int) (n : Nat) (hn : n ≤ 1) (v : Int) :
    contains ⟨lo, 1, n⟩ v = true ↔ 0 < n ∧ v = lo := by
  rw [contains_iff_mem]
  constructor
  · rintro ⟨k, hk, hval⟩
    change k < n at hk
    have hk0 : k = 0 := by omega
    subst k
    simp [progVal] at hval
    exact ⟨by omega, hval⟩
  · rintro ⟨hnpos, hv⟩
    refine ⟨0, by omega, ?_⟩
    simp [progVal, hv]

private theorem contains_bounds (a : Progression) (v : Int) (h : contains a v = true) :
    a.lo ≤ v ∧ v ≤ lastVal a := by
  have hm := (contains_iff_mem a v).mp h
  obtain ⟨k, hk, hval⟩ := hm
  unfold progVal at hval
  have hk_le : k ≤ a.n - 1 := by omega
  have hkint : (k : Int) ≤ ((a.n - 1 : Nat) : Int) := by exact_mod_cast hk_le
  have hstep : 0 ≤ (a.step : Int) := by exact_mod_cast (Nat.zero_le a.step)
  have hmul := Int.mul_le_mul_of_nonneg_right hkint hstep
  unfold lastVal
  constructor
  · rw [hval]
    exact Int.le_add_of_nonneg_right (Int.mul_nonneg (by exact_mod_cast Nat.zero_le k) hstep)
  · rw [hval]
    exact Int.add_le_add_left hmul a.lo

private theorem contains_dvd (a : Progression) (v : Int) (h : contains a v = true) :
    (a.step : Int) ∣ v - a.lo := by
  obtain ⟨k, _, hval⟩ := (contains_iff_mem a v).mp h
  unfold progVal at hval
  refine ⟨(k : Int), ?_⟩
  rw [hval]
  rw [Int.mul_comm]
  omega

private theorem common_gap_dvd (a b : Progression) (v : Int)
    (ha : contains a v = true) (hb : contains b v = true) :
    (a.step.gcd b.step : Int) ∣ b.lo - a.lo := by
  obtain ⟨i, _, hi⟩ := (contains_iff_mem a v).mp ha
  obtain ⟨j, _, hj⟩ := (contains_iff_mem b v).mp hb
  unfold progVal at hi hj
  have hgdvd_r : (a.step.gcd b.step : Int) ∣ (b.step : Int) := by
    exact_mod_cast (Nat.gcd_dvd_right a.step b.step)
  have hgdvd_l : (a.step.gcd b.step : Int) ∣ (a.step : Int) := by
    exact_mod_cast (Nat.gcd_dvd_left a.step b.step)
  have h1 : (a.step.gcd b.step : Int) ∣ (b.step : Int) * (j : Int) :=
    Int.dvd_mul_of_dvd_left (c := (j : Int)) hgdvd_r
  have h2 : (a.step.gcd b.step : Int) ∣ (a.step : Int) * (i : Int) :=
    Int.dvd_mul_of_dvd_left (c := (i : Int)) hgdvd_l
  have hi' : v = a.lo + (a.step : Int) * (i : Int) := by
    rw [Int.mul_comm] at hi
    exact hi
  have hj' : v = b.lo + (b.step : Int) * (j : Int) := by
    rw [Int.mul_comm] at hj
    exact hj
  have heq : (a.step : Int) * (i : Int) - (b.step : Int) * (j : Int) = b.lo - a.lo := by
    omega
  rw [← heq]
  exact Int.dvd_sub h2 h1

private theorem empty_progressions_no_mem (p : Progression) (hn : p.n = 0) (v : Int) :
    contains p v ≠ true := by
  intro h
  obtain ⟨k, hk, _⟩ := (contains_iff_mem p v).mp h
  omega

private theorem intersection_empty_left (a b : Progression) (v : Int) (han : a.n = 0) :
    contains (intersection a b) v ≠ true := by
  intro h
  obtain ⟨k, hk, _⟩ := (contains_iff_mem (intersection a b) v).mp h
  simp [intersection, han] at hk

private theorem intersection_empty_right (a b : Progression) (v : Int) (hbn : b.n = 0) :
    contains (intersection a b) v ≠ true := by
  intro h
  obtain ⟨k, hk, _⟩ := (contains_iff_mem (intersection a b) v).mp h
  by_cases han : a.n = 0
  · simp [intersection, han] at hk
  · simp [intersection, han, hbn] at hk

private theorem const_prog_contains_eq (lo : Int) (n : Nat) (v : Int) :
    contains ⟨lo, 0, n⟩ v = true ↔ 0 < n ∧ v = lo := contains_const_iff lo n v

private theorem common_diff_dvd_period (a b : Progression)
    (hasp : 0 < a.step) (hbsp : 0 < b.step) (x : Int)
    (ha : (a.step : Int) ∣ x) (hb : (b.step : Int) ∣ x) :
    (((a.step / a.step.gcd b.step) * b.step : Nat) : Int) ∣ x := by
  let g := a.step.gcd b.step
  let A := a.step / g
  let B := b.step / g
  have hgpos : 0 < g := Nat.gcd_pos_of_pos_left b.step hasp
  have hg_dvd_a : g ∣ a.step := Nat.gcd_dvd_left a.step b.step
  have hg_dvd_b : g ∣ b.step := Nat.gcd_dvd_right a.step b.step
  have hAs : (a.step : Int) = (g : Int) * (A : Int) := by
    dsimp [A, g]
    exact_mod_cast (Nat.mul_div_cancel' hg_dvd_a).symm
  have hBs : (b.step : Int) = (g : Int) * (B : Int) := by
    dsimp [B, g]
    exact_mod_cast (Nat.mul_div_cancel' hg_dvd_b).symm
  obtain ⟨p, hp⟩ := ha
  obtain ⟨q, hq⟩ := hb
  have hp' : x = (g : Int) * (A : Int) * p := by
    rw [hp, hAs]
  have hq' : x = (g : Int) * (B : Int) * q := by
    rw [hq, hBs]
  have hAB : (A : Int) * p = (B : Int) * q := by
    have h := hp'.symm.trans hq'
    have hgne : (g : Int) ≠ 0 := by exact_mod_cast (by omega : g ≠ 0)
    have h' : (g : Int) * ((A : Int) * p) = (g : Int) * ((B : Int) * q) := by
      calc
        (g : Int) * ((A : Int) * p) = (g : Int) * (A : Int) * p := by ac_rfl
        _ = (g : Int) * (B : Int) * q := h
        _ = (g : Int) * ((B : Int) * q) := by ac_rfl
    exact Int.eq_of_mul_eq_mul_left hgne h'
  have hbez := bezout_spec A B
  rw [show A.gcd B = 1 by
    simpa [A, B, g] using reduced_coprime a.step b.step hasp hbsp] at hbez
  simp only [Int.ofNat_one] at hbez
  have hBdiv : (B : Int) ∣ p := by
    refine ⟨(bezout A B).1 * q + (bezout A B).2 * p, ?_⟩
    calc
      p = ((A : Int) * (bezout A B).1 + (B : Int) * (bezout A B).2) * p := by
        rw [← hbez]
        simp
      _ = ((A : Int) * (bezout A B).1) * p + ((B : Int) * (bezout A B).2) * p := by
        rw [Int.add_mul]
      _ = (bezout A B).1 * ((A : Int) * p) + (B : Int) * ((bezout A B).2 * p) := by
        ac_rfl
      _ = (bezout A B).1 * ((B : Int) * q) + (B : Int) * ((bezout A B).2 * p) := by rw [hAB]
      _ = (B : Int) * ((bezout A B).1 * q + (bezout A B).2 * p) := by
        rw [Int.mul_add]
        ac_rfl
  obtain ⟨r, hr⟩ := hBdiv
  refine ⟨r, ?_⟩
  change x = (((a.step / g) * b.step : Nat) : Int) * r
  rw [hp', hr]
  have hper : (((a.step / g) * b.step : Nat) : Int) = (g : Int) * (A : Int) * (B : Int) := by
    have hb_eq : b.step = g * B := by dsimp [B, g]; exact (Nat.mul_div_cancel' hg_dvd_b).symm
    rw (occs := .pos [1]) [hb_eq]
    dsimp [A]
    push_cast
    ac_rfl
  rw [hper]
  ac_rfl

private theorem intersection_contains (a b : Progression) (v : Int) :
    contains (intersection a b) v = true ↔ contains a v = true ∧ contains b v = true := by
  by_cases han : a.n = 0
  · have ha0 := empty_progressions_no_mem a han v
    have hi0 := intersection_empty_left a b v han
    simp [ha0, hi0]
  · by_cases hbn : b.n = 0
    · have hb0 := empty_progressions_no_mem b hbn v
      have hi0 := intersection_empty_right a b v hbn
      simp [hb0, hi0]
    · have hanp : 0 < a.n := by omega
      have hbnp : 0 < b.n := by omega
      by_cases has : a.step = 0
      · have hIntEq : intersection a b = ⟨a.lo, 1, if contains b a.lo then 1 else 0⟩ := by
          unfold intersection
          simp [han, hbn, has]
        have hnle : (if contains b a.lo then 1 else 0) ≤ 1 := by split <;> omega
        have hleft := contains_step_one_small a.lo (if contains b a.lo then 1 else 0) hnle v
        rw [hIntEq, hleft]
        constructor
        · rintro ⟨hnpos, hv⟩
          have hb_lo : contains b a.lo = true := by
            by_cases hb : contains b a.lo = true
            · exact hb
            · simp [hb] at hnpos
          have ha_v : contains a v = true := by
            rw [hv]
            exact (contains_const_prog a has a.lo).mpr ⟨hanp, rfl⟩
          exact ⟨ha_v, by simpa [hv] using hb_lo⟩
        · rintro ⟨ha, hb⟩
          have hv : v = a.lo := ((contains_const_prog a has v).mp ha).2
          refine ⟨?_, hv⟩
          have hb' : contains b a.lo = true := by simpa [hv] using hb
          simpa [hb']
      · by_cases hbs : b.step = 0
        · have hIntEq : intersection a b = ⟨b.lo, 1, if contains a b.lo then 1 else 0⟩ := by
            unfold intersection
            simp [han, hbn, has, hbs]
          have hnle : (if contains a b.lo then 1 else 0) ≤ 1 := by split <;> omega
          have hleft := contains_step_one_small b.lo (if contains a b.lo then 1 else 0) hnle v
          rw [hIntEq, hleft]
          constructor
          · rintro ⟨hnpos, hv⟩
            have ha_lo : contains a b.lo = true := by
              by_cases ha : contains a b.lo = true
              · exact ha
              · simp [ha] at hnpos
            have hb_v : contains b v = true := by
              rw [hv]
              exact (contains_const_prog b hbs b.lo).mpr ⟨hbnp, rfl⟩
            exact ⟨by simpa [hv] using ha_lo, hb_v⟩
          · rintro ⟨ha, hb⟩
            have hv : v = b.lo := ((contains_const_prog b hbs v).mp hb).2
            refine ⟨?_, hv⟩
            have ha' : contains a b.lo = true := by simpa [hv] using ha
            simpa [ha']
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
          have hgpos : 0 < g := Nat.gcd_pos_of_pos_left b.step hasp
          have hdivpos : 0 < a.step / g := Nat.div_pos (Nat.gcd_le_left b.step hasp) hgpos
          have hperposNat : 0 < period := by dsimp [period]; exact Nat.mul_pos hdivpos hbsp
          have hp : (0 : Int) < period := by exact_mod_cast hperposNat
          have hgapdef : gap = b.lo - a.lo := rfl
          by_cases hgapmod : gap % (g : Int) = 0
          · have hgap : (g : Int) ∣ gap := Int.dvd_of_emod_eq_zero hgapmod
            have hroot := root_dvd a.step b.step hasp hbsp a.lo b.lo gap hgapdef hgap
            have hfirst : first = low + (root - low) % (period : Int) := rfl
            have hfroot := first_root_dvd low root first period hp hfirst
            have hflow : low ≤ first := by
              dsimp [first]
              exact Int.le_add_of_nonneg_right (Int.emod_nonneg _ (by omega))
            have hg_dvd_a : g ∣ a.step := Nat.gcd_dvd_left a.step b.step
            have hg_dvd_b : g ∣ b.step := Nat.gcd_dvd_right a.step b.step
            have hb_eq : b.step = g * (b.step / g) := (Nat.mul_div_cancel' hg_dvd_b).symm
            have ha_eq : a.step = g * (a.step / g) := (Nat.mul_div_cancel' hg_dvd_a).symm
            have hper_g : period = (a.step / g) * (b.step / g) * g := by
              dsimp [period]
              rw (occs := .pos [1]) [hb_eq]
              ac_rfl
            have hper_a : period = a.step * (b.step / g) := by
              rw [hper_g]
              calc
                (a.step / g) * (b.step / g) * g = (g * (a.step / g)) * (b.step / g) := by ac_rfl
                _ = a.step * (b.step / g) := by rw [Nat.mul_div_cancel' hg_dvd_a]
            have hper_b : period = b.step * (a.step / g) := by
              rw [hper_g]
              calc
                (a.step / g) * (b.step / g) * g = (g * (b.step / g)) * (a.step / g) := by ac_rfl
                _ = b.step * (a.step / g) := by rw [Nat.mul_div_cancel' hg_dvd_b]
            have hpa : (a.step : Int) ∣ (period : Int) := by
              refine ⟨((b.step / g : Nat) : Int), ?_⟩
              exact_mod_cast hper_a
            have hpb : (b.step : Int) ∣ (period : Int) := by
              refine ⟨((a.step / g : Nat) : Int), ?_⟩
              exact_mod_cast hper_b
            have han_beq : (a.n == 0) = false := beq_eq_false_iff_ne.mpr han
            have hbn_beq : (b.n == 0) = false := beq_eq_false_iff_ne.mpr hbn
            have has_beq : (a.step == 0) = false := beq_eq_false_iff_ne.mpr has
            have hbs_beq : (b.step == 0) = false := beq_eq_false_iff_ne.mpr hbs
            have hgap_beq : ((b.lo - a.lo) % (g : Int) != 0) = false := by
              have hz : (b.lo - a.lo) % (g : Int) = 0 := by simpa [gap] using hgapmod
              simp [hz]
            have hIntEq : intersection a b =
                ⟨first, period, if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ := by
              unfold intersection
              simp [han_beq, hbn_beq, has_beq, hbs_beq, hgap_beq,
                g, gap, period, unit, root, low, first, high]
            rw [hIntEq]
            have hspan := span_contains_iff first (period : Int) high v hp
            simp only [Int.toNat_natCast] at hspan
            rw [hspan]
            constructor
            · rintro ⟨hfirstx, hxhigh, hxfirst⟩
              have hxroot : (period : Int) ∣ v - root := by
                have heq : v - root = (v - first) + (first - root) := by omega
                rw [heq]
                exact Int.dvd_add hxfirst hfroot
              have hxroot_a : (a.step : Int) ∣ v - root := Int.dvd_trans hpa hxroot
              have hxroot_b : (b.step : Int) ∣ v - root := Int.dvd_trans hpb hxroot
              have hxalo : (a.step : Int) ∣ v - a.lo := by
                have hsum : (a.step : Int) ∣ (v - root) + (root - a.lo) :=
                  Int.dvd_add hxroot_a hroot.1
                have heq : (v - root) + (root - a.lo) = v - a.lo := by omega
                rwa [heq] at hsum
              have hxblo : (b.step : Int) ∣ v - b.lo := by
                have hsum : (b.step : Int) ∣ (v - root) + (root - b.lo) :=
                  Int.dvd_add hxroot_b hroot.2
                have heq : (v - root) + (root - b.lo) = v - b.lo := by omega
                rwa [heq] at hsum
              have hxlow : low ≤ v := by omega
              constructor
              · exact mem_of_bounds_dvd a v hanp hasp (by omega) (by omega) hxalo
              · exact mem_of_bounds_dvd b v hbnp hbsp (by omega) (by omega) hxblo
            · rintro ⟨ha, hb⟩
              have hab := contains_bounds a v ha
              have hbb := contains_bounds b v hb
              have hxalo := contains_dvd a v ha
              have hxblo := contains_dvd b v hb
              have hdiffa : (a.step : Int) ∣ v - root := by
                have h0 : (a.step : Int) ∣ (v - a.lo) - (root - a.lo) :=
                  Int.dvd_sub hxalo hroot.1
                have heq0 : (v - a.lo) - (root - a.lo) = v - root := by omega
                rwa [heq0] at h0
              have hdiffb : (b.step : Int) ∣ v - root := by
                have h0 : (b.step : Int) ∣ (v - b.lo) - (root - b.lo) :=
                  Int.dvd_sub hxblo hroot.2
                have heq0 : (v - b.lo) - (root - b.lo) = v - root := by omega
                rwa [heq0] at h0
              have hxroot0 := common_diff_dvd_period a b hasp hbsp (v - root) hdiffa hdiffb
              have hxroot : (period : Int) ∣ v - root := by
                simpa [period] using hxroot0
              have hxfirst : (period : Int) ∣ v - first := by
                have heq : v - first = (v - root) - (first - root) := by omega
                rw [heq]
                exact Int.dvd_sub hxroot hfroot
              have hfv : first ≤ v :=
                first_le_of_congruence low root first v (period : Int) hp rfl (by omega) hxroot
              exact ⟨hfv, by dsimp [high]; omega, hxfirst⟩
          · have hgapne : gap % (g : Int) ≠ 0 := by simpa using hgapmod
            have han_beq : (a.n == 0) = false := beq_eq_false_iff_ne.mpr han
            have hbn_beq : (b.n == 0) = false := beq_eq_false_iff_ne.mpr hbn
            have has_beq : (a.step == 0) = false := beq_eq_false_iff_ne.mpr has
            have hbs_beq : (b.step == 0) = false := beq_eq_false_iff_ne.mpr hbs
            have hIntEq : intersection a b = ⟨0, 1, 0⟩ := by
              have hgapmod' : (b.lo - a.lo) % (a.step.gcd b.step : Int) ≠ 0 := by
                simpa [gap, g] using hgapmod
              unfold intersection
              simp [han_beq, hbn_beq, has_beq, hbs_beq, hgapmod']
            rw [hIntEq]
            simp only [contains]
            constructor
            · intro h
              simp at h
            · rintro ⟨ha, hb⟩
              exfalso
              have hg := common_gap_dvd a b v ha hb
              exact hgapne (Int.emod_eq_zero_of_dvd hg)


/-! ## Counting progressions and the value set of a lane -/

private def progNums (r : Progression) : List Int :=
  (List.range r.n).map (fun k => progVal r k)

private def countProg (r : Progression) (v : Int) : Nat :=
  (progNums r).countP (fun w => w == v)

private theorem mem_progNums_iff (r : Progression) (v : Int) :
    v ∈ progNums r ↔ contains r v = true := by
  rw [contains_iff_mem]
  constructor
  · intro hv
    rcases List.mem_map.mp hv with ⟨k, hk, hval⟩
    exact ⟨k, List.mem_range.mp hk, hval.symm⟩
  · rintro ⟨k, hk, hval⟩
    exact List.mem_map.mpr ⟨k, List.mem_range.mpr hk, hval.symm⟩

private theorem progNums_nodup_of_pos (r : Progression) (hs : 0 < r.step) :
    (progNums r).Nodup := by
  unfold progNums progVal
  apply List.Pairwise.map (R := fun i j : Nat => i ≠ j) (S := fun x y : Int => x ≠ y)
      (f := fun k : Nat => r.lo + (k : Int) * r.step)
  · intro i j hne hEq
    have hstep : (r.step : Int) ≠ 0 := by exact_mod_cast (Nat.ne_of_gt hs)
    have hm : (i : Int) * r.step = (j : Int) * r.step := by omega
    have hij : (i : Int) = (j : Int) := (Int.mul_eq_mul_right_iff hstep).mp hm
    exact hne (by exact_mod_cast hij)
  · exact List.nodup_range

private theorem countP_beq_zero_of_not_mem (l : List Int) (v : Int) (hv : v ∉ l) :
    l.countP (fun w => w == v) = 0 := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      have hxne : x ≠ v := by intro h; exact hv (by simp [h])
      have hxs : v ∉ xs := by intro h; exact hv (by simp [h])
      simp [beq_eq_false_iff_ne.mpr hxne, ih hxs]

private theorem countP_beq_one_of_nodup_mem (l : List Int) (hnd : l.Nodup)
    (v : Int) (hv : v ∈ l) : l.countP (fun w => w == v) = 1 := by
  induction l with
  | nil => simp at hv
  | cons x xs ih =>
      simp only [List.countP_cons, List.mem_cons] at hv ⊢
      by_cases hx : x = v
      · subst x
        have hfst := (List.pairwise_cons.mp hnd).1
        have hnot : v ∉ xs := by
          intro hv'
          exact hfst v hv' rfl
        rw [countP_beq_zero_of_not_mem xs v hnot]
        simp
      · have htail := (List.pairwise_cons.mp hnd).2
        have hvxs : v ∈ xs := by
          rcases hv with h | h
          · exact (hx h.symm).elim
          · exact h
        rw [beq_eq_false_iff_ne.mpr hx, ih htail hvxs]
        simp

private theorem countP_perm {α : Type} (p : α → Bool) {l₁ l₂ : List α}
    (h : l₁.Perm l₂) : l₁.countP p = l₂.countP p := by
  rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
  exact (h.filter p).length_eq

private theorem countP_const (lo v : Int) (n : Nat) :
    ((List.range n).map (fun _ : Nat => lo)).countP (fun w => w == v) =
      if lo == v then n else 0 := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.countP_append]
      by_cases h : lo == v <;> simp [h, ih]

private theorem progNums_of_step_zero (r : Progression) (hs : r.step = 0) :
    progNums r = (List.range r.n).map (fun _ : Nat => r.lo) := by
  unfold progNums progVal
  apply List.map_congr_left
  intro k hk
  simp [hs]

private theorem countProg_eq (r : Progression) (v : Int) :
    countProg r v = if contains r v then (if r.step == 0 then r.n else 1) else 0 := by
  by_cases hs : r.step = 0
  · have hbeq : (r.step == 0) = true := by simp [hs]
    unfold countProg
    rw [progNums_of_step_zero r hs]
    rw [countP_const]
    by_cases hn : r.n = 0
    · simp [hn, contains, hs]
    by_cases hvbeq : r.lo == v
    · have hveq : r.lo = v := beq_iff_eq.mp hvbeq
      have hc : contains r v = true := (contains_const_prog r hs v).mpr ⟨by omega, hveq.symm⟩
      simp [hvbeq, hc, hbeq, hn]
    · have hcf : contains r v = false := by
        cases hcv : contains r v
        · rfl
        · have hveq' : v = r.lo := ((contains_const_prog r hs v).mp hcv).2
          exact (hvbeq (by simp [hveq'])).elim
      simp [hvbeq, hcf]
  · have hsp : 0 < r.step := Nat.pos_of_ne_zero hs
    have hbeq : (r.step == 0) = false := beq_eq_false_iff_ne.mpr hs
    have hnd := progNums_nodup_of_pos r hsp
    unfold countProg
    have hbeq' : ¬ (r.step == 0) = true := by simp [hbeq]
    by_cases hv : v ∈ progNums r
    · have hc : contains r v = true := (mem_progNums_iff r v).mp hv
      rw [if_pos hc, if_neg hbeq']
      exact countP_beq_one_of_nodup_mem (progNums r) hnd v hv
    · have hcf : contains r v = false := by
        cases hcv : contains r v
        · rfl
        · exact (hv ((mem_progNums_iff r v).mpr hcv)).elim
      rw [if_neg (by simp [hcf])]
      exact countP_beq_zero_of_not_mem (progNums r) v hv
private theorem range_reverse_eq_map (n : Nat) :
    (List.range n).reverse = (List.range n).map (fun k => n - 1 - k) := by
  apply List.ext_get
  · simp
  · intro k hk1 hk2
    change (List.range n).reverse[k] = ((List.range n).map (fun k => n - 1 - k))[k]
    rw [List.getElem_reverse]
    simp only [List.length_range]
    rw [List.getElem_map, List.getElem_range]
    rw [List.getElem_range]

private theorem range_rev_index_perm (n : Nat) :
    ((List.range n).map (fun k => n - 1 - k)).Perm (List.range n) := by
  rw [← range_reverse_eq_map n]
  exact List.reverse_perm (List.range n)

private theorem values_progNums_perm (b : Lane) :
    (progNums (values b)).Perm
      ((List.range b.n).map (fun j : Nat => b.base + b.step * (j : Int))) := by
  by_cases hneg : b.step < 0
  · have hsabs : ((b.step.natAbs : Nat) : Int) = -b.step :=
      Int.ofNat_natAbs_of_nonpos (by omega)
    have hvals : values b = ⟨b.base + b.step * ((b.n - 1 : Nat) : Int), b.step.natAbs, b.n⟩ := by
      simp [values, hneg, Int.mul_comm]
    rw [hvals]
    unfold progNums progVal
    simp only
    have hp := (range_rev_index_perm b.n).map (fun k : Nat => b.base + b.step * (k : Int))
    rw [show (List.range b.n).map (fun k : Nat =>
          (b.base + b.step * ((b.n - 1 : Nat) : Int)) + (k : Int) * (b.step.natAbs : Int)) =
        (List.range b.n).map (fun k : Nat => b.base + b.step * ((b.n - 1 - k : Nat) : Int)) by
      apply List.map_congr_left
      intro k hk
      have hkn : k < b.n := List.mem_range.mp hk
      rw [hsabs]
      rw [Int.natCast_sub (by omega : k ≤ b.n - 1)]
      rw [Int.mul_sub]
      rw [Int.mul_neg, Int.mul_comm (k : Int) b.step]
      omega]
    simpa [List.map_map, Function.comp_def] using hp
  · have hsabs : ((b.step.natAbs : Nat) : Int) = b.step :=
      Int.natAbs_of_nonneg (by omega)
    have hvals : values b = ⟨b.base, b.step.natAbs, b.n⟩ := by
      simp [values, hneg]
    rw [hvals]
    unfold progNums progVal
    simp only
    rw [show (List.range b.n).map (fun k : Nat => b.base + (k : Int) * (b.step.natAbs : Int)) =
        (List.range b.n).map (fun j : Nat => b.base + b.step * (j : Int)) by
      apply List.map_congr_left
      intro j hj
      rw [hsabs, Int.mul_comm]]


private theorem sum_ite_nat {α : Type} (l : List α) (p : α → Bool) (v : Nat) :
    (l.map (fun x => if p x then v else 0)).sum = v * (l.filter p).length := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih, Nat.mul_add] <;> omega

private theorem mem_values_progNums (a : Lane) (v : Int) :
    v ∈ progNums (values a) ↔ ∃ i, i < a.n ∧ a.base + (i : Int) * a.step = v := by
  have hp := values_progNums_perm a
  constructor
  · intro hv
    have hv' : v ∈ (List.range a.n).map (fun i : Nat => a.base + a.step * (i : Int)) :=
      (hp.mem_iff).mp hv
    rcases List.mem_map.mp hv' with ⟨i, hi, hval⟩
    exact ⟨i, List.mem_range.mp hi, by simpa [Int.mul_comm] using hval⟩
  · rintro ⟨i, hi, hval⟩
    apply (hp.mem_iff).mpr
    exact List.mem_map.mpr ⟨i, List.mem_range.mpr hi, by simpa [Int.mul_comm] using hval⟩

private theorem values_step_pos_of_ne (a : Lane) (hs : a.step ≠ 0) :
    0 < (values a).step := by
  by_cases hneg : a.step < 0
  · simp [values, hneg]
    exact hs
  · simp [values, hneg]
    exact hs

private theorem intersection_step_pos (p q : Progression) : 0 < (intersection p q).step := by
  unfold intersection
  by_cases hn : p.n == 0 || q.n == 0
  · simp [hn]
  by_cases hp : p.step == 0
  · simp [hn, hp]
  by_cases hq : q.step == 0
  · simp [hn, hp, hq]
  by_cases hg : (q.lo - p.lo) % (p.step.gcd q.step : Int) != 0
  · simp [hn, hp, hq, hg]
  · simp [hn, hp, hq, hg]
    have hppos : 0 < p.step := Nat.pos_of_ne_zero (by simpa using hp)
    have hqpos : 0 < q.step := Nat.pos_of_ne_zero (by simpa using hq)
    exact Nat.mul_pos (Nat.div_pos (Nat.gcd_le_left q.step hppos)
      (Nat.gcd_pos_of_pos_left q.step hppos)) hqpos

private theorem intersection_n_eq_common_count (p q : Progression) (hp : 0 < p.step) :
    (intersection p q).n = ((progNums p).filter (fun v => contains q v)).length := by
  have hmem : ∀ v, v ∈ progNums (intersection p q) ↔
      v ∈ (progNums p).filter (fun v => contains q v) := by
    intro v
    rw [List.mem_filter, mem_progNums_iff, mem_progNums_iff]
    exact intersection_contains p q v
  have hndp : (progNums p).Nodup := progNums_nodup_of_pos p hp
  have hndf : ((progNums p).filter (fun v => contains q v)).Nodup :=
    List.Pairwise.filter (fun v => contains q v) hndp
  have hperm : (progNums (intersection p q)).Perm ((progNums p).filter (fun v => contains q v)) :=
    (List.perm_ext_iff_of_nodup (progNums_nodup_of_pos _ (intersection_step_pos p q)) hndf).mpr hmem
  rw [← hperm.length_eq]
  simp [progNums]

private theorem countP_values_lane (a : Lane) (p : Int → Bool) :
    ((List.range a.n).countP (fun i : Nat => p (a.base + (i : Int) * a.step))) =
      ((progNums (values a)).countP p) := by
  rw [show (List.range a.n).countP (fun i : Nat => p (a.base + (i : Int) * a.step)) =
      ((List.range a.n).map (fun i : Nat => a.base + (i : Int) * a.step)).countP p by
    rw [List.countP_map]
    rfl]
  rw [show (List.range a.n).map (fun i : Nat => a.base + (i : Int) * a.step) =
      (List.range a.n).map (fun i : Nat => a.base + a.step * (i : Int)) by
    apply List.map_congr_left
    intro i hi
    rw [Int.mul_comm]]
  exact countP_perm p (values_progNums_perm a).symm

private theorem lane_value_frequency (a : Lane) (v : Int) :
    ((List.range a.n).filter (fun i : Nat => a.base + (i : Int) * a.step == v)).length =
      (if contains (values a) v then 1 else 0) * (if a.step == 0 then a.n else 1) := by
  rw [← List.countP_eq_length_filter]
  rw [show ((List.range a.n).countP (fun i : Nat => a.base + (i : Int) * a.step == v)) =
      ((progNums (values a)).countP (fun w : Int => w == v)) by
    exact countP_values_lane a (fun w : Int => w == v)]
  change countProg (values a) v = _
  rw [countProg_eq]
  by_cases hs : a.step = 0
  · have hbeq : ((values a).step == 0) = true := by simp [values, hs]
    rw [if_pos hbeq]
    rw [show values a = ⟨a.base, 0, a.n⟩ by simp [values, hs]]
    by_cases hcv : contains ⟨a.base, 0, a.n⟩ v <;> simp [hs, hcv]
  · have hbeq : ((values a).step == 0) = false := by
      have hp := values_step_pos_of_ne a hs
      simp [Nat.ne_of_gt hp]
    have hbeq' : ¬ (((values a).step == 0) = true) := by simp [hbeq]
    rw [if_neg hbeq']
    by_cases hcv : contains (values a) v <;> simp [hs, hcv]

private theorem values_const_of_step_zero (a : Lane) (hs : a.step = 0) :
    values a = ⟨a.base, 0, a.n⟩ := by
  simp [values, hs]

private theorem lane_join_count (a b : Lane) :
    ((lanePoints a).map (fun r =>
      (List.range b.n).countP (fun i : Nat => r.2 == b.base + b.step * (i : Int)))).sum =
      joinCount a b := by
  unfold lanePoints
  simp only [List.map_map]
  change (((List.range a.n).map (fun i : Nat =>
    (List.range b.n).countP (fun j : Nat => a.base + (i : Int) * a.step == b.base + b.step * (j : Int))))).sum = _
  have hcount : ∀ i : Nat,
      (List.range b.n).countP (fun j : Nat => a.base + (i : Int) * a.step == b.base + b.step * (j : Int)) =
        countProg (values b) (a.base + (i : Int) * a.step) := by
    intro i
    have h1 : (List.range b.n).countP (fun j : Nat => a.base + (i : Int) * a.step == b.base + b.step * (j : Int)) =
        ((List.range b.n).map (fun j : Nat => b.base + b.step * (j : Int))).countP
          (fun w : Int => w == a.base + (i : Int) * a.step) := by
      rw [List.countP_map]
      apply List.countP_congr
      intro j hj
      change (a.base + (i : Int) * a.step == b.base + b.step * (j : Int)) ↔
        ((b.base + b.step * (j : Int)) == a.base + (i : Int) * a.step)
      rw [beq_iff_eq, beq_iff_eq]
      constructor <;> intro h <;> exact h.symm
    unfold countProg
    rw [h1]
    exact countP_perm (fun w : Int => w == a.base + (i : Int) * a.step) (values_progNums_perm b).symm
  rw [show (List.range a.n).map (fun i : Nat =>
      (List.range b.n).countP (fun j : Nat => a.base + (i : Int) * a.step == b.base + b.step * (j : Int))) =
      (List.range a.n).map (fun i : Nat => countProg (values b) (a.base + (i : Int) * a.step)) by
    apply List.map_congr_left
    intro i hi
    exact hcount i]
  rw [joinCount]
  by_cases hbz : b.step = 0
  · by_cases hbn : b.n = 0
    · rw [show (List.range a.n).map (fun i : Nat => countProg (values b) (a.base + (i : Int) * a.step)) =
          (List.range a.n).map (fun _ : Nat => 0) by
        apply List.map_congr_left
        intro i hi
        rw [← hcount i]
        simp [hbn]]
      have hzero : ((List.range a.n).map (fun _ : Nat => 0)).sum = 0 := by
        induction a.n with
        | zero => simp
        | succ n ih => rw [List.range_succ, List.map_append, List.sum_append]; simp [ih]
      rw [hzero]
      simp [hbn, hbz]
    · have hvsB : values b = ⟨b.base, 0, b.n⟩ := values_const_of_step_zero b hbz
      have hbzbeq : (b.step == 0) = true := by simp [hbz]
      have hsB : ((values b).step == 0) = true := by simp [hvsB]
      rw [show (List.range a.n).map (fun i : Nat => countProg (values b) (a.base + (i : Int) * a.step)) =
          (List.range a.n).map (fun i : Nat => if contains (values b) (a.base + (i : Int) * a.step) then b.n else 0) by
        apply List.map_congr_left
        intro i hi
        rw [countProg_eq]
        rw [if_pos hsB]
        simp [hvsB]]
      rw [sum_ite_nat]
      rw [hvsB]
      by_cases han : a.n = 0
      · by_cases haz : a.step = 0
        · simp [han, haz]
        · have hvaN : (values a).n = 0 := by simp [values, han]
          have hIntN : (intersection (values a) ⟨b.base, 0, b.n⟩).n = 0 := by
            unfold intersection
            simp [hvaN]
          simp [han, haz, hIntN]
      · have hIntN : (intersection (values a) ⟨b.base, 0, b.n⟩).n =
            if contains (values a) b.base then 1 else 0 := by
          by_cases haz : a.step = 0
          · rw [values_const_of_step_zero a haz]
            unfold intersection
            simp [han, hbn, contains_const_iff]
            have hanpos : 0 < a.n := by omega
            have hbnpos : 0 < b.n := by omega
            by_cases heq : a.base = b.base <;> simp [heq, hanpos, hbnpos] <;> omega
          · have hsp := values_step_pos_of_ne a haz
            have hsneq : (values a).step ≠ 0 := by omega
            unfold intersection
            simp [han, hbn, hsneq, values, haz]
        simp [hbzbeq]
        rw [hIntN]
        have hfilt : ((List.range a.n).filter
            (fun i : Nat => contains ⟨b.base, 0, b.n⟩ (a.base + (i : Int) * a.step))).length =
            (if contains (values a) b.base then 1 else 0) * (if a.step == 0 then a.n else 1) := by
          have hc : ∀ i : Nat, contains ⟨b.base, 0, b.n⟩ (a.base + (i : Int) * a.step) =
              (a.base + (i : Int) * a.step == b.base) := by
            intro i
            by_cases hv : a.base + (i : Int) * a.step == b.base
            · have hveq := beq_iff_eq.mp hv
              have hc1 : contains ⟨b.base, 0, b.n⟩ (a.base + (i : Int) * a.step) = true :=
                (contains_const_iff b.base b.n (a.base + (i : Int) * a.step)).mpr ⟨by omega, hveq⟩
              rw [hc1, hv]
            · have hvne : a.base + (i : Int) * a.step ≠ b.base := by
                intro h
                exact hv (by simp [h])
              have hf : contains ⟨b.base, 0, b.n⟩ (a.base + (i : Int) * a.step) = false := by
                cases hcv : contains ⟨b.base, 0, b.n⟩ (a.base + (i : Int) * a.step)
                · rfl
                · have hh := (contains_const_iff b.base b.n (a.base + (i : Int) * a.step)).mp hcv
                  exact (hvne hh.2).elim
              have hvfalse : (a.base + (i : Int) * a.step == b.base) = false := by
                cases hb : (a.base + (i : Int) * a.step == b.base) <;> simp_all
              rw [hf, hvfalse]
          rw [show (List.range a.n).filter
                (fun i : Nat => contains ⟨b.base, 0, b.n⟩ (a.base + (i : Int) * a.step)) =
              (List.range a.n).filter (fun i : Nat => a.base + (i : Int) * a.step == b.base) by
            apply filter_congr_bool
            intro i hi
            exact hc i]
          exact lane_value_frequency a b.base
        rw [hfilt]
        simpa [Nat.mul_comm]
  · have hbzbeq : (b.step == 0) = false := beq_eq_false_iff_ne.mpr hbz
    simp [hbzbeq]
    have hsB : ((values b).step == 0) = false := by
      have hp := values_step_pos_of_ne b hbz
      simp [Nat.ne_of_gt hp]
    rw [show (List.range a.n).map (fun i : Nat => countProg (values b) (a.base + (i : Int) * a.step)) =
        (List.range a.n).map (fun i : Nat => if contains (values b) (a.base + (i : Int) * a.step) then 1 else 0) by
      apply List.map_congr_left
      intro i hi
      rw [countProg_eq]
      have hsB' : ¬ (((values b).step == 0) = true) := by simp [hsB]
      rw [if_neg hsB']]
    rw [show ((List.range a.n).map (fun i : Nat =>
        if contains (values b) (a.base + (i : Int) * a.step) then 1 else 0)).sum =
        ((List.range a.n).filter (fun i : Nat =>
          contains (values b) (a.base + (i : Int) * a.step))).length by
      rw [← List.countP_eq_length_filter]
      exact (countP_eq_sum (List.range a.n)
        (fun i : Nat => contains (values b) (a.base + (i : Int) * a.step))).symm]
    have hfilt : ((List.range a.n).filter (fun i : Nat =>
          contains (values b) (a.base + (i : Int) * a.step))).length =
        ((progNums (values a)).filter (fun v : Int => contains (values b) v)).length := by
      rw [← List.countP_eq_length_filter, ← List.countP_eq_length_filter]
      exact countP_values_lane a (fun v : Int => contains (values b) v)
    rw [hfilt]
    by_cases haz : a.step = 0
    · have hvsA : values a = ⟨a.base, 0, a.n⟩ := values_const_of_step_zero a haz
      by_cases han : a.n = 0
      · simp [han, values, haz, progNums]
      · by_cases hbn : (values b).n = 0
        · have hvn : (values b).n = b.n := by simp [values]
          have hbn' : b.n = 0 := by rw [← hvn]; exact hbn
          rw [show values b = ⟨b.base, b.step.natAbs, 0⟩ by simp [values, hbn']]
          rw [hvsA]
          simp [progNums, progVal]
          have hc0 : contains ⟨b.base, b.step.natAbs, 0⟩ a.base = false := by
            have hn' : contains ⟨b.base, b.step.natAbs, 0⟩ a.base ≠ true := by
              intro h
              obtain ⟨k, hk, _⟩ := (contains_iff_mem ⟨b.base, b.step.natAbs, 0⟩ a.base).mp h
              exact (Nat.not_lt_zero k hk).elim
            cases hcv : contains ⟨b.base, b.step.natAbs, 0⟩ a.base <;> simp_all
          rw [show (List.filter (fun v : Int => contains ⟨b.base, b.step.natAbs, 0⟩ v)
              ((List.range a.n).map (fun _ : Nat => a.base))).length = 0 by
            rw [← List.countP_eq_length_filter, List.countP_map]
            change (List.range a.n).countP (fun _ : Nat =>
              contains ⟨b.base, b.step.natAbs, 0⟩ a.base) = 0
            simp [hc0]]
          simp [intersection]
        · have hIntN : (intersection (values a) (values b)).n =
              if contains (values b) a.base then 1 else 0 := by
            rw [hvsA]
            have hvn : (values b).n = b.n := by simp [values]
            rw [hvn] at hbn
            have hbnpos : 0 < b.n := by omega
            have hbneqv : (values b).n ≠ 0 := by rw [hvn]; omega
            have haneq : (a.n == 0) = false := beq_eq_false_iff_ne.mpr han
            unfold intersection
            simp [hbneqv, haneq]
          rw [hIntN]
          simp [haz]
          have hfilt' : ((progNums (values a)).filter (fun v : Int => contains (values b) v)).length =
              (if contains (values b) a.base then 1 else 0) * a.n := by
            rw [hvsA]
            simp [progNums, progVal]
            rw [← List.countP_eq_length_filter]
            rw [List.countP_map]
            change (List.range a.n).countP (fun _ : Nat => contains (values b) a.base) = _
            by_cases hc : contains (values b) a.base <;> simp [hc]
          rw [hfilt']
    · have hposA : 0 < (values a).step := values_step_pos_of_ne a haz
      rw [intersection_n_eq_common_count (values a) (values b) hposA]
      simp [haz]

private theorem list_sum_perm_nat {l₁ l₂ : List Nat} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simp [ih]
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem sum_map_flatMap {α β : Type} (l : List α) (f : α → List β) (g : β → Int) :
    ((l.flatMap f).map g).sum = (l.map (fun a => ((f a).map g).sum)).sum := by
  induction l with
  | nil => simp
  | cons a as ih => simp [ih]

private theorem sum_map_flatMap_nat {α β : Type} (l : List α) (f : α → List β) (g : β → Nat) :
    ((l.flatMap f).map g).sum = (l.map (fun a => ((f a).map g).sum)).sum := by
  induction l with
  | nil => simp
  | cons a as ih => simp [ih]

private theorem residueJoin_of_perm (lay : Layout) (c : Config) (b : Lane)
    (hperm : (rows lay c).Perm ((lay.lanes c).flatMap lanePoints)) :
    ((rows lay c).map (fun r =>
      (List.range b.n).countP (fun i : Nat => r.2 == b.base + b.step * (i : Int)))).sum =
      ((lay.lanes c).map (joinCount · b)).sum := by
  let g : Nat × Int → Nat := fun r =>
    (List.range b.n).countP (fun i : Nat => r.2 == b.base + b.step * (i : Int))
  have hp : ((rows lay c).map g).Perm (((lay.lanes c).flatMap lanePoints).map g) := hperm.map g
  rw [show ((rows lay c).map g).sum = (((lay.lanes c).flatMap lanePoints).map g).sum by
    exact list_sum_perm_nat hp]
  rw [show (((lay.lanes c).flatMap lanePoints).map (fun r =>
      (List.range b.n).countP (fun i : Nat => r.2 == b.base + b.step * (i : Int)))).sum =
      ((lay.lanes c).map (fun a =>
        ((lanePoints a).map (fun r =>
          (List.range b.n).countP (fun i : Nat => r.2 == b.base + b.step * (i : Int)))).sum)).sum by
    exact sum_map_flatMap_nat (lay.lanes c) lanePoints g]
  apply congrArg List.sum
  apply List.map_congr_left
  intro a ha
  exact lane_join_count a b

private theorem flatMap_congr_left {α β : Type} (l : List α) (f g : α → List β)
    (h : ∀ a ∈ l, f a = g a) : l.flatMap f = l.flatMap g := by
  induction l with
  | nil => rfl
  | cons a as ih => simp [h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private theorem rows_range_map (lay : Layout) (c : Config) (f : Nat → Int)
    (h : ∀ i, i < c.n → lay.read c i = some (f i)) :
    rows lay c = (List.range c.n).map (fun i => (i, f i)) := by
  unfold rows
  exact range_filterMap_all c.n (fun i => (lay.read c i).map (fun v => (i, v)))
    (fun i => (i, f i)) (by intro i hi; simp [h i hi])
namespace MirroredTail

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.MirroredTail

private theorem rows_map (c : Config) :
    rows layout c = (List.range c.n).map (fun i =>
      (i, if i < c.split then c.base + c.step * (i : Int)
        else c.base + c.step * ((c.n - 1 - i : Nat) : Int))) := by
  apply rows_range_map
  intro i _
  by_cases h : i < c.split <;> simp [layout, h]

private theorem rows_lanes (c : Config) :
    rows layout c = (layout.lanes c).flatMap lanePoints := by
  rw [rows_map]
  simp only [layout, List.flatMap_cons, List.flatMap_nil, List.append_nil]
  by_cases h : c.split ≤ c.n
  · have h1 : span c 0 c.split c.base c.step = ⟨0, 1, c.split, c.base, c.step⟩ := by
      unfold span lane
      simp [Nat.min_eq_right h]
    have h2 : span c c.split c.n
        (c.base + c.step * ((c.n - 1 - c.split : Nat) : Int)) (-c.step) =
        ⟨c.split, 1, c.n - c.split,
          c.base + c.step * ((c.n - 1 - c.split : Nat) : Int), -c.step⟩ := by
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
          else c.base + c.step * ((c.n - 1 - i : Nat) : Int))) =
        (List.range c.split).map (fun i : Nat => (0 + i * 1, c.base + (i : Int) * c.step)) := by
      apply List.map_congr_left
      intro i hi
      have hi' : i < c.split := List.mem_range.mp hi
      simp [hi', Int.mul_comm]
    have hsecond : (List.range (c.n - c.split)).map (fun i =>
        (c.split + i, if c.split + i < c.split then c.base + c.step * ((c.split + i : Nat) : Int)
          else c.base + c.step * ((c.n - 1 - (c.split + i) : Nat) : Int))) =
        (List.range (c.n - c.split)).map (fun i : Nat =>
          (c.split + i * 1,
            (c.base + c.step * ((c.n - 1 - c.split : Nat) : Int)) + (i : Int) * (-c.step))) := by
      apply List.map_congr_left
      intro i hi
      have hnot : ¬ c.split + i < c.split := by omega
      have hi' : c.split + i < c.n := by
        have := List.mem_range.mp hi
        omega
      have hsub : c.n - 1 - (c.split + i) = c.n - 1 - c.split - i := by omega
      simp only [hnot, if_false, hsub]
      rw [Int.natCast_sub (by omega : i ≤ c.n - 1 - c.split)]
      rw [Int.natCast_sub (by omega : c.split ≤ c.n - 1)]
      grind
    rw [hfirst, hsecond]
  · have h1 : span c 0 c.split c.base c.step = ⟨0, 1, c.n, c.base, c.step⟩ := by
      unfold span lane
      simp [Nat.min_eq_left (by omega : c.n ≤ c.split)]
    have h2 : span c c.split c.n
        (c.base + c.step * ((c.n - 1 - c.split : Nat) : Int)) (-c.step) =
        ⟨c.split, 1, 0, c.base + c.step * ((c.n - 1 - c.split : Nat) : Int), -c.step⟩ := by
      unfold span lane
      simp [Nat.sub_eq_zero_of_le (by omega : c.n ≤ c.split)]
    rw [h1, h2]
    unfold lanePoints
    simp
    intro i hi
    have hi' : i < c.n := hi
    have his : i < c.split := by omega
    simp [his, Int.mul_comm]

end MirroredTail

namespace BucketRuns

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.BucketRuns


private theorem width_pos (c : Config) : 0 < width c := by dsimp [width]; omega

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

end BucketRuns

private theorem positions_flatMap_nodup_bound (n p m : Nat) (hp : 0 < p) (hm : m ≤ p) :
    ((List.range m).flatMap
      (fun r => (List.range ((n - r + p - 1) / p)).map (fun q => r + q * p))).Nodup := by
  induction m with
  | zero => simp
  | succ k ih =>
      rw [List.range_succ, List.flatMap_append]
      apply List.nodup_append.mpr
      refine ⟨ih (by omega), ?_, ?_⟩
      · simp only [List.flatMap_cons, List.flatMap_nil, List.append_nil]
        apply List.Pairwise.map (R := fun i j : Nat => i ≠ j) (S := fun x y : Nat => x ≠ y)
          (f := fun q : Nat => k + q * p)
        · intro q₁ q₂ hne heq
          have hmul : q₁ * p = q₂ * p := by omega
          exact hne (Nat.eq_of_mul_eq_mul_right hp hmul)
        · exact List.nodup_range
      · intro a ha b hb hEq
        have hmod' : ∀ x ∈ (List.range k).flatMap
            (fun r => (List.range ((n - r + p - 1) / p)).map (fun q => r + q * p)), x % p < k := by
          intro x hx
          rcases List.mem_flatMap.mp hx with ⟨r, hr, hxmem⟩
          rcases List.mem_map.mp hxmem with ⟨q, hq, hqeq⟩
          have hrk : r < k := List.mem_range.mp hr
          have hrp : r < p := by omega
          rw [← hqeq]
          rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hrp]
          exact hrk
        have hak : a % p < k := hmod' a ha
        simp only [List.flatMap_cons, List.flatMap_nil, List.append_nil] at hb
        rcases List.mem_map.mp hb with ⟨q, hq, hqeq⟩
        have hkp : k < p := by omega
        have hbk : b % p = k := by
          rw [← hqeq]
          rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hkp]
        have hmodEq : a % p = b % p := by rw [hEq]
        omega

private theorem positions_flatMap_nodup (n p : Nat) (hp : 0 < p) :
    ((List.range (min p n)).flatMap
      (fun r => (List.range ((n - r + p - 1) / p)).map (fun q => r + q * p))).Nodup :=
  positions_flatMap_nodup_bound n p (min p n) hp (Nat.min_le_left p n)

private theorem flatMap_residue_perm (n p : Nat) (hp : 0 < p) (f : Nat → Int)
    (hval : ∀ r q, r < p → f (r + q * p) = f r) :
    ((List.range (min p n)).flatMap
      (fun r => (List.range ((n - r + p - 1) / p)).map (fun q => (r + q * p, f r)))).Perm
      ((List.range n).map (fun i => (i, f i))) := by
  let P := (List.range (min p n)).flatMap
      (fun r => (List.range ((n - r + p - 1) / p)).map (fun q => r + q * p))
  let L := (List.range (min p n)).flatMap
      (fun r => (List.range ((n - r + p - 1) / p)).map (fun q => (r + q * p, f r)))
  let R := (List.range n).map (fun i => (i, f i))
  have hLR : L = P.map (fun i => (i, f i)) := by
    unfold L P
    rw [List.map_flatMap]
    apply flatMap_congr_left
    intro r hr
    rw [List.map_map]
    apply List.map_congr_left
    intro q hq
    have hrp : r < p := by
      have := List.mem_range.mp hr
      omega
    change (r + q * p, f r) = (r + q * p, f (r + q * p))
    rw [hval r q hrp]
  have hnodP : P.Nodup := by
    unfold P
    exact positions_flatMap_nodup n p hp
  have hnodL : L.Nodup := by
    rw [hLR]
    apply List.Pairwise.map (R := fun i j : Nat => i ≠ j) (S := fun x y : Nat × Int => x ≠ y)
      (f := fun i : Nat => (i, f i))
    · intro i j hne hEq
      exact hne (Prod.ext_iff.mp hEq).1
    · exact hnodP
  have hnodR : R.Nodup := by
    unfold R
    apply List.Pairwise.map (R := fun i j : Nat => i ≠ j) (S := fun x y : Nat × Int => x ≠ y)
      (f := fun i : Nat => (i, f i))
    · intro i j hne hEq
      exact hne (Prod.ext_iff.mp hEq).1
    · exact List.nodup_range
  apply (List.perm_ext_iff_of_nodup hnodL hnodR).mpr
  intro v
  constructor
  · intro hv
    rw [hLR, List.mem_map] at hv
    rcases hv with ⟨i, hi, hiv⟩
    unfold P at hi
    rcases List.mem_flatMap.mp hi with ⟨r, hr, hxmem⟩
    rcases List.mem_map.mp hxmem with ⟨j, hj, hjeq⟩
    have hrp : r < p := by
      have := List.mem_range.mp hr
      omega
    have hq : j < (n - r + p - 1) / p := List.mem_range.mp hj
    have hpos : r + j * p < n := (pos_lt_iff r p j n hp).mpr hq
    subst i
    unfold R
    exact List.mem_map.mpr ⟨r + j * p, List.mem_range.mpr hpos, hiv⟩
  · intro hv
    unfold R at hv
    rcases List.mem_map.mp hv with ⟨i, hi, hiv⟩
    have hin : i < n := List.mem_range.mp hi
    let r := i % p
    let q := i / p
    have hrp : r < p := Nat.mod_lt i hp
    have hri : i % p ≤ i := Nat.mod_le i p
    have hmin : r < min p n := by dsimp [r]; omega
    have hdecomp : r + q * p = i := by
      dsimp [r, q]
      rw [Nat.add_comm, Nat.mul_comm, Nat.div_add_mod]
    have hq : q < (n - r + p - 1) / p := by
      apply (pos_lt_iff r p q n hp).mp
      rw [hdecomp]
      exact hin
    unfold L
    apply List.mem_flatMap.mpr
    refine ⟨r, List.mem_range.mpr hmin, ?_⟩
    exact List.mem_map.mpr ⟨q, List.mem_range.mpr hq,
      by
        have hsnd : f r = f i := by rw [← hdecomp, hval r q hrp]
        exact (Prod.ext hdecomp hsnd).trans hiv⟩

namespace WrappingSequence

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.WrappingSequence

private theorem rows_lanes (c : Config) :
    (rows layout c).Perm ((layout.lanes c).flatMap lanePoints) := by
  let p := width c
  have hp : 0 < p := by dsimp [p, width]; omega
  let f : Nat → Int := fun i => c.base + c.step * (i % p : Nat)
  have hrows : rows layout c = (List.range c.n).map (fun i => (i, f i)) := by
    unfold rows
    apply range_filterMap_all
    intro i hi
    simp [layout, f, p]
  have hlanes : layout.lanes c =
      (List.range (min p c.n)).map (fun r => lane c.n r p (c.base + c.step * (r : Int)) 0) := by
    rfl
  rw [hrows, hlanes]
  have hval : ∀ r q : Nat, r < p → f (r + q * p) = f r := by
    intro r q hr
    change c.base + c.step * (((r + q * p) % p : Nat) : Int) =
      c.base + c.step * ((r % p : Nat) : Int)
    rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hr]
  have hL : (List.range (min p c.n)).flatMap
        (fun r => lanePoints (lane c.n r p (c.base + c.step * (r : Int)) 0)) =
      (List.range (min p c.n)).flatMap
        (fun r => (List.range ((c.n - r + p - 1) / p)).map (fun q => (r + q * p, f r))) := by
    apply flatMap_congr_left
    intro r hr
    have hrp : r < p := by
      have := List.mem_range.mp hr
      omega
    unfold lanePoints lane
    simp only
    apply List.map_congr_left
    intro a ha
    congr 1
    dsimp [f]
    rw [Int.emod_eq_of_lt]
    · simp
    · exact_mod_cast Nat.zero_le r
    · exact_mod_cast hrp
  rw [List.flatMap_map]
  rw [hL]
  exact (flatMap_residue_perm c.n p hp f hval).symm

end WrappingSequence

/-! ## Assigned goals: ResidueJoin -/

theorem MirroredTail_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.MirroredTail.ResidueJoin.Goal := by
  intro c b
  have hp : (rows MirroredTail.layout c).Perm ((MirroredTail.layout.lanes c).flatMap lanePoints) := by
    rw [MirroredTail.rows_lanes c]
  exact residueJoin_of_perm MirroredTail.layout c b hp

theorem BucketRuns_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.BucketRuns.ResidueJoin.Goal := by
  intro c b
  have hp : (rows BucketRuns.layout c).Perm ((BucketRuns.layout.lanes c).flatMap lanePoints) := by
    rw [BucketRuns.rows_lanes c]
  exact residueJoin_of_perm BucketRuns.layout c b hp

theorem WrappingSequence_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.WrappingSequence.ResidueJoin.Goal := by
  intro c b
  exact residueJoin_of_perm WrappingSequence.layout c b (WrappingSequence.rows_lanes c)
