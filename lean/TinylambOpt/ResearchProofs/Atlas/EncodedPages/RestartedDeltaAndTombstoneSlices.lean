import TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `RestartedDelta.SliceState` — `∀ c lo hi, lo ≤ hi → let selected := (rows layout
  c).filter (fun r => lo ≤ r.1 && r.1 < hi) let ls := layout.lanes c
  (selected.length,(selected.map Prod.snd).sum) = ((ls.map (fun a => beforePos a hi -
  beforePos a lo)).sum, (ls.map (fun a => partialSum a (beforePos a hi)-partialSum a
  (beforePos a lo))).sum)`
- `RotatedPage.SliceState` — `∀ c lo hi, lo ≤ hi → let selected := (rows layout
  c).filter (fun r => lo ≤ r.1 && r.1 < hi) let ls := layout.lanes c
  (selected.length,(selected.map Prod.snd).sum) = ((ls.map (fun a => beforePos a hi -
  beforePos a lo)).sum, (ls.map (fun a => partialSum a (beforePos a hi)-partialSum a
  (beforePos a lo))).sum)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages.RestartedDeltaAndTombstoneSlices

open TinylambOpt.ResearchGoals.Atlas.EncodedPages

-- Basic list helpers
private theorem list_sum_perm {l₁ l₂ : List Int} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem map_sum_perm {α : Type} (f : α → Int) {l₁ l₂ : List α} (p : l₁.Perm l₂) :
    (l₁.map f).sum = (l₂.map f).sum := list_sum_perm (p.map f)

private theorem sum_sub_list (l : List Int) (f g : Int → Int) :
    ((l.map fun x => f x - g x).sum) = (l.map f).sum - (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem filterMap_some_eq_map {α β : Type} (l : List α) (f : α → β) :
    l.filterMap (fun x => some (f x)) = l.map f := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]

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

-- Int/nat arithmetic helpers
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

-- Lane positions and slice helpers
private def lanePoints (a : Lane) : List (Nat × Int) :=
  (List.range a.n).map (fun i : Nat => (a.start + i * a.stride, a.base + (i : Int) * a.step))

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

private theorem ceilDiv_eq (d s : Nat) (hs : 0 < s) :
    d + s - 1 - (s - 1) = d := by
  omega

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

private theorem lanePoints_filter_eq (a : Lane) (lo hi : Nat) (_hs : 0 < a.stride) :
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

private theorem lane_filter_indices_eq (a : Lane) (lo hi : Nat) (_hle : lo ≤ hi) (hs : 0 < a.stride) :
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
  rw [lane_filter_indices_eq a lo hi hle hs]
  change (((List.range a.n).filter (fun i : Nat => tlo ≤ i && i < thi)).map
    (fun i : Nat => a.base + (i : Int) * a.step)).sum =
      partialSum a (beforePos a hi) - partialSum a (beforePos a lo)
  rw [range_sum_between a.n tlo thi (fun i : Nat => a.base + (i : Int) * a.step) htle]
  rw [← partialSum_eq_sum a (min a.n thi), ← partialSum_eq_sum a (min a.n tlo)]
  unfold beforePos
  rfl

private theorem perm_length {α : Type} {l₁ l₂ : List α} (p : l₁.Perm l₂) : l₁.length = l₂.length := by
  induction p with
  | nil => rfl
  | cons a p ih => simp [ih]
  | swap a b l => simp
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

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

private theorem lane_slice_count_sum (ls : List Lane) (lo hi : Nat) (hle : lo ≤ hi)
    (hs : ∀ a ∈ ls, 0 < a.stride) :
    (ls.map (fun a => ((lanePoints a).filter (fun r => lo ≤ r.1 && r.1 < hi)).length)).sum =
      (ls.map (fun a => beforePos a hi - beforePos a lo)).sum := by
  induction ls with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [lane_slice_count a lo hi hle (hs a (by simp)),
          ih (fun b hb => hs b (by simp [hb]))]

private theorem lane_slice_sum_sum (ls : List Lane) (lo hi : Nat) (hle : lo ≤ hi)
    (hs : ∀ a ∈ ls, 0 < a.stride) :
    (ls.map (fun a => (((lanePoints a).filter (fun r => lo ≤ r.1 && r.1 < hi)).map Prod.snd).sum)).sum =
      (ls.map (fun a => partialSum a (beforePos a hi) - partialSum a (beforePos a lo))).sum := by
  induction ls with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [lane_slice_sum a lo hi hle (hs a (by simp)),
          ih (fun b hb => hs b (by simp [hb]))]

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
    exact lane_slice_count_sum (lay.lanes c) lo hi hle hs
  · rw [list_sum_perm (hf.map Prod.snd), flatMap_filter_snd_sum]
    exact lane_slice_sum_sum (lay.lanes c) lo hi hle hs

namespace RestartedDelta

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.RestartedDelta

private theorem rows_map (c : Config) :
    rows layout c =
      (List.range c.n).map (fun i => (i, if i < c.split then c.base + c.step*(i:Int)
        else c.bias + c.step*(i-c.split:Nat))) := by
  unfold rows
  simp only [layout]
  rw [← filterMap_some_eq_map (List.range c.n) (fun i => (i, if i < c.split then c.base + c.step*(i:Int)
        else c.bias + c.step*(i-c.split:Nat)))]
  apply filterMap_congr'
  intro i hi
  by_cases hi' : i < c.split <;> simp [hi']

private theorem rows_lanes (c : Config) :
    rows layout c = lanePoints (span c 0 c.split c.base c.step) ++
      lanePoints (span c c.split c.n c.bias c.step) := by
  rw [rows_map]
  by_cases h : c.split ≤ c.n
  · have hlane1 : span c 0 c.split c.base c.step = ⟨0, 1, c.split, c.base, c.step⟩ := by
      unfold span lane
      have hm : min c.n c.split = c.split := Nat.min_eq_right h
      simp [hm]
    have hlane2 : span c c.split c.n c.bias c.step = ⟨c.split, 1, c.n - c.split, c.bias, c.step⟩ := by
      unfold span lane
      have hm : min c.n c.n = c.n := Nat.min_eq_left (by omega)
      simp [hm]
    rw [hlane1, hlane2]
    unfold lanePoints
    simp only
    have hn : c.split + (c.n - c.split) = c.n := Nat.add_sub_of_le h
    rw [← hn, List.range_add, List.map_append, List.map_map]
    have hsub : c.split + (c.n - c.split) - c.split = c.n - c.split := by omega
    rw [hsub]
    have hfirst : List.map (fun i => (i, if i < c.split then c.base + c.step * (i:Int) else c.bias + c.step * (i - c.split : Nat))) (List.range c.split) =
        List.map (fun i : Nat => (0 + i * 1, c.base + (i:Int) * c.step)) (List.range c.split) := by
      apply List.map_congr_left
      intro i hi
      have hi' : i < c.split := List.mem_range.mp hi
      simp [hi', Int.mul_comm]
    have hsecond : List.map ((fun i => (i, if i < c.split then c.base + c.step * (i:Int) else c.bias + c.step * (i - c.split : Nat))) ∘ fun x => c.split + x) (List.range (c.n - c.split)) =
        List.map (fun i : Nat => (c.split + i * 1, c.bias + (i:Int) * c.step)) (List.range (c.n - c.split)) := by
      apply List.map_congr_left
      intro j hj
      have hnot : ¬ c.split + j < c.split := by omega
      have hsub : c.split + j - c.split = j := by omega
      simp [hnot, hsub, Int.mul_comm]
    rw [hfirst, hsecond]
  · have hlane1 : span c 0 c.split c.base c.step = ⟨0, 1, c.n, c.base, c.step⟩ := by
      unfold span lane
      have hm : min c.n c.split = c.n := Nat.min_eq_left (by omega)
      simp [hm]
    have hlane2 : span c c.split c.n c.bias c.step = ⟨c.split, 1, 0, c.bias, c.step⟩ := by
      unfold span lane
      have hm : min c.n c.n = c.n := Nat.min_eq_left (by omega)
      simp [hm]
      omega
    rw [hlane1, hlane2]
    unfold lanePoints
    simp
    intro i hi
    have hi' : i < c.n := hi
    have hi'' : i < c.split := by omega
    simp [hi'', Int.mul_comm]

theorem SliceState_sound : SliceState.Goal := by
  intro c lo hi hle
  apply slice_from_perm layout c lo hi hle
  · rw [rows_lanes]
    simp only [layout, List.flatMap_cons, List.flatMap_nil, List.append_nil]
    change (lanePoints (span c 0 c.split c.base c.step) ++ lanePoints (span c c.split c.n c.bias c.step)).Perm
      (lanePoints (span c 0 c.split c.base c.step) ++ lanePoints (span c c.split c.n c.bias c.step))
    exact List.Perm.refl _
  · intro a ha
    simp [layout] at ha
    rcases ha with rfl | rfl <;> simp [span, lane]

end RestartedDelta

namespace RotatedPage

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.RotatedPage

private theorem rows_lanes (c : Config) :
    rows layout c =
      lanePoints (span c 0 (c.n - c.split % c.n) (c.base + c.step*((c.split % c.n : Nat) : Int)) c.step) ++
      lanePoints (span c (c.n - c.split % c.n) c.n c.base c.step) := by
  unfold rows
  simp only [layout]
  by_cases hn : c.n = 0
  · simp [hn, span, lane, lanePoints]
  · have hpos : 0 < c.n := by omega
    have hmod : c.split % c.n < c.n := Nat.mod_lt _ hpos
    let s := c.split % c.n
    have hlane1 : span c 0 (c.n - s) (c.base + c.step*(s:Int)) c.step =
        ⟨0, 1, c.n - s, c.base + c.step*(s:Int), c.step⟩ := by
      unfold span lane
      have hm : min c.n (c.n - s) = c.n - s := Nat.min_eq_right (by omega)
      simp [hm]
    have hlane2 : span c (c.n - s) c.n c.base c.step = ⟨c.n - s, 1, s, c.base, c.step⟩ := by
      unfold span lane
      have hm : min c.n c.n = c.n := Nat.min_eq_left (by omega)
      have hsub : c.n - (c.n - s) = s := by omega
      simp [hm, hsub]
    rw [hlane1, hlane2]
    unfold lanePoints
    simp only
    have hn1 : (c.n - s) + s = c.n := by omega
    have hrange : List.range c.n = List.range (c.n - s) ++ List.map (fun x => c.n - s + x) (List.range s) := by
      calc
        List.range c.n = List.range ((c.n - s) + s) := by rw [hn1]
        _ = List.range (c.n - s) ++ List.map (fun x => c.n - s + x) (List.range s) := List.range_add
    rw [hrange]
    simp only [List.filterMap_append, Option.map_some, filterMap_some_eq_map]
    have hfirst : List.map (fun i => (i, c.base + c.step * (((i+s)%c.n : Nat) : Int))) (List.range (c.n - s)) =
        List.map (fun i : Nat => (0 + i*1, (c.base + c.step*(s:Int)) + (i:Int)*c.step)) (List.range (c.n - s)) := by
      apply List.map_congr_left
      intro i hi
      have hi' : i < c.n - s := List.mem_range.mp hi
      have hlt : i + s < c.n := by omega
      have hm : (i + s) % c.n = i + s := Nat.mod_eq_of_lt hlt
      rw [hm]
      simp [Int.natCast_add, Int.mul_add, Int.mul_comm, Int.add_assoc]
      omega
    have hsecond : List.map (fun x => (x, c.base + c.step * (((x + s)%c.n : Nat) : Int)))
        (List.map (fun x => c.n - s + x) (List.range s)) =
        List.map (fun i : Nat => (c.n - s + i*1, c.base + (i:Int)*c.step)) (List.range s) := by
      rw [List.map_map]
      apply List.map_congr_left
      intro j hj
      have hj' : j < s := List.mem_range.mp hj
      have hsum : c.n - s + j + s = c.n + j := by omega
      have hmod' : (c.n + j) % c.n = j := by
        rw [Nat.add_comm, Nat.add_mod_right, Nat.mod_eq_of_lt (by omega : j < c.n)]
      have hm : (c.n - s + j + s) % c.n = j := by rw [hsum, hmod']
      change (c.n - s + j, c.base + c.step * (((c.n - s + j + s) % c.n : Nat) : Int)) =
        (c.n - s + j * 1, c.base + (j:Int)*c.step)
      rw [hm]
      simp [Int.mul_comm]
    rw [hfirst, hsecond]

theorem SliceState_sound : SliceState.Goal := by
  intro c lo hi hle
  apply slice_from_perm layout c lo hi hle
  · rw [rows_lanes]
    simp only [layout, List.flatMap_cons, List.flatMap_nil, List.append_nil]
    exact List.Perm.refl _
  · intro a ha
    simp [layout] at ha
    rcases ha with rfl | rfl <;> simp [span, lane]

end RotatedPage

-- TODO(DeletedSpan.SliceState): pending
-- TODO(DeletedSpan.SeekSurvivor): pending
-- TODO(DeletedSpan.ResidueJoin): pending
-- TODO(DeletedSpan.DeleteProgression): pending
-- TODO(DeletedSpan.AlignedProduct): pending
-- TODO(RotatedPage.PermutationAggregate): pending
-- TODO(RotatedPage.ResidueJoin): pending
-- TODO(RotatedPage.DeleteProgression): pending
-- TODO(RotatedPage.AlignedProduct): pending
-- TODO(GrayTile.SliceState): pending
-- TODO(GrayTile.PowerOfTwo): pending
-- TODO(GrayTile.ResidueJoin): pending
-- TODO(GrayTile.DeleteProgression): pending
-- TODO(GrayTile.AlignedProduct): pending
-- TODO(RestartedDelta.PredicateRank): pending
-- TODO(RestartedDelta.ResidueJoin): pending
-- TODO(RestartedDelta.DeleteProgression): pending
-- TODO(RestartedDelta.AlignedProduct): pending
-- TODO(PagedTombstones.SliceState): pending
-- TODO(PagedTombstones.SkipEpochs): pending
-- TODO(PagedTombstones.ResidueJoin): pending
-- TODO(PagedTombstones.DeleteProgression): pending
-- TODO(PagedTombstones.AlignedProduct): pending

end TinylambOpt.ResearchProofs.Atlas.EncodedPages.RestartedDeltaAndTombstoneSlices
