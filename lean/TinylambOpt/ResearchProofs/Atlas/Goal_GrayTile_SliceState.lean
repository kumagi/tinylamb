import TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `GrayTile.SliceState` — `∀ c lo hi, lo ≤ hi → let selected := (rows layout c).filter
  (fun r => lo ≤ r.1 && r.1 < hi) let ls := layout.lanes c
  (selected.length,(selected.map Prod.snd).sum) = ((ls.map (fun a => beforePos a hi -
  beforePos a lo)).sum, (ls.map (fun a => partialSum a (beforePos a hi)-partialSum a
  (beforePos a lo))).sum)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.Atlas.Goal_GrayTile_SliceState

open TinylambOpt.ResearchGoals.Atlas.EncodedPages

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

private theorem list_sum_perm {l₁ l₂ : List Int} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem sum_map_add {α : Type} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem sum_map_sub {α : Type} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x - g x)).sum = (l.map f).sum - (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem sum_map_mul_int (l : List Int) (c : Int) :
    (l.map (fun x => c * x)).sum = c * l.sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Int.mul_add]

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

private theorem flatMap_congr_left {α β : Type} (l : List α) (f g : α → List β)
    (h : ∀ a ∈ l, f a = g a) : l.flatMap f = l.flatMap g := by
  induction l with
  | nil => rfl
  | cons a as ih => simp [h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private theorem map_pair_filter {α β : Type} (xs : List α) (f : α → Nat) (g : α → β)
    (p : Nat → Bool) :
    (xs.map (fun x => (f x, g x))).filter (fun r => p r.1) =
      (xs.filter (fun x => p (f x))).map (fun x => (f x, g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih => by_cases h : p (f x) <;> simp [h, ih]

private theorem filter_map_pair_snd {α : Type} [BEq α] (xs : List Nat) (f : Nat → α) (v : α) :
    ((xs.map (fun i => (i, f i))).filter (fun r => r.2 == v)).map Prod.fst =
      xs.filter (fun i => f i == v) := by
  induction xs with
  | nil => simp
  | cons x xs ih => by_cases h : f x == v <;> simp [h, ih]

private theorem countP_sum_indicator {α : Type} (l : List α) (p : α → Bool) :
    l.countP p = ((l.map (fun x => if p x then 1 else 0)).sum : Nat) := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih] <;> omega

private theorem countP_eq_sum (l : List (Nat × Int)) (f : Int → Bool) :
    l.countP (fun x => f x.2) = ((l.map (fun x => if f x.2 then 1 else 0)).sum : Nat) := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : f x.2 <;> simp [h, ih] <;> omega

private theorem perm_length {α : Type} {l₁ l₂ : List α} (p : l₁.Perm l₂) : l₁.length = l₂.length := by
  induction p with
  | nil => rfl
  | cons a p ih => simp [ih]
  | swap a b l => simp
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

/-! ## Lane arithmetic and slices -/

private theorem choose2_succ (k : Nat) :
    TinylambOpt.ResearchGoals.PairAggregates.choose2 (k + 1) =
      TinylambOpt.ResearchGoals.PairAggregates.choose2 k + k := by
  unfold TinylambOpt.ResearchGoals.PairAggregates.choose2
  by_cases hk : k = 0
  · simp [hk]
  · have hkm1 : k + 1 - 1 = k := by omega
    rw [hkm1]
    have hmul1 : (k + 1) * k = k * k + k := by rw [Nat.add_mul, Nat.one_mul]
    have hmul2 : k * (k - 1) = k * k - k := by
      rw [Nat.mul_sub_left_distrib, Nat.mul_one]
    have hle : k ≤ k * k := by
      simpa using Nat.mul_le_mul_left k (show 1 ≤ k by omega)
    have hcan : k * k - k + k = k * k := Nat.sub_add_cancel hle
    have hEq : (k + 1) * k = k * (k - 1) + 2 * k := by rw [hmul1, hmul2]; omega
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
      rw [partialSum_succ, ih, List.range_succ, List.map_append, List.sum_append]
      simp

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

private theorem pos_lt_iff (start stride i p : Nat) (hs : 0 < stride) :
    start + i * stride < p ↔ i < (p - start + stride - 1) / stride := by
  by_cases hps : p ≤ start
  · have hp0 : p - start = 0 := by omega
    have hzero : (p - start + stride - 1) / stride = 0 := by
      rw [hp0, Nat.zero_add]
      exact Nat.div_eq_of_lt (by omega)
    rw [hzero]
    omega
  · rw [Nat.lt_div_iff_mul_lt hs]
    have hrewrite : p - start + stride - 1 - (stride - 1) = p - start := by omega
    rw [hrewrite]
    omega

private theorem beforePos_eq_min (a : Lane) (p : Nat) (hs : 0 < a.stride) :
    beforePos a p = ((List.range a.n).filter (fun i => a.start + i * a.stride < p)).length := by
  unfold beforePos
  have hiff : ∀ i : Nat, (a.start + i * a.stride < p) ↔
      (i < (p - a.start + a.stride - 1) / a.stride) :=
    fun i => pos_lt_iff a.start a.stride i p hs
  rw [filter_congr_bool _ _ _ (fun i _ => decide_eq_of_iff (hiff i)), range_count_lt]

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
  have htle : tlo ≤ thi := by dsimp [tlo, thi]; apply Nat.div_le_div_right; omega
  rw [hfilter, range_count_between a.n tlo thi htle]

private theorem lane_slice_sum (a : Lane) (lo hi : Nat) (hle : lo ≤ hi) (hs : 0 < a.stride) :
    (((lanePoints a).filter (fun r => lo ≤ r.1 && r.1 < hi)).map Prod.snd).sum =
      partialSum a (beforePos a hi) - partialSum a (beforePos a lo) := by
  rw [lanePoints_filter_eq a lo hi hs, List.map_map]
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
  have htle : tlo ≤ thi := by dsimp [tlo, thi]; apply Nat.div_le_div_right; omega
  rw [hfilter]
  change (((List.range a.n).filter (fun i : Nat => tlo ≤ i && i < thi)).map f).sum =
    partialSum a (beforePos a hi) - partialSum a (beforePos a lo)
  rw [range_sum_between a.n tlo thi f htle]
  have bh : beforePos a hi = min a.n thi := by
    rw [beforePos_eq_min a hi hs, filter_congr_bool _ _ _ (fun i _ => decide_eq_of_iff (hiff_hi i)), range_count_lt]
  have bl : beforePos a lo = min a.n tlo := by
    rw [beforePos_eq_min a lo hs, filter_congr_bool _ _ _ (fun i _ => decide_eq_of_iff (hiff_lo i)), range_count_lt]
  rw [bh, bl, ← partialSum_eq_sum, ← partialSum_eq_sum]

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

private theorem slice_from_rows_eq (lay : Layout) (c : Config) (lo hi : Nat) (hle : lo ≤ hi)
    (heq : rows lay c = (lay.lanes c).flatMap lanePoints)
    (hs : ∀ a ∈ lay.lanes c, 0 < a.stride) :
    let selected := (rows lay c).filter (fun r => lo ≤ r.1 && r.1 < hi)
    let ls := lay.lanes c
    (selected.length,(selected.map Prod.snd).sum) =
      ((ls.map (fun a => beforePos a hi - beforePos a lo)).sum,
       (ls.map (fun a => partialSum a (beforePos a hi)-partialSum a (beforePos a lo))).sum) := by
  dsimp only
  rw [heq]
  let p : Nat × Int → Bool := fun r => lo ≤ r.1 && r.1 < hi
  apply Prod.ext
  · rw [flatMap_filter_length]
    apply congrArg List.sum
    apply List.map_congr_left
    intro a ha
    exact lane_slice_count a lo hi hle (hs a ha)
  · rw [flatMap_filter_snd_sum]
    apply congrArg List.sum
    apply List.map_congr_left
    intro a ha
    exact lane_slice_sum a lo hi hle (hs a ha)

private theorem sum_range_const_nat (m v : Nat) :
    ((List.range m).map (fun _ => v)).sum = m * v := by
  induction m with
  | zero => simp
  | succ m ih =>
      rw [List.range_succ, List.map_append, List.sum_append]
      simp [ih, Nat.succ_mul]

private theorem sum_map_zero_nat (l : List Nat) : (l.map (fun _ => (0 : Nat))).sum = 0 := by
  induction l <;> simp [*]

private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] (l : List α) :
    l.eraseDups.Nodup := by
  cases l with
  | nil => simp
  | cons a as =>
      rw [List.eraseDups_cons]
      apply List.nodup_cons.mpr
      constructor
      · intro hm
        have hm' := List.mem_eraseDups.mp hm
        have hf := (List.mem_filter.mp hm').2
        simp at hf
      · exact nodup_eraseDups (as.filter (fun x => !x == a))
termination_by l.length
decreasing_by
  have h := List.length_filter_le (fun x : α => !x == a) as
  simp only [List.length_cons]
  omega

private theorem eraseDups_perm {α : Type} [BEq α] [LawfulBEq α] {l₁ l₂ : List α}
    (p : l₁.Perm l₂) : l₁.eraseDups.Perm l₂.eraseDups := by
  apply (List.perm_ext_iff_of_nodup (nodup_eraseDups l₁) (nodup_eraseDups l₂)).mpr
  intro a
  rw [List.mem_eraseDups, List.mem_eraseDups]
  exact p.mem_iff

/-! ## LinearOrdinal -/


private theorem slice_from_perm (lay : Layout) (c : Config) (lo hi : Nat) (hle : lo ≤ hi)
    (heq : (rows lay c).Perm ((lay.lanes c).flatMap lanePoints))
    (hs : ∀ a ∈ lay.lanes c, 0 < a.stride) :
    let selected := (rows lay c).filter (fun r => lo ≤ r.1 && r.1 < hi)
    let ls := lay.lanes c
    (selected.length,(selected.map Prod.snd).sum) =
      ((ls.map (fun a => beforePos a hi - beforePos a lo)).sum,
       (ls.map (fun a => partialSum a (beforePos a hi)-partialSum a (beforePos a lo))).sum) := by
  dsimp only
  have hp := heq.filter (fun r : Nat × Int => lo ≤ r.1 && r.1 < hi)
  apply Prod.ext
  · rw [hp.length_eq, flatMap_filter_length]
    apply congrArg List.sum
    apply List.map_congr_left
    intro a ha
    exact lane_slice_count a lo hi hle (hs a ha)
  · rw [list_sum_perm (hp.map Prod.snd), flatMap_filter_snd_sum]
    apply congrArg List.sum
    apply List.map_congr_left
    intro a ha
    exact lane_slice_sum a lo hi hle (hs a ha)
private theorem transpose_perm (P n : Nat) (hP : 0 < P) (keep : Nat → Bool) (V : Nat → Nat → Int) :
    (((List.range n).filter (fun i => keep (i % P))).map (fun i => (i, V (i % P) (i / P)))).Perm
    (((List.range (min P n)).filter keep).flatMap (fun r =>
      (List.range ((n - r + P - 1)/P)).map (fun j => (r + j*P, V r j)))) := by
  apply (List.perm_ext_iff_of_nodup ?_ ?_).mpr
  · intro a
    obtain ⟨i0, v0⟩ := a
    simp only [List.mem_map, List.mem_filter, List.mem_range, List.mem_flatMap, Prod.mk.injEq,
      decide_eq_true_eq]
    constructor
    · rintro ⟨i, ⟨hi, hk⟩, rfl, rfl⟩
      refine ⟨i % P, ⟨?_, hk⟩, i / P, ?_, ?_, rfl⟩
      · have := Nat.mod_lt i hP
        have := Nat.mod_le i P
        omega
      · have h := (pos_lt_iff (i % P) P (i / P) n hP).mp (by
          have := Nat.mod_add_div i P
          rw [Nat.mul_comm] at this
          omega)
        exact h
      · have := Nat.mod_add_div i P
        rw [Nat.mul_comm] at this
        omega
    · rintro ⟨r, ⟨hr, hk⟩, j, hj, rfl, rfl⟩
      have hrP : r < P := by omega
      have hlt := (pos_lt_iff r P j n hP).mpr hj
      have hmod : (r + j * P) % P = r := by
        rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hrP]
      have hdiv : (r + j * P) / P = j := by
        rw [Nat.add_mul_div_right _ _ hP, Nat.div_eq_of_lt hrP]; omega
      refine ⟨r + j * P, ⟨hlt, ?_⟩, rfl, ?_⟩
      · rw [hmod]; exact hk
      · rw [hmod, hdiv]
  · have hnd : ((List.range n).filter (fun i => keep (i % P))).Pairwise (· ≠ ·) :=
      List.Pairwise.sublist List.filter_sublist List.nodup_range
    exact List.Pairwise.map _ (fun a b h hab => h (Prod.mk.inj hab).1) hnd
  · show List.Pairwise (· ≠ ·) _
    rw [List.pairwise_flatMap]
    constructor
    · intro r _
      have hnd : (List.range ((n - r + P - 1)/P)).Pairwise (· ≠ ·) := List.nodup_range
      exact List.Pairwise.map _ (fun a b h hab => by
        have := (Prod.mk.inj hab).1
        exact h (Nat.eq_of_mul_eq_mul_right hP (Nat.add_left_cancel this))) hnd
    · have hnd : ((List.range (min P n)).filter keep).Pairwise (· ≠ ·) :=
        List.Pairwise.sublist List.filter_sublist List.nodup_range
      apply hnd.imp_of_mem
      intro r r' hr hr' hne
      intro a ha b hb hab
      simp only [List.mem_map, List.mem_range] at ha hb
      obtain ⟨j, hj, rfl⟩ := ha
      obtain ⟨j', hj', rfl⟩ := hb
      have h1 := (Prod.mk.inj hab).1
      have hrP : r < P := by
        have := (List.mem_filter.mp hr).1
        simp only [List.mem_range] at this; omega
      have hrP' : r' < P := by
        have := (List.mem_filter.mp hr').1
        simp only [List.mem_range] at this; omega
      have e1 : (r + j * P) % P = r := by rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hrP]
      have e2 : (r' + j' * P) % P = r' := by rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hrP']
      rw [h1] at e1
      exact hne (by omega)

private theorem rows_eq_filter_map (lay : Layout) (c : Config) (p : Nat → Bool) (g : Nat → Int)
    (h : ∀ i, lay.read c i = if p i then some (g i) else none) :
    rows lay c = ((List.range c.n).filter p).map (fun i => (i, g i)) := by
  unfold rows
  induction (List.range c.n) with
  | nil => rfl
  | cons x xs ih =>
    simp only [List.filterMap_cons, List.filter_cons, h x]
    by_cases hx : p x = true <;> simp [hx, ih]

private theorem rows_perm_lanes (lay : Layout) (c : Config) (P : Nat) (hP : 0 < P)
    (p keep : Nat → Bool) (g : Nat → Int) (V : Nat → Nat → Int)
    (h : ∀ i, lay.read c i = if p i then some (g i) else none)
    (hp : ∀ i, i < c.n → p i = keep (i % P)) (hg : ∀ i, i < c.n → g i = V (i % P) (i / P))
    (hl : (lay.lanes c).flatMap lanePoints =
      ((List.range (min P c.n)).filter keep).flatMap (fun r =>
        (List.range ((c.n - r + P - 1)/P)).map (fun j => (r + j*P, V r j)))) :
    (rows lay c).Perm ((lay.lanes c).flatMap lanePoints) := by
  rw [hl, rows_eq_filter_map lay c p g h]
  have e : ((List.range c.n).filter p).map (fun i => (i, g i)) =
      ((List.range c.n).filter (fun i => keep (i % P))).map (fun i => (i, V (i % P) (i / P))) := by
    rw [List.filter_congr (fun i hi => hp i (List.mem_range.mp hi))]
    apply List.map_congr_left
    intro i hi
    have := (List.mem_filter.mp hi).1
    rw [hg i (List.mem_range.mp this)]
  rw [e]
  exact transpose_perm P c.n hP keep V
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.GrayTile

private theorem rows_perm (c : Config) :
    (rows layout c).Perm ((layout.lanes c).flatMap lanePoints) := by
  have hw : 0 < width c := by unfold width; omega
  refine rows_perm_lanes layout c (width c) hw (fun _ => true) (fun _ => true)
    (fun i => c.base + c.step * (((i / width c) * width c + (i % width c ^^^ (i % width c) / 2) : Nat) : Int))
    (fun r j => (c.base + c.step * ((r ^^^ r / 2 : Nat) : Int)) + (j : Int) * (c.step * (width c : Int)))
    (fun i => rfl) (fun i _ => rfl) ?_ ?_
  · intro i _
    push_cast
    simp [Int.mul_add, Int.mul_comm, Int.mul_left_comm, Int.add_assoc]
    exact Int.add_comm _ _
  · simp only [layout, striped, List.flatMap_map]
    rw [List.filter_eq_self.mpr (fun _ _ => rfl)]
    simp [lanePoints, lane]

theorem sound_GrayTile_SliceState : GrayTile.SliceState.Goal := by
  intro c lo hi hle
  exact slice_from_perm layout c lo hi hle (rows_perm c) (by
    intro a ha
    simp only [layout, striped, List.mem_map] at ha
    obtain ⟨r, _, rfl⟩ := ha
    have : 0 < width c := by unfold width; omega
    simpa [lane] using this)


end TinylambOpt.ResearchProofs.Atlas.Goal_GrayTile_SliceState
