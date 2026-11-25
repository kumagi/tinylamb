import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages1

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

namespace LinearOrdinal

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.LinearOrdinal

private theorem rows_lanes (c : Config) :
    rows layout c = (layout.lanes c).flatMap lanePoints := by
  rw [rows_range_map layout c (fun i : Nat => c.base + c.step * (i : Int)) (by
    intro i _
    rfl)]
  simp only [layout, List.flatMap_cons, List.flatMap_nil, List.append_nil]
  change (List.range c.n).map (fun i : Nat => (i, c.base + c.step * (i : Int))) =
    lanePoints (lane c.n 0 1 c.base c.step)
  unfold lanePoints lane
  simp
  intro i _
  simp [Int.mul_comm]

theorem sound_LinearOrdinal_SliceState : LinearOrdinal.SliceState.Goal := by
  intro c lo hi hle
  exact slice_from_rows_eq layout c lo hi hle (rows_lanes c) (by
    intro a ha
    simp [layout] at ha
    rcases ha with rfl | rfl <;> simp [lane])

theorem sound_LinearOrdinal_InverseLookup : LinearOrdinal.InverseLookup.Goal := by
  intro c v hs
  let d := v - c.base
  let q := d / c.step
  let qn := q.toNat
  have hrows : rows layout c = (List.range c.n).map (fun i : Nat => (i, c.base + c.step * (i : Int))) :=
    rows_range_map layout c (fun i : Nat => c.base + c.step * (i : Int)) (by intro i _; rfl)
  rw [hrows, filter_map_pair_snd]
  change (List.range c.n).filter (fun i : Nat => c.base + c.step * (i : Int) == v) =
    if d % c.step == 0 && 0 ≤ q && q < (c.n : Int) then [qn] else []
  by_cases hmod : d % c.step == 0
  · have hmod' : d % c.step = 0 := of_decide_eq_true hmod
    have hd : d = c.step * q := by
      have h := Int.emod_add_mul_ediv d c.step
      calc
        d = d % c.step + c.step * (d / c.step) := h.symm
        _ = c.step * (d / c.step) := by rw [hmod']; simp
        _ = c.step * q := rfl
    by_cases hq0 : 0 ≤ q
    · by_cases hqn : q < (c.n : Int)
      · have hcast : (qn : Int) = q := Int.toNat_of_nonneg hq0
        have hqnlt : qn < c.n := by
          apply Int.ofNat_lt.mp
          rw [hcast]
          exact hqn
        have hpred : (List.range c.n).filter (fun i : Nat => c.base + c.step * (i : Int) == v) =
            (List.range c.n).filter (fun i => i == qn) := by
          apply filter_congr_bool
          intro i hi
          by_cases heq : i = qn
          · subst i
            rw [hcast]
            change v - c.base = c.step * q at hd
            have hv : c.base + c.step * q = v := by omega
            simp [hv]
          · have hne : c.base + c.step * (i : Int) ≠ v := by
              intro hv
              have hm : c.step * (i : Int) = c.step * q := by
                dsimp [d] at hd
                omega
              have hiq : (i : Int) = q := (Int.mul_eq_mul_left_iff hs).mp hm
              exact heq (by exact_mod_cast hiq.trans hcast.symm)
            rw [beq_eq_false_iff_ne.mpr hne, beq_eq_false_iff_ne.mpr heq]
        rw [hpred, List.filter_beq,
          (List.nodup_range (n := c.n)).count_of_mem (List.mem_range.mpr hqnlt)]
        simp [hqnlt, hmod, hq0, hqn]
      · have hfalse : (List.range c.n).filter (fun i : Nat => c.base + c.step * (i : Int) == v) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro i hi hv
          have hib : i < c.n := List.mem_range.mp hi
          have heq : c.base + c.step * (i : Int) = v := of_decide_eq_true hv
          have hm : c.step * (i : Int) = c.step * q := by dsimp [d] at hd; omega
          have hiq : (i : Int) = q := (Int.mul_eq_mul_left_iff hs).mp hm
          exact hqn (by omega)
        rw [hfalse]
        simp [hmod, hq0, hqn]
    · have hfalse : (List.range c.n).filter (fun i : Nat => c.base + c.step * (i : Int) == v) = [] := by
        apply List.filter_eq_nil_iff.mpr
        intro i hi hv
        have heq : c.base + c.step * (i : Int) = v := of_decide_eq_true hv
        have hm : c.step * (i : Int) = c.step * q := by dsimp [d] at hd; omega
        have hiq : (i : Int) = q := (Int.mul_eq_mul_left_iff hs).mp hm
        have hi0 : 0 ≤ (i : Int) := Int.natCast_nonneg i
        exact hq0 (by omega)
      rw [hfalse]
      simp [hmod, hq0]
  · have hfalse : (List.range c.n).filter (fun i : Nat => c.base + c.step * (i : Int) == v) = [] := by
      apply List.filter_eq_nil_iff.mpr
      intro i hi hv
      have heq : c.base + c.step * (i : Int) = v := of_decide_eq_true hv
      have hz : d % c.step = 0 := by
        dsimp [d]
        rw [show v - c.base = c.step * (i : Int) by omega]
        exact Int.emod_eq_zero_of_dvd ⟨(i : Int), rfl⟩
      exact hmod (by simp [hz])
    rw [hfalse]
    simp [hmod]

end LinearOrdinal

/-! ## SaturatingCounter -/

namespace SaturatingCounter

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.SaturatingCounter

private theorem rows_map (c : Config) :
    rows layout c = (List.range c.n).map (fun i =>
      (i, if i < c.split then c.base + c.step * (i : Int)
        else c.base + c.step * (c.split : Int))) := by
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
    have h2 : span c c.split c.n (c.base + c.step * (c.split : Int)) 0 =
        ⟨c.split, 1, c.n - c.split, c.base + c.step * (c.split : Int), 0⟩ := by
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
          else c.base + c.step * (c.split : Int))) =
        (List.range c.split).map (fun i : Nat => (0 + i * 1, c.base + (i : Int) * c.step)) := by
      apply List.map_congr_left
      intro i hi
      have hi' : i < c.split := List.mem_range.mp hi
      simp [hi', Int.mul_comm]
    have hsecond : (List.range (c.n - c.split)).map (fun i =>
        (c.split + i, if c.split + i < c.split then c.base + c.step * ((c.split + i : Nat) : Int)
          else c.base + c.step * (c.split : Int))) =
        (List.range (c.n - c.split)).map (fun i : Nat =>
          (c.split + i * 1, (c.base + c.step * (c.split : Int)) + (i : Int) * 0)) := by
      apply List.map_congr_left
      intro i hi
      have hnot : ¬ c.split + i < c.split := by omega
      simp [hnot, Int.mul_comm]
    rw [hfirst, hsecond]
  · have h1 : span c 0 c.split c.base c.step = ⟨0, 1, c.n, c.base, c.step⟩ := by
      unfold span lane
      simp [Nat.min_eq_left (by omega : c.n ≤ c.split)]
    have h2 : span c c.split c.n (c.base + c.step * (c.split : Int)) 0 =
        ⟨c.split, 1, 0, c.base + c.step * (c.split : Int), 0⟩ := by
      unfold span lane
      simp [Nat.sub_eq_zero_of_le (by omega : c.n ≤ c.split)]
    rw [h1, h2]
    unfold lanePoints
    simp
    intro i hi
    have hi' : i < c.n := hi
    have his : i < c.split := by omega
    simp [his, Int.mul_comm]

theorem sound_SaturatingCounter_SliceState : SaturatingCounter.SliceState.Goal := by
  intro c lo hi hle
  exact slice_from_rows_eq layout c lo hi hle (rows_lanes c) (by
    intro a ha
    simp [layout] at ha
    rcases ha with rfl | rfl <;> simp [span, lane])

theorem sound_SaturatingCounter_PlateauFrequency : SaturatingCounter.PlateauFrequency.Goal := by
  intro c hs
  let p := c.base + c.step * (c.split : Int)
  have hinj : ∀ i : Nat, i < c.split →
      (c.base + c.step * (i : Int) == p) = false := by
    intro i hi
    have hne : c.base + c.step * (i : Int) ≠ p := by
      intro h
      have hm : c.step * (i : Int) = c.step * (c.split : Int) := Int.add_left_cancel h
      have hi' : (i : Int) = (c.split : Int) := (Int.mul_eq_mul_left_iff hs).mp hm
      exact (Nat.ne_of_lt hi) (by exact_mod_cast hi')
    exact beq_eq_false_iff_ne.mpr hne
  unfold occurrence
  rw [rows_map, List.countP_map]
  change (List.range c.n).countP (fun i : Nat =>
    (if i < c.split then c.base + c.step * (i : Int) else p) == p) = c.n - c.split
  rw [countP_sum_indicator]
  change ((List.range c.n).map (fun i =>
    if (if i < c.split then c.base + c.step * (i : Int) else p) == p then 1 else 0)).sum =
      c.n - c.split
  by_cases hle : c.split ≤ c.n
  · have hn : c.split + (c.n - c.split) = c.n := Nat.add_sub_of_le hle
    have hrange : List.range c.n =
        List.range c.split ++ List.map (fun x => c.split + x) (List.range (c.n - c.split)) := by
      calc
        List.range c.n = List.range (c.split + (c.n - c.split)) := congrArg List.range hn.symm
        _ = _ := List.range_add
    rw [hrange, List.map_append, List.sum_append]
    have hfirst : ((List.range c.split).map (fun i =>
        if (if i < c.split then c.base + c.step * (i : Int) else p) == p then 1 else 0)).sum = 0 := by
      rw [show (List.range c.split).map (fun i =>
          if (if i < c.split then c.base + c.step * (i : Int) else p) == p then 1 else 0) =
          (List.range c.split).map (fun _ => 0) by
        apply List.map_congr_left
        intro i hi
        have his : i < c.split := List.mem_range.mp hi
        rw [if_pos his, hinj i his]
        rfl]
      exact sum_map_zero_nat _
    have hsecond : ((List.map (fun x => c.split + x) (List.range (c.n - c.split))).map (fun i =>
        if (if i < c.split then c.base + c.step * (i : Int) else p) == p then 1 else 0)).sum =
        c.n - c.split := by
      rw [List.map_map]
      simp only [Function.comp_def]
      rw [show (List.range (c.n - c.split)).map (fun x =>
          if (if c.split + x < c.split then c.base + c.step * ((c.split + x : Nat) : Int) else p) == p
            then 1 else 0) = (List.range (c.n - c.split)).map (fun _ => 1) by
        apply List.map_congr_left
        intro x hx
        have hnot : ¬ c.split + x < c.split := by omega
        simp [hnot]]
      simpa using sum_range_const_nat (c.n - c.split) 1
    rw [hfirst, hsecond, Nat.zero_add]
  · have hsub : c.n - c.split = 0 := Nat.sub_eq_zero_of_le (by omega)
    rw [hsub]
    have hall : ((List.range c.n).map (fun i =>
        if (if i < c.split then c.base + c.step * (i : Int) else p) == p then 1 else 0)).sum = 0 := by
      rw [show (List.range c.n).map (fun i =>
          if (if i < c.split then c.base + c.step * (i : Int) else p) == p then 1 else 0) =
          (List.range c.n).map (fun _ => 0) by
        apply List.map_congr_left
        intro i hi
        have his : i < c.split := by have := List.mem_range.mp hi; omega
        rw [if_pos his, hinj i his]
        rfl]
      exact sum_map_zero_nat _
    exact hall

end SaturatingCounter

/-! ## SegmentedSlope -/

namespace SegmentedSlope

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.SegmentedSlope

private theorem rows_map (c : Config) :
    rows layout c = (List.range c.n).map (fun i =>
      (i, if i < c.split then c.base + c.step * (i : Int)
        else c.base + c.step * (c.split : Int) + c.bias * ((i - c.split : Nat) : Int))) := by
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
    have h2 : span c c.split c.n (c.base + c.step * (c.split : Int)) c.bias =
        ⟨c.split, 1, c.n - c.split, c.base + c.step * (c.split : Int), c.bias⟩ := by
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
          else c.base + c.step * (c.split : Int) + c.bias * ((i - c.split : Nat) : Int))) =
        (List.range c.split).map (fun i : Nat => (0 + i * 1, c.base + (i : Int) * c.step)) := by
      apply List.map_congr_left
      intro i hi
      have hi' : i < c.split := List.mem_range.mp hi
      simp [hi', Int.mul_comm]
    have hsecond : (List.range (c.n - c.split)).map (fun i =>
        (c.split + i, if c.split + i < c.split then c.base + c.step * ((c.split + i : Nat) : Int)
          else c.base + c.step * (c.split : Int) + c.bias * (((c.split + i - c.split : Nat) : Int)))) =
        (List.range (c.n - c.split)).map (fun i : Nat =>
          (c.split + i * 1, (c.base + c.step * (c.split : Int)) + (i : Int) * c.bias)) := by
      apply List.map_congr_left
      intro i hi
      have hnot : ¬ c.split + i < c.split := by omega
      have hsub : c.split + i - c.split = i := by omega
      simp [hnot, hsub, Int.mul_comm]
    rw [hfirst, hsecond]
  · have h1 : span c 0 c.split c.base c.step = ⟨0, 1, c.n, c.base, c.step⟩ := by
      unfold span lane
      simp [Nat.min_eq_left (by omega : c.n ≤ c.split)]
    have h2 : span c c.split c.n (c.base + c.step * (c.split : Int)) c.bias =
        ⟨c.split, 1, 0, c.base + c.step * (c.split : Int), c.bias⟩ := by
      unfold span lane
      simp [Nat.sub_eq_zero_of_le (by omega : c.n ≤ c.split)]
    rw [h1, h2]
    unfold lanePoints
    simp
    intro i hi
    have hi' : i < c.n := hi
    have his : i < c.split := by omega
    simp [his, Int.mul_comm]

theorem sound_SegmentedSlope_SliceState : SegmentedSlope.SliceState.Goal := by
  intro c lo hi hle
  exact slice_from_rows_eq layout c lo hi hle (rows_lanes c) (by
    intro a ha
    simp [layout] at ha
    rcases ha with rfl | rfl <;> simp [span, lane])

private def segVal (c : Config) (i : Nat) : Int :=
  if i < c.split then c.base + c.step * (i : Int)
  else c.base + c.step * (c.split : Int) + c.bias * ((i - c.split : Nat) : Int)

private theorem segVal_boundary (c : Config) (i : Nat) (hi : i < c.n) :
    ∃ j, j < c.n ∧ (j = 0 ∨ j = c.split - 1 ∨ j = c.split ∨ j = c.n - 1) ∧
      segVal c i ≤ segVal c j := by
  by_cases his : i < c.split
  · by_cases hs : 0 ≤ c.step
    · by_cases hsn : c.split ≤ c.n
      · refine ⟨c.split - 1, ?_, Or.inr (Or.inl rfl), ?_⟩
        have h1 : c.split - 1 < c.split := Nat.sub_lt (Nat.zero_lt_of_lt his) (by decide)
        exact Nat.lt_of_lt_of_le h1 hsn
        have hile : (i : Int) ≤ ((c.split - 1 : Nat) : Int) := by
          exact_mod_cast Nat.le_pred_of_lt his
        have hmul := Int.mul_le_mul_of_nonneg_left hile hs
        have hlt : c.split - 1 < c.split := Nat.sub_lt (Nat.zero_lt_of_lt his) (by decide)
        simpa [segVal, his, hlt] using hmul
      · refine ⟨c.n - 1, Nat.sub_lt (Nat.zero_lt_of_lt hi) (by decide),
          Or.inr (Or.inr (Or.inr rfl)), ?_⟩
        have hile : (i : Int) ≤ ((c.n - 1 : Nat) : Int) := by
          exact_mod_cast Nat.le_pred_of_lt hi
        have hmul := Int.mul_le_mul_of_nonneg_left hile hs
        have hnls : c.n < c.split := by omega
        have hjn : c.n - 1 < c.split := Nat.lt_trans (n := c.n - 1) (m := c.n) (k := c.split)
          (Nat.sub_lt (n := c.n) (m := 1) (Nat.zero_lt_of_lt hi) (by decide)) hnls
        simpa [segVal, his, hjn] using hmul
    · refine ⟨0, by omega, Or.inl rfl, ?_⟩
      have hmul := Int.mul_le_mul_of_nonpos_left (by omega : c.step ≤ 0) (Int.natCast_nonneg i)
      have hle : c.base + c.step * (i : Int) ≤ c.base := by omega
      by_cases hsp : 0 < c.split <;> simp [segVal, his, hsp] <;> omega
  · by_cases hb : 0 ≤ c.bias
    · refine ⟨c.n - 1, by omega, Or.inr (Or.inr (Or.inr rfl)), ?_⟩
      have hile : ((i - c.split : Nat) : Int) ≤ ((c.n - 1 - c.split : Nat) : Int) := by
        exact_mod_cast (by omega : i - c.split ≤ c.n - 1 - c.split)
      have hmul := Int.mul_le_mul_of_nonneg_left hile hb
      have hbase : c.base + c.step * (c.split : Int) + c.bias * ((i - c.split : Nat) : Int) ≤
          c.base + c.step * (c.split : Int) + c.bias * ((c.n - 1 - c.split : Nat) : Int) := by
        omega
      simpa [segVal, his, show ¬ c.n - 1 < c.split by omega] using hbase
    · refine ⟨c.split, by omega, Or.inr (Or.inr (Or.inl rfl)), ?_⟩
      have hmul := Int.mul_le_mul_of_nonpos_left (by omega : c.bias ≤ 0) (Int.natCast_nonneg (i - c.split))
      have hle := Int.add_le_add_left hmul (c.base + c.step * (c.split : Int))
      simpa [segVal, his] using hle

private theorem seg_cand_index (c : Config) (j : Int) (hj : j ∈
    ([0, c.split - 1, c.split, c.n - 1].filter (· < c.n)).filterMap (layout.read c)) :
    ∃ k, k < c.n ∧ segVal c k = j := by
  rcases List.mem_filterMap.mp hj with ⟨k, hk, hread⟩
  have hkn : k < c.n := by simpa using (List.mem_filter.mp hk).2
  refine ⟨k, hkn, ?_⟩
  by_cases h : k < c.split <;> simpa [layout, segVal, h] using hread

theorem sound_SegmentedSlope_TurnExtrema : SegmentedSlope.TurnExtrema.Goal := by
  intro c
  let val := segVal c
  let cand := ([0, c.split - 1, c.split, c.n - 1].filter (· < c.n)).filterMap (layout.read c)
  have hrows : (rows layout c).map Prod.snd = (List.range c.n).map val := by
    rw [rows_map]
    simp [val, segVal]
  have hcand_mem : ∀ j, j < c.n →
      (j = 0 ∨ j = c.split - 1 ∨ j = c.split ∨ j = c.n - 1) → val j ∈ cand := by
    intro j hj hbd
    apply List.mem_filterMap.mpr
    refine ⟨j, ?_, ?_⟩
    · apply List.mem_filter.mpr
      constructor
      · rcases hbd with rfl | rfl | rfl | rfl <;> simp
      · simpa using hj
    · dsimp [layout, val, segVal]
      by_cases h : j < c.split <;> simp [h]
  change ((rows layout c).map Prod.snd).max? = cand.max?
  by_cases hn : c.n = 0
  · simp [hn, rows, layout, cand, val, segVal]
  · have hnpos : 0 < c.n := by omega
    have hzero : val 0 ∈ cand := hcand_mem 0 (by omega) (Or.inl rfl)
    have hsome := List.isSome_max?_of_mem hzero
    rcases Option.isSome_iff_exists.mp hsome with ⟨M, hM⟩
    have hMrow : M ∈ (rows layout c).map Prod.snd := by
      rw [hrows]
      rcases seg_cand_index c M (List.max?_mem hM) with ⟨k, hkn, hkv⟩
      exact List.mem_map.mpr ⟨k, List.mem_range.mpr hkn, hkv⟩
    have hbound : ∀ b ∈ (rows layout c).map Prod.snd, b ≤ M := by
      intro b hb
      rw [hrows] at hb
      rcases List.mem_map.mp hb with ⟨i, hi, rfl⟩
      have hin : i < c.n := List.mem_range.mp hi
      rcases segVal_boundary c i hin with ⟨j, hj, hbd, hle⟩
      exact Int.le_trans hle ((List.max?_le_iff hM).mp (Int.le_refl M) (val j) (hcand_mem j hj hbd))
    rw [hrows] at hMrow hbound
    rw [hrows, List.max?_eq_some_iff.mpr ⟨hMrow, hbound⟩, hM]

end SegmentedSlope

/-! ## MirroredTail -/

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

theorem sound_MirroredTail_SliceState : MirroredTail.SliceState.Goal := by
  intro c lo hi hle
  exact slice_from_rows_eq layout c lo hi hle (rows_lanes c) (by
    intro a ha
    simp [layout] at ha
    rcases ha with rfl | rfl <;> simp [span, lane])

theorem sound_MirroredTail_OverlapDistinct : MirroredTail.OverlapDistinct.Goal := by
  intro c hs
  let a := min c.n c.split
  let b := c.n - c.split
  let L := max a b
  let f : Nat → Int := fun k => c.base + c.step * (k : Int)
  have ha_n : a ≤ c.n := Nat.min_le_left _ _
  have hb_n : b ≤ c.n := by dsimp [b]; omega
  have ha_L : a ≤ L := Nat.le_max_left _ _
  have hb_L : b ≤ L := Nat.le_max_right _ _
  have hrows : (rows layout c).map Prod.snd =
      (List.range c.n).map (fun i => if i < c.split then f i else f (c.n - 1 - i)) := by
    rw [rows_map]
    simp [f]
  have hmem : ∀ v, v ∈ ((rows layout c).map Prod.snd).eraseDups ↔ v ∈ (List.range L).map f := by
    intro v
    rw [List.mem_eraseDups, hrows, List.mem_map, List.mem_map]
    constructor
    · rintro ⟨i, hi, rfl⟩
      have hin : i < c.n := List.mem_range.mp hi
      by_cases his : i < c.split
      · have hia : i < a := Nat.lt_min.mpr ⟨hin, his⟩
        refine ⟨i, List.mem_range.mpr (Nat.lt_of_lt_of_le hia ha_L), ?_⟩
        simp [his]
      · have hk : c.n - 1 - i < b := by dsimp [b]; omega
        refine ⟨c.n - 1 - i, List.mem_range.mpr (Nat.lt_of_lt_of_le hk hb_L), ?_⟩
        simp [his]
    · rintro ⟨k, hk, rfl⟩
      have hkL : k < L := List.mem_range.mp hk
      by_cases hka : k < a
      · have hkn : k < c.n := Nat.lt_of_lt_of_le hka ha_n
        refine ⟨k, List.mem_range.mpr hkn, ?_⟩
        have hks : k < c.split := Nat.lt_of_lt_of_le hka (Nat.min_le_right _ _)
        simp [hks]
      · have hkb : k < b := by
          dsimp [L] at hkL
          omega
        have hki : c.split ≤ c.n - 1 - k := by dsimp [b] at hkb; omega
        have hin : c.n - 1 - k < c.n := by omega
        refine ⟨c.n - 1 - k, List.mem_range.mpr hin, ?_⟩
        have hnot : ¬ c.n - 1 - k < c.split := by omega
        have heq : c.n - 1 - (c.n - 1 - k) = k := by omega
        simp [hnot, heq]
  have hf_inj : ∀ x y, f x = f y → x = y := by
    intro x y h
    have hm : c.step * (x : Int) = c.step * (y : Int) := Int.add_left_cancel h
    have hxy : (x : Int) = (y : Int) := (Int.mul_eq_mul_left_iff hs).mp hm
    exact_mod_cast hxy
  have hnd : ((List.range L).map f).Nodup := by
    exact List.Pairwise.map f (fun _ _ hne => fun h => hne (hf_inj _ _ h)) List.nodup_range
  have hp : ((rows layout c).map Prod.snd).eraseDups.Perm ((List.range L).map f) :=
    (List.perm_ext_iff_of_nodup (nodup_eraseDups _) hnd).mpr hmem
  change ((rows layout c).map Prod.snd).eraseDups.length = L
  rw [perm_length hp, List.length_map, List.length_range]

end MirroredTail

/-! ## BucketRuns -/

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

theorem sound_BucketRuns_SliceState : BucketRuns.SliceState.Goal := by
  intro c lo hi hle
  exact slice_from_rows_eq layout c lo hi hle (rows_lanes c) (by
    intro a ha
    simp [layout] at ha
    rcases ha with ⟨j, hj, rfl⟩
    simp [span, lane])

theorem sound_BucketRuns_PartialBucket : BucketRuns.PartialBucket.Goal := by
  intro c j hs
  have hp : 0 < width c := width_pos c
  let v := c.base + c.step * (j : Int)
  have hval : ∀ i : Nat,
      (c.base + c.step * ((i / width c : Nat) : Int) == v) = (i / width c == j) := by
    intro i
    by_cases heq : i / width c = j
    · subst j
      simp [v]
    · have hne : c.base + c.step * ((i / width c : Nat) : Int) ≠ v := by
        intro h
        have hm := Int.add_left_cancel h
        have hc : ((i / width c : Nat) : Int) = (j : Int) := (Int.mul_eq_mul_left_iff hs).mp hm
        exact heq (by exact_mod_cast hc)
      rw [beq_eq_false_iff_ne.mpr hne, beq_eq_false_iff_ne.mpr heq]
  have hrows : rows layout c = (List.range c.n).map (fun i =>
      (i, c.base + c.step * ((i / width c : Nat) : Int))) :=
    rows_range_map layout c (fun i => c.base + c.step * ((i / width c : Nat) : Int)) (by intro i _; rfl)
  unfold occurrence
  rw [hrows, List.countP_map]
  change (List.range c.n).countP (fun i : Nat =>
    c.base + c.step * ((i / width c : Nat) : Int) == v) = min (width c) (c.n - j * width c)
  rw [List.countP_eq_length_filter]
  rw [show (List.range c.n).filter (fun i : Nat =>
      c.base + c.step * ((i / width c : Nat) : Int) == v) =
      (List.range c.n).filter (fun i => i / width c == j) by
    apply filter_congr_bool
    intro i hi
    exact hval i]
  rw [show (List.range c.n).filter (fun i => i / width c == j) =
      (List.range c.n).filter (fun i => j * width c ≤ i && i < (j + 1) * width c) by
    apply filter_congr_bool
    intro i hi
    have hiff : i / width c = j ↔ j * width c ≤ i ∧ i < (j + 1) * width c := by
      rw [Nat.div_eq_iff hp]
      constructor
      · intro h
        constructor
        · exact h.1
        · rw [Nat.add_mul, Nat.one_mul]
          omega
      · intro h
        rw [Nat.add_mul, Nat.one_mul] at h
        constructor
        · exact h.1
        · omega
    exact decide_eq_of_iff hiff]
  change ((List.range c.n).filter (fun i => j * width c ≤ i && i < (j + 1) * width c)).length = _
  rw [range_count_between c.n (j * width c) ((j + 1) * width c) (by
    rw [Nat.add_mul, Nat.one_mul]
    exact Nat.le_add_right _ _)]
  by_cases hle : j * width c ≤ c.n
  · rw [Nat.min_eq_right hle]
    have hmin : min c.n ((j + 1) * width c) = j * width c + min (width c) (c.n - j * width c) := by
      rw [show (j + 1) * width c = j * width c + width c by rw [Nat.add_mul, Nat.one_mul],
        show c.n = j * width c + (c.n - j * width c) from (Nat.add_sub_of_le hle).symm,
        Nat.add_min_add_left]
      congr 1
      rw [show j * width c + (c.n - j * width c) - j * width c = c.n - j * width c from
        Nat.add_sub_cancel_left (j * width c) (c.n - j * width c)]
      exact Nat.min_comm _ _
    rw [hmin]
    exact Nat.add_sub_cancel_left (j * width c) (min (width c) (c.n - j * width c))
  · have hnle : c.n ≤ j * width c := by omega
    have hend : c.n ≤ (j + 1) * width c := by
      rw [Nat.add_mul, Nat.one_mul]
      exact Nat.le_trans hnle (Nat.le_add_right _ _)
    rw [Nat.min_eq_left hnle, Nat.min_eq_left hend]
    have hsub : c.n - j * width c = 0 := Nat.sub_eq_zero_of_le hnle
    simp [hsub]

end BucketRuns

end TinylambOpt.ResearchProofs.Atlas.EncodedPages1
