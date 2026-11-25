import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages3

open TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-! ## List helpers -/

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

private theorem sum_congr_int {α : Type} (l : List α) (f g : α → Int)
    (h : ∀ x ∈ l, f x = g x) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem list_sum_perm {l₁ l₂ : List Int} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem perm_length {α : Type} {l₁ l₂ : List α} (p : l₁.Perm l₂) :
    l₁.length = l₂.length := by
  induction p with
  | nil => rfl
  | cons a p ih => simp [ih]
  | swap a b l => simp
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

private theorem map_pair_filter {α β : Type} (xs : List α) (f : α → Nat) (g : α → β)
    (p : Nat → Bool) :
    (xs.map (fun x => (f x, g x))).filter (fun r => p r.1) =
      (xs.filter (fun x => p (f x))).map (fun x => (f x, g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih => by_cases h : p (f x) <;> simp [h, ih]

private theorem countP_eq_sum (l : List (Nat × Int)) (f : Int → Bool) :
    l.countP (fun x => f x.2) = ((l.map (fun x => if f x.2 then 1 else 0)).sum : Nat) := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : f x.2 <;> simp [h, ih] <;> omega

private theorem beq_comm_bool {α : Type} [BEq α] [LawfulBEq α] (a b : α) :
    (a == b) = (b == a) := by
  by_cases h : a = b
  · subst b
    rfl
  · rw [beq_eq_false_iff_ne.mpr h, beq_eq_false_iff_ne.mpr (fun hb => h hb.symm)]

private theorem countP_eq_count_map {α : Type} [BEq α] [LawfulBEq α]
    (l : List Nat) (f : Nat → α) (v : α) :
    l.countP (fun i => f i == v) = (l.map f).count v := by
  induction l with
  | nil => simp
  | cons i is ih =>
      rw [List.countP_cons, List.map_cons, List.count_cons, ih]

private theorem nodup_map_of_injective {α β : Type} (l : List α) (f : α → β)
    (h : ∀ a ∈ l, ∀ b ∈ l, f a = f b → a = b) (hl : l.Nodup) : (l.map f).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.nodup_cons] at hl
      rw [List.map_cons, List.nodup_cons]
      constructor
      · intro hm
        rcases List.mem_map.mp hm with ⟨b, hb, he⟩
        exact hl.1 (by rw [← h b (by simp [hb]) a (by simp) he]; exact hb)
      · exact ih (fun x hx y hy => h x (by simp [hx]) y (by simp [hy])) hl.2

private theorem sum_range_add (f g : Nat → Nat) (n : Nat) :
    ((List.range n).map (fun i => f i + g i)).sum =
      ((List.range n).map f).sum + ((List.range n).map g).sum := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append,
        List.map_append, List.sum_append, List.map_append, List.sum_append]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
      rw [ih]
      omega

private theorem sum_range_mul_const (f : Nat → Nat) (n c : Nat) :
    ((List.range n).map (fun i => c * f i)).sum = c * ((List.range n).map f).sum := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
      rw [ih]
      simp only [List.map_append, List.sum_append, List.map_cons, List.map_nil,
        List.sum_cons, List.sum_nil]
      rw [Nat.mul_add]
      simp

private theorem sum_range_const_nat (m v : Nat) :
    ((List.range m).map (fun _ => v)).sum = m * v := by
  induction m with
  | zero => simp
  | succ m ih =>
      rw [show m + 1 = Nat.succ m by omega, List.range_succ, List.map_append, List.sum_append, ih]
      simp [Nat.succ_mul]

private theorem sum_map_zero_int (l : List Nat) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l <;> simp [*]

private theorem sum_map_zero_nat (l : List Nat) : (l.map (fun _ => (0 : Nat))).sum = 0 := by
  induction l <;> simp [*]

private theorem sum_ite_filter {α : Type} (l : List α) (p : α → Bool) (v : Int) :
    ((l.map (fun x => if p x then v else 0)).sum) = (((l.filter p).map (fun _ => v)).sum) := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

private theorem sum_filter_const {α : Type} (l : List α) (p : α → Bool) (v : Int) :
    (((l.filter p).map (fun _ => v)).sum) = v * ((l.filter p).length : Int) := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih, Int.mul_add] <;> omega

private theorem range_succ_map_sum (f : Nat → Int) (n : Nat) :
    ((List.range (n+1)).map f).sum = ((List.range n).map f).sum + f n := by
  rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
  simp

private theorem sum_range_add_int (f g : Nat → Int) (n : Nat) :
    ((List.range n).map (fun i => f i + g i)).sum =
      ((List.range n).map f).sum + ((List.range n).map g).sum := by
  induction n with
  | zero => simp
  | succ n ih => rw [range_succ_map_sum, range_succ_map_sum, range_succ_map_sum, ih]; omega

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
  | succ k ih => rw [partialSum_succ, ih, List.range_succ, List.map_append, List.sum_append]; simp

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
      · simp [h1, h2]; rw [ih]
        have hmthi : min n thi = n := Nat.min_eq_left (by omega)
        have hmtlo : min n tlo = tlo := Nat.min_eq_right (by omega)
        have hmnthi : min (n + 1) thi = n + 1 := Nat.min_eq_left (by omega)
        have hmntlo : min (n + 1) tlo = tlo := Nat.min_eq_right (by omega)
        rw [hmthi, hmtlo, hmnthi, hmntlo]; simp [List.range_succ]; omega
      · simp [h1, h2]; rw [ih]
        have hnthi : min (n + 1) thi = min n thi := by omega
        have hntlo : min (n + 1) tlo = min n tlo := by omega
        rw [hnthi, hntlo]
      · simp [h1, h2]; rw [ih]
        have hmthi : min n thi = n := Nat.min_eq_left (by omega)
        have hmtlo : min n tlo = n := Nat.min_eq_left (by omega)
        have hmnthi : min (n + 1) thi = n + 1 := Nat.min_eq_left (by omega)
        have hmntlo : min (n + 1) tlo = n + 1 := Nat.min_eq_left (by omega)
        rw [hmthi, hmtlo, hmnthi, hmntlo]; simp [List.range_succ]
      · simp [h1, h2]; omega

private theorem ceilDiv_eq (d s : Nat) (hs : 0 < s) : d + s - 1 - (s - 1) = d := by omega

private theorem pos_lt_iff (start stride i p : Nat) (hs : 0 < stride) :
    start + i * stride < p ↔ i < (p - start + stride - 1) / stride := by
  by_cases hps : p ≤ start
  · have hp0 : p - start = 0 := by omega
    have hzero : (p - start + stride - 1) / stride = 0 := by
      rw [hp0, Nat.zero_add]
      exact Nat.div_eq_of_lt (by omega)
    rw [hzero]; omega
  · rw [Nat.lt_div_iff_mul_lt hs]
    have hrewrite : p - start + stride - 1 - (stride - 1) = p - start :=
      ceilDiv_eq (p - start) stride hs
    rw [hrewrite]; omega

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
    ((lanePoints a).filter (fun r => lo ≤ r.1 && r.1 < hi)).length =
      beforePos a hi - beforePos a lo := by
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
    rw [beforePos_eq_min a hi hs,
      filter_congr_bool _ _ _ (fun i _ => decide_eq_of_iff (hiff_hi i)), range_count_lt]
  have bl : beforePos a lo = min a.n tlo := by
    rw [beforePos_eq_min a lo hs,
      filter_congr_bool _ _ _ (fun i _ => decide_eq_of_iff (hiff_lo i)), range_count_lt]
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
  have hf := hperm.filter p
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

private theorem slice_from_rows_eq (lay : Layout) (c : Config) (lo hi : Nat) (hle : lo ≤ hi)
    (heq : rows lay c = (lay.lanes c).flatMap lanePoints)
    (hs : ∀ a ∈ lay.lanes c, 0 < a.stride) :
    let selected := (rows lay c).filter (fun r => lo ≤ r.1 && r.1 < hi)
    let ls := lay.lanes c
    (selected.length,(selected.map Prod.snd).sum) =
      ((ls.map (fun a => beforePos a hi - beforePos a lo)).sum,
       (ls.map (fun a => partialSum a (beforePos a hi)-partialSum a (beforePos a lo))).sum) := by
  apply slice_from_perm lay c lo hi hle _ hs
  rw [heq]

/-! ## Residue partitions and striped layouts -/

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
    have hm : n % 1 = 0 := by have := Nat.mod_lt n (by omega : 0 < 1); omega
    simp [hm]
  · have hp' : 1 < p := by omega
    have h1 : (n + 1) % p = (n % p + 1 % p) % p := Nat.add_mod n 1 p
    have h1p : 1 % p = 1 := Nat.mod_eq_of_lt hp'
    rw [h1, h1p]
    by_cases h : n % p + 1 < p
    · rw [Nat.mod_eq_of_lt h]; simp [h]
    · have heq : n % p + 1 = p := by have hs := Nat.mod_lt n hp; omega
      rw [heq, Nat.mod_self]; simp [h]

private theorem succ_div_step (n p : Nat) (hp : 0 < p) :
    (n + 1) / p = n / p + if n % p + 1 < p then 0 else 1 := by
  have hd := Nat.div_add_mod n p
  have hs := Nat.mod_lt n hp
  by_cases h : n % p + 1 < p
  · have hEq : n + 1 = p * (n / p) + (n % p + 1) := by grind
    rw [hEq, Nat.mul_add_div hp (n / p) (n % p + 1), Nat.div_eq_of_lt h]; simp [h]
  · have heq : n % p + 1 = p := by omega
    have hEq : n + 1 = p * (n / p + 1) := by grind
    rw [hEq, Nat.mul_comm p (n / p + 1), Nat.mul_div_left _ hp]; simp [h]

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
  · have hltold : r < n % p ∨ n % p < r := by
      by_cases hrlt : r < n % p
      · exact Or.inl hrlt
      · right
        have hs := Nat.mod_lt n hp
        have hne : n % p ≠ r := by
          intro he
          exact hn (by simp [he])
        omega
    by_cases h : n % p + 1 < p
    · rcases hltold with hlt | hgt
      · have hltnew : r < n % p + 1 := by omega
        simp [hn, h, hlt, hltnew]
      · have hltnew : ¬ r < n % p + 1 := by omega
        have hnlt : ¬ r < n % p := by omega
        simp [hn, h, hnlt, hltnew]
    · have heq : n % p + 1 = p := by have hs' := Nat.mod_lt n hp; omega
      have hlt : r < n % p := by omega
      have hltnew : ¬ r < 0 := by omega
      simp [hn, h, hlt, hltnew]

private theorem count_residue_mod (n p r : Nat) (hp : 0 < p) (hr : r < p) :
    ((List.range n).filter (fun i => i % p == r)).length = (n - r + p - 1) / p := by
  induction n with
  | zero =>
      have hsub : 0 - r = 0 := by omega
      rw [hsub, Nat.zero_add, Nat.div_eq_of_lt (Nat.sub_lt hp (by omega : 0 < 1))]
      simp
  | succ n ih =>
      rw [List.range_succ, List.filter_append, List.length_append, ih,
        residue_count_succ n p r hp hr]
      by_cases h : n % p == r <;> simp [h]

private theorem residue_count_eq_zero_of_ge (N r p : Nat) (hr : N ≤ r) (hp : 0 < p) :
    (N - r + p - 1) / p = 0 := by
  have hsub : N - r = 0 := by omega
  rw [hsub, Nat.zero_add]
  exact Nat.div_eq_of_lt (Nat.sub_lt hp (by omega : 0 < 1))

private theorem sum_update_int (p r0 : Nat) (hr0 : r0 < p) (s : Nat → Int) (v : Int) :
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
          apply map_congr
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

private theorem sum_eq_single_int (p r : Nat) (hr : r < p) (f : Nat → Int) :
    ((List.range p).map (fun x => if x == r then f x else 0)).sum = f r := by
  have hconst : ∀ x : Nat, (if x == r then f x else 0) = if x == r then f r else 0 := by
    intro x
    by_cases h : x == r
    · have h' : x = r := Nat.eq_of_beq_eq_true h
      simp [h, h']
    · simp [h]
  rw [show (List.range p).map (fun x => if x == r then f x else 0) =
      (List.range p).map (fun x => if x == r then f r else 0) from
    map_congr _ _ _ (fun x _ => hconst x)]
  rw [show (List.range p).map (fun x => if x == r then f r else 0) =
      (List.range p).map (fun x => if x == r then f r + 0 else 0) by
    apply map_congr
    intro x _
    by_cases h : x == r <;> simp [h]]
  rw [sum_update_int p r hr (fun _ : Nat => 0) (f r)]
  simp [sum_map_zero_int]

private theorem residue_partition_sum_full (n p : Nat) (hp : 0 < p) (f : Nat → Int) :
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
          apply congrArg List.sum
          apply map_congr
          intro r hr
          have hrp : r < p := by have := List.mem_range.mp hr; omega
          have hnmod : n % p = n := Nat.mod_eq_of_lt hnp
          have hne : ¬ n % p == r := by
            intro hh
            have heq : n % p = r := Nat.eq_of_beq_eq_true hh
            have hmem := List.mem_range.mp hr
            omega
          rw [residue_count_succ n p r hp hrp]; simp [hne]
        rw [hsum]
        have hlast : ((List.range ((n+1-n+p-1)/p)).map (fun q => f (n+q*p))).sum = f n := by
          have hcnt : (n+1-n+p-1)/p = 1 := by
            have hn1 : n + 1 - n = 1 := by omega
            rw [hn1]
            have hp1 : 1 + p - 1 = p := by omega
            rw [hp1, Nat.div_self hp]
          rw [hcnt]; simp
        simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, hlast]
        omega
      · have hminn : min p n = p := Nat.min_eq_left (by omega)
        have hmins : min p (n+1) = p := Nat.min_eq_left (by omega)
        rw [hminn, hmins]
        have hsum : ((List.range p).map (fun r =>
            ((List.range ((n+1-r+p-1)/p)).map (fun q => f (r+q*p))).sum)).sum =
            ((List.range p).map (fun r =>
              ((List.range ((n-r+p-1)/p)).map (fun q => f (r+q*p))).sum +
                if n % p == r then f n else 0)).sum := by
          apply congrArg List.sum
          apply map_congr
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
            rw [hval]; simp [hnr]
          · simp [hnr]
        rw [hsum, sum_range_add_int]
        have hif : ((List.range p).map (fun r => if n % p == r then f n else 0)).sum = f n := by
          have hsym : (List.range p).map (fun r => if n % p == r then f n else 0) =
              (List.range p).map (fun r => if r == n % p then f n else 0) := by
            apply map_congr
            intro r _
            rw [beq_comm_bool (n % p) r]
          rw [hsym]
          exact sum_eq_single_int p (n % p) (Nat.mod_lt n hp) (fun _ => f n)
        rw [hif]

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
        have hz : (n - k + p - 1) / p = 0 := residue_count_eq_zero_of_ge n k p hkge hp
        simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, hz]
        simp
      · have hkeq : k + 1 = min p n := by omega
        rw [show k + 1 = min p n by omega]
        exact residue_partition_sum_full n p hp f

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
      apply Nat.div_le_div_right; omega
    rw [Nat.min_eq_left hle]
  · have ht : t < c.n := by omega
    have hmin : min c.n t = t := Nat.min_eq_right (by omega)
    rw [hmin]
    have hle : (t - r + p - 1) / p ≤ (c.n - r + p - 1) / p := by
      apply Nat.div_le_div_right; omega
    rw [Nat.min_eq_right hle]

private theorem lane_value_of_striped (p : Nat) (hp : 0 < p) (r q : Nat) (hr : r < p)
    (f : Nat → Int) (make : Nat → Int × Int)
    (hval : ∀ i, f i = (make (i % p)).1 + (make (i % p)).2 * ((i / p : Nat) : Int)) :
    f (r + q*p) = (make r).1 + (make r).2 * (q : Int) := by
  have hmod : (r + q*p) % p = r := by rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hr]
  have hdiv : (r + q*p) / p = q := by
    rw [Nat.add_mul_div_right r q hp, Nat.div_eq_of_lt hr]; simp
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
    simp only [Nat.min_def]; split <;> split <;> omega
  have min_mono_left (a b c : Nat) (h : a ≤ b) : min a c ≤ min b c := by
    simp only [Nat.min_def]; split <;> split <;> omega
  have hkhi : min p Nhi ≤ k := by dsimp [k]; exact min_mono_right Nhi c.n p hNhi_le
  have hklo : min p Nlo ≤ k := by dsimp [k]; exact min_mono_right Nlo c.n p hNlo_le
  have hLenL : ((((List.range c.n).map (fun i => (i, f i))).filter (fun r => lo ≤ r.1 && r.1 < hi)).length) = Nhi - Nlo := by
    rw [filter_map_pair_range]; simp only [List.length_map]
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
      dsimp [k]; apply map_congr; intro r hr
      have hrp : r < p := by have := List.mem_range.mp hr; omega
      rw [beforePos_lane_eq_count c p r hi (make r).1 (make r).2 hp hrp,
        beforePos_lane_eq_count c p r lo (make r).1 (make r).2 hp hrp]]
    have hpoint : ∀ r ∈ List.range k, (Nlo - r + p - 1) / p ≤ (Nhi - r + p - 1) / p := by
      intro r _
      apply Nat.div_le_div_right
      dsimp [Nhi, Nlo]
      have hlohi : min c.n lo ≤ min c.n hi := by
        rw [Nat.min_comm c.n lo, Nat.min_comm c.n hi]
        exact min_mono_left lo hi c.n hle
      omega
    rw [sum_map_sub_nat (h := hpoint),
      residue_partition_count_range Nhi p k hp hkhi hk_le_p,
      residue_partition_count_range Nlo p k hp hklo hk_le_p]
  have hSumL : ((((List.range c.n).map (fun i => (i, f i))).filter (fun r => lo ≤ r.1 && r.1 < hi)).map Prod.snd).sum =
      ((List.range Nhi).map f).sum - ((List.range Nlo).map f).sum := by
    rw [filter_map_pair_range]; simp only [List.map_map, Function.comp_apply]
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
      dsimp [k]; apply map_congr; intro r hr
      have hrp : r < p := by have := List.mem_range.mp hr; omega
      rw [beforePos_lane_eq_count c p r hi (make r).1 (make r).2 hp hrp,
        beforePos_lane_eq_count c p r lo (make r).1 (make r).2 hp hrp,
        partialSum_eq_sum, partialSum_eq_sum]
      unfold lane; dsimp
      congr 1
      · apply sum_congr_int; intro q hq
        have hq' : q < (Nhi - r + p - 1)/p := List.mem_range.mp hq
        have hrq : r + q*p < Nhi := residue_lt_of_count Nhi p r q hp hrp hq'
        have hpos : r + q*p < c.n := by omega
        rw [lane_value_of_striped p hp r q hrp f make hval]; simp [Int.mul_comm]
      · apply sum_congr_int; intro q hq
        have hq' : q < (Nlo - r + p - 1)/p := List.mem_range.mp hq
        have hrq : r + q*p < Nlo := residue_lt_of_count Nlo p r q hp hrp hq'
        have hpos : r + q*p < c.n := by omega
        rw [lane_value_of_striped p hp r q hrp f make hval]; simp [Int.mul_comm]]
    rw [sum_map_sub,
      residue_partition_sum_range Nhi p k hp hkhi hk_le_p f,
      residue_partition_sum_range Nlo p k hp hklo hk_le_p f]
  apply Prod.ext <;> simp only [Prod.fst, Prod.snd]
  · exact hLenL.trans hLenR.symm
  · exact hSumL.trans hSumR.symm

/-! ## Residue-list enumeration and uniqueness -/

private theorem filterMap_ite_eq {α β : Type} (l : List α) (p : α → Bool) (f : α → β) :
    l.filterMap (fun x => if p x then some (f x) else none) = (l.filter p).map f := by
  induction l with
  | nil => rfl
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

private theorem mem_of_mem_filter {α : Type} {l : List α} {p : α → Bool} {a : α}
    (h : a ∈ l.filter p) : a ∈ l := (List.mem_filter.mp h).1

private theorem mem_of_mem_flatMap {α β : Type} {l : List α} {f : α → List β} {b : β}
    (h : b ∈ l.flatMap f) : ∃ a, a ∈ l ∧ b ∈ f a := List.mem_flatMap.mp h

private theorem nodup_filter {α : Type} (l : List α) (p : α → Bool) (hl : l.Nodup) :
    (l.filter p).Nodup := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      rw [List.nodup_cons] at hl
      by_cases h : p x
      · rw [List.filter_cons, if_pos h, List.nodup_cons]
        exact ⟨fun hm => hl.1 (List.mem_filter.mp hm).1, ih hl.2⟩
      · rw [List.filter_cons, if_neg h]
        exact ih hl.2

private theorem range_filter_mod_eq (n p r : Nat) (hp : 0 < p) (hr : r < p) :
    (List.range n).filter (fun i => i % p == r) =
      (List.range ((n - r + p - 1) / p)).map (fun q => r + q * p) := by
  induction n with
  | zero =>
      rw [residue_count_eq_zero_of_ge 0 r p (by omega) hp]
      simp
  | succ n ih =>
      rw [List.range_succ, List.filter_append, ih,
        mod_step_count n p r hp hr, mod_step_count (n+1) p r hp hr,
        succ_mod_step n p hp, succ_div_step n p hp]
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
            have hs := Nat.mod_lt n hp
            omega
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

private theorem flatMap_congr_left {α β : Type} (l : List α) (f g : α → List β)
    (h : ∀ x ∈ l, f x = g x) : l.flatMap f = l.flatMap g := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem nodup_pairs_range (n : Nat) (f : Nat → Int) :
    ((List.range n).map (fun i => (i, f i))).Nodup := by
  apply nodup_map_of_injective _ _ _ List.nodup_range
  intro a _ b _ he
  have ha : a = b := congrArg Prod.fst he
  exact ha

private theorem lanePoints_nodup (a : Lane) (hs : 0 < a.stride) : (lanePoints a).Nodup := by
  unfold lanePoints
  apply nodup_map_of_injective _ _ _ List.nodup_range
  intro i _ j _ he
  have hij : i * a.stride = j * a.stride := by
    have hh : a.start + i * a.stride = a.start + j * a.stride := congrArg Prod.fst he
    omega
  exact (Nat.eq_of_mul_eq_mul_left hs (by simpa [Nat.mul_comm] using hij))

private theorem pairwise_range_rel (p : Nat) (R : Nat → Nat → Prop)
    (h : ∀ a < p, ∀ b < p, a ≠ b → R a b) : (List.range p).Pairwise R := by
  induction p with
  | zero => simp
  | succ p ih =>
      rw [show p + 1 = Nat.succ p by omega, List.range_succ, List.pairwise_append]
      refine ⟨ih (fun a ha b hb hab => h a (by omega) b (by omega) hab), by simp, ?_⟩
      intro a ha
      have ha' : a < p := List.mem_range.mp ha
      intro b hb
      have hb' : b = p := by simpa using hb
      subst b
      exact h a (by omega) p (by omega) (by omega)

private theorem residue_flatMap_nodup (n p k : Nat) (hp : 0 < p) (hk : k ≤ p)
    (base step : Int) :
    (((List.range (min k n)).flatMap fun r =>
      (List.range ((n - r + p - 1) / p)).map fun q =>
        (r + q*p, base + step*(r + q*p : Nat)))).Nodup := by
  let F : Nat → List (Nat × Int) := fun r =>
    (List.range ((n - r + p - 1) / p)).map fun q =>
      (r + q*p, base + step*(r + q*p : Nat))
  change List.Pairwise (fun a b : Nat × Int => a ≠ b) (List.flatMap F (List.range (min k n)))
  rw [List.pairwise_flatMap]
  constructor
  · intro r _
    unfold F
    apply nodup_map_of_injective _ _ _ List.nodup_range
    intro q _ q' _ he
    have hh : r + q*p = r + q'*p := congrArg Prod.fst he
    exact Nat.eq_of_mul_eq_mul_left hp (by
      simpa [Nat.mul_comm] using Nat.add_left_cancel hh)
  · apply pairwise_range_rel (min k n)
    intro a ha b hb hab x hx y hy he
    rcases List.mem_map.mp hx with ⟨qx, hqx, rfl⟩
    rcases List.mem_map.mp hy with ⟨qy, hqy, rfl⟩
    have ha' : a < p := by omega
    have hb' : b < p := by omega
    have hpos : a + qx*p = b + qy*p := congrArg Prod.fst he
    have hamod : (a + qx*p) % p = a := by
      rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt ha']
    have hbmod : (b + qy*p) % p = b := by
      rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hb']
    have hab' : a = b := by rw [← hamod, ← hbmod, hpos]
    exact hab hab'

private theorem residue_rows_perm (n p k : Nat) (hp : 0 < p) (hk : k ≤ p)
    (base step : Int) :
    (((List.range n).filter (fun i => i % p < k)).map (fun i => (i, base + step*((i:Nat):Int)))).Perm
      (((List.range (min k n)).flatMap fun r =>
        (List.range ((n - r + p - 1) / p)).map fun q =>
          (r + q*p, base + step*(r + q*p : Nat)))) := by
  refine (List.perm_ext_iff_of_nodup ?_ ?_).mpr ?_
  · exact nodup_map_of_injective _ _ (by
      intro a _ b _ he
      exact congrArg Prod.fst he) (nodup_filter _ _ List.nodup_range)
  · exact residue_flatMap_nodup n p k hp hk base step
  intro z
  constructor
  · intro hz
    rcases List.mem_map.mp hz with ⟨i, hi, rfl⟩
    have hin : i < n := List.mem_range.mp (mem_of_mem_filter hi)
    have hik : i % p < k := of_decide_eq_true (List.mem_filter.mp hi).2
    have hrp : i % p < p := Nat.mod_lt i hp
    have hrK : i % p < min k n := Nat.lt_min.mpr ⟨hik, by
      have := Nat.mod_le i p
      omega⟩
    have hq : i / p < (n - i % p + p - 1) / p := by
      have hdecomp : i % p + (i / p) * p = i := by
        have hd := Nat.div_add_mod i p
        rw [Nat.mul_comm p (i / p)] at hd
        omega
      have hh := (pos_lt_iff (i % p) p (i / p) n hp).mp (by rw [hdecomp]; exact hin)
      exact hh
    apply List.mem_flatMap.mpr
    refine ⟨i % p, List.mem_range.mpr hrK, ?_⟩
    apply List.mem_map.mpr
    refine ⟨i / p, List.mem_range.mpr hq, ?_⟩
    have hdecomp : i % p + (i / p) * p = i := by
      have hd := Nat.div_add_mod i p
      rw [Nat.mul_comm p (i / p)] at hd
      omega
    simp [hdecomp]
  · intro hz
    rcases mem_of_mem_flatMap hz with ⟨r, hr, hz⟩
    rcases List.mem_map.mp hz with ⟨q, hq, rfl⟩
    have hrp : r < p := by
      have := List.mem_range.mp hr
      omega
    have hrk : r < k := by
      have := List.mem_range.mp hr
      omega
    have hrn : r + q*p < n := residue_lt_of_count n p r q hp hrp (List.mem_range.mp hq)
    apply List.mem_map.mpr
    refine ⟨r + q*p, List.mem_filter.mpr ⟨List.mem_range.mpr hrn, ?_⟩, rfl⟩
    have hmod : (r + q*p) % p = r := by
      rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hrp]
    rw [hmod]
    simp [hrk]

/-! ## The five assigned layouts: slice-state proofs -/

namespace LaneMajor

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.LaneMajor

theorem sound_LaneMajor_SliceState : SliceState.Goal := by
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
    apply rows_range_map
    intro i _
    rfl
  rw [hrows]
  have hlanes : layout.lanes c = striped c (width c)
      (fun r => (c.base + c.bias*r, c.step)) := rfl
  rw [hlanes]
  exact stripedSlice c (width c) hw (fun r => (c.base + c.bias*r, c.step))
    (fun i => c.base + c.step*(i/width c:Nat) + c.bias*(i%width c:Nat))
    (fun i => by simpa using hval i) lo hi hle

end LaneMajor

namespace Boustrophedon

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.Boustrophedon

theorem sound_Boustrophedon_SliceState : SliceState.Goal := by
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
      have hinside : (i / w * w + (if (i / w) % 2 == 0 then i % w else w - 1 - (i % w) : Nat)) =
          (2*q) * w + s := by
        rw [hdivw, hmodw]
        simp [hpar]
      change c.base + c.step * ((i / w * w +
        (if (i / w) % 2 == 0 then i % w else w - 1 - (i % w) : Nat)) : Int) =
        (make (i % per)).1 + (make (i % per)).2 * (i / per : Int)
      rw [Int.ofNat_ediv_ofNat, Int.ofNat_ediv_ofNat,
        ← Int.natCast_mul (i / w) w,
        ← Int.natCast_add (i / w * w) (if (i / w) % 2 == 0 then i % w else w - 1 - (i % w) : Nat),
        hinside]
      have hmodper : (i % per) = s := rfl
      have hdivper : (i / per) = q := rfl
      rw [hmodper, hdivper]
      dsimp [make]
      rw [if_pos hsw]
      dsimp [per]
      simp [Int.ofNat_ediv_ofNat, Int.natCast_add, Int.natCast_mul, Int.mul_add,
        Int.mul_assoc, Int.mul_comm, Int.mul_left_comm]
      omega
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
      have hinside : (i / w * w + (if (i / w) % 2 == 0 then i % w else w - 1 - (i % w) : Nat)) =
          (2*q+1) * w + (w-1-t) := by
        rw [hdivw, hmodw]
        simp [hpar]
      change c.base + c.step * ((i / w * w +
        (if (i / w) % 2 == 0 then i % w else w - 1 - (i % w) : Nat)) : Int) =
        (make (i % per)).1 + (make (i % per)).2 * (i / per : Int)
      rw [Int.ofNat_ediv_ofNat, Int.ofNat_ediv_ofNat,
        ← Int.natCast_mul (i / w) w,
        ← Int.natCast_add (i / w * w) (if (i / w) % 2 == 0 then i % w else w - 1 - (i % w) : Nat),
        hinside]
      have hmodper : (i % per) = s := rfl
      have hdivper : (i / per) = q := rfl
      rw [hmodper, hdivper]
      dsimp [make]
      rw [if_neg hsw, hs_eq]
      have hsub : 2*w - 1 - (w+t-w) = w + (w-1-t) := by omega
      rw [hsub]
      dsimp [per]
      simp [Int.ofNat_ediv_ofNat, Int.natCast_add, Int.natCast_mul, Int.mul_add,
        Int.mul_assoc, Int.mul_comm, Int.mul_left_comm]
      omega
  have hrows : rows layout c = (List.range c.n).map (fun i => (i, f i)) := by
    apply rows_range_map
    intro i _
    rfl
  rw [hrows]
  have hlanes : layout.lanes c = striped c per make := by
    dsimp [layout, striped, make, per, w]
  rw [hlanes]
  exact stripedSlice c per hper make f hval lo hi hle

end Boustrophedon

namespace RaggedStripe

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.RaggedStripe

private theorem rows_lanes_perm (c : Config) :
    (rows layout c).Perm ((layout.lanes c).flatMap lanePoints) := by
  let w := width c
  have hw : 0 < w := by dsimp [w, width]; omega
  have hrows : rows layout c =
      ((List.range c.n).filter (fun i => !(i % w == w-1))).map
        (fun i => (i, c.base + c.step*((i:Nat):Int))) := by
    unfold rows
    simp only [layout]
    rw [← filterMap_ite_eq]
    apply filterMap_congr'
    intro i _
    dsimp [w]
    by_cases h : i % width c = width c - 1 <;> simp [layout, h]
  rw [hrows]
  have hpred : (fun i : Nat => !(i % w == w-1)) = (fun i : Nat => decide (i % w < w-1)) := by
    funext i
    by_cases h : i % w < w-1
    · have he : i % w ≠ w-1 := by omega
      simp [h, he]
    · have he : i % w = w-1 := by
        have hm := Nat.mod_lt i hw
        omega
      simp [h, he]
  rw [hpred]
  refine List.Perm.trans (residue_rows_perm c.n w (w-1) hw (by omega) c.base c.step) ?_
  have hflat : ((layout.lanes c).flatMap lanePoints) =
      ((List.range (min (w-1) c.n)).flatMap fun r =>
        (List.range ((c.n - r + w - 1) / w)).map fun q =>
          (r + q*w, c.base + c.step*(r + q*w : Nat))) := by
    simp only [layout, lanePoints, lane, List.flatMap_map, List.map_map, Function.comp_apply]
    rw [Nat.min_comm c.n (w-1)]
    apply flatMap_congr_left
    intro r _
    apply map_congr
    intro q _
    apply Prod.ext
    · rfl
    · dsimp [w]
      simp [Int.mul_add, Int.mul_assoc, Int.mul_comm, Int.mul_left_comm,
        Int.add_assoc, Int.add_comm, Int.add_left_comm]
  rw [← hflat]

theorem sound_RaggedStripe_SliceState : SliceState.Goal := by
  intro c lo hi hle
  apply slice_from_perm layout c lo hi hle (rows_lanes_perm c)
  intro a ha
  simp [layout] at ha
  rcases ha with ⟨r, hr, rfl⟩
  have hw : 0 < width c := by dsimp [width]; omega
  simp [lane]
  exact hw

end RaggedStripe

namespace PeriodicPatch

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.PeriodicPatch

theorem sound_PeriodicPatch_SliceState : SliceState.Goal := by
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
      have himod : i % w == 0 := by simp [hi, hr0']
      dsimp [f, make]
      rw [hcast, ← hnum, Int.ofNat_mod_ofNat, Int.ofNat_ediv_ofNat, hq', hr']
      simp [himod, Int.mul_add, Int.mul_comm, Int.mul_assoc, Int.natCast_add]
      omega
    · have hr0' : r ≠ 0 := by intro he; exact hr0 (by rw [he]; simp)
      have himod : ¬ i % w == 0 := by
        rw [show i % w = r by
          simp [hi, Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hrlt]]
        exact hr0
      dsimp [f, make]
      rw [hcast, ← hnum, Int.ofNat_mod_ofNat, Int.ofNat_ediv_ofNat, hq', hr']
      simp [himod, Int.mul_add, Int.mul_comm, Int.mul_assoc, Int.natCast_add]
      omega
  have hrows : rows layout c = (List.range c.n).map (fun i => (i, f i)) := by
    apply rows_range_map
    intro i _
    rfl
  rw [hrows]
  have hlanes : layout.lanes c = striped c w make := by
    dsimp [layout, striped, make, w]
  rw [hlanes]
  exact stripedSlice c w hw make f hval lo hi hle

end PeriodicPatch

namespace SparseSample

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.SparseSample

private theorem rows_lanes_eq (c : Config) :
    rows layout c = (layout.lanes c).flatMap lanePoints := by
  let w := width c
  let r := c.split % w
  have hw : 0 < w := by dsimp [w, width]; omega
  have hr : r < w := by dsimp [r]; exact Nat.mod_lt _ hw
  have hrows : rows layout c =
      ((List.range c.n).filter (fun i => i % w == r)).map
        (fun i => (i, c.base + c.step*((i / w : Nat) : Int))) := by
    unfold rows
    simp only [layout]
    rw [← filterMap_ite_eq]
    apply filterMap_congr'
    intro i _
    by_cases h : i % w == r <;> simp [layout, w, r, h]
  rw [hrows, range_filter_mod_eq c.n w r hw hr]
  simp only [List.map_map, Function.comp_apply]
  simp only [layout, List.flatMap_cons, List.flatMap_nil, List.append_nil]
  unfold lanePoints lane
  dsimp
  apply map_congr
  intro q _
  have hdiv : (r + q*w) / w = q := by
    rw [Nat.add_mul_div_right r q hw, Nat.div_eq_of_lt hr]
    simp
  apply Prod.ext
  · rfl
  · simp [hdiv, Int.natCast_add, Int.natCast_mul, Int.mul_add, Int.mul_comm, Int.mul_assoc]
    have hcast : (((r + q*w) / w : Nat) : Int) = (q : Int) := by exact_mod_cast hdiv
    simpa [Int.natCast_add, Int.natCast_mul] using congrArg (fun z => c.step*z) hcast

theorem sound_SparseSample_SliceState : SliceState.Goal := by
  intro c lo hi hle
  apply slice_from_rows_eq layout c lo hi hle (rows_lanes_eq c)
  intro a ha
  have haeq : a = lane c.n (c.split % width c) (width c) c.base c.step := by
    simpa [layout] using ha
  subst a
  have hw : 0 < width c := by dsimp [width]; omega
  simp [lane]
  exact hw

end SparseSample

/-! ## Additional finite-list and integer helpers -/

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

private theorem eraseDups_eq_self_of_nodup {α : Type} [BEq α] [LawfulBEq α]
    (l : List α) (hl : l.Nodup) : l.eraseDups = l := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      rw [List.nodup_cons] at hl
      rw [List.eraseDups_cons]
      have hf : (as.filter (fun b => !b == a)) = as := by
        apply List.filter_eq_self.mpr
        intro b hb
        have hba : b ≠ a := by intro he; exact hl.1 (he ▸ hb)
        simp [hba]
      rw [hf, ih hl.2]

private theorem even_or_odd (n : Nat) : n % 2 = 0 ∨ n % 2 = 1 := by
  have h := Nat.mod_lt n (by omega : 0 < 2)
  omega

private theorem natAbs_sub_lt_of_lt (a b w : Nat) (ha : a < w) (hb : b < w) :
    ((a : Int) - (b : Int)).natAbs < w := by
  by_cases h : a ≤ b
  · have hnon : 0 ≤ (b : Int) - (a : Int) := Int.sub_nonneg.mpr (by
      exact_mod_cast (show a ≤ b by omega))
    have hneg : (a : Int) - (b : Int) = -((b : Int) - (a : Int)) := by omega
    rw [hneg, Int.natAbs_neg,
      Int.natAbs_sub_of_nonneg_of_le (by omega : 0 ≤ (a : Int)) (by exact_mod_cast h),
      Int.natAbs_natCast, Int.natAbs_natCast]
    omega
  · have hnon : 0 ≤ (a : Int) - (b : Int) := Int.sub_nonneg.mpr (by
      exact_mod_cast (show b ≤ a by omega))
    rw [Int.natAbs_sub_of_nonneg_of_le (by omega : 0 ≤ (b : Int)) (by exact_mod_cast (show b ≤ a by omega)),
      Int.natAbs_natCast, Int.natAbs_natCast]
    omega

private theorem natAbs_sub_le_of_le (a b B : Nat) (ha : a ≤ B) (hb : b ≤ B) :
    ((a : Int) - (b : Int)).natAbs ≤ B := by
  have hlt := natAbs_sub_lt_of_lt a b (B+1) (by omega) (by omega)
  omega

private theorem reverse_range_map (w : Nat) :
    (List.range w).reverse = (List.range w).map (fun r => w-1-r) := by
  simpa [List.range_eq_range'] using (List.reverse_range' (s := 0) (n := w))

/-! ## Additional model-specific goals -/

namespace LaneMajor

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.LaneMajor

theorem sound_LaneMajor_FrequencySeparation : FrequencySeparation.Goal := by
  intro c hs hsep
  let w := width c
  have hw : 0 < w := by dsimp [w, width]; omega
  let val : Nat → Int := fun i =>
    c.base + c.step*(i/w:Nat) + c.bias*(i%w:Nat)
  have hinj : ∀ i < c.n, ∀ j < c.n, val i = val j → i = j := by
    intro i hi j hj he
    change c.step.natAbs*(c.n/w) < c.bias.natAbs at hsep
    let qi := i / w
    let ri := i % w
    let qj := j / w
    let rj := j % w
    have hri : ri < w := by dsimp [ri]; exact Nat.mod_lt i hw
    have hrj : rj < w := by dsimp [rj]; exact Nat.mod_lt j hw
    have hqi : qi ≤ c.n / w := by
      dsimp [qi]
      exact Nat.div_le_div_right (by omega : i ≤ c.n)
    have hqj : qj ≤ c.n / w := by
      dsimp [qj]
      exact Nat.div_le_div_right (by omega : j ≤ c.n)
    have hrel : c.bias * ((ri : Int) - (rj : Int)) =
        c.step * ((qj : Int) - (qi : Int)) := by
      change c.base + c.step*(qi:Int) + c.bias*(ri:Int) =
        c.base + c.step*(qj:Int) + c.bias*(rj:Int) at he
      simp only [Int.add_assoc] at he
      have h1 := Int.add_left_cancel he
      rw [Int.mul_sub, Int.mul_sub]
      omega
    have hrreq : ri = rj := by
      by_cases hne : ri = rj
      · exact hne
      · have habs := congrArg Int.natAbs hrel
        simp only [Int.natAbs_mul] at habs
        have hrdiff : ((ri : Int) - (rj : Int)).natAbs < w :=
          natAbs_sub_lt_of_lt ri rj w hri hrj
        have hqdiff : ((qj : Int) - (qi : Int)).natAbs ≤ c.n / w :=
          natAbs_sub_le_of_le qj qi (c.n / w) hqj hqi
        have hpos : 0 < ((ri : Int) - (rj : Int)).natAbs := by
          rw [Int.natAbs_pos]
          intro hz
          have hc : (ri : Int) = (rj : Int) := by omega
          exact hne (by exact_mod_cast hc)
        have hleft : c.bias.natAbs ≤ c.bias.natAbs * ((ri : Int) - (rj : Int)).natAbs := by
          simpa using Nat.mul_le_mul_left c.bias.natAbs (Nat.succ_le_of_lt hpos)
        have hright : c.step.natAbs * ((qj : Int) - (qi : Int)).natAbs ≤
            c.step.natAbs * (c.n / w) := Nat.mul_le_mul_left _ hqdiff
        have hchain : c.bias.natAbs ≤ c.step.natAbs * (c.n / w) := by
          calc
            c.bias.natAbs ≤ c.bias.natAbs * ((ri : Int) - (rj : Int)).natAbs := hleft
            _ = c.step.natAbs * ((qj : Int) - (qi : Int)).natAbs := habs
            _ ≤ c.step.natAbs * (c.n / w) := hright
        exact (Nat.not_lt_of_ge hchain hsep).elim
    have hqreq : qi = qj := by
      have hzero : c.step * ((qi : Int) - (qj : Int)) = 0 := by
        rw [hrreq] at hrel
        simp only [Int.sub_self, Int.mul_zero] at hrel
        rw [Int.mul_sub] at hrel ⊢
        omega
      have hdiff : (qi : Int) = (qj : Int) := by
        apply (Int.mul_eq_mul_left_iff hs).mp
        rw [Int.mul_sub] at hzero
        omega
      exact_mod_cast hdiff
    have hdi := Nat.div_add_mod i w
    have hdj := Nat.div_add_mod j w
    have hdii : i = qi*w+ri := by
      simpa [qi, ri, Nat.mul_comm] using hdi.symm
    have hdjj : j = qj*w+rj := by
      simpa [qj, rj, Nat.mul_comm] using hdj.symm
    rw [hqreq, hrreq] at hdii
    omega
  have hrows : (rows layout c).map Prod.snd = (List.range c.n).map val := by
    unfold rows
    simp only [layout, Option.map_some, filterMap_some_eq_map, List.map_map,
      Function.comp_apply, val]
    rfl
  unfold distinctValues
  rw [hrows]
  have hnd : ((List.range c.n).map val).Nodup := by
    apply nodup_map_of_injective _ _ _ List.nodup_range
    intro i hi j hj he
    exact hinj i (List.mem_range.mp hi) j (List.mem_range.mp hj) he
  rw [eraseDups_eq_self_of_nodup _ hnd, List.length_map, List.length_range]

end LaneMajor

namespace Boustrophedon

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.Boustrophedon

private def bIdx (w i : Nat) : Nat :=
  let k := i / w
  let r := i % w
  k*w + (if k%2 == 0 then r else w-1-r)

private theorem bIdx_add (w k r : Nat) (hw : 0 < w) (hr : r < w) :
    bIdx w (k*w+r) = k*w + (if k%2 == 0 then r else w-1-r) := by
  unfold bIdx
  rw [Nat.add_comm (k*w) r, Nat.add_mul_div_right r k hw, Nat.div_eq_of_lt hr,
    Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hr]
  simp [Nat.mul_comm]

private theorem bIdx_perm (w : Nat) (hw : 0 < w) : ∀ q,
    ((List.range (q*w)).map (bIdx w)).Perm (List.range (q*w)) := by
  intro q
  induction q with
  | zero => simp
  | succ q ih =>
      have hmul : (q+1)*w = q*w+w := by simp [Nat.succ_mul]
      rw [hmul]
      rw [List.range_add]
      rw [List.map_append]
      apply List.Perm.append ih
      have htail : (List.map (bIdx w) (List.map (fun r => q*w+r) (List.range w))) =
          (List.map (fun r => q*w + (if q%2 == 0 then r else w-1-r)) (List.range w)) := by
        rw [List.map_map]
        apply map_congr
        intro r hr
        have hr' : r < w := List.mem_range.mp hr
        change bIdx w (q*w+r) = q*w + (if q%2 == 0 then r else w-1-r)
        rw [bIdx_add w q r hw hr']
      rw [htail]
      rcases even_or_odd q with hq | hq
      · have hb : (q % 2 == 0) = true := by simp [hq]
        simp [hb]
      · have hb : (q % 2 == 0) = false := beq_eq_false_iff_ne.mpr (by omega)
        simp [hb]
        have hrev : ((List.range w).map (fun r => w-1-r)).Perm (List.range w) := by
          rw [← reverse_range_map w]
          exact List.reverse_perm _
        simpa only [List.map_map, Function.comp_def] using hrev.map (fun r => q*w+r)

theorem sound_Boustrophedon_FullTiles : FullTiles.Goal := by
  intro c hn
  let w := width c
  have hw : 0 < w := by dsimp [w, width]; omega
  let q := c.n / w
  have hcq : c.n = q*w := by
    dsimp [q]
    have hd := Nat.div_add_mod c.n w
    rw [hn] at hd
    simpa [Nat.mul_comm] using hd.symm
  have hb : (rows layout c).map Prod.snd =
      (List.range (q*w)).map (fun i => c.base+c.step*(bIdx w i : Nat)) := by
    unfold rows
    rw [hcq]
    simp [layout, bIdx, w]
  have hl : (rows LinearOrdinal.layout c).map Prod.snd =
      (List.range (q*w)).map (fun i => c.base+c.step*((i:Nat):Int)) := by
    unfold rows
    rw [hcq]
    simp only [LinearOrdinal.layout, Option.map_some, filterMap_some_eq_map,
      List.map_map, Function.comp_apply]
    rfl
  rw [hb, hl]
  simpa only [List.map_map, Function.comp_def] using
    (bIdx_perm w hw q).map (fun i : Nat => c.base+c.step*(i:Int))

end Boustrophedon

namespace RaggedStripe

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.RaggedStripe

theorem sound_RaggedStripe_NullCount : NullCount.Goal := by
  intro c
  let w := width c
  have hw : 0 < w := by dsimp [w, width]; omega
  have hlen : (rows layout c).length =
      ((List.range c.n).filter (fun i => !(i % w == w-1))).length := by
    unfold rows
    rw [length_filterMap_isSome]
    apply congrArg List.length
    apply filter_congr_bool
    intro i _
    dsimp [w]
    by_cases h : i % width c = width c - 1 <;> simp [layout, h]
  rw [hlen, length_filter_not]
  have hnone := count_residue_mod c.n w (w-1) hw (by omega : w-1 < w)
  have hwcount : (c.n - (w-1) + w - 1) / w = c.n / w := by
    by_cases hge : w-1 ≤ c.n
    · have hsub : c.n - (w-1) + w - 1 = c.n := by omega
      rw [hsub]
    · have hsub : c.n - (w-1) = 0 := by omega
      rw [hsub, Nat.zero_add, Nat.div_eq_of_lt (by omega : w-1 < w),
        Nat.div_eq_of_lt (by omega : c.n < w)]
  rw [hnone, hwcount]
  dsimp [w]
  simp

end RaggedStripe

namespace PeriodicPatch

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.PeriodicPatch

theorem sound_PeriodicPatch_PatchBudget : PatchBudget.Goal := by
  intro c
  dsimp
  let w := width c
  have hw : 0 < w := by dsimp [w, width]; omega
  have hrowsPair : rows layout c = (List.range c.n).map (fun i : Nat =>
      (i, c.base + c.step*(i:Int) + (if i%w == 0 then c.bias else 0))) := by
    apply rows_range_map
    intro i _
    rfl
  have hrows : (rows layout c).map Prod.snd =
      (List.range c.n).map (fun i : Nat =>
        c.base + c.step*(i:Int) + (if i%w == 0 then c.bias else 0)) := by
    rw [hrowsPair]
    simp only [List.map_map, Function.comp_apply]
    rfl
  rw [hrows, sum_range_add_int]
  have hbase : ((List.range c.n).map (fun i : Nat => c.base + c.step*(i:Int))).sum =
      ((rows LinearOrdinal.layout c).map Prod.snd).sum := by
    unfold rows
    simp only [LinearOrdinal.layout, Option.map_some, filterMap_some_eq_map,
      List.map_map, Function.comp_apply]
    rfl
  rw [hbase]
  have hite : ((List.range c.n).map (fun i : Nat => if i % w == 0 then c.bias else 0)).sum =
      c.bias * ((c.n+w-1)/w) := by
    rw [sum_ite_filter, sum_filter_const, count_residue_mod c.n w 0 hw (by omega)]
    rw [Nat.sub_zero, show c.n+w-1 = c.n+(w-1) by omega,
      ← Int.ofNat_ediv_ofNat, Int.natCast_add, Int.natCast_sub (by omega : 1 ≤ w)]
    simp only [Int.add_sub_assoc]
    simp
  rw [hite]
  conv => rhs; rw [show c.n+width c-1 = c.n+(width c-1) by omega,
    Int.natCast_add, Int.natCast_sub (by omega : 1 ≤ width c)]
  simp [w, Int.add_sub_assoc]

end PeriodicPatch

/-! ## Value-band counting -/

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

private theorem lt_ceil_iff (d : Int) (hd : 0 < d) (i : Nat) (x : Int) :
    d * (i : Int) < x ↔ i < ((x+d-1)/d).toNat := by
  let q := (x+d-1)/d
  constructor
  · intro h
    have hceil : d * ((i : Int)+1) ≤ x+d-1 := by
      rw [Int.mul_add]
      omega
    have hq : (i : Int)+1 ≤ q :=
      (Int.le_ediv_iff_mul_le hd).mpr (by simpa [Int.mul_comm] using hceil)
    change i < q.toNat
    exact (Int.toNat_lt_toNat (n := (i : Int)) (by omega : 0 < q)).mpr (by omega)
  · intro h
    have hiq : (i : Int) < q := Int.lt_of_toNat_lt h
    have hceil0 : ((i : Int)+1) * d ≤ x+d-1 :=
      (Int.le_ediv_iff_mul_le hd).mp (by omega : (i : Int)+1 ≤ q)
    have hceil : d * ((i : Int)+1) ≤ x+d-1 := by
      simpa [Int.mul_comm] using hceil0
    rw [Int.mul_add] at hceil
    omega

private theorem gt_floor_iff (d : Int) (hd : 0 < d) (i : Nat) (x : Int) :
    d * (i : Int) > x ↔ ((x/d)+1).toNat ≤ i := by
  let q := x/d
  constructor
  · intro h
    have h1 : d*q ≤ x := by
      simpa [Int.mul_comm] using Int.ediv_mul_le x (by omega : d ≠ 0)
    have h2 : d*q < d*(i:Int) := by omega
    have h3 : q < (i:Int) := Int.lt_of_mul_lt_mul_left h2 (by omega : 0 ≤ d)
    have hqi : q+1 ≤ (i:Int) := by omega
    change (q+1).toNat ≤ i
    rw [Int.toNat_le]
    exact hqi
  · intro h
    have hiq : q+1 ≤ (i:Int) := (Int.toNat_le).mp h
    have hmul : d*(q+1) ≤ d*(i:Int) :=
      Int.mul_le_mul_of_nonneg_left hiq (by omega : 0 ≤ d)
    have hlt : x < d*(q+1) := by
      simpa [Int.mul_comm] using Int.lt_ediv_add_one_mul_self x hd
    omega

private theorem beforeVal_eq_count (a : Lane) (cut : Int) :
    beforeVal a cut = ((List.range a.n).filter
      (fun i => a.base + a.step*((i:Nat):Int) < cut)).length := by
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
      have hf : (List.range a.n).filter (fun i => a.base + a.step*((i:Nat):Int) < cut) =
          (List.range a.n).filter (fun i => i <
            ((cut-a.base+a.step-1)/a.step).toNat) := by
        apply filter_congr_bool
        intro i _
        apply decide_eq_of_iff
        have hh := lt_ceil_iff a.step hp i (cut-a.base)
        change a.step*(i:Int) < cut-a.base ↔ _ at hh
        constructor <;> intro h <;> omega
      rw [hf, range_count_lt]
    · have hn : a.step < 0 := by omega
      unfold beforeVal
      rw [if_neg (by simp [hz]), if_neg hp]
      have hd : 0 < -a.step := by omega
      have hf : (List.range a.n).filter (fun i => a.base + a.step*((i:Nat):Int) < cut) =
          (List.range a.n).filter (fun i => ((a.base-cut)/(-a.step)+1).toNat ≤ i) := by
        apply filter_congr_bool
        intro i _
        apply decide_eq_of_iff
        have hh := gt_floor_iff (-a.step) hd i (a.base-cut)
        change (-a.step)*(i:Int) > a.base-cut ↔ _ at hh
        rw [Int.neg_mul] at hh
        omega
      rw [hf, range_count_ge]

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
          · exfalso; exact hx (by simp [hpx])
        by_cases hqx : q x = true
        · have hi := ih (fun y hy => h y (by simp [hy]))
          have hp : (x :: xs).filter p = xs.filter p := by simp [hx0]
          have hq : (x :: xs).filter q = x :: (xs.filter q) := by simp [hqx]
          rw [hp, hq, List.length_cons]
          omega
        · have hqx0 : q x = false := by
            cases hqx' : q x
            · rfl
            · exfalso; exact hqx (by simp [hqx'])
          have hi := ih (fun y hy => h y (by simp [hy]))
          have hp : (x :: xs).filter p = xs.filter p := by simp [hx0]
          have hq : (x :: xs).filter q = xs.filter q := by simp [hqx0]
          rw [hp, hq]
          omega

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

private theorem lane_band (a : Lane) (lo hi : Int) :
    band a lo hi = (lanePoints a).countP (fun r => lo ≤ r.2 && r.2 < hi) := by
  rw [band, beforeVal_eq_count a hi, beforeVal_eq_count a lo]
  rw [lanePoints]
  rw [List.countP_map]
  have hcong : ∀ i ∈ List.range a.n,
      ((fun r => lo ≤ r.2 && r.2 < hi) ∘ (fun i => (a.start + i * a.stride, a.base + (i : Int) * a.step))) i ↔
      (lo ≤ a.base + a.step*((i:Nat):Int) && a.base + a.step*((i:Nat):Int) < hi) := by
    intro i _
    simp only [Function.comp_apply, Prod.snd]
    rw [Int.mul_comm]
  rw [List.countP_congr hcong]
  rw [List.countP_eq_length_filter]
  by_cases hle : lo ≤ hi
  · have hpart := count_lt_partition (List.range a.n) (fun i => a.base + a.step*((i:Nat):Int)) lo hi hle
    omega
  · have hb : (List.range a.n).filter (fun i => lo ≤ a.base + a.step*((i:Nat):Int) && a.base + a.step*((i:Nat):Int) < hi) = [] := by
      apply List.filter_eq_nil_iff.mpr
      intro i _
      have hlt : hi < lo := by omega
      have hfalse : (lo ≤ a.base + a.step*((i:Nat):Int) && a.base + a.step*((i:Nat):Int) < hi) = false := by
        by_cases hv : a.base + a.step*((i:Nat):Int) < hi
        · have hvlo : a.base + a.step*((i:Nat):Int) < lo := by omega
          have hge : ¬ (lo ≤ a.base + a.step*((i:Nat):Int)) := by omega
          simp [hv, hge]
        · simp [hv]
      simp [hfalse]
    have hmono : ((List.range a.n).filter (fun i => a.base + a.step*((i:Nat):Int) < hi)).length ≤
        ((List.range a.n).filter (fun i => a.base + a.step*((i:Nat):Int) < lo)).length := by
      apply filter_subset_length
      intro i _ hh
      have hhv : a.base + a.step*((i:Nat):Int) < hi := of_decide_eq_true hh
      have hlt : hi < lo := by omega
      have hlti : a.base + a.step*((i:Nat):Int) < lo := by omega
      simp [hlti]
    rw [hb]
    simp
    omega

private theorem flatMap_countP (ls : List Lane) (p : Nat × Int → Bool) :
    (ls.flatMap lanePoints).countP p =
      (ls.map (fun a => (lanePoints a).countP p)).sum := by
  induction ls with
  | nil => simp [lanePoints]
  | cons a as ih => simp [List.countP_append, ih]

namespace SparseSample

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.SparseSample

theorem sound_SparseSample_PredicateRank : PredicateRank.Goal := by
  intro c lo hi
  rw [rows_lanes_eq, flatMap_countP]
  apply congrArg List.sum
  apply map_congr
  intro a _
  exact (lane_band a lo hi).symm

end SparseSample

end TinylambOpt.ResearchProofs.Atlas.EncodedPages3
