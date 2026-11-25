import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DevinBatch.EP_SegmentedSlope_MirroredTail

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

private theorem sum_map_zero_nat (l : List Nat) : (l.map (fun _ => (0 : Nat))).sum = 0 := by
  induction l <;> simp [*]

private theorem sum_map_zero_int (l : List Nat) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l <;> simp [*]

private theorem sum_range_const_nat (m v : Nat) :
    ((List.range m).map (fun _ => v)).sum = m * v := by
  induction m with
  | zero => simp
  | succ m ih =>
      rw [List.range_succ, List.map_append, List.sum_append]
      simp [ih, Nat.succ_mul]

private theorem sum_const_one_range (k : Nat) :
    ((List.range k).map (fun _ => (1 : Nat))).sum = k := by
  simpa using sum_range_const_nat k 1

private theorem list_sum_nat_perm {l₁ l₂ : List Nat} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

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

private theorem sum_map_add_nat' {α : Type} (l : List α) (f g : α → Nat) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem sum_map_add_int' {α : Type} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem sum_map_sub_int' {α : Type} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x - g x)).sum = (l.map f).sum - (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem sum_map_mul_int' {α : Type} (l : List α) (f : α → Int) (c : Int) :
    (l.map (fun x => c * f x)).sum = c * (l.map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Int.mul_add]

private theorem sum_swap_nat {m k : Nat} (F : Nat → Nat → Nat) :
    ((List.range m).map (fun j => ((List.range k).map (fun t => F j t)).sum)).sum =
      ((List.range k).map (fun t => ((List.range m).map (fun j => F j t)).sum)).sum := by
  induction m with
  | zero => simp [sum_map_zero_nat]
  | succ m ih =>
      have hL : ((List.range (m + 1)).map
            (fun j => ((List.range k).map (fun t => F j t)).sum)).sum =
          ((List.range m).map (fun j => ((List.range k).map (fun t => F j t)).sum)).sum +
            ((List.range k).map (fun t => F m t)).sum := by
        rw [show m + 1 = Nat.succ m by omega, List.range_succ, List.map_append, List.sum_append]
        simp
      have hR : ((List.range k).map
            (fun t => ((List.range (m + 1)).map (fun j => F j t)).sum)).sum =
          ((List.range k).map
            (fun t => ((List.range m).map (fun j => F j t)).sum + F m t)).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro t _
        rw [show m + 1 = Nat.succ m by omega, List.range_succ, List.map_append, List.sum_append]
        simp
      have hsplit : ((List.range k).map
            (fun t => ((List.range m).map (fun j => F j t)).sum + F m t)).sum =
          ((List.range k).map (fun t => ((List.range m).map (fun j => F j t)).sum)).sum +
            ((List.range k).map (fun t => F m t)).sum := by
        rw [← sum_map_add_nat' (List.range k)
          (fun t => ((List.range m).map (fun j => F j t)).sum) (fun t => F m t)]
      rw [hL, hR, hsplit, ih]

private theorem sum_swap_int {m k : Nat} (F : Nat → Nat → Int) :
    ((List.range m).map (fun j => ((List.range k).map (fun t => F j t)).sum)).sum =
      ((List.range k).map (fun t => ((List.range m).map (fun j => F j t)).sum)).sum := by
  induction m with
  | zero => simp [sum_map_zero_int]
  | succ m ih =>
      have hL : ((List.range (m + 1)).map
            (fun j => ((List.range k).map (fun t => F j t)).sum)).sum =
          ((List.range m).map (fun j => ((List.range k).map (fun t => F j t)).sum)).sum +
            ((List.range k).map (fun t => F m t)).sum := by
        rw [show m + 1 = Nat.succ m by omega, List.range_succ, List.map_append, List.sum_append]
        simp
      have hR : ((List.range k).map
            (fun t => ((List.range (m + 1)).map (fun j => F j t)).sum)).sum =
          ((List.range k).map
            (fun t => ((List.range m).map (fun j => F j t)).sum + F m t)).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro t _
        rw [show m + 1 = Nat.succ m by omega, List.range_succ, List.map_append, List.sum_append]
        simp
      have hsplit : ((List.range k).map
            (fun t => ((List.range m).map (fun j => F j t)).sum + F m t)).sum =
          ((List.range k).map (fun t => ((List.range m).map (fun j => F j t)).sum)).sum +
            ((List.range k).map (fun t => F m t)).sum := by
        rw [← sum_map_add_int' (List.range k)
          (fun t => ((List.range m).map (fun j => F j t)).sum) (fun t => F m t)]
      rw [hL, hR, hsplit, ih]

private theorem sum_range_add_int (f g : Nat → Int) (n : Nat) :
    ((List.range n).map (fun i => f i + g i)).sum =
      ((List.range n).map f).sum + ((List.range n).map g).sum := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
      rw [ih]
      simp only [List.map_append, List.sum_append, List.map_cons, List.map_nil,
        List.sum_cons, List.sum_nil]
      omega

private theorem sum_range_mul_const_int (f : Nat → Int) (n : Nat) (c : Int) :
    ((List.range n).map (fun i => c * f i)).sum = c * ((List.range n).map f).sum := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
      rw [ih]
      simp only [List.map_append, List.sum_append, List.map_cons, List.map_nil,
        List.sum_cons, List.sum_nil]
      rw [Int.mul_add]
      simp

private theorem sum_range_succ_int (f : Nat → Int) (n : Nat) :
    ((List.range (n + 1)).map f).sum = ((List.range n).map f).sum + f n := by
  rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
  simp

/-! ## Arithmetic progressions: lane points and slices -/

private def lanePoints (a : Lane) : List (Nat × Int) :=
  (List.range a.n).map (fun i : Nat => (a.start + i * a.stride, a.base + (i : Int) * a.step))

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

private theorem sum_range_id_int (n : Nat) :
    ((List.range n).map (fun t : Nat => (t : Int))).sum = tri n := by
  induction n with
  | zero => simp [tri, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  | succ n ih =>
      rw [sum_range_succ_int, ih, tri_succ]

private theorem sum_range_sq_int (n : Nat) :
    ((List.range n).map (fun t : Nat => ((t : Int) * (t : Int)))).sum = squares n := by
  induction n with
  | zero => simp [squares]
  | succ n ih =>
      rw [sum_range_succ_int, ih, squares_succ]

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
    have hrewrite : p - start + stride - 1 - (stride - 1) = p - start := ceilDiv_eq (p - start) stride hs
    rw [hrewrite]
    omega

private theorem beforePos_eq_min (a : Lane) (p : Nat) (hs : 0 < a.stride) :
    beforePos a p = min a.n ((p - a.start + a.stride - 1) / a.stride) := by
  unfold beforePos
  rfl

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
  simp [contains, lastVal]
  constructor <;> intro h <;> omega

private theorem contains_bounds (a : Progression) (v : Int) (h : contains a v = true) :
    a.lo ≤ v ∧ v ≤ lastVal a := by
  obtain ⟨k, hk, hval⟩ := (contains_iff_mem a v).mp h
  unfold progVal at hval
  have hkint : (k : Int) ≤ (a.n - 1 : Nat) := by exact_mod_cast (by omega)
  have hstep0 : 0 ≤ (a.step : Int) := Int.natCast_nonneg _
  have hk0 : 0 ≤ (k : Int) := Int.natCast_nonneg _
  have hnn : 0 ≤ (k : Int) * (a.step : Int) := Int.mul_nonneg hk0 hstep0
  have hmul' : (k : Int) * (a.step : Int) ≤ ((a.n - 1 : Nat) : Int) * (a.step : Int) :=
    Int.mul_le_mul_of_nonneg_right hkint hstep0
  unfold lastVal
  constructor <;> omega

private theorem contains_dvd (a : Progression) (v : Int) (h : contains a v = true) :
    (a.step : Int) ∣ v - a.lo := by
  obtain ⟨k, _, hval⟩ := (contains_iff_mem a v).mp h
  unfold progVal at hval
  refine ⟨(k : Int), ?_⟩
  rw [Int.mul_comm (a.step : Int) (k : Int)]
  omega

private theorem common_gap_dvd (a b : Progression) (v : Int)
    (ha : contains a v = true) (hb : contains b v = true) :
    (a.step.gcd b.step : Int) ∣ b.lo - a.lo := by
  obtain ⟨i, _, hi⟩ := (contains_iff_mem a v).mp ha
  obtain ⟨j, _, hj⟩ := (contains_iff_mem b v).mp hb
  unfold progVal at hi hj
  have hgcd_a : ((a.step.gcd b.step : Nat) : Int) ∣ (a.step : Int) := by
    have h : a.step.gcd b.step ∣ a.step := Nat.gcd_dvd_left _ _
    exact_mod_cast h
  have hgcd_b : ((a.step.gcd b.step : Nat) : Int) ∣ (b.step : Int) := by
    have h : a.step.gcd b.step ∣ b.step := Nat.gcd_dvd_right _ _
    exact_mod_cast h
  have h1 : ((a.step.gcd b.step : Nat) : Int) ∣ (j : Int) * (b.step : Int) :=
    Int.dvd_mul_of_dvd_right hgcd_b
  have h2 : ((a.step.gcd b.step : Nat) : Int) ∣ (i : Int) * (a.step : Int) :=
    Int.dvd_mul_of_dvd_right hgcd_a
  have heq : b.lo - a.lo = (i : Int) * (a.step : Int) - (j : Int) * (b.step : Int) := by omega
  rw [heq]
  exact Int.dvd_sub h2 h1

private theorem intersection_contains (a b : Progression) (v : Int) :
    contains (intersection a b) v = true ↔ contains a v = true ∧ contains b v = true := by
  by_cases han : a.n = 0
  · have hi : intersection a b = ⟨0, 1, 0⟩ := by
      rw [intersection.eq_def]
      simp [han]
    have hca : contains a v = false := by simp [contains, han]
    have hL : contains (⟨0, 1, 0⟩ : Progression) v = false := by simp [contains]
    rw [hi, hL]
    constructor
    · intro h; simp at h
    · rintro ⟨ha, -⟩; simp [hca] at ha
  · by_cases hbn : b.n = 0
    · have hi : intersection a b = ⟨0, 1, 0⟩ := by
        rw [intersection.eq_def]
        simp [hbn]
      have hcb : contains b v = false := by simp [contains, hbn]
      have hL : contains (⟨0, 1, 0⟩ : Progression) v = false := by simp [contains]
      rw [hi, hL]
      constructor
      · intro h; simp at h
      · rintro ⟨-, hb⟩; simp [hcb] at hb
    · have hanp : 0 < a.n := by omega
      have hbnp : 0 < b.n := by omega
      by_cases has : a.step = 0
      · have hstep : (a.step : Int) = 0 := by simp [has]
        have hca_iff : contains a v = true ↔ v = a.lo := by
          constructor
          · intro h
            have h1 := (contains_bounds a v h).1
            have h2 := (contains_bounds a v h).2
            unfold lastVal at h2
            rw [hstep] at h2
            simp at h2
            omega
          · intro hv
            rw [hv]
            simp [contains, lastVal, has, hanp]
        have hi : intersection a b =
            ⟨a.lo, 1, if contains b a.lo = true then 1 else 0⟩ := by
          rw [intersection.eq_def]
          simp [han, has]
        rw [hi]
        by_cases hbc : contains b a.lo = true
        · rw [if_pos hbc]
          have hr : contains (⟨a.lo, 1, 1⟩ : Progression) v = true ↔ v = a.lo := by
            constructor
            · intro h
              have h1 := (contains_bounds ⟨a.lo, 1, 1⟩ v h).1
              have h2 := (contains_bounds ⟨a.lo, 1, 1⟩ v h).2
              unfold lastVal at h2
              simp at h2
              omega
            · intro hv
              rw [hv]
              simp [contains, lastVal]
          rw [hr, hca_iff]
          constructor
          · rintro ⟨hv, -⟩; exact hv
          · intro hv
            refine ⟨hv, ?_⟩
            rw [hv]
            exact hbc
        · rw [if_neg hbc]
          have hr : contains (⟨a.lo, 1, 0⟩ : Progression) v = false := by simp [contains]
          rw [hr]
          constructor
          · intro h; simp at h
          · rintro ⟨ha, hb⟩
            have hv : v = a.lo := hca_iff.mp ha
            rw [hv] at hb
            exact absurd hb hbc
      · by_cases hbs : b.step = 0
        · have hstep : (b.step : Int) = 0 := by simp [hbs]
          have hcb_iff : contains b v = true ↔ v = b.lo := by
            constructor
            · intro h
              have h1 := (contains_bounds b v h).1
              have h2 := (contains_bounds b v h).2
              unfold lastVal at h2
              rw [hstep] at h2
              simp at h2
              omega
            · intro hv
              rw [hv]
              simp [contains, lastVal, hbs, hbnp]
          have hi : intersection a b =
              ⟨b.lo, 1, if contains a b.lo = true then 1 else 0⟩ := by
            rw [intersection.eq_def]
            simp [han, has, hbs]
          rw [hi]
          by_cases hac : contains a b.lo = true
          · rw [if_pos hac]
            have hr : contains (⟨b.lo, 1, 1⟩ : Progression) v = true ↔ v = b.lo := by
              constructor
              · intro h
                have h1 := (contains_bounds ⟨b.lo, 1, 1⟩ v h).1
                have h2 := (contains_bounds ⟨b.lo, 1, 1⟩ v h).2
                unfold lastVal at h2
                simp at h2
                omega
              · intro hv
                rw [hv]
                simp [contains, lastVal]
            rw [hr, hcb_iff]
            constructor
            · intro hv
              refine ⟨?_, hv⟩
              rw [hv]
              exact hac
            · rintro ⟨-, hv⟩; exact hv
          · rw [if_neg hac]
            have hr : contains (⟨b.lo, 1, 0⟩ : Progression) v = false := by simp [contains]
            rw [hr]
            constructor
            · intro h; simp at h
            · rintro ⟨ha, hb⟩
              have hv : v = b.lo := hcb_iff.mp hb
              rw [hv] at ha
              exact absurd ha hac
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
          have hp_nat : 0 < period :=
            Nat.mul_pos (Nat.div_gcd_pos_of_pos_left b.step hasp) hbsp
          have hp : (0 : Int) < period := by exact_mod_cast hp_nat
          have hgapdef : gap = b.lo - a.lo := rfl
          by_cases hgapmod : gap % (g : Int) = 0
          · have hgap : (g : Int) ∣ gap := Int.dvd_of_emod_eq_zero hgapmod
            have hroot := root_dvd a.step b.step hasp hbsp a.lo b.lo gap hgapdef hgap
            have hfirst : first = low + (root - low) % (period : Int) := rfl
            have hfroot := first_root_dvd low root first period hp hfirst
            have hremnon : 0 ≤ (root - low) % (period : Int) := Int.emod_nonneg _ (by omega)
            have hflow : low ≤ first := by dsimp [first]; omega
            have hper : period = a.step * (b.step / g) := by
              have e1 : g * (a.step / g) = a.step := by
                have h := Nat.mul_div_cancel' (Nat.gcd_dvd_left a.step b.step)
                simpa [g] using h
              have e2 : b.step = g * (b.step / g) := by
                have h := Nat.mul_div_cancel' (Nat.gcd_dvd_right a.step b.step)
                simpa [g] using h.symm
              dsimp [period]
              calc (a.step / g) * b.step = (a.step / g) * (g * (b.step / g)) := by rw [e2]
                _ = ((a.step / g) * g) * (b.step / g) := by rw [Nat.mul_assoc]
                _ = (g * (a.step / g)) * (b.step / g) := by rw [Nat.mul_comm (a.step / g) g]
                _ = a.step * (b.step / g) := by rw [e1]
            have hpa : (a.step : Int) ∣ (period : Int) := by
              rw [hper]
              refine ⟨((b.step / g : Nat) : Int), ?_⟩
              simp [Int.natCast_mul]
            have hpb : (b.step : Int) ∣ (period : Int) := by
              have hper' : period = b.step * (a.step / g) := by
                rw [hper, Nat.mul_comm a.step (b.step / g)]
                have e1 : g * (a.step / g) = a.step := by
                  have h := Nat.mul_div_cancel' (Nat.gcd_dvd_left a.step b.step)
                  simpa [g] using h
                have e2 : b.step = g * (b.step / g) := by
                  have h := Nat.mul_div_cancel' (Nat.gcd_dvd_right a.step b.step)
                  simpa [g] using h.symm
                calc a.step * (b.step / g) = (g * (a.step / g)) * (b.step / g) := by rw [← e1]
                  _ = g * ((a.step / g) * (b.step / g)) := by ring
                  _ = (g * (b.step / g)) * (a.step / g) := by ring
                  _ = b.step * (a.step / g) := by rw [e2]
              rw [hper']
              refine ⟨((a.step / g : Nat) : Int), ?_⟩
              simp [Int.natCast_mul]
            unfold intersection
            simp only [if_neg (by simp [han]), if_neg (by simp [hbn]),
              if_neg (by simp [has]), if_neg (by simp [hbs]), if_neg (by simp [hgapmod])]
            change contains ⟨first, period,
              if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ v = true ↔ _
            rw [span_contains_iff first period high v hp]
            constructor
            · rintro ⟨hfirstx, hxhigh, hxfirst⟩
              have hxroot : (period : Int) ∣ v - root := by
                have heq : v - root = (v - first) + (first - root) := by omega
                rw [heq]
                exact Int.dvd_add hxfirst hfroot
              have hxalo : (a.step : Int) ∣ v - a.lo :=
                Int.dvd_trans hpa (Int.dvd_add hxroot hroot.1)
              have hxblo : (b.step : Int) ∣ v - b.lo :=
                Int.dvd_trans hpb (Int.dvd_add hxroot hroot.2)
              have hxlow : low ≤ v := by omega
              constructor
              · exact mem_of_bounds_dvd a v hanp hasp (by omega) (by omega) hxalo
              · exact mem_of_bounds_dvd b v hbnp hbsp (by omega) (by omega) hxblo
            · rintro ⟨ha, hb⟩
              have hab := contains_bounds a v ha
              have hbb := contains_bounds b v hb
              have hxblo := contains_dvd b v hb
              have hxroot : (period : Int) ∣ v - root :=
                Int.dvd_trans hpb (Int.dvd_sub hxblo hroot.2)
              have hxfirst : (period : Int) ∣ v - first := by
                have heq : v - first = (v - root) - (first - root) := by omega
                rw [heq]
                exact Int.dvd_sub hxroot hfroot
              exact ⟨by omega, by omega, hxfirst⟩
          · have hgapne : gap % (g : Int) ≠ 0 := by simpa using hgapmod
            unfold intersection
            simp only [if_neg (by simp [han]), if_neg (by simp [hbn]),
              if_neg (by simp [has]), if_neg (by simp [hbs]), if_pos hgapmod]
            simp only [contains]
            constructor
            · intro h
              simp at h
            · rintro ⟨ha, hb⟩
              have hg := common_gap_dvd a b v ha hb
              exact hgapne (Int.emod_eq_zero_of_dvd hg)

end TinylambOpt.ResearchProofs.DevinBatch.EP_SegmentedSlope_MirroredTail
