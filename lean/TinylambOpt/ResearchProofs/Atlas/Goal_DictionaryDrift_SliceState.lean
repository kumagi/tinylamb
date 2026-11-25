import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_DictionaryDrift_SliceState

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.DictionaryDrift

/-! ## Generic list and arithmetic helpers -/

private theorem list_sum_perm {l₁ l₂ : List Int} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem range_succ_map_sum (f : Nat → Int) (n : Nat) :
    ((List.range (n+1)).map f).sum = ((List.range n).map f).sum + f n := by
  rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
  simp

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

private theorem countP_eq_sum (l : List (Nat × Int)) (f : Int → Bool) :
    l.countP (fun x => f x.2) = ((l.map (fun x => if f x.2 then 1 else 0)).sum : Nat) := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : f x.2 <;> simp [h, ih] <;> omega

private theorem sum_map_zero_nat (l : List Nat) : (l.map (fun _ => (0 : Nat))).sum = 0 := by
  induction l <;> simp [*]

private theorem sum_range_const_nat (m v : Nat) :
    ((List.range m).map (fun _ => v)).sum = m * v := by
  induction m with
  | zero => simp
  | succ m ih =>
      rw [show m + 1 = Nat.succ m by omega, List.range_succ, List.map_append]
      rw [List.sum_append]
      simp [ih, Nat.succ_mul]

private theorem sum_range_ite_nat (m : Nat) (b : Bool) (v : Nat) :
    ((List.range m).map (fun _ => if b then v else 0)).sum = if b then m * v else 0 := by
  by_cases h : b <;> simp [h, sum_range_const_nat, sum_map_zero_nat]

private theorem sum_update_nat : ∀ (p r0 : Nat), r0 < p → ∀ (s : Nat → Nat) (v : Nat),
    ((List.range p).map (fun r => if r == r0 then v + s r else s r)).sum =
      v + ((List.range p).map s).sum := by
  intro p
  induction p with
  | zero => intro r0 h; omega
  | succ p ih =>
      intro r0 hr0 s v
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

private theorem sum_eq_single (p r : Nat) (hr : r < p) (f : Nat → Nat) :
    ((List.range p).map (fun x => if x == r then f x else 0)).sum = f r := by
  have hconst : ∀ x, (if x == r then f x else 0) = if x == r then f r else 0 := by
    intro x
    by_cases h : x == r
    · have h' : x = r := Nat.eq_of_beq_eq_true h
      simp [h, h']
    · simp [h]
  rw [show (List.range p).map (fun x => if x == r then f x else 0) =
      (List.range p).map (fun x => if x == r then f r else 0) from
      List.map_congr_left (fun x hx => hconst x)]
  rw [show (List.range p).map (fun x => if x == r then f r else 0) =
      (List.range p).map (fun x => if x == r then f r + 0 else 0) by
        apply List.map_congr_left
        intro x hx
        by_cases h : x == r <;> simp [h]]
  rw [sum_update_nat p r hr (fun _ : Nat => 0) (f r), sum_map_zero_nat]
  simp

private theorem range_succ_map' (f : Nat → Nat) (n : Nat) :
    (List.range (n+1)).map f = f 0 :: (List.range n).map (fun i => f (i+1)) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n + 1 + 1 = Nat.succ (n+1) by omega, List.range_succ, List.map_append, ih]
      simp [List.range_succ]

private theorem sum_range_get_count : ∀ (l : List Int) (v : Int),
    ((List.range l.length).map (fun r => if l[r]?.getD 0 == v then 1 else 0)).sum = l.count v := by
  intro l
  induction l with
  | nil => intro v; simp
  | cons a as ih =>
      intro v
      rw [List.count_cons]
      simp only [List.length_cons]
      rw [range_succ_map']
      simp only [List.map_cons, List.sum_cons, List.getElem?_cons_zero, Option.getD_some,
        List.getElem?_cons_succ]
      rw [ih]
      omega

private theorem count_take_get : ∀ (k : Nat) (l : List Int) (v : Int),
    (l.take k).count v =
      ((List.range k).map (fun r => if r < l.length && l[r]?.getD 0 == v then 1 else 0)).sum := by
  intro k
  induction k with
  | zero => intro l v; simp
  | succ k ih =>
      intro l v
      cases l with
      | nil => simp [sum_map_zero_nat]
      | cons a as =>
          simp only [List.length_cons]
          rw [show k + 1 = Nat.succ k by omega, List.take_succ_cons]
          rw [List.count_cons]
          rw [ih as v]
          rw [range_succ_map']
          simp only [List.sum_cons, List.getElem?_cons_zero, Option.getD_some,
            List.getElem?_cons_succ]
          have hsimp : ∀ i : Nat,
              (if i + 1 < as.length + 1 && as[i]?.getD 0 == v then 1 else 0) =
              if i < as.length && as[i]?.getD 0 == v then 1 else 0 := by
            intro i
            by_cases h : i < as.length && as[i]?.getD 0 == v <;> simp [h] <;> omega
          rw [show (List.range k).map (fun i : Nat =>
              if i + 1 < as.length + 1 && as[i]?.getD 0 == v then 1 else 0) =
              (List.range k).map (fun i : Nat =>
              if i < as.length && as[i]?.getD 0 == v then 1 else 0) from
              List.map_congr_left (fun i hi => hsimp i)]
          simp
          omega

private theorem sum_range_trunc (f : Nat → Nat) (k p : Nat) (hk : k ≤ p) :
    ((List.range p).map (fun r => if r < k then f r else 0)).sum = ((List.range k).map f).sum := by
  obtain ⟨d, rfl⟩ := Nat.exists_eq_add_of_le hk
  rw [List.range_add, List.map_append, List.sum_append]
  have hzero : ((List.map (fun x => k + x) (List.range d)).map
      (fun r => if r < k then f r else 0)).sum = 0 := by
    rw [show (List.map (fun x => k + x) (List.range d)).map (fun r => if r < k then f r else 0) =
        (List.map (fun x => k + x) (List.range d)).map (fun _ => 0) by
      apply List.map_congr_left
      intro r hr
      simp only [List.mem_map, List.mem_range] at hr
      rcases hr with ⟨x, hx, rfl⟩
      by_cases hlt : k + x < k
      · omega
      · simp [hlt]]
    apply sum_map_zero_nat
  rw [hzero]
  simp only [Nat.add_zero]
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  have hrlt : r < k := List.mem_range.mp hr
  simp [hrlt]

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
      simp only [List.map_append, List.sum_append, List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
      rw [Nat.mul_add]
      simp

private theorem choose2_succ (k : Nat) :
    TinylambOpt.ResearchGoals.PairAggregates.choose2 (k + 1) =
      TinylambOpt.ResearchGoals.PairAggregates.choose2 k + k := by
  unfold TinylambOpt.ResearchGoals.PairAggregates.choose2
  by_cases hk : k = 0
  · simp [hk]
  · have hkm1 : k + 1 - 1 = k := by omega
    rw [hkm1]
    have hmul1 : (k + 1) * k = k * k + k := by rw [Nat.add_mul, Nat.one_mul]
    have hmul2 : k * (k - 1) = k * k - k := by rw [Nat.mul_sub_left_distrib, Nat.mul_one]
    have hle : k ≤ k * k := by
      have := Nat.mul_le_mul_left k (show 1 ≤ k by omega)
      simpa using this
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
      rw [partialSum_succ, ih]
      rw [List.range_succ, List.map_append, List.sum_append]
      simp

/-! ## Lane slices -/

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
  have htle : tlo ≤ thi := by dsimp [tlo, thi]; apply Nat.div_le_div_right; omega
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
  have htle : tlo ≤ thi := by dsimp [tlo, thi]; apply Nat.div_le_div_right; omega
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
      · rw [ih1]; simp [hlen]
      · rw [ih2]; simp [hsum]

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
  have hslic := slice_flatMap (lay.lanes c) hs lo hi hle
  apply Prod.ext
  · rw [hf.length_eq]
    simpa [p] using congrArg Prod.fst hslic
  · rw [list_sum_perm (hf.map Prod.snd)]
    simpa [p] using congrArg Prod.snd hslic

/-! ## Modular residue partitions and striped lanes -/

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
    · have heq : n % p + 1 = p := by have hs' := Nat.mod_lt n hp; omega
      have hlt : r < n % p := by omega
      have hltnew : ¬ r < 0 := by omega
      simp [hn, h, hlt, hltnew]

private theorem residue_count_eq_zero_of_ge (N r p : Nat) (hr : N ≤ r) (hp : 0 < p) :
    (N - r + p - 1) / p = 0 := by
  have hsub : N - r = 0 := by omega
  rw [hsub, Nat.zero_add]
  exact Nat.div_eq_of_lt (Nat.sub_lt hp (by omega : 0 < 1))

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
          have hrp : r < p := by have := List.mem_range.mp hr; omega
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
          rw [hcnt]; simp
        simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
        rw [hlast]; omega
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
            rw [hval]; simp [hnr]
          · simp [hnr]
        rw [hsum, sum_map_add_int]
        have hif : ((List.range p).map (fun r => if n % p == r then f n else 0)).sum = f n := by
          rw [sum_ite_filter, range_filter_mod_singleton n p hp]
          simp
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
        simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
        rw [hz]; simp
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
      apply Nat.div_le_div_right; omega
    rw [Nat.min_eq_left hle]
  · have ht : t < c.n := by omega
    have hmin : min c.n t = t := Nat.min_eq_right (by omega)
    rw [hmin]
    have hle : (t - r + p - 1) / p ≤ (c.n - r + p - 1) / p := by
      apply Nat.div_le_div_right; omega
    rw [Nat.min_eq_right hle]

private theorem residue_lt_of_count (N p r q : Nat) (hp : 0 < p) (hr : r < p)
    (hq : q < (N - r + p - 1) / p) : r + q * p < N := by
  by_cases hle : r ≤ N
  · have hmul : q * p < N - r + p - 1 - (p - 1) := (Nat.lt_div_iff_mul_lt hp).mp hq
    have hdiff : N - r + p - 1 - (p - 1) = N - r := by omega
    rw [hdiff] at hmul
    omega
  · have hz : (N - r + p - 1) / p = 0 := residue_count_eq_zero_of_ge N r p (by omega) hp
    omega

private theorem lane_value_of_striped (p : Nat) (hp : 0 < p) (r q : Nat) (hr : r < p)
    (f : Nat → Int) (make : Nat → Int × Int)
    (hval : ∀ i, f i = (make (i % p)).1 + (make (i % p)).2 * ((i / p : Nat) : Int)) :
    f (r + q*p) = (make r).1 + (make r).2 * (q : Int) := by
  have hmod : (r + q*p) % p = r := by rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hr]
  have hdiv : (r + q*p) / p = q := by rw [Nat.add_mul_div_right r q hp, Nat.div_eq_of_lt hr]; simp
  rw [hval, hmod, hdiv]

private theorem filter_map_pair_range (n lo hi : Nat) (f : Nat → Int) :
    ((List.range n).map (fun i => (i, f i))).filter (fun r => lo ≤ r.1 && r.1 < hi) =
      ((List.range n).filter (fun i => lo ≤ i && i < hi)).map (fun i => (i, f i)) := by
  simpa using (map_pair_filter (List.range n) (fun i : Nat => i) f (fun x : Nat => lo ≤ x && x < hi))

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
          have hrp : r < p := by have := List.mem_range.mp hr; omega
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
          have hrp : r < p := by have := List.mem_range.mp hr; omega
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

theorem sound_DictionaryDrift_SliceState : SliceState.Goal := by
  intro c lo hi hle
  dsimp
  by_cases hne : c.dict = []
  · have hempty : rows layout c = [] := by
      unfold rows
      apply List.filterMap_eq_nil_iff.mpr
      intro i hi
      simp [layout, hne]
    have hlanes0 : layout.lanes c = [] := by simp [layout, hne]
    rw [hempty, hlanes0]
    simp
  · let p := c.dict.length
    have hp : 0 < p := List.length_pos_iff.mpr hne
    let make : Nat → Int × Int := fun r => (c.dict[r]?.getD 0, c.step)
    let f : Nat → Int := fun i => c.dict[i%p]?.getD 0 + c.step * ((i/p:Nat):Int)
    have hval : ∀ i, f i = (make (i%p)).1 + (make (i%p)).2*((i/p:Nat):Int) := by
      intro i
      simp [f, make]
    have hrows : rows layout c = (List.range c.n).map (fun i => (i, f i)) := by
      unfold rows
      apply range_filterMap_all
      intro i hi
      have hlt : i % c.dict.length < c.dict.length := Nat.mod_lt i hp
      have hget : c.dict[i % c.dict.length]? = some (c.dict[i % c.dict.length]) := by
        rw [List.getElem?_eq_some_iff]
        exact ⟨hlt, rfl⟩
      simp [layout, f, p, hget]
    have hlanes : layout.lanes c = striped c p make := by
      rfl
    rw [hrows, hlanes]
    exact stripedSlice c p hp make f hval lo hi hle

end TinylambOpt.ResearchProofs.Atlas.Goal_DictionaryDrift_SliceState
