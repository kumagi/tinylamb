import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E1

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

private theorem countP_eq_sum {α : Type} (l : List α) (p : α → Bool) :
    l.countP p = (l.map (fun x => if p x then 1 else 0)).sum := by
  induction l with
  | nil => rfl
  | cons x xs ih => by_cases h : p x <;> simp [h, ih] <;> omega

private theorem sum_map_ite_zero {α : Type} (l : List α) (p : α → Bool) (f : α → Int) :
    ((l.filter p).map f).sum = (l.map (fun x => if p x then f x else 0)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

private theorem sum_map_add_int {α : Type} (l : List α) (f g : α → Int) :
    ((l.map fun x => f x + g x).sum) = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem sum_map_sub_int {α : Type} (l : List α) (f g : α → Int) :
    ((l.map fun x => f x - g x).sum) = (l.map f).sum - (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem sum_map_mul_int (l : List Int) (c : Int) :
    (l.map (fun x => c * x)).sum = c * l.sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Int.mul_add]

private theorem sum_map_mul_int_nat (l : List Nat) (c : Int) :
    (l.map (fun x : Nat => c * (x : Int))).sum = c * (l.map (fun x : Nat => (x : Int))).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Int.natCast_add, Int.mul_add]

private theorem sum_map_zero_int (l : List Nat) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih => simpa using ih

private theorem sum_map_const_int {α : Type} (l : List α) (v : Int) :
    (l.map (fun _ => v)).sum = (l.length : Int) * v := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Int.natCast_add, Int.add_mul]; omega

private theorem flatMap_filter_length {α β : Type} (ls : List α) (f : α → List β) (p : β → Bool) :
    ((ls.flatMap f).filter p).length = (ls.map (fun a => ((f a).filter p).length)).sum := by
  induction ls with
  | nil => simp
  | cons a as ih => simp [List.filter_append, ih]

private theorem flatMap_map_filter_sum {α : Type} (ls : List α) (f : α → List (Nat × Int))
    (p : Nat × Int → Bool) :
    (((ls.flatMap f).filter p).map Prod.snd).sum =
      (ls.map (fun a => (((f a).filter p).map Prod.snd).sum)).sum := by
  induction ls with
  | nil => simp
  | cons a as ih => simp [List.filter_append, List.map_append, List.sum_append, ih]

private theorem flatMap_map_sum {α β : Type} (ls : List α) (f : α → List β) (g : β → Int) :
    ((ls.flatMap f).map g).sum = (ls.map (fun a => ((f a).map g).sum)).sum := by
  induction ls with
  | nil => simp
  | cons a as ih => simp [List.map_append, List.sum_append, ih]

private theorem map_pair_filter {α β : Type} (xs : List α) (f : α → Nat) (g : α → β)
    (p : Nat → Bool) :
    (xs.map (fun x => (f x, g x))).filter (fun r => p r.1) =
      (xs.filter (fun x => p (f x))).map (fun x => (f x, g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih => by_cases h : p (f x) <;> simp [h, ih]

private theorem flatMap_map_sum_nat {α β : Type} (ls : List α) (f : α → List β) (g : β → Nat) :
    ((ls.flatMap f).map g).sum = (ls.map (fun a => ((f a).map g).sum)).sum := by
  induction ls with
  | nil => simp
  | cons a as ih => simp [List.map_append, List.sum_append, ih]

private def lanePoints (a : Lane) : List (Nat × Int) :=
  (List.range a.n).map (fun i : Nat => (a.start + i * a.stride, a.base + (i : Int) * a.step))

/-! ## Lane arithmetic -/

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
      rw [partialSum_succ, ih, List.range_succ, List.map_append, List.sum_append]
      simp

private theorem sum_squares_eq (k : Nat) :
    ((List.range k).map (fun i : Nat => (i : Int) * (i : Int))).sum = (squares k : Int) := by
  induction k with
  | zero => simp [squares]
  | succ k ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih, squares_succ]
      simp [Int.natCast_add, Int.natCast_mul]

/-! ## Rows as lane points for the three models -/

private theorem rows_lanes_LO (c : Config) :
    rows LinearOrdinal.layout c = ((LinearOrdinal.layout.lanes c).flatMap lanePoints) := by
  rw [rows_range_map LinearOrdinal.layout c (fun i : Nat => c.base + c.step * (i : Int)) (by
    intro i _
    rfl)]
  simp only [LinearOrdinal.layout, List.flatMap_cons, List.flatMap_nil, List.append_nil]
  change (List.range c.n).map (fun i : Nat => (i, c.base + c.step * (i : Int))) =
    lanePoints (lane c.n 0 1 c.base c.step)
  unfold lanePoints lane
  simp
  intro i _
  simp [Int.mul_comm]

private theorem rows_map_SC (c : Config) :
    rows SaturatingCounter.layout c = (List.range c.n).map (fun i =>
      (i, if i < c.split then c.base + c.step * (i : Int)
        else c.base + c.step * (c.split : Int))) := by
  apply rows_range_map
  intro i _
  by_cases h : i < c.split <;> simp [SaturatingCounter.layout, h]

private theorem rows_lanes_SC (c : Config) :
    rows SaturatingCounter.layout c = ((SaturatingCounter.layout.lanes c).flatMap lanePoints) := by
  rw [rows_map_SC]
  simp only [SaturatingCounter.layout, List.flatMap_cons, List.flatMap_nil, List.append_nil]
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
    have his : i < c.split := by omega
    simp [his, Int.mul_comm]

private theorem rows_map_SS (c : Config) :
    rows SegmentedSlope.layout c = (List.range c.n).map (fun i =>
      (i, if i < c.split then c.base + c.step * (i : Int)
        else c.base + c.step * (c.split : Int) + c.bias * ((i - c.split : Nat) : Int))) := by
  apply rows_range_map
  intro i _
  by_cases h : i < c.split <;> simp [SegmentedSlope.layout, h]

private theorem rows_lanes_SS (c : Config) :
    rows SegmentedSlope.layout c = ((SegmentedSlope.layout.lanes c).flatMap lanePoints) := by
  rw [rows_map_SS]
  simp only [SegmentedSlope.layout, List.flatMap_cons, List.flatMap_nil, List.append_nil]
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
    have his : i < c.split := by omega
    simp [his, Int.mul_comm]

/-! ## Progressions, Bezout, and intersection -/

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
      simp [hs'] at hhi hlo ⊢
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

private theorem lcm_eq_div_mul (as bs : Nat) (ha : 0 < as) :
    as.lcm bs = (as / as.gcd bs) * bs := by
  have hg : 0 < as.gcd bs := Nat.gcd_pos_of_pos_left bs ha
  have hdiv : as / as.gcd bs * as.gcd bs = as :=
    Nat.div_mul_cancel (Nat.gcd_dvd_left as bs)
  have hprod : as * bs = as.gcd bs * ((as / as.gcd bs) * bs) := by
    calc
      as * bs = (as / as.gcd bs * as.gcd bs) * bs := by rw [hdiv]
      _ = as.gcd bs * ((as / as.gcd bs) * bs) := by ac_rfl
  exact Nat.eq_of_mul_eq_mul_left hg (by
    rw [Nat.gcd_mul_lcm]
    exact hprod)

private theorem dvd_of_dvd_pair (as bs : Nat) (ha : 0 < as) (hb : 0 < bs) (d : Int)
    (hda : (as : Int) ∣ d) (hdb : (bs : Int) ∣ d) :
    (((as / as.gcd bs) * bs : Nat) : Int) ∣ d := by
  have hna : as ∣ d.natAbs := by
    rcases hda with ⟨k, hk⟩
    refine ⟨k.natAbs, ?_⟩
    rw [hk, Int.natAbs_mul, Int.natAbs_natCast]
  have hnb : bs ∣ d.natAbs := by
    rcases hdb with ⟨k, hk⟩
    refine ⟨k.natAbs, ?_⟩
    rw [hk, Int.natAbs_mul, Int.natAbs_natCast]
  have hl : as.lcm bs ∣ d.natAbs := Nat.lcm_dvd hna hnb
  rw [lcm_eq_div_mul as bs ha] at hl
  have hlI : (((as / as.gcd bs) * bs : Nat) : Int) ∣ (d.natAbs : Int) :=
    Int.ofNat_dvd.mpr hl
  rcases Int.natAbs_eq d with hd | hd
  · rcases hlI with ⟨k, hk⟩
    refine ⟨k, ?_⟩
    rw [hd]
    exact hk
  · rcases hlI with ⟨k, hk⟩
    refine ⟨-k, ?_⟩
    calc
      d = -(d.natAbs : Int) := hd
      _ = -(((((as / as.gcd bs) * bs : Nat) : Int)) * k) := by rw [hk]
      _ = (((as / as.gcd bs) * bs : Nat) : Int) * (-k) := by simp only [Int.mul_neg]

private theorem contains_const_iff (lo : Int) (n : Nat) (v : Int) :
    contains ⟨lo, 0, n⟩ v = true ↔ 0 < n ∧ v = lo := by
  unfold contains lastVal
  simp
  omega

private theorem contains_step_one_ite (lo : Int) (c : Bool) (v : Int) :
    contains ⟨lo, 1, if c then 1 else 0⟩ v = true ↔ c = true ∧ v = lo := by
  cases c <;> simp [contains, lastVal] <;> omega

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
  refine ⟨(k : Int), ?_⟩
  grind

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
              have hsuma : (v - root) + (root - a.lo) = v - a.lo := by omega
              have hsumb : (v - root) + (root - b.lo) = v - b.lo := by omega
              have haroot_step : (a.step : Int) ∣ v - root := Int.dvd_trans hpa hxroot
              have hbroot_step : (b.step : Int) ∣ v - root := Int.dvd_trans hpb hxroot
              have hxalo : (a.step : Int) ∣ v - a.lo := by
                rw [← hsuma]
                exact Int.dvd_add haroot_step hroot.1
              have hxblo : (b.step : Int) ∣ v - b.lo := by
                rw [← hsumb]
                exact Int.dvd_add hbroot_step hroot.2
              constructor
              · exact mem_of_bounds_dvd a v hanp hasp (by omega) (by omega) hxalo
              · exact mem_of_bounds_dvd b v hbnp hbsp (by omega) (by omega) hxblo
            · rintro ⟨ha, hb⟩
              have hab := contains_bounds a v ha
              have hbb := contains_bounds b v hb
              have hxalo := contains_dvd a v ha
              have hxblo := contains_dvd b v hb
              have hdiffa : (v - a.lo) - (root - a.lo) = v - root := by omega
              have hdiffb : (v - b.lo) - (root - b.lo) = v - root := by omega
              have haroot : (a.step : Int) ∣ v - root := by
                rw [← hdiffa]
                exact Int.dvd_sub hxalo hroot.1
              have hbroot : (b.step : Int) ∣ v - root := by
                rw [← hdiffb]
                exact Int.dvd_sub hxblo hroot.2
              have hxroot : (period : Int) ∣ v - root := by
                simpa [period] using dvd_of_dvd_pair a.step b.step hasp hbsp (v - root) haroot hbroot
              have hxfirst : (period : Int) ∣ v - first := by
                have heq : v - first = (v - root) - (first - root) := by omega
                rw [heq]
                exact Int.dvd_sub hxroot hfroot
              have hlowv : low ≤ v := by
                dsimp [low]
                by_cases h : a.lo ≤ b.lo
                · rw [Int.max_eq_right h]
                  exact hbb.1
                · rw [Int.max_eq_left (by omega)]
                  exact hab.1
              have hvhigh : v ≤ high := by
                dsimp [high]
                by_cases h : lastVal a ≤ lastVal b
                · rw [Int.min_eq_left h]
                  exact hab.2
                · rw [Int.min_eq_right (by omega)]
                  exact hbb.2
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

/-! ## Lane values and counts -/

private theorem laneVal_mem_values (a : Lane) (i : Nat) (hi : i < a.n) :
    contains (values a) (a.base + (i : Int) * a.step) = true := by
  apply (contains_iff_mem (values a) _).mpr
  by_cases hs : a.step < 0
  · have hnatabs : (a.step.natAbs : Int) = -a.step := by
      have h := Int.natAbs_of_nonneg (show 0 ≤ -a.step by omega)
      have h2 : (-a.step).natAbs = a.step.natAbs := Int.natAbs_neg a.step
      simpa [h2] using h
    refine ⟨a.n - 1 - i, ?_, ?_⟩
    · simp [values]
      omega
    · unfold progVal
      simp [values, hs, hnatabs]
      have hcast : ((a.n - 1 - i : Nat) : Int) = ((a.n - 1 : Nat) : Int) - (i : Int) := by omega
      rw [hcast]
      grind
  · have hnonneg : 0 ≤ a.step := by omega
    have hnatabs : (a.step.natAbs : Int) = a.step := Int.natAbs_of_nonneg hnonneg
    refine ⟨i, ?_, ?_⟩
    · simp [values]
      omega
    · unfold progVal
      simp [values, hs, hnatabs]

private theorem laneVal_inj (a : Lane) (ha : a.step ≠ 0) (i j : Nat)
    (hi : i < a.n) (hj : j < a.n) (h : a.base + (i : Int) * a.step = a.base + (j : Int) * a.step) :
    i = j := by
  have hm0 : (i : Int) * a.step = (j : Int) * a.step := by omega
  have hm : a.step * (i : Int) = a.step * (j : Int) := by simpa [Int.mul_comm] using hm0
  have hij : (i : Int) = (j : Int) := (Int.mul_eq_mul_left_iff (by simpa using ha)).mp hm
  exact_mod_cast hij

private theorem values_cover (a : Lane) {v : Int} (h : contains (values a) v = true) :
    ∃ i, i < a.n ∧ v = a.base + (i : Int) * a.step := by
  obtain ⟨k, hk, hv⟩ := (contains_iff_mem (values a) v).mp h
  unfold progVal at hv
  by_cases hs : a.step < 0
  · have hnatabs : (a.step.natAbs : Int) = -a.step := by
      have hh := Int.natAbs_of_nonneg (show 0 ≤ -a.step by omega)
      have h2 : (-a.step).natAbs = a.step.natAbs := Int.natAbs_neg a.step
      simpa [h2] using hh
    simp [values, hs] at hk hv
    rw [hnatabs] at hv
    have hkle : k ≤ a.n - 1 := by omega
    refine ⟨a.n - 1 - k, ?_, ?_⟩
    · omega
    · have hcast : ((a.n - 1 - k : Nat) : Int) = ((a.n - 1 : Nat) : Int) - (k : Int) := by omega
      rw [hcast]
      grind
  · have hnonneg : 0 ≤ a.step := by omega
    have hnatabs : (a.step.natAbs : Int) = a.step := Int.natAbs_of_nonneg hnonneg
    simp [values, hs, hnatabs] at hk hv
    refine ⟨k, hk, ?_⟩
    omega

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

private theorem intersection_step_pos (a b : Progression) : 0 < (intersection a b).step := by
  by_cases han : a.n = 0
  · simp [intersection, han]
  · by_cases hbn : b.n = 0
    · simp [intersection, han, hbn]
    · by_cases has : a.step = 0
      · simp [intersection, han, hbn, has]
      · by_cases hbs : b.step = 0
        · simp [intersection, han, hbn, has, hbs]
        · have hg : 0 < a.step.gcd b.step := Nat.gcd_pos_of_pos_left b.step (Nat.pos_of_ne_zero has)
          have hdiv : 0 < a.step / a.step.gcd b.step :=
            Nat.div_pos (Nat.le_of_dvd (Nat.pos_of_ne_zero has) (Nat.gcd_dvd_left a.step b.step)) hg
          have hp : 0 < (a.step / a.step.gcd b.step) * b.step := Nat.mul_pos hdiv (Nat.pos_of_ne_zero hbs)
          rw [intersection_main_form a b han hbn has hbs]
          by_cases hgap : (b.lo - a.lo) % (a.step.gcd b.step : Int) = 0 <;> simp [hgap, hp]

private theorem count_values_mem (a : Lane) (q : Progression) :
    (List.range a.n).countP (fun i : Nat => contains q (a.base + (i : Int) * a.step)) =
      (intersection (values a) q).n * (if a.step = 0 then a.n else 1) := by
  by_cases ha0 : a.step = 0
  · rw [if_pos ha0]
    by_cases han : a.n = 0
    · simp [han, contains]
    · by_cases hqn : q.n = 0
      · have hleft : (List.range a.n).countP
            (fun i : Nat => contains q (a.base + (i : Int) * a.step)) = 0 := by
          apply List.countP_eq_zero.mpr
          intro i hi
          simp [contains, hqn]
        rw [hleft]
        simp [intersection, hqn, contains]
      · rw [intersection_step0_left (values a) q
            (by simpa [values, ha0] using han) hqn (by simp [values, ha0])]
        have hvalo : (values a).lo = a.base := by simp [values, ha0]
        rw [hvalo]
        by_cases hc : contains q a.base
        · have hleft : (List.range a.n).countP
              (fun i : Nat => contains q (a.base + (i : Int) * a.step)) = a.n := by
            rw [show (List.range a.n).countP (fun i : Nat => contains q (a.base + (i : Int) * a.step)) =
                (List.range a.n).countP (fun _ : Nat => true) by
              apply List.countP_congr
              intro i hi
              simp [ha0, hc]]
            simp
          rw [hleft]
          simp [contains_step_one_ite, hc, Nat.mul_comm]
        · have hleft : (List.range a.n).countP
              (fun i : Nat => contains q (a.base + (i : Int) * a.step)) = 0 := by
            apply List.countP_eq_zero.mpr
            intro i hi
            simp [ha0, hc]
          rw [hleft]
          simp [contains_step_one_ite, hc]
  · rw [if_neg ha0, Nat.mul_one]
    let R := intersection (values a) q
    let L : List Int := ((List.range a.n).filter
      (fun i : Nat => contains q (a.base + (i : Int) * a.step))).map
        (fun i : Nat => a.base + (i : Int) * a.step)
    let L2 : List Int := (List.range R.n).map (progVal R)
    have hnodL : L.Nodup := by
      dsimp [L]
      apply nodup_map_of_injective
      · intro i hi j hj h
        exact laneVal_inj a ha0 i j
          (List.mem_range.mp (List.mem_filter.mp hi).1)
          (List.mem_range.mp (List.mem_filter.mp hj).1) h
      · exact List.Sublist.nodup (List.filter_sublist (p := fun i : Nat =>
          contains q (a.base + (i : Int) * a.step)) (l := List.range a.n)) List.nodup_range
    have hnodL2 : L2.Nodup := by
      dsimp [L2]
      apply nodup_map_of_injective
      · intro i hi j hj h
        have hm0 : (i : Int) * (R.step : Int) = (j : Int) * (R.step : Int) := by
          unfold progVal at h
          omega
        have hm : R.step * (i : Int) = R.step * (j : Int) := by simpa [Int.mul_comm] using hm0
        have hRnz : (R.step : Int) ≠ 0 := by
          exact_mod_cast (Nat.ne_of_gt (intersection_step_pos (values a) q))
        have hij : (i : Int) = (j : Int) := (Int.mul_eq_mul_left_iff hRnz).mp hm
        exact_mod_cast hij
      · exact List.nodup_range
    have hmem : ∀ x : Int, x ∈ L ↔ x ∈ L2 := by
      intro x
      constructor
      · intro hx
        rcases List.mem_map.mp hx with ⟨i, hi, rfl⟩
        have hxi : i < a.n := List.mem_range.mp (List.mem_filter.mp hi).1
        have hxv : contains q (a.base + (i : Int) * a.step) = true := (List.mem_filter.mp hi).2
        have hxa : contains (values a) (a.base + (i : Int) * a.step) = true :=
          laneVal_mem_values a i hxi
        have hxR : contains R (a.base + (i : Int) * a.step) = true :=
          (intersection_contains (values a) q _).mpr ⟨hxa, hxv⟩
        obtain ⟨k, hk, hv⟩ := (contains_iff_mem R _).mp hxR
        apply List.mem_map.mpr
        refine ⟨k, List.mem_range.mpr hk, ?_⟩
        exact hv.symm
      · intro hx
        rcases List.mem_map.mp hx with ⟨k, hk, hv⟩
        have hR : contains R (progVal R k) = true := by
          apply (contains_iff_mem R _).mpr
          exact ⟨k, List.mem_range.mp hk, rfl⟩
        have hboth := (intersection_contains (values a) q _).mp hR
        obtain ⟨i, hi, hival⟩ := values_cover a hboth.1
        apply List.mem_map.mpr
        refine ⟨i, ?_, ?_⟩
        · apply List.mem_filter.mpr
          refine ⟨List.mem_range.mpr hi, ?_⟩
          rw [hival] at hboth
          exact hboth.2
        · rw [← hival]
          exact hv
    have hperm : L.Perm L2 := (List.perm_ext_iff_of_nodup hnodL hnodL2).mpr hmem
    rw [List.countP_eq_length_filter]
    dsimp [L] at hperm
    dsimp [L2] at hperm
    have hlen := hperm.length_eq
    rw [List.length_map, List.length_map, List.length_range] at hlen
    exact hlen

/-! ## ResidueJoin -/

private theorem countP_range_beq (n j0 : Nat) (hj : j0 < n) :
    (List.range n).countP (fun j => j == j0) = 1 := by
  rw [List.countP_eq_length_filter]
  have ht : (List.range n).take (j0 + 1) = List.range (j0 + 1) := by
    induction n with
    | zero => omega
    | succ n ih =>
        by_cases h : j0 < n
        · rw [List.range_succ, List.take_append_of_le_length]
          · rw [ih h]
          · simp
            omega
        · have hn : n = j0 := by omega
          subst n
          simp
  have hsplit : List.range n = List.range (j0 + 1) ++ (List.range n).drop (j0 + 1) := by
    calc
      List.range n = (List.range n).take (j0 + 1) ++ (List.range n).drop (j0 + 1) :=
        (List.take_append_drop (j0 + 1) (List.range n)).symm
      _ = List.range (j0 + 1) ++ (List.range n).drop (j0 + 1) := by rw [ht]
  rw [hsplit, List.filter_append]
  have hfirst : (List.range (j0 + 1)).filter (fun j => j == j0) = [j0] := by
    rw [show j0 + 1 = Nat.succ j0 by rfl, List.range_succ, List.filter_append]
    have hnone : (List.range j0).filter (fun j => j == j0) = [] := by
      apply List.filter_eq_nil_iff.mpr
      intro j hjmem
      have hjlt : j < j0 := List.mem_range.mp hjmem
      intro h
      exact Nat.ne_of_lt hjlt (eq_of_beq h)
    rw [hnone]
    simp
  have hlast : ((List.range n).drop (j0 + 1)).filter (fun j => j == j0) = [] := by
    apply List.filter_eq_nil_iff.mpr
    intro j hjmem
    rcases (List.mem_drop_iff_getElem.mp hjmem) with ⟨k, hk, hget⟩
    have hget' : (List.range n)[k + (j0 + 1)]'hk = k + (j0 + 1) := by simp
    have hget'' : (List.range n)[j0 + 1 + k] = k + (j0 + 1) := by
      simpa [Nat.add_comm, Nat.add_left_comm, Nat.add_assoc] using hget'
    rw [hget''] at hget
    subst j
    simp [Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]
  rw [hfirst, hlast]
  simp

private theorem contains_values_of_step0 (b : Lane) (hb0 : b.step = 0) (v : Int) :
    contains (values b) v = true ↔ 0 < b.n ∧ v == b.base := by
  rw [contains_iff_mem]
  constructor
  · rintro ⟨k, hk, hv⟩
    unfold progVal at hv
    simp [values, hb0] at hk hv
    exact ⟨by omega, by simpa [Int.mul_comm] using hv⟩
  · rintro ⟨hn, hv⟩
    refine ⟨0, hn, ?_⟩
    unfold progVal
    have hvv : v = b.base := by simpa using (eq_of_beq hv)
    subst v
    simp [values, hb0]

private theorem count_lane_val (b : Lane) (v : Int) :
    (List.range b.n).countP (fun j : Nat => v == b.base + b.step * (j : Int)) =
      if contains (values b) v then (if b.step = 0 then b.n else 1) else 0 := by
  by_cases hb0 : b.step = 0
  · rw [if_pos hb0]
    by_cases hv : v == b.base
    · by_cases hn : 0 < b.n
      · have hc : contains (values b) v = true :=
          (contains_values_of_step0 b hb0 v).mpr ⟨hn, hv⟩
        simp [hb0, hv, hc]
      · have hzero : b.n = 0 := by omega
        have hc : contains (values b) v = false := by
          simp only [Bool.eq_false_iff]
          intro h
          exact hn ((contains_values_of_step0 b hb0 v).mp h).1
        simp [hb0, hv, hzero, hc]
    · have hc : contains (values b) v = false := by
        simp only [Bool.eq_false_iff]
        intro h
        exact hv ((contains_values_of_step0 b hb0 v).mp h).2
      simp [hb0, hv, hc]
  · rw [if_neg hb0]
    by_cases hv : contains (values b) v
    · obtain ⟨j0, hj0, hvj⟩ := values_cover b hv
      have hp : (List.range b.n).countP (fun j : Nat => v == b.base + b.step * (j : Int)) =
          (List.range b.n).countP (fun j : Nat => j == j0) := by
        apply List.countP_congr
        intro j hj
        constructor
        · intro he
          have hveq : v = b.base + (j : Int) * b.step := by
            have he' := of_decide_eq_true he
            simpa [Int.mul_comm] using he'
          have hji : b.base + (j : Int) * b.step = b.base + (j0 : Int) * b.step := by omega
          have hij : j = j0 := laneVal_inj b hb0 j j0 (List.mem_range.mp hj) hj0 hji
          simp [hij]
        · intro he
          have hij : j = j0 := by simpa using (eq_of_beq he)
          subst j
          rw [hvj]
          simp [Int.mul_comm]
      rw [hp, countP_range_beq b.n j0 hj0]
      simp [hv]
    · have hz : (List.range b.n).countP (fun j : Nat => v == b.base + b.step * (j : Int)) = 0 := by
        apply List.countP_eq_zero.mpr
        intro j hj he
        apply hv
        have hveq : v = b.base + (j : Int) * b.step := by
          have he' := of_decide_eq_true he
          simpa [Int.mul_comm] using he'
        rw [hveq]
        exact laneVal_mem_values b j (List.mem_range.mp hj)
      rw [hz]
      simp [hv]

private theorem sum_map_zero_nat (l : List Nat) : (l.map (fun _ => (0 : Nat))).sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih => simpa using ih

private theorem sum_ite_nat {α : Type} (l : List α) (p : α → Bool) (v : Nat) :
    (l.map (fun x => if p x then v else 0)).sum = v * l.countP p := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x <;> simp [h, ih, Nat.mul_add, Nat.mul_one] <;> omega

private theorem lane_join_count (a b : Lane) :
    ((List.range a.n).map (fun i : Nat =>
      (List.range b.n).countP (fun j : Nat =>
        (a.base + (i : Int) * a.step) == (b.base + (j : Int) * b.step)))).sum = joinCount a b := by
  by_cases hbn : b.n = 0
  · rw [show ((List.range a.n).map (fun i : Nat =>
        (List.range b.n).countP (fun j : Nat =>
          (a.base + (i : Int) * a.step) == (b.base + (j : Int) * b.step)))).sum =
        ((List.range a.n).map (fun _ : Nat => (0 : Nat))).sum by
      apply congrArg List.sum
      apply List.map_congr_left
      intro i hi
      simp [hbn]]
    rw [sum_map_zero_nat]
    unfold joinCount
    have hvzero : (values b).n = 0 := by simp [values, hbn]
    simp [intersection, hvzero, hbn]
  · by_cases hb0 : b.step = 0
    · have hcount : ∀ i : Nat,
          (List.range b.n).countP (fun j : Nat =>
            (a.base + (i : Int) * a.step) == (b.base + (j : Int) * b.step)) =
          if (a.base + (i : Int) * a.step) == b.base then b.n else 0 := by
        intro i
        by_cases h : (a.base + (i : Int) * a.step) == b.base
        · have h' := of_decide_eq_true h
          rw [show (List.range b.n).countP (fun j : Nat =>
              (a.base + (i : Int) * a.step) == (b.base + (j : Int) * b.step)) =
              (List.range b.n).countP (fun _ : Nat => true) by
            apply List.countP_congr
            intro j hj
            simp [hb0, h']]
          simp [h]
        · have h' : a.base + (i : Int) * a.step ≠ b.base := by
            intro he
            exact h (by simp [he])
          rw [show (List.range b.n).countP (fun j : Nat =>
              (a.base + (i : Int) * a.step) == (b.base + (j : Int) * b.step)) =
              (List.range b.n).countP (fun _ : Nat => false) by
            apply List.countP_congr
            intro j hj
            simp [hb0, h']]
          simp [h]
      rw [show ((List.range a.n).map (fun i : Nat =>
          (List.range b.n).countP (fun j : Nat =>
            (a.base + (i : Int) * a.step) == (b.base + (j : Int) * b.step)))).sum =
          ((List.range a.n).map (fun i : Nat =>
            if (a.base + (i : Int) * a.step) == b.base then b.n else 0)).sum by
        apply congrArg List.sum
        apply List.map_congr_left
        intro i hi
        exact hcount i]
      rw [show ((List.range a.n).map (fun i : Nat =>
          if (a.base + (i : Int) * a.step) == b.base then b.n else 0)).sum =
          b.n * (List.range a.n).countP (fun i : Nat =>
            (a.base + (i : Int) * a.step) == b.base) by
        exact sum_ite_nat (List.range a.n) (fun i : Nat =>
          (a.base + (i : Int) * a.step) == b.base) b.n]
      have hnpos : 0 < b.n := by omega
      have hcount2 : (List.range a.n).countP (fun i : Nat =>
            (a.base + (i : Int) * a.step) == b.base) =
          (List.range a.n).countP (fun i : Nat =>
            contains (values b) (a.base + (i : Int) * a.step)) := by
        apply List.countP_congr
        intro i hi
        rw [contains_values_of_step0 b hb0]
        simp [hnpos]
      rw [hcount2, count_values_mem]
      unfold joinCount
      simp [hb0, Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm]
    · have hcount : ∀ i : Nat,
          (List.range b.n).countP (fun j : Nat =>
            (a.base + (i : Int) * a.step) == (b.base + (j : Int) * b.step)) =
          if contains (values b) (a.base + (i : Int) * a.step) then 1 else 0 := by
        intro i
        have hc := count_lane_val b (a.base + (i : Int) * a.step)
        rw [if_neg hb0] at hc
        simpa [Int.mul_comm] using hc
      rw [show ((List.range a.n).map (fun i : Nat =>
          (List.range b.n).countP (fun j : Nat =>
            (a.base + (i : Int) * a.step) == (b.base + (j : Int) * b.step)))).sum =
          ((List.range a.n).map (fun i : Nat =>
            if contains (values b) (a.base + (i : Int) * a.step) then 1 else 0)).sum by
        apply congrArg List.sum
        apply List.map_congr_left
        intro i hi
        exact hcount i]
      rw [← countP_eq_sum]
      rw [count_values_mem]
      unfold joinCount
      simp [hb0]

private theorem residueJoin_of_rows_lanes (lay : Layout) (c : Config) (b : Lane)
    (hrows : rows lay c = (lay.lanes c).flatMap lanePoints) :
    ((rows lay c).map (fun r => (List.range b.n).countP
      (fun i : Nat => r.2 == b.base + b.step * (i : Int)))).sum =
      ((lay.lanes c).map (joinCount · b)).sum := by
  rw [hrows]
  calc
    (((lay.lanes c).flatMap lanePoints).map (fun r => (List.range b.n).countP
      (fun i : Nat => r.2 == b.base + b.step * (i : Int)))).sum =
      ((lay.lanes c).map (fun a => ((lanePoints a).map (fun r =>
        (List.range b.n).countP (fun i : Nat => r.2 == b.base + b.step * (i : Int)))).sum)).sum :=
      flatMap_map_sum_nat (lay.lanes c) lanePoints (fun r => (List.range b.n).countP
        (fun i : Nat => r.2 == b.base + b.step * (i : Int)))
    _ = ((lay.lanes c).map (joinCount · b)).sum := by
      apply congrArg List.sum
      apply List.map_congr_left
      intro a ha
      unfold lanePoints
      rw [List.map_map]
      simpa [Function.comp_def, Int.mul_comm] using lane_join_count a b

theorem sound_LinearOrdinal_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.LinearOrdinal.ResidueJoin.Goal := by
  intro c b
  exact residueJoin_of_rows_lanes LinearOrdinal.layout c b (rows_lanes_LO c)

theorem sound_SaturatingCounter_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.SaturatingCounter.ResidueJoin.Goal := by
  intro c b
  exact residueJoin_of_rows_lanes SaturatingCounter.layout c b (rows_lanes_SC c)

theorem sound_SegmentedSlope_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.SegmentedSlope.ResidueJoin.Goal := by
  intro c b
  exact residueJoin_of_rows_lanes SegmentedSlope.layout c b (rows_lanes_SS c)

/-! ## Position overlaps -/

private theorem list_sum_perm {l₁ l₂ : List Int} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem positions_contains_index (a : Lane) (ha1 : a.stride = 1) (i : Nat) (hi : i < a.n) :
    contains (positions a) ((a.start : Int) + (i : Int)) = true := by
  apply (contains_iff_mem _ _).mpr
  refine ⟨i, hi, ?_⟩
  unfold progVal positions
  simp [ha1]

private theorem positions_cover (a : Lane) (ha1 : a.stride = 1) {p : Int}
    (h : contains (positions a) p = true) :
    ∃ i, i < a.n ∧ p = (a.start : Int) + (i : Int) := by
  obtain ⟨i, hi, hv⟩ := (contains_iff_mem (positions a) p).mp h
  unfold progVal positions at hv
  simp [ha1] at hv
  exact ⟨i, hi, hv⟩

private theorem firstAt_stride_one (a : Lane) (ha1 : a.stride = 1) (r : Progression) :
    firstAt a r = a.base + (r.lo - (a.start : Int)) * a.step := by
  unfold firstAt
  simp [ha1]

private theorem slopeAt_stride_one (a : Lane) (ha1 : a.stride = 1) (r : Progression) :
    slopeAt a r = (r.step : Int) * a.step := by
  unfold slopeAt
  simp [ha1]

private theorem sum_affine (x d : Int) (n : Nat) :
    ((List.range n).map (fun k : Nat => x + (k : Int) * d)).sum = (n : Int) * x + tri n * d := by
  rw [show ((List.range n).map (fun k : Nat => x + (k : Int) * d)).sum =
      partialSum ⟨0, 1, n, x, d⟩ n from (partialSum_eq_sum ⟨0, 1, n, x, d⟩ n).symm]
  simp [partialSum]

private theorem affine_inj (x d : Int) (hd : d ≠ 0) (i j : Nat)
    (h : x + (i : Int) * d = x + (j : Int) * d) : i = j := by
  have h0 : (i : Int) * d = (j : Int) * d := by omega
  have hm : d * (i : Int) = d * (j : Int) := by simpa [Int.mul_comm] using h0
  have hij : (i : Int) = (j : Int) := (Int.mul_eq_mul_left_iff hd).mp hm
  exact_mod_cast hij

private theorem count_prog_mem_pos (P Q : Progression) (hP : 0 < P.step) :
    (List.range P.n).countP (fun i : Nat => contains Q (progVal P i)) = (intersection P Q).n := by
  let O := intersection P Q
  let L : List Int := ((List.range P.n).filter (fun i : Nat => contains Q (progVal P i))).map (progVal P)
  let L2 : List Int := (List.range O.n).map (progVal O)
  have hnodL : L.Nodup := by
    dsimp [L]
    apply nodup_map_of_injective
    · intro i hi j hj h
      have hm0 : (i : Int) * (P.step : Int) = (j : Int) * (P.step : Int) := by
        unfold progVal at h
        omega
      have hm : P.step * (i : Int) = P.step * (j : Int) := by simpa [Int.mul_comm] using hm0
      have hPnz : (P.step : Int) ≠ 0 := by exact_mod_cast (Nat.ne_of_gt hP)
      have hij : (i : Int) = (j : Int) := (Int.mul_eq_mul_left_iff hPnz).mp hm
      exact_mod_cast hij
    · exact List.Sublist.nodup (List.filter_sublist (p := fun i : Nat =>
        contains Q (progVal P i)) (l := List.range P.n)) List.nodup_range
  have hnodL2 : L2.Nodup := by
    dsimp [L2]
    apply nodup_map_of_injective
    · intro i hi j hj h
      have hm0 : (i : Int) * (O.step : Int) = (j : Int) * (O.step : Int) := by
        unfold progVal at h
        omega
      have hm : O.step * (i : Int) = O.step * (j : Int) := by simpa [Int.mul_comm] using hm0
      have hOnz : (O.step : Int) ≠ 0 := by
        exact_mod_cast (Nat.ne_of_gt (intersection_step_pos P Q))
      have hij : (i : Int) = (j : Int) := (Int.mul_eq_mul_left_iff hOnz).mp hm
      exact_mod_cast hij
    · exact List.nodup_range
  have hmem : ∀ x : Int, x ∈ L ↔ x ∈ L2 := by
    intro x
    constructor
    · intro hx
      rcases List.mem_map.mp hx with ⟨i, hi, rfl⟩
      have hxi : i < P.n := List.mem_range.mp (List.mem_filter.mp hi).1
      have hxQ : contains Q (progVal P i) = true := (List.mem_filter.mp hi).2
      have hxP : contains P (progVal P i) = true := by
        apply (contains_iff_mem _ _).mpr
        exact ⟨i, hxi, rfl⟩
      have hxO : contains O (progVal P i) = true :=
        (intersection_contains P Q _).mpr ⟨hxP, hxQ⟩
      obtain ⟨k, hk, hv⟩ := (contains_iff_mem O _).mp hxO
      apply List.mem_map.mpr
      refine ⟨k, List.mem_range.mpr hk, ?_⟩
      exact hv.symm
    · intro hx
      rcases List.mem_map.mp hx with ⟨k, hk, hv⟩
      have hO : contains O (progVal O k) = true := by
        apply (contains_iff_mem _ _).mpr
        exact ⟨k, List.mem_range.mp hk, rfl⟩
      have hboth := (intersection_contains P Q _).mp hO
      obtain ⟨i, hi, hival⟩ := (contains_iff_mem P _).mp hboth.1
      have hQ : contains Q (progVal P i) = true := by
        rw [← hival]
        exact hboth.2
      apply List.mem_map.mpr
      refine ⟨i, ?_, ?_⟩
      · apply List.mem_filter.mpr
        exact ⟨List.mem_range.mpr hi, hQ⟩
      · rw [← hival]
        exact hv
  have hperm : L.Perm L2 := (List.perm_ext_iff_of_nodup hnodL hnodL2).mpr hmem
  rw [List.countP_eq_length_filter]
  dsimp [L] at hperm
  dsimp [L2] at hperm
  have hlen := hperm.length_eq
  rw [List.length_map, List.length_map, List.length_range] at hlen
  exact hlen

private theorem deleted_sum_lane (a : Lane) (mask : Progression) (ha1 : a.stride = 1) :
    (((List.range a.n).filter (fun i : Nat => contains mask ((a.start : Int) + (i : Int)))).map
      (fun i : Nat => a.base + (i : Int) * a.step)).sum = deletedSum a mask := by
  let O := intersection (positions a) mask
  by_cases ha0 : a.step = 0
  · have hcnt : (List.range a.n).countP
        (fun i : Nat => contains mask ((a.start : Int) + (i : Int))) = O.n := by
      simpa [O, positions, ha1, progVal] using
        (count_prog_mem_pos (positions a) mask (by simp [positions, ha1]))
    have hcnt' : ((List.range a.n).filter (fun i : Nat =>
        contains mask ((a.start : Int) + (i : Int)))).length = O.n := by
      rw [← List.countP_eq_length_filter]
      exact hcnt
    have hvalmap : (((List.range a.n).filter (fun i : Nat =>
        contains mask ((a.start : Int) + (i : Int)))).map
          (fun i : Nat => a.base + (i : Int) * a.step)).sum =
        (((List.range a.n).filter (fun i : Nat =>
          contains mask ((a.start : Int) + (i : Int)))).map (fun _ : Nat => a.base)).sum := by
      apply congrArg List.sum
      apply List.map_congr_left
      intro i hi
      simp [ha0]
    rw [hvalmap]
    rw [sum_map_const_int, hcnt']
    dsimp [deletedSum]
    rw [firstAt_stride_one a ha1, slopeAt_stride_one a ha1]
    simp [O, ha0]
  · have hOpos : 0 < O.step := intersection_step_pos (positions a) mask
    let L := ((List.range a.n).filter (fun i : Nat =>
      contains mask ((a.start : Int) + (i : Int)))).map (fun i : Nat => a.base + (i : Int) * a.step)
    let L2 := (List.range O.n).map (fun k : Nat =>
      firstAt a O + (k : Int) * slopeAt a O)
    have hnodL : L.Nodup := by
      dsimp [L]
      apply nodup_map_of_injective
      · intro i hi j hj h
        exact laneVal_inj a ha0 i j (List.mem_range.mp (List.mem_filter.mp hi).1)
          (List.mem_range.mp (List.mem_filter.mp hj).1) h
      · exact List.Sublist.nodup (List.filter_sublist (p := fun i : Nat =>
          contains mask ((a.start : Int) + (i : Int))) (l := List.range a.n)) List.nodup_range
    have hslope : slopeAt a O ≠ 0 := by
      rw [slopeAt_stride_one a ha1]
      exact Int.mul_ne_zero (by exact_mod_cast (Nat.ne_of_gt hOpos)) (by simpa using ha0)
    have hnodL2 : L2.Nodup := by
      dsimp [L2]
      apply nodup_map_of_injective
      · intro i hi j hj h
        exact affine_inj _ _ hslope i j h
      · exact List.nodup_range
    have hmem : ∀ x : Int, x ∈ L ↔ x ∈ L2 := by
      intro x
      constructor
      · intro hx
        rcases List.mem_map.mp hx with ⟨i, hi, rfl⟩
        have hiN : i < a.n := List.mem_range.mp (List.mem_filter.mp hi).1
        have hiM : contains mask ((a.start : Int) + (i : Int)) = true := (List.mem_filter.mp hi).2
        have hiP : contains (positions a) ((a.start : Int) + (i : Int)) = true :=
          positions_contains_index a ha1 i hiN
        have hiO : contains O ((a.start : Int) + (i : Int)) = true :=
          (intersection_contains (positions a) mask _).mpr ⟨hiP, hiM⟩
        obtain ⟨k, hk, hkv⟩ := (contains_iff_mem O _).mp hiO
        apply List.mem_map.mpr
        refine ⟨k, List.mem_range.mpr hk, ?_⟩
        unfold progVal at hkv
        rw [firstAt_stride_one a ha1, slopeAt_stride_one a ha1]
        have hiEq : (i : Int) = (O.lo - (a.start : Int)) + (k : Int) * (O.step : Int) := by omega
        rw [hiEq]
        grind
      · intro hx
        rcases List.mem_map.mp hx with ⟨k, hk, hval⟩
        have hO : contains O (progVal O k) = true := by
          apply (contains_iff_mem _ _).mpr
          exact ⟨k, List.mem_range.mp hk, rfl⟩
        have hboth := (intersection_contains (positions a) mask _).mp hO
        obtain ⟨i, hi, hip⟩ := positions_cover a ha1 hboth.1
        apply List.mem_map.mpr
        refine ⟨i, ?_, ?_⟩
        · apply List.mem_filter.mpr
          refine ⟨List.mem_range.mpr hi, ?_⟩
          rw [← hip]
          exact hboth.2
        · unfold progVal at hip
          rw [← hval]
          rw [firstAt_stride_one a ha1, slopeAt_stride_one a ha1]
          have hiEq : (i : Int) = (O.lo - (a.start : Int)) + (k : Int) * (O.step : Int) := by omega
          rw [hiEq]
          grind
    have hperm : L.Perm L2 := (List.perm_ext_iff_of_nodup hnodL hnodL2).mpr hmem
    dsimp [L] at hperm
    dsimp [L2] at hperm
    rw [show ((List.range a.n).filter (fun i : Nat =>
        contains mask ((a.start : Int) + (i : Int)))).map
          (fun i : Nat => a.base + (i : Int) * a.step) = L by rfl]
    rw [list_sum_perm hperm]
    rw [sum_affine]
    dsimp [deletedSum]

private theorem sum_filter_not_int {α : Type} (l : List α) (p : α → Bool) (f : α → Int) :
    ((l.filter (fun x => !p x)).map f).sum = (l.map f).sum - ((l.filter p).map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x <;> simp [h, ih] <;> omega

private theorem delete_lane (a : Lane) (mask : Progression) (ha1 : a.stride = 1) :
    (((List.range a.n).filter (fun i : Nat => !contains mask (a.start + i * a.stride))).map
      (fun i : Nat => a.base + (i : Int) * a.step)).sum =
      partialSum a a.n - deletedSum a mask := by
  have hp : (fun i : Nat => !contains mask (a.start + i * a.stride)) =
      (fun i : Nat => !contains mask ((a.start : Int) + (i : Int))) := by
    funext i
    simp [ha1]
  rw [hp]
  rw [sum_filter_not_int]
  rw [deleted_sum_lane a mask ha1]
  rw [partialSum_eq_sum]

private theorem deleteProgression_of_rows_lanes (lay : Layout) (c : Config) (mask : Progression)
    (hrows : rows lay c = (lay.lanes c).flatMap lanePoints)
    (hs : ∀ a ∈ lay.lanes c, a.stride = 1) :
    (((rows lay c).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
      ((lay.lanes c).map (fun a => partialSum a a.n - deletedSum a mask)).sum := by
  rw [hrows]
  rw [flatMap_map_filter_sum]
  apply congrArg List.sum
  apply List.map_congr_left
  intro a ha
  unfold lanePoints
  rw [show (List.filter (fun r : Nat × Int => !contains mask (r.1 : Int))
        ((List.range a.n).map (fun i : Nat =>
          (a.start + i * a.stride, a.base + (i : Int) * a.step)))) =
      ((List.range a.n).filter (fun i : Nat => !contains mask (a.start + i * a.stride))).map
        (fun i : Nat => (a.start + i * a.stride, a.base + (i : Int) * a.step)) by
    exact map_pair_filter (List.range a.n) (fun i : Nat => a.start + i * a.stride)
      (fun i : Nat => a.base + (i : Int) * a.step) (fun n : Nat => !contains mask (n : Int))]
  rw [List.map_map]
  simpa [Function.comp_def] using delete_lane a mask (hs a ha)

theorem sound_LinearOrdinal_DeleteProgression :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.LinearOrdinal.DeleteProgression.Goal := by
  intro c mask
  exact deleteProgression_of_rows_lanes LinearOrdinal.layout c mask (rows_lanes_LO c) (by
    intro a ha
    simp [LinearOrdinal.layout] at ha
    rcases ha with rfl | rfl <;> simp [lane])

theorem sound_SaturatingCounter_DeleteProgression :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.SaturatingCounter.DeleteProgression.Goal := by
  intro c mask
  exact deleteProgression_of_rows_lanes SaturatingCounter.layout c mask (rows_lanes_SC c) (by
    intro a ha
    simp [SaturatingCounter.layout] at ha
    rcases ha with rfl | rfl <;> simp [span, lane])

theorem sound_SegmentedSlope_DeleteProgression :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.SegmentedSlope.DeleteProgression.Goal := by
  intro c mask
  exact deleteProgression_of_rows_lanes SegmentedSlope.layout c mask (rows_lanes_SS c) (by
    intro a ha
    simp [SegmentedSlope.layout] at ha
    rcases ha with rfl | rfl <;> simp [span, lane])

/-! ## AlignedProduct -/

private theorem sum_range_k (n : Nat) : ((List.range n).map (fun k : Nat => (k : Int))).sum = tri n := by
  rw [show ((List.range n).map (fun k : Nat => (k : Int))).sum =
      ((List.range n).map (fun k : Nat => (0 : Int) + (k : Int) * 1)).sum by
    apply congrArg List.sum
    apply List.map_congr_left
    intro k hk
    omega]
  rw [← partialSum_eq_sum ⟨0, 1, n, 0, 1⟩ n]
  simp [partialSum]

private theorem sum_prod_affine (x y dx dy : Int) (n : Nat) :
    ((List.range n).map (fun k : Nat => (x + (k : Int) * dx) * (y + (k : Int) * dy))).sum =
      (n : Int) * x * y + tri n * (x * dy + y * dx) + squares n * dx * dy := by
  let c := x * dy + y * dx
  let d := dx * dy
  have hpoint : ∀ k : Nat,
      (x + (k : Int) * dx) * (y + (k : Int) * dy) =
        x * y + ((k : Int) * c + ((k : Int) * (k : Int)) * d) := by
    intro k
    dsimp [c, d]
    grind
  rw [show ((List.range n).map (fun k : Nat =>
      (x + (k : Int) * dx) * (y + (k : Int) * dy))).sum =
      ((List.range n).map (fun k : Nat =>
        x * y + ((k : Int) * c + ((k : Int) * (k : Int)) * d))).sum by
    apply congrArg List.sum
    apply List.map_congr_left
    intro k hk
    exact hpoint k]
  rw [sum_map_add_int, sum_map_const_int]
  have hmul : ((List.range n).map (fun k : Nat => (k : Int) * c)).sum =
      ((List.range n).map (fun k : Nat => c * (k : Int))).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro k hk
    rw [Int.mul_comm]
  have hmul2 : ((List.range n).map (fun k : Nat => ((k : Int) * (k : Int)) * d)).sum =
      ((List.range n).map (fun k : Nat => d * ((k : Int) * (k : Int)))).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro k hk
    rw [Int.mul_comm]
  rw [show ((List.range n).map (fun k : Nat =>
      (k : Int) * c + ((k : Int) * (k : Int)) * d)).sum =
      ((List.range n).map (fun k : Nat => (k : Int) * c)).sum +
      ((List.range n).map (fun k : Nat => ((k : Int) * (k : Int)) * d)).sum by
    exact sum_map_add_int (List.range n)
      (fun k : Nat => (k : Int) * c) (fun k : Nat => ((k : Int) * (k : Int)) * d)]
  rw [hmul, hmul2, sum_map_mul_int_nat, sum_range_k]
  rw [show ((List.range n).map (fun k : Nat => d * ((k : Int) * (k : Int)))).sum =
      d * ((List.range n).map (fun k : Nat => (k : Int) * (k : Int))).sum by
    rw [show ((List.range n).map (fun k : Nat => d * ((k : Int) * (k : Int)))) =
        ((List.range n).map (fun k : Nat => (k : Int) * (k : Int))).map (fun x : Int => d * x) by
      rw [List.map_map]
      rfl]
    rw [sum_map_mul_int]]
  rw [sum_squares_eq]
  dsimp [c, d]
  grind

private theorem overlap_exists (a b : Lane) (ha1 : a.stride = 1) (hb : 0 < b.stride)
    (k : Nat) (hk : k < (intersection (positions a) (positions b)).n) :
    ∃ i j : Nat, i < a.n ∧ j < b.n ∧
      progVal (intersection (positions a) (positions b)) k = (a.start : Int) + (i : Int) ∧
      progVal (intersection (positions a) (positions b)) k =
        (b.start : Int) + (j : Int) * (b.stride : Int) := by
  let O := intersection (positions a) (positions b)
  have hO : contains O (progVal O k) = true := by
    apply (contains_iff_mem O _).mpr
    exact ⟨k, hk, rfl⟩
  have hboth := (intersection_contains (positions a) (positions b) _).mp hO
  obtain ⟨i, hi, hip⟩ := positions_cover a ha1 hboth.1
  obtain ⟨j, hj, hjp⟩ := (contains_iff_mem (positions b) _).mp hboth.2
  unfold progVal positions at hjp
  exact ⟨i, j, hi, hj, hip, hjp⟩

private theorem intersection_step_one_right (a b : Lane) (ha1 : a.stride = 1) (hb : 0 < b.stride)
    (hn : 0 < (intersection (positions a) (positions b)).n) :
    (intersection (positions a) (positions b)).step = b.stride := by
  have hPn : (positions a).n ≠ 0 := by
    intro h
    have : (intersection (positions a) (positions b)).n = 0 := by
      simp [intersection, h]
    omega
  have hQn : (positions b).n ≠ 0 := by
    intro h
    have : (intersection (positions a) (positions b)).n = 0 := by
      simp [intersection, h]
    omega
  have hPs : (positions a).step ≠ 0 := by simp [positions, ha1]
  have hQs : (positions b).step ≠ 0 := by
    simp [positions]
    omega
  rw [intersection_main_form (positions a) (positions b) hPn hQn hPs hQs]
  simp [positions, ha1, hb]

private theorem nodup_flatMap_unique {α β : Type} (l : List α) (f : α → List β)
    (hl : l.Nodup)
    (hdisj : ∀ a ∈ l, ∀ b ∈ l, a ≠ b → ∀ x ∈ f a, ∀ y ∈ f b, x ≠ y)
    (hnod : ∀ a ∈ l, (f a).Nodup) : (l.flatMap f).Nodup := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hxnot : x ∉ xs := (List.nodup_cons.mp hl).1
      have htail : xs.Nodup := (List.nodup_cons.mp hl).2
      rw [List.flatMap_cons, List.nodup_append]
      refine ⟨hnod x (by simp), ?_, ?_⟩
      · exact ih htail (fun a ha b hb hne => hdisj a (by simp [ha]) b (by simp [hb]) hne)
          (fun a ha => hnod a (by simp [ha]))
      · intro a ha b hb
        rcases List.mem_flatMap.mp hb with ⟨y, hy, hby⟩
        have hxy : x ≠ y := by
          intro heq
          subst y
          exact hxnot hy
        exact hdisj x (by simp) y (by simp [hy]) hxy a ha b hby

private theorem aligned_lane (a b : Lane) (ha1 : a.stride = 1) (hb : 0 < b.stride) :
    ((List.range a.n).map (fun i : Nat =>
      ((List.range b.n).map (fun j : Nat =>
        if a.start + i * a.stride == b.start + j * b.stride then
          (a.base + (i : Int) * a.step) * (b.base + b.step * (j : Int))
        else 0)).sum)).sum = aligned a b := by
  let P := positions a
  let Q := positions b
  let O := intersection P Q
  have hOpos : 0 < O.step := intersection_step_pos P Q
  let P := positions a
  let Q := positions b
  let O := intersection P Q
  have hOpos : 0 < O.step := intersection_step_pos P Q
  by_cases hOn : O.n = 0
  · have hinner : ∀ i : Nat, i < a.n →
        ((List.range b.n).map (fun j : Nat =>
          if a.start + i * a.stride == b.start + j * b.stride then
            (a.base + (i : Int) * a.step) * (b.base + b.step * (j : Int))
          else 0)).sum = 0 := by
      intro i hi
      rw [← sum_map_ite_zero]
      have hf : (List.range b.n).filter
          (fun j : Nat => a.start + i * a.stride == b.start + j * b.stride) = [] := by
        apply List.filter_eq_nil_iff.mpr
        intro j hj hmatch
        have hP : contains P ((a.start : Int) + (i : Int)) = true := by
          dsimp [P]
          exact positions_contains_index a ha1 i hi
        have hQ : contains Q ((a.start : Int) + (i : Int)) = true := by
          dsimp [Q]
          rw [contains_iff_mem]
          refine ⟨j, List.mem_range.mp hj, ?_⟩
          unfold progVal positions
          simp [ha1] at hmatch ⊢
          exact_mod_cast hmatch
        have hOc : contains O ((a.start : Int) + (i : Int)) = true :=
          (intersection_contains P Q _).mpr ⟨hP, hQ⟩
        obtain ⟨_, hk, _⟩ := (contains_iff_mem O _).mp hOc
        omega
      rw [hf]
      simp
    rw [show ((List.range a.n).map (fun i : Nat =>
        ((List.range b.n).map (fun j : Nat =>
          if a.start + i * a.stride == b.start + j * b.stride then
            (a.base + (i : Int) * a.step) * (b.base + b.step * (j : Int))
          else 0)).sum)).sum =
        ((List.range a.n).map (fun _ : Nat => (0 : Int))).sum by
      apply congrArg List.sum
      apply List.map_congr_left
      intro i hi
      exact hinner i (List.mem_range.mp hi)]
    rw [sum_map_zero_int]
    dsimp [aligned]
    rw [hOn]
    simp [tri, squares, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  · have hOstep : O.step = b.stride := by
      have hOn' : 0 < (intersection (positions a) (positions b)).n := by
        simpa [O, P, Q] using (show 0 < O.n from by omega)
      exact intersection_step_one_right a b ha1 hb hOn'
    let pairsL : List (Nat × Nat) :=
      (List.range a.n).flatMap (fun i : Nat =>
        ((List.range b.n).filter (fun j : Nat =>
          a.start + i * a.stride == b.start + j * b.stride)).map (fun j : Nat => (i, j)))
    let hexOf (k : Nat) (hk : k < O.n) : ∃ i j : Nat, i < a.n ∧ j < b.n ∧
        progVal O k = (a.start : Int) + (i : Int) ∧
        progVal O k = (b.start : Int) + (j : Int) * (b.stride : Int) := by
      simpa [O, P, Q] using overlap_exists a b ha1 hb k hk
    let idx (k : Nat) : Nat := if hk : k < O.n then Classical.choose (hexOf k hk) else 0
    let jdx (k : Nat) : Nat := if hk : k < O.n then
      Classical.choose (Classical.choose_spec (hexOf k hk)) else 0
    let pairsO : List (Nat × Nat) := (List.range O.n).map (fun k : Nat => (idx k, jdx k))
    have hidx_spec : ∀ k : Nat, k < O.n →
        progVal O k = (a.start : Int) + (idx k : Int) ∧
        progVal O k = (b.start : Int) + (jdx k : Int) * (b.stride : Int) := by
      intro k hk
      have hs := Classical.choose_spec (Classical.choose_spec (hexOf k hk))
      dsimp [idx]
      rw [dif_pos hk]
      dsimp [jdx]
      rw [dif_pos hk]
      exact ⟨hs.2.2.1, hs.2.2.2⟩
    have hnodO : pairsO.Nodup := by
      dsimp [pairsO]
      apply nodup_map_of_injective
      · intro k hk l hl hpair
        have hk' : k < O.n := List.mem_range.mp hk
        have hl' : l < O.n := List.mem_range.mp hl
        have hfirstNat : idx k = idx l := congrArg Prod.fst hpair
        have hfirst : (idx k : Int) = (idx l : Int) := by exact_mod_cast hfirstNat
        have hpk := (hidx_spec k hk').1
        have hpl := (hidx_spec l hl').1
        have hv : progVal O k = progVal O l := by omega
        unfold progVal at hv
        have hm0 : (k : Int) * (O.step : Int) = (l : Int) * (O.step : Int) := by omega
        have hm : O.step * (k : Int) = O.step * (l : Int) := by simpa [Int.mul_comm] using hm0
        have hOnz : (O.step : Int) ≠ 0 := by exact_mod_cast (Nat.ne_of_gt hOpos)
        have hkl : (k : Int) = (l : Int) := (Int.mul_eq_mul_left_iff hOnz).mp hm
        exact_mod_cast hkl
      · exact List.nodup_range
    have hnodL : pairsL.Nodup := by
      dsimp [pairsL]
      apply nodup_flatMap_unique
      · exact List.nodup_range
      · intro i hi j hj hne x hx y hy hEq
        rcases List.mem_map.mp hx with ⟨j1, hj1, rfl⟩
        rcases List.mem_map.mp hy with ⟨j2, hj2, rfl⟩
        have hij : i = j := by
          have hfst := congrArg Prod.fst hEq
          simpa using hfst
        exact hne hij
      · intro i hi
        apply nodup_map_of_injective
        · intro j hj k hk h
          exact congrArg Prod.snd h
        · exact List.Sublist.nodup (List.filter_sublist (p := fun j : Nat =>
            a.start + i * a.stride == b.start + j * b.stride)
            (l := List.range b.n)) List.nodup_range
    have hmem : ∀ p : Nat × Nat, p ∈ pairsL ↔ p ∈ pairsO := by
      intro p
      constructor
      · intro hp
        rcases List.mem_flatMap.mp hp with ⟨i, hi, hp⟩
        rcases List.mem_map.mp hp with ⟨j, hj, rfl⟩
        have hiN : i < a.n := List.mem_range.mp hi
        have hjN : j < b.n := List.mem_range.mp (List.mem_filter.mp hj).1
        have hpos : (a.start : Int) + (i : Int) =
            (b.start : Int) + (j : Int) * (b.stride : Int) := by
          have hn : a.start + i * a.stride = b.start + j * b.stride := by
            simpa using (List.mem_filter.mp hj).2
          simp [ha1] at hn
          exact_mod_cast hn
        have hP : contains P ((a.start : Int) + (i : Int)) = true := by
          dsimp [P]
          exact positions_contains_index a ha1 i hiN
        have hQ : contains Q ((a.start : Int) + (i : Int)) = true := by
          dsimp [Q]
          rw [contains_iff_mem]
          exact ⟨j, hjN, hpos⟩
        have hOc : contains O ((a.start : Int) + (i : Int)) = true :=
          (intersection_contains P Q _).mpr ⟨hP, hQ⟩
        obtain ⟨k, hk, hkv⟩ := (contains_iff_mem O _).mp hOc
        unfold progVal at hkv
        apply List.mem_map.mpr
        refine ⟨k, List.mem_range.mpr hk, ?_⟩
        apply Prod.ext
        · have hkspec := (hidx_spec k hk).1
          unfold progVal at hkspec
          have hieq : (idx k : Int) = (i : Int) := by omega
          exact_mod_cast hieq
        · have hkspec := (hidx_spec k hk).2
          unfold progVal at hkspec
          have hstep : (b.stride : Int) ≠ 0 := by exact_mod_cast (Nat.ne_of_gt hb)
          have hm0 : (jdx k : Int) * (b.stride : Int) = (j : Int) * (b.stride : Int) := by omega
          have hm : (b.stride : Int) * (jdx k : Int) =
              (b.stride : Int) * (j : Int) := by simpa [Int.mul_comm] using hm0
          have hjq : (jdx k : Int) = (j : Int) := (Int.mul_eq_mul_left_iff hstep).mp hm
          exact_mod_cast hjq
      · intro hp
        rcases List.mem_map.mp hp with ⟨k, hk, rfl⟩
        have hk' : k < O.n := List.mem_range.mp hk
        have hsp1 := Classical.choose_spec (hexOf k hk')
        have hsp2 := Classical.choose_spec hsp1
        have hiN : idx k < a.n := by
          dsimp [idx]
          rw [dif_pos hk']
          exact hsp2.1
        have hjN : jdx k < b.n := by
          dsimp [jdx]
          rw [dif_pos hk']
          exact hsp2.2.1
        have hspec := hidx_spec k hk'
        unfold progVal at hspec
        have hposA : (a.start : Int) + (idx k : Int) =
            (b.start : Int) + (jdx k : Int) * (b.stride : Int) := by omega
        apply List.mem_flatMap.mpr
        refine ⟨idx k, List.mem_range.mpr hiN, ?_⟩
        apply List.mem_map.mpr
        refine ⟨jdx k, ?_, rfl⟩
        apply List.mem_filter.mpr
        refine ⟨List.mem_range.mpr hjN, ?_⟩
        have hn' : a.start + idx k = b.start + jdx k * b.stride := by
          exact_mod_cast hposA
        have hn : a.start + idx k * a.stride = b.start + jdx k * b.stride := by
          simpa [ha1] using hn'
        simpa using hn
    have hperm : pairsL.Perm pairsO := (List.perm_ext_iff_of_nodup hnodL hnodO).mpr hmem
    have hrewrite : ((List.range a.n).map (fun i : Nat =>
        ((List.range b.n).map (fun j : Nat =>
          if a.start + i * a.stride == b.start + j * b.stride then
            (a.base + (i : Int) * a.step) * (b.base + b.step * (j : Int))
          else 0)).sum)).sum =
        (pairsL.map (fun p : Nat × Nat =>
          (a.base + (p.1 : Int) * a.step) * (b.base + b.step * (p.2 : Int)))).sum := by
      calc
        ((List.range a.n).map (fun i : Nat =>
          ((List.range b.n).map (fun j : Nat =>
            if a.start + i * a.stride == b.start + j * b.stride then
              (a.base + (i : Int) * a.step) * (b.base + b.step * (j : Int))
            else 0)).sum)).sum =
          ((List.range a.n).map (fun i : Nat =>
            (((List.range b.n).filter (fun j : Nat =>
              a.start + i * a.stride == b.start + j * b.stride)).map
                (fun j : Nat => (a.base + (i : Int) * a.step) *
                  (b.base + b.step * (j : Int)))).sum)).sum := by
            apply congrArg List.sum
            apply List.map_congr_left
            intro i hi
            rw [sum_map_ite_zero]
        _ = (pairsL.map (fun p : Nat × Nat =>
            (a.base + (p.1 : Int) * a.step) * (b.base + b.step * (p.2 : Int)))).sum := by
          dsimp [pairsL]
          rw [flatMap_map_sum]
          apply congrArg List.sum
          apply List.map_congr_left
          intro i hi
          rw [List.map_map]
          simp [Function.comp_def]
    rw [hrewrite]
    rw [list_sum_perm (hperm.map (fun p : Nat × Nat =>
        (a.base + (p.1 : Int) * a.step) * (b.base + b.step * (p.2 : Int))))]
    dsimp [pairsO]
    rw [List.map_map]
    change ((List.range O.n).map (fun k : Nat =>
        (a.base + (idx k : Int) * a.step) * (b.base + b.step * (jdx k : Int)))).sum = aligned a b
    rw [show ((List.range O.n).map (fun k : Nat =>
        (fun p : Nat × Nat => (a.base + (p.1 : Int) * a.step) *
          (b.base + b.step * (p.2 : Int))) (idx k, jdx k))).sum =
        ((List.range O.n).map (fun k : Nat =>
          (firstAt a O + (k : Int) * slopeAt a O) *
          (firstAt b O + (k : Int) * slopeAt b O))).sum by
      apply congrArg List.sum
      apply List.map_congr_left
      intro k hk
      have hk' : k < O.n := List.mem_range.mp hk
      have hspec := hidx_spec k hk'
      have hstep : (b.stride : Int) ≠ 0 := by exact_mod_cast (Nat.ne_of_gt hb)
      have hpa : a.base + (idx k : Int) * a.step =
          firstAt a O + (k : Int) * slopeAt a O := by
        rw [firstAt_stride_one a ha1, slopeAt_stride_one a ha1]
        have hpv := hspec.1
        unfold progVal at hpv
        have hiEq : (idx k : Int) = (O.lo - (a.start : Int)) + (k : Int) * (O.step : Int) := by omega
        rw [hiEq]
        grind
      have hbdiv : (O.lo - (b.start : Int)) / (b.stride : Int) =
          (jdx k : Int) - (k : Int) := by
        have hpv := hspec.2
        unfold progVal at hpv
        have hmul : (O.lo - (b.start : Int)) =
            ((jdx k : Int) - (k : Int)) * (b.stride : Int) := by
          rw [hOstep] at hpv
          grind
        rw [hmul, Int.mul_comm]
        exact Int.mul_ediv_cancel_left _ hstep
      have hpb : b.base + b.step * (jdx k : Int) =
          firstAt b O + (k : Int) * slopeAt b O := by
        rw [firstAt, slopeAt, hOstep]
        have hdivself : b.stride / b.stride = 1 := Nat.div_self hb
        simp [hdivself]
        rw [hbdiv]
        rw [Int.sub_mul]
        rw [Int.mul_comm b.step (jdx k : Int)]
        omega
      change (a.base + (idx k : Int) * a.step) * (b.base + b.step * (jdx k : Int)) =
        (firstAt a O + (k : Int) * slopeAt a O) * (firstAt b O + (k : Int) * slopeAt b O)
      rw [hpa, hpb]]
    rw [sum_prod_affine]
    dsimp [aligned]

private theorem alignedProduct_of_rows_lanes (lay : Layout) (c : Config) (b : Lane)
    (hrows : rows lay c = (lay.lanes c).flatMap lanePoints)
    (hs : ∀ a ∈ lay.lanes c, a.stride = 1) (hb : 0 < b.stride) :
    ((rows lay c).map (fun r => ((List.range b.n).map (fun i =>
      if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum)).sum =
      ((lay.lanes c).map (aligned · b)).sum := by
  rw [hrows]
  rw [flatMap_map_sum]
  apply congrArg List.sum
  apply List.map_congr_left
  intro a ha
  unfold lanePoints
  rw [List.map_map]
  simpa [Function.comp_def] using aligned_lane a b (hs a ha) hb

theorem sound_LinearOrdinal_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.LinearOrdinal.AlignedProduct.Goal := by
  intro c b hb
  exact alignedProduct_of_rows_lanes LinearOrdinal.layout c b (rows_lanes_LO c) (by
    intro a ha
    simp [LinearOrdinal.layout] at ha
    rcases ha with rfl | rfl <;> simp [lane]) hb

theorem sound_SaturatingCounter_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.SaturatingCounter.AlignedProduct.Goal := by
  intro c b hb
  exact alignedProduct_of_rows_lanes SaturatingCounter.layout c b (rows_lanes_SC c) (by
    intro a ha
    simp [SaturatingCounter.layout] at ha
    rcases ha with rfl | rfl <;> simp [span, lane]) hb

theorem sound_SegmentedSlope_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.SegmentedSlope.AlignedProduct.Goal := by
  intro c b hb
  exact alignedProduct_of_rows_lanes SegmentedSlope.layout c b (rows_lanes_SS c) (by
    intro a ha
    simp [SegmentedSlope.layout] at ha
    rcases ha with rfl | rfl <;> simp [span, lane]) hb

end TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E1
