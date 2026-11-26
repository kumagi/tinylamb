import TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved (7 claims, including the following)

- `BucketRuns.PartialBucket` — `∀ c (j : Nat), c.step ≠ 0 → occurrence layout c
  (c.base+c.step*j) = min (width c) (c.n-j*width c)`
- `LinearOrdinal.InverseLookup` — `∀ c v, c.step ≠ 0 → ((rows layout c).filter (fun r
  => r.2 == v)).map Prod.fst = let i := (v-c.base)/c.step if (v-c.base)%c.step == 0
  && 0 ≤ i && i < c.n then [i.toNat] else []`
- `LinearOrdinal.SliceState` — `∀ c lo hi, lo ≤ hi → let selected := (rows layout
  c).filter (fun r => lo ≤ r.1 && r.1 < hi) let ls := layout.lanes c
  (selected.length,(selected.map Prod.snd).sum) = ((ls.map (fun a => beforePos a hi -
  beforePos a lo)).sum, (ls.map (fun a => partialSum a (beforePos a hi)-partialSum a
  (beforePos a lo))).sum)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages.OrdinalCounterAndBucketSlices

open TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-! ## Generic list arithmetic helpers -/

private theorem list_sum_append_int (l₁ l₂ : List Int) :
    (l₁ ++ l₂).sum = l₁.sum + l₂.sum := by
  induction l₁ with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

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
      · have hmthi : min n thi = n := Nat.min_eq_left (by omega)
        have hmtlo : min n tlo = tlo := Nat.min_eq_right (by omega)
        have hmnthi : min (n + 1) thi = n + 1 := Nat.min_eq_left (by omega)
        have hmntlo : min (n + 1) tlo = tlo := Nat.min_eq_right (by omega)
        simp [h1, h2, ih, List.range_succ, hmthi, hmtlo, hmnthi, hmntlo]
        omega
      · have hmthi : min n thi = thi := Nat.min_eq_right (by omega)
        have hmtlo : min n tlo = tlo := Nat.min_eq_right (by omega)
        have hmnthi : min (n + 1) thi = thi := Nat.min_eq_right (by omega)
        have hmntlo : min (n + 1) tlo = tlo := Nat.min_eq_right (by omega)
        simp [h1, h2, ih, hmthi, hmtlo, hmnthi, hmntlo]
      · have hmthi : min n thi = n := Nat.min_eq_left (by omega)
        have hmtlo : min n tlo = n := Nat.min_eq_left (by omega)
        have hmnthi : min (n + 1) thi = n + 1 := Nat.min_eq_left (by omega)
        have hmntlo : min (n + 1) tlo = n + 1 := Nat.min_eq_left (by omega)
        simp [h1, h2, ih, List.range_succ, hmthi, hmtlo, hmnthi, hmntlo]
      · simp [h1, h2]
        omega

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

/-! ## Lane points and the generic slice theorem -/

private def lanePoints (a : Lane) : List (Nat × Int) :=
  (List.range a.n).map (fun i : Nat => (a.start + i * a.stride, a.base + (i : Int) * a.step))

private def allPoints (ls : List Lane) : List (Nat × Int) := ls.flatMap lanePoints

private theorem filter_flatMap {α β : Type} (ls : List α) (f : α → List β) (p : β → Bool) :
    (ls.flatMap f).filter p = ls.flatMap (fun x => (f x).filter p) := by
  induction ls with
  | nil => rfl
  | cons x xs ih => simp [ih, List.filter_append]

private theorem length_flatMap {α β : Type} (ls : List α) (f : α → List β) :
    (ls.flatMap f).length = (ls.map fun x => (f x).length).sum := by
  induction ls with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem map_sum_flatMap {α : Type} (ls : List α) (f : α → List (Nat × Int)) :
    ((ls.flatMap f).map Prod.snd).sum = (ls.map (fun x => ((f x).map Prod.snd).sum)).sum := by
  induction ls with
  | nil => simp
  | cons x xs ih => simp [List.map_append, list_sum_append_int, ih]

private theorem beforePos_stride1 (a : Lane) (hs : a.stride = 1) (p : Nat) :
    beforePos a p = min a.n (p - a.start) := by
  unfold beforePos
  rw [hs]
  simp

private theorem range_count_shift (n start lo hi : Nat) (hle : lo ≤ hi) :
    ((List.range n).filter (fun i => lo ≤ start + i && start + i < hi)).length =
      min n (hi - start) - min n (lo - start) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.filter_append]
      by_cases h1 : lo ≤ start + n <;> by_cases h2 : start + n < hi <;> simp [h1, h2, ih] <;> omega

private theorem range_sum_shift (n start lo hi : Nat) (f : Nat → Int) (hle : lo ≤ hi) :
    (((List.range n).filter (fun i => lo ≤ start + i && start + i < hi)).map f).sum =
      ((List.range (min n (hi - start))).map f).sum -
        ((List.range (min n (lo - start))).map f).sum := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.filter_append]
      by_cases h1 : lo ≤ start + n <;> by_cases h2 : start + n < hi
      · have hmhi : min n (hi - start) = n := Nat.min_eq_left (by omega)
        have hmlo : min n (lo - start) = lo - start := Nat.min_eq_right (by omega)
        have hmnhi : min (n+1) (hi - start) = n + 1 := Nat.min_eq_left (by omega)
        have hmnlo : min (n+1) (lo - start) = lo - start := Nat.min_eq_right (by omega)
        simp [h1, h2, ih, List.range_succ, hmhi, hmlo, hmnhi, hmnlo]
        omega
      · have hnhi : min (n+1) (hi - start) = min n (hi - start) := by omega
        have hnlo : min (n+1) (lo - start) = min n (lo - start) := by omega
        simp [h1, h2, ih, hnhi, hnlo]
      · have hmhi : min n (hi - start) = n := Nat.min_eq_left (by omega)
        have hmlo : min n (lo - start) = n := Nat.min_eq_left (by omega)
        have hmnhi : min (n+1) (hi - start) = n + 1 := Nat.min_eq_left (by omega)
        have hmnlo : min (n+1) (lo - start) = n + 1 := Nat.min_eq_left (by omega)
        simp [h1, h2, ih, List.range_succ, hmhi, hmlo, hmnhi, hmnlo]
      · simp [h1, h2]
        omega

private theorem filter_range_shift_pair (n start lo hi : Nat) (g : Nat → Int) :
    ((List.range n).map (fun i => (start + i, g i))).filter (fun r => lo ≤ r.1 && r.1 < hi) =
      ((List.range n).filter (fun i => lo ≤ start + i && start + i < hi)).map
        (fun i => (start + i, g i)) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.filter_append, ih]
      by_cases h : lo ≤ start + n ∧ start + n < hi <;> simp [h]

private theorem filter_lanePoints1 (a : Lane) (hs : a.stride = 1) (lo hi : Nat) :
    (lanePoints a).filter (fun r => lo ≤ r.1 && r.1 < hi) =
      ((List.range a.n).filter (fun i => lo ≤ a.start + i && a.start + i < hi)).map
        (fun i => (a.start + i, a.base + (i : Int) * a.step)) := by
  unfold lanePoints
  rw [hs]
  simp only [Nat.mul_one]
  exact filter_range_shift_pair a.n a.start lo hi (fun i => a.base + (i : Int) * a.step)

private theorem lane_slice_count1 (a : Lane) (hs : a.stride = 1) (lo hi : Nat) (hle : lo ≤ hi) :
    ((lanePoints a).filter (fun r => lo ≤ r.1 && r.1 < hi)).length =
      beforePos a hi - beforePos a lo := by
  rw [filter_lanePoints1 a hs lo hi]
  simp only [List.length_map]
  rw [range_count_shift a.n a.start lo hi hle]
  rw [beforePos_stride1 a hs hi, beforePos_stride1 a hs lo]

private theorem lane_slice_sum1 (a : Lane) (hs : a.stride = 1) (lo hi : Nat) (hle : lo ≤ hi) :
    (((lanePoints a).filter (fun r => lo ≤ r.1 && r.1 < hi)).map Prod.snd).sum =
      partialSum a (beforePos a hi) - partialSum a (beforePos a lo) := by
  rw [filter_lanePoints1 a hs lo hi]
  rw [List.map_map]
  change (((List.range a.n).filter (fun i => lo ≤ a.start + i && a.start + i < hi)).map
    (fun i : Nat => a.base + (i : Int) * a.step)).sum = _
  rw [range_sum_shift a.n a.start lo hi (fun i => a.base + (i : Int) * a.step) hle]
  rw [beforePos_stride1 a hs hi, beforePos_stride1 a hs lo]
  rw [← partialSum_eq_sum, ← partialSum_eq_sum]

private theorem allPoints_slice_count (ls : List Lane) (hsl : ∀ a ∈ ls, a.stride = 1)
    (lo hi : Nat) (hle : lo ≤ hi) :
    ((allPoints ls).filter (fun r => lo ≤ r.1 && r.1 < hi)).length =
      (ls.map (fun a => beforePos a hi - beforePos a lo)).sum := by
  induction ls with
  | nil => simp [allPoints]
  | cons a ls ih =>
      have ha : a.stride = 1 := hsl a (by simp)
      have hls : ∀ b ∈ ls, b.stride = 1 := fun b hb => hsl b (by simp [hb])
      simp only [allPoints, List.flatMap_cons, filter_flatMap, List.filter_append,
        List.length_append, List.map_cons, List.sum_cons]
      rw [lane_slice_count1 a ha lo hi hle]
      have htail : (List.flatMap
          (fun x => (lanePoints x).filter (fun r => lo ≤ r.1 && r.1 < hi)) ls).length =
          (ls.map (fun a => beforePos a hi - beforePos a lo)).sum := by
        rw [← filter_flatMap]
        simpa [allPoints] using ih hls
      rw [htail]

private theorem allPoints_slice_sum (ls : List Lane) (hsl : ∀ a ∈ ls, a.stride = 1)
    (lo hi : Nat) (hle : lo ≤ hi) :
    (((allPoints ls).filter (fun r => lo ≤ r.1 && r.1 < hi)).map Prod.snd).sum =
      (ls.map (fun a => partialSum a (beforePos a hi) - partialSum a (beforePos a lo))).sum := by
  induction ls with
  | nil => simp [allPoints]
  | cons a ls ih =>
      have ha : a.stride = 1 := hsl a (by simp)
      have hls : ∀ b ∈ ls, b.stride = 1 := fun b hb => hsl b (by simp [hb])
      simp only [allPoints, List.flatMap_cons, filter_flatMap, List.filter_append,
        List.map_append, list_sum_append_int, List.map_cons, List.sum_cons]
      rw [lane_slice_sum1 a ha lo hi hle]
      have htail : ((List.flatMap
          (fun x => (lanePoints x).filter (fun r => lo ≤ r.1 && r.1 < hi)) ls).map Prod.snd).sum =
          (ls.map (fun a => partialSum a (beforePos a hi) - partialSum a (beforePos a lo))).sum := by
        rw [map_sum_flatMap]
        rw [List.map_congr_left (fun b hb => lane_slice_sum1 b (hls b hb) lo hi hle)]
      rw [htail]

private theorem slice_of_rows_eq (L : Layout) (c : Config) (lo hi : Nat) (hle : lo ≤ hi)
    (hrows : rows L c = allPoints (L.lanes c))
    (hsl : ∀ a ∈ L.lanes c, a.stride = 1) :
    let selected := (rows L c).filter (fun r => lo ≤ r.1 && r.1 < hi)
    let ls := L.lanes c
    (selected.length, (selected.map Prod.snd).sum) =
      ((ls.map (fun a => beforePos a hi - beforePos a lo)).sum,
       (ls.map (fun a => partialSum a (beforePos a hi) - partialSum a (beforePos a lo))).sum) := by
  dsimp
  rw [hrows]
  exact Prod.ext (allPoints_slice_count (L.lanes c) hsl lo hi hle)
    (allPoints_slice_sum (L.lanes c) hsl lo hi hle)


private theorem range_split_if {α : Type} (n split : Nat) (f g : Nat → α) :
    (List.range n).map (fun i => if i < split then f i else g i) =
      (List.range (min n split)).map f ++ (List.range (n - split)).map (fun j => g (split + j)) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append, ih]
      by_cases h : n < split
      · have hm1 : min n split = n := Nat.min_eq_left (by omega)
        have hm2 : min (n + 1) split = n + 1 := Nat.min_eq_left (by omega)
        have hs0 : n + 1 - split = 0 := by omega
        have hs0' : n - split = 0 := by omega
        simp [h, hm1, hm2, hs0, hs0', List.range_succ]
      · have hm1 : min n split = split := Nat.min_eq_right (by omega)
        have hm2 : min (n + 1) split = split := Nat.min_eq_right (by omega)
        have hs1 : n + 1 - split = (n - split) + 1 := by omega
        have hnsub : split + (n - split) = n := by omega
        simp [h, hm1, hm2, hs1, List.range_succ, hnsub]

private theorem option_map_if_some {α β : Type} (p : Prop) [Decidable p] (f : α → β) (a b : α) :
    Option.map f (if p then some a else some b) = if p then some (f a) else some (f b) := by
  by_cases h : p <;> simp [h]

private theorem filterMap_if_some_eq_map {α β : Type} (l : List α) (p : α → Bool)
    (f g : α → β) :
    l.filterMap (fun x => if p x then some (f x) else some (g x)) =
      l.map (fun x => if p x then f x else g x) := by
  induction l with
  | nil => rfl
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

private theorem filterMap_if_some_eq_map_prop {α β : Type} (l : List α) (p : α → Prop)
    [DecidablePred p] (f g : α → β) :
    l.filterMap (fun x => if p x then some (f x) else some (g x)) =
      l.map (fun x => if p x then f x else g x) := by
  induction l with
  | nil => rfl
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

private theorem countP_map_general {α β : Type} (l : List α) (f : α → β) (p : β → Bool) :
    (l.map f).countP p = l.countP (fun x => p (f x)) := by
  induction l with
  | nil => rfl
  | cons x xs ih => by_cases h : p (f x) <;> simp [h, ih]

private theorem filterMap_some_eq_map {α β : Type} (l : List α) (f : α → β) :
    l.filterMap (fun x => some (f x)) = l.map f := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]

namespace LinearOrdinal

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.LinearOrdinal

private theorem rows_eq (c : Config) : rows layout c = allPoints (layout.lanes c) := by
  unfold rows
  simp [layout, lanePoints, allPoints, lane, Int.mul_comm]

private theorem lanes_stride (c : Config) : ∀ a ∈ layout.lanes c, a.stride = 1 := by
  simp [layout, lane]

theorem SliceState_sound : SliceState.Goal := by
  intro c lo hi hle
  exact slice_of_rows_eq layout c lo hi hle (rows_eq c) (lanes_stride c)

private theorem filter_map_pair_snd {α : Type} [BEq α] (xs : List Nat) (f : Nat → α) (v : α) :
    ((xs.map (fun i => (i, f i))).filter (fun r => r.2 == v)).map Prod.fst =
      xs.filter (fun i => f i == v) := by
  induction xs with
  | nil => simp
  | cons x xs ih => by_cases h : f x == v <;> simp [h, ih]

private theorem filter_range_eq (n k : Nat) :
    (List.range n).filter (fun i => i == k) = if k < n then [k] else [] := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.filter_append, ih]
      by_cases hk : k = n
      · subst k
        simp at ih ⊢
      · have hnk : n ≠ k := by omega
        by_cases h : k < n
        · simp [ih, h, hnk]
          omega
        · have hkgt : n < k := by omega
          simp [ih, h, hkgt, hnk]
          omega

private theorem filter_eq_nil_of_never {α : Type} (l : List α) (p : α → Bool)
    (h : ∀ x ∈ l, p x = false) : l.filter p = [] := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      rw [List.filter_cons, h x (by simp), ih (fun y hy => h y (by simp [hy]))]
      simp

theorem InverseLookup_sound : InverseLookup.Goal := by
  intro c v hs
  unfold rows
  simp only [layout, Option.map_some]
  rw [filterMap_some_eq_map]
  rw [filter_map_pair_snd]
  let q : Int := (v - c.base) / c.step
  by_cases hd : (v - c.base) % c.step == 0
  · have hd' : (v - c.base) % c.step = 0 := (beq_iff_eq.mp hd)
    have hdvd : c.step ∣ v - c.base := Int.dvd_of_emod_eq_zero hd'
    have hqeq : c.step * q = v - c.base := by
      dsimp [q]
      rw [Int.mul_ediv_cancel' hdvd]
    by_cases hq : 0 ≤ q ∧ q < (c.n : Int)
    · have hqn : q.toNat < c.n := by
        have hqcast : ((q.toNat : Nat) : Int) = q := Int.toNat_of_nonneg hq.1
        omega
      have hdivi : ∀ i : Nat, (c.step * (i : Int)) / c.step = (i : Int) := by
        intro i
        rw [Int.mul_comm]
        exact Int.mul_ediv_cancel _ hs
      have hchar : ∀ i ∈ List.range c.n,
          (c.base + c.step * (i : Int) == v) = (i == q.toNat) := by
        intro i hi
        by_cases hieq : i = q.toNat
        · subst i
          have hqi : ((q.toNat : Nat) : Int) = q := Int.toNat_of_nonneg hq.1
          have hvq : c.base + c.step * (q.toNat : Int) = v := by
            rw [hqi]
            omega
          rw [beq_self_eq_true]
          exact (beq_iff_eq).mpr hvq
        · have hbne : c.base + c.step * (i : Int) ≠ v := by
            intro hv
            have hmul : c.step * (i : Int) = v - c.base := by omega
            have hdiv : (c.step * (i : Int)) / c.step = (v - c.base) / c.step := by rw [hmul]
            rw [hdivi] at hdiv
            have hiq : (i : Int) = q := hdiv
            have hqt : ((q.toNat : Nat) : Int) = q := Int.toNat_of_nonneg hq.1
            apply hieq
            omega
          rw [beq_eq_false_iff_ne.mpr hbne]
          rw [beq_eq_false_iff_ne.mpr hieq]
      rw [List.filter_congr hchar, filter_range_eq]
      have hpos : 0 ≤ (v - c.base) / c.step := by simpa [q] using hq.1
      have hlt : (v - c.base) / c.step < (c.n : Int) := by simpa [q] using hq.2
      simp [q, hd, hpos, hlt, hqn]
    · have hdivi : ∀ i : Nat, (c.step * (i : Int)) / c.step = (i : Int) := by
        intro i
        rw [Int.mul_comm]
        exact Int.mul_ediv_cancel _ hs
      have hnever : ∀ i ∈ List.range c.n, (c.base + c.step * (i : Int) == v) = false := by
        intro i hi
        have hbne : c.base + c.step * (i : Int) ≠ v := by
          intro hv
          have hmul : c.step * (i : Int) = v - c.base := by omega
          have hdiv : (c.step * (i : Int)) / c.step = (v - c.base) / c.step := by rw [hmul]
          rw [hdivi] at hdiv
          have hiq : (i : Int) = q := hdiv
          have hqnonneg : 0 ≤ q := by
            rw [← hiq]
            exact_mod_cast (Nat.zero_le i)
          have hqlt : q < (c.n : Int) := by
            rw [← hiq]
            exact_mod_cast (List.mem_range.mp hi)
          exact hq ⟨hqnonneg, hqlt⟩
        exact beq_eq_false_iff_ne.mpr hbne
      rw [filter_eq_nil_of_never _ _ hnever]
      have hnot : ¬ (0 ≤ (v - c.base) / c.step ∧ (v - c.base) / c.step < (c.n : Int)) := by
        simpa [q] using hq
      simp [hd, hnot]
  · have hnever : ∀ i ∈ List.range c.n, (c.base + c.step * (i : Int) == v) = false := by
      intro i hi
      have hbne : c.base + c.step * (i : Int) ≠ v := by
        intro hv
        have hmul : c.step * (i : Int) = v - c.base := by omega
        have hdvd : c.step ∣ v - c.base := ⟨(i : Int), by rw [← hmul, Int.mul_comm]⟩
        have hz : (v - c.base) % c.step = 0 := Int.emod_eq_zero_of_dvd hdvd
        exact hd (beq_iff_eq.mpr hz)
      exact beq_eq_false_iff_ne.mpr hbne
    rw [filter_eq_nil_of_never _ _ hnever]
    simp [hd]

end LinearOrdinal


namespace SaturatingCounter

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.SaturatingCounter

private theorem rows_eq (c : Config) : rows layout c = allPoints (layout.lanes c) := by
  unfold rows
  simp only [layout, option_map_if_some]
  rw [filterMap_if_some_eq_map_prop]
  rw [range_split_if]
  simp [allPoints, lanePoints, span, lane, Int.mul_comm]

private theorem lanes_stride (c : Config) : ∀ a ∈ layout.lanes c, a.stride = 1 := by
  simp [layout, span, lane]

theorem SliceState_sound : SliceState.Goal := by
  intro c lo hi hle
  exact slice_of_rows_eq layout c lo hi hle (rows_eq c) (lanes_stride c)

private theorem countP_map {α β : Type} (l : List α) (f : α → β) (p : β → Bool) :
    (l.map f).countP p = l.countP (fun x => p (f x)) := by
  induction l with
  | nil => rfl
  | cons x xs ih => by_cases h : p (f x) <;> simp [h, ih]

private theorem countP_all_false {α : Type} (l : List α) (p : α → Bool)
    (h : ∀ x ∈ l, p x = false) : l.countP p = 0 := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem countP_all_true {α : Type} (l : List α) (p : α → Bool)
    (h : ∀ x ∈ l, p x = true) : l.countP p = l.length := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

theorem PlateauFrequency_sound : PlateauFrequency.Goal := by
  intro c hs
  unfold occurrence
  rw [rows_eq]
  simp only [allPoints, List.flatMap_cons, List.flatMap_nil, List.append_nil]
  simp [layout, span, lane, List.countP_append, lanePoints, countP_map]
  change (List.range (min c.n c.split)).countP
      (fun (i : Nat) => c.base + (i : Int) * c.step == c.base + c.step * (c.split : Int)) +
      (List.range (c.n - c.split)).countP
      (fun (i : Nat) => c.base + c.step * (c.split : Int) == c.base + c.step * (c.split : Int)) =
    c.n - c.split
  have hfirst : (List.range (min c.n c.split)).countP
      (fun (x : Nat) => c.base + (x : Int) * c.step ==
        c.base + c.step * (c.split : Int)) = 0 := by
    apply countP_all_false
    intro i hi
    have hil : i < c.split := by
      have him : i < min c.n c.split := List.mem_range.mp hi
      omega
    have hne : c.base + (i : Int) * c.step ≠ c.base + c.step * (c.split : Int) := by
      intro heq
      have hm : (i : Int) * c.step = c.step * (c.split : Int) := by omega
      rw [Int.mul_comm (i : Int) c.step] at hm
      have hi_eq : (i : Int) = (c.split : Int) := (Int.mul_eq_mul_left_iff hs).mp hm
      have : i = c.split := by exact_mod_cast hi_eq
      omega
    exact beq_eq_false_iff_ne.mpr hne
  have hsecond : (List.range (c.n - c.split)).countP
      (fun (x : Nat) => c.base + c.step * (c.split : Int) ==
        c.base + c.step * (c.split : Int)) = c.n - c.split := by
    simpa using (countP_all_true (List.range (c.n - c.split))
      (fun (x : Nat) => c.base + c.step * (c.split : Int) ==
        c.base + c.step * (c.split : Int)) (by intro j hj; simp))
  rw [hfirst, hsecond]
  simp

end SaturatingCounter


namespace SegmentedSlope

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.SegmentedSlope

private theorem rows_eq (c : Config) : rows layout c = allPoints (layout.lanes c) := by
  unfold rows
  simp only [layout, option_map_if_some]
  rw [filterMap_if_some_eq_map_prop]
  rw [range_split_if]
  simp [allPoints, lanePoints, span, lane, Int.mul_comm]

private theorem lanes_stride (c : Config) : ∀ a ∈ layout.lanes c, a.stride = 1 := by
  simp [layout, span, lane]

theorem SliceState_sound : SliceState.Goal := by
  intro c lo hi hle
  exact slice_of_rows_eq layout c lo hi hle (rows_eq c) (lanes_stride c)

end SegmentedSlope

namespace MirroredTail

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.MirroredTail

private theorem mirrored_val (n split j : Nat) (hsj : split + j < n) (b s : Int) :
    b + s * ((n - 1 - split : Nat) : Int) + (-s) * (j : Int) =
      b + s * ((n - 1 - (split + j) : Nat) : Int) := by
  have hsub : n - 1 - (split + j) = n - 1 - split - j := by omega
  rw [hsub]
  have hcast : ((n - 1 - split - j : Nat) : Int) = ((n - 1 - split : Nat) : Int) - (j : Int) := by omega
  rw [hcast, Int.mul_sub, Int.neg_mul]
  omega

private theorem rows_eq (c : Config) : rows layout c = allPoints (layout.lanes c) := by
  unfold rows
  simp only [layout, option_map_if_some]
  rw [filterMap_if_some_eq_map_prop]
  rw [range_split_if]
  simp only [allPoints, List.flatMap_cons, List.flatMap_nil, List.append_nil]
  have h1 : lanePoints (span c 0 c.split c.base c.step) =
      (List.range (min c.n c.split)).map (fun (i : Nat) => (i, c.base + c.step * (i : Int))) := by
    simp [lanePoints, span, lane, Int.mul_comm]
  have h2 : lanePoints (span c c.split c.n
      (c.base + c.step * ((c.n - 1 - c.split : Nat) : Int)) (-c.step)) =
      (List.range (c.n - c.split)).map (fun (j : Nat) =>
        (c.split + j, c.base + c.step * ((c.n - 1 - (c.split + j) : Nat) : Int))) := by
    simp [lanePoints, span, lane]
    intro j hj
    have hsj : c.split + j < c.n := by omega
    have hsub : c.n - 1 - (c.split + j) = c.n - 1 - c.split - j := by omega
    rw [hsub]
    have hcast : ((c.n - 1 - c.split - j : Nat) : Int) = ((c.n - 1 - c.split : Nat) : Int) - (j : Int) := by omega
    rw [hcast, Int.mul_sub]
    have hcomm : (j : Int) * (-c.step) = -(c.step * (j : Int)) := by
      rw [Int.mul_comm, Int.neg_mul]
    rw [hcomm]
    omega
  rw [h1, h2]

private theorem lanes_stride (c : Config) : ∀ a ∈ layout.lanes c, a.stride = 1 := by
  simp [layout, span, lane]

theorem SliceState_sound : SliceState.Goal := by
  intro c lo hi hle
  exact slice_of_rows_eq layout c lo hi hle (rows_eq c) (lanes_stride c)

end MirroredTail


namespace BucketRuns

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.BucketRuns

private theorem count_bucket (n w j : Nat) (hw : 0 < w) :
    (List.range n).countP (fun i => i / w == j) = min w (n - j * w) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.countP_append, ih]
      have hmod := Nat.mod_lt n hw
      by_cases heq : n / w = j
      · simp [heq]
        have hdm := Nat.div_add_mod n w
        rw [heq] at hdm
        rw [Nat.mul_comm w j] at hdm
        have hr : n - j * w = n % w := by omega
        have hr2 : n + 1 - j * w = n % w + 1 := by omega
        rw [hr, hr2]
        have hle1 : n % w ≤ w := by omega
        have hle2 : n % w + 1 ≤ w := by omega
        rw [Nat.min_eq_right hle1, Nat.min_eq_right hle2]
      · by_cases hlt : n / w < j
        · have hnlt : n < j * w := (Nat.div_lt_iff_lt_mul hw).mp hlt
          have hz1 : n - j * w = 0 := by omega
          have hz2 : n + 1 - j * w = 0 := by omega
          simp [heq, hz1, hz2]
        · have hgt : j < n / w := by omega
          have hj1 : j + 1 ≤ n / w := by omega
          have hmul : (j + 1) * w ≤ (n / w) * w := Nat.mul_le_mul_right w hj1
          have hdivmul : (n / w) * w ≤ n := Nat.div_mul_le_self n w
          have hle_n : (j + 1) * w ≤ n := Nat.le_trans hmul hdivmul
          rw [Nat.add_mul, Nat.one_mul] at hle_n
          have hle1 : w ≤ n - j * w := by omega
          have hle2 : w ≤ n + 1 - j * w := by omega
          simp [heq, Nat.min_eq_left hle1, Nat.min_eq_left hle2]

private theorem rows_eq (c : Config) :
    rows layout c = (List.range c.n).map
      (fun i : Nat => (i, c.base + c.step * ((i / width c : Nat) : Int))) := by
  unfold rows
  simp [layout, filterMap_some_eq_map]

theorem PartialBucket_sound : PartialBucket.Goal := by
  intro c j hs
  unfold occurrence
  rw [rows_eq]
  rw [countP_map_general]
  have hbeq : ∀ i : Nat,
      ((i, c.base + c.step * ((i / width c : Nat) : Int)).2 ==
        c.base + c.step * (j : Int)) = (i / width c == j) := by
    intro i
    by_cases h : i / width c = j
    · simp [h]
    · have hne : c.base + c.step * ((i / width c : Nat) : Int) ≠
          c.base + c.step * (j : Int) := by
        intro he
        have hm : c.step * ((i / width c : Nat) : Int) = c.step * (j : Int) := by omega
        have hc : ((i / width c : Nat) : Int) = (j : Int) := (Int.mul_eq_mul_left_iff hs).mp hm
        have hn : i / width c = j := by exact_mod_cast hc
        exact h hn
      rw [beq_eq_false_iff_ne.mpr hne, beq_eq_false_iff_ne.mpr h]
  have hc : List.countP
      (fun i : Nat => (i, c.base + c.step * ((i / width c : Nat) : Int)).2 ==
        c.base + c.step * (j : Int)) (List.range c.n) =
      List.countP (fun i : Nat => i / width c == j) (List.range c.n) := by
    apply List.countP_congr
    intro i hi
    rw [hbeq i]
  rw [hc]
  exact count_bucket c.n (width c) j (by dsimp [width]; omega)

-- TODO(LinearOrdinal.ResidueJoin)
-- TODO(LinearOrdinal.DeleteProgression)
-- TODO(LinearOrdinal.AlignedProduct)
-- TODO(SaturatingCounter.ResidueJoin)
-- TODO(SaturatingCounter.DeleteProgression)
-- TODO(SaturatingCounter.AlignedProduct)
-- TODO(SegmentedSlope.TurnExtrema)
-- TODO(SegmentedSlope.ResidueJoin)
-- TODO(SegmentedSlope.DeleteProgression)
-- TODO(SegmentedSlope.AlignedProduct)
-- TODO(MirroredTail.OverlapDistinct)
-- TODO(MirroredTail.ResidueJoin)
-- TODO(MirroredTail.DeleteProgression)
-- TODO(MirroredTail.AlignedProduct)
-- TODO(BucketRuns.SliceState)
-- TODO(BucketRuns.ResidueJoin)
-- TODO(BucketRuns.DeleteProgression)
-- TODO(BucketRuns.AlignedProduct)

end BucketRuns

end TinylambOpt.ResearchProofs.Atlas.EncodedPages.OrdinalCounterAndBucketSlices
