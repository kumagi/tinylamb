/-!
# Individual Query Gap Proofs (Post-12/12 Sweep Deep Dive)

Six machine-checked soundness theorems targeting the largest remaining
individual-query latency gaps against DuckDB 2.0.0 and ClickHouse 26.10
across the 12 benchmark suites:

1. `LexicographicDimPrefixTopK_sound`        — TPC-DS Q65 (2.44x vs DuckDB)
2. `TwoPassGroupAvgThreshold_sound`          — TPC-DS Q65 (window-sort elision)
3. `MultiChannelExistsBitsetPushdown_sound`  — TPC-DS Q35 / Q10 / Q69 (2.07x vs DuckDB)
4. `CompositeKeySemiJoinChainReduction_sound`— TPC-DS Q25 (2.60x vs DuckDB, worst ratio)
5. `LeftJoinRightSideKeyFilterPushdown_sound`— TPC-DS Q75 (2.15x vs DuckDB, worst abs gap)
6. `FrequencyUpperBoundHavingPrune_sound`    — ClickBench Q28 / Q32 (1.92x / 1.50x vs CH)
-/

namespace TinylambOpt.ResearchProofs.SemijoinFiltersAndGroupThresholds

/-! ## 1. Lexicographic Dimension-Prefix Top-K Early Cutoff (TPC-DS Q65) -/

/-- Take just enough leading dimension groups (already ordered by the primary
    dimension sort key, e.g. `s_store_name`) until the cumulative row count
    reaches `k`, skipping all remaining groups completely. -/
def takeGroupsUntil {α : Type} : Nat → List (List α) → List (List α)
  | 0, _ => []
  | _, [] => []
  | k + 1, g :: gs =>
      if k + 1 ≤ g.length then [g]
      else g :: takeGroupsUntil (k + 1 - g.length) gs

/-- Taking `k` rows from the flattened stream of primary-key-ordered groups
    equals taking `k` rows from only the minimal prefix of groups whose total
    length reaches `k`. Remaining groups need neither dimension joins nor wide
    string materialization (`i_item_desc`). -/
theorem LexicographicDimPrefixTopK_sound {α : Type}
    (k : Nat) (groups : List (List α)) :
    (takeGroupsUntil k groups).flatten.take k = groups.flatten.take k := by
  induction groups generalizing k with
  | nil =>
      cases k <;> simp [takeGroupsUntil]
  | cons g gs ih =>
      cases k with
      | zero =>
          simp [takeGroupsUntil]
      | succ k' =>
          simp only [takeGroupsUntil]
          split
          · rename_i hle
            simp [List.take_append_of_le_length hle]
          · rename_i hlt
            have hlt' : g.length < k' + 1 := Nat.lt_of_not_le hlt
            simp only [List.flatten_cons, List.take_append]
            have htake_g : g.take (k' + 1) = g :=
              List.take_of_length_le (Nat.le_of_lt hlt')
            rw [htake_g]
            congr 1
            exact ih (k' + 1 - g.length)

/-! ## 2. Two-Pass Group-Average Threshold Filter (TPC-DS Q65) -/

/-- Exact integer group total `(sum, count)` over a single pass. -/
def groupSumCount {α K : Type} [DecidableEq K]
    (keyOf : α → K) (valOf : α → Int) (rows : List α) (g : K) : Int × Nat :=
  rows.foldl
    (fun acc r => if keyOf r = g then (acc.1 + valOf r, acc.2 + 1) else acc)
    (0, 0)

/-- Canonical filter-then-sum/count specification for group `g`. -/
def specGroupSum {α K : Type} [DecidableEq K]
    (keyOf : α → K) (valOf : α → Int) (rows : List α) (g : K) : Int :=
  (rows.filter (fun r => decide (keyOf r = g))).foldl (fun s r => s + valOf r) 0

def specGroupCount {α K : Type} [DecidableEq K]
    (keyOf : α → K) (rows : List α) (g : K) : Nat :=
  (rows.filter (fun r => decide (keyOf r = g))).length

private theorem groupSumCount_aux {α K : Type} [DecidableEq K]
    (keyOf : α → K) (valOf : α → Int) (rows : List α) (g : K)
    (s0 : Int) (c0 : Nat) :
    rows.foldl
      (fun acc r => if keyOf r = g then (acc.1 + valOf r, acc.2 + 1) else acc)
      (s0, c0) =
    ((rows.filter (fun r => decide (keyOf r = g))).foldl (fun s r => s + valOf r) s0,
     c0 + (rows.filter (fun r => decide (keyOf r = g))).length) := by
  induction rows generalizing s0 c0 with
  | nil => simp
  | cons r rs ih =>
      by_cases h : keyOf r = g
      · simp only [List.foldl_cons, h, ite_true, List.filter_cons, decide_eq_true_eq,
          List.length_cons]
        rw [ih (s0 + valOf r) (c0 + 1)]
        exact Prod.ext rfl (by omega)
      · simp only [List.foldl_cons, h, ite_false, List.filter_cons, decide_eq_true_eq]
        exact ih s0 c0

/-- Cross-multiplied threshold predicate `denom * cnt * val ≤ numer * sum`
    (exact rational form of `val ≤ (numer / denom) * AVG(val)` with `cnt > 0`). -/
def avgThresholdHolds (numer denom : Int) (val sum : Int) (cnt : Nat) : Bool :=
  decide (0 < cnt ∧ denom * (cnt : Int) * val ≤ numer * sum)

/-- Two-pass array accumulation + threshold filter produces the exact same rows
    as computing each group's window `AVG` over the filtered partition and
    filtering afterwards (eliminating the 2.3M-row window sort in TPC-DS Q65). -/
theorem TwoPassGroupAvgThreshold_sound {α K : Type} [DecidableEq K]
    (keyOf : α → K) (valOf : α → Int) (numer denom : Int) (rows : List α) :
    rows.filter (fun r =>
      let sc := groupSumCount keyOf valOf rows (keyOf r)
      avgThresholdHolds numer denom (valOf r) sc.1 sc.2) =
    rows.filter (fun r =>
      avgThresholdHolds numer denom (valOf r)
        (specGroupSum keyOf valOf rows (keyOf r))
        (specGroupCount keyOf rows (keyOf r))) := by
  apply List.filter_congr
  intro r _
  have h := groupSumCount_aux keyOf valOf rows (keyOf r) 0 0
  simp only [Nat.zero_add] at h
  simp only [groupSumCount, specGroupSum, specGroupCount, h]

/-! ## 3. Multi-Channel EXISTS Bitset Pushdown (TPC-DS Q35 / Q10 / Q69) -/

/-- Disjunctive multi-channel `EXISTS`:
    `EXISTS(S) ∧ (EXISTS(W) ∨ EXISTS(K))` equals intersecting `S` into both
    `W` and `K` before taking their union (allows `S`'s surviving customer
    bitset to act as a runtime semi-filter on both `web_sales` and
    `catalog_sales`). -/
theorem MultiChannelExistsBitsetPushdown_sound {α K : Type} [DecidableEq K]
    (keyOf : α → K) (customers : List α) (S W Kset : List K) :
    customers.filter (fun c =>
      let k := keyOf c
      (k ∈ S) && ((k ∈ W) || (k ∈ Kset))) =
    customers.filter (fun c =>
      let k := keyOf c
      let W' := W.filter (fun x => decide (x ∈ S))
      let K' := Kset.filter (fun x => decide (x ∈ S))
      (k ∈ W') || (k ∈ K')) := by
  apply List.filter_congr
  intro c _
  simp only [List.mem_filter, decide_eq_true_eq]
  by_cases hS : keyOf c ∈ S <;> by_cases hW : keyOf c ∈ W <;> by_cases hK : keyOf c ∈ Kset <;>
    simp [hS, hW, hK]

/-- Anti-join multi-channel `EXISTS` (TPC-DS Q69):
    `EXISTS(S) ∧ ¬EXISTS(W) ∧ ¬EXISTS(K)` equals first restricting `W` and `K`
    to `S` and then erasing them from `S`. -/
theorem MultiChannelAntiExistsBitsetPushdown_sound {α K : Type} [DecidableEq K]
    (keyOf : α → K) (customers : List α) (S W Kset : List K) :
    customers.filter (fun c =>
      let k := keyOf c
      (k ∈ S) && !(k ∈ W) && !(k ∈ Kset)) =
    customers.filter (fun c =>
      let k := keyOf c
      let W' := W.filter (fun x => decide (x ∈ S))
      let K' := Kset.filter (fun x => decide (x ∈ S))
      (k ∈ S) && !(k ∈ W') && !(k ∈ K')) := by
  apply List.filter_congr
  intro c _
  simp only [List.mem_filter, decide_eq_true_eq]
  by_cases hS : keyOf c ∈ S <;> by_cases hW : keyOf c ∈ W <;> by_cases hK : keyOf c ∈ Kset <;>
    simp [hS, hW, hK]

/-! ## 4. Composite-Key Semi-Join Chain Reduction (TPC-DS Q25) -/

/-- Three-way equijoin chain `F1 ⋈_{k1 = k2} F2 ⋈_{j2 = j3} F3`
    (models `store_sales ⋈_{(cust,item,ticket)} store_returns ⋈_{(cust,item)} catalog_sales`). -/
def chainJoin3 {A B C K12 K23 : Type} [DecidableEq K12] [DecidableEq K23]
    (k1 : A → K12) (k2 : B → K12) (j2 : B → K23) (j3 : C → K23)
    (f1 : List A) (f2 : List B) (f3 : List C) : List (A × B × C) :=
  f1.flatMap fun a =>
    (f2.filter (fun b => decide (k1 a = k2 b))).flatMap fun b =>
      (f3.filter (fun c => decide (j2 b = j3 c))).map fun c => (a, b, c)

private theorem filter_filter_of_imp {α : Type} (xs : List α) (p q : α → Bool)
    (h : ∀ x, q x = true → p x = true) :
    (xs.filter p).filter q = xs.filter q := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.filter_cons]
      cases hq : q x
      · cases hp : p x
        · exact ih
        · simp [hq, ih]
      · have hp : p x = true := h x hq
        simp [hp, hq, ih]

private theorem flatMap_congr_mem {α β : Type} (xs : List α) (f g : α → List β)
    (h : ∀ x ∈ xs, f x = g x) :
    xs.flatMap f = xs.flatMap g := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.flatMap_cons]
      rw [h x (List.mem_cons_self ..), ih (fun y hy => h y (List.mem_cons_of_mem x hy))]

/-- Two-stage Yannakakis semi-join reduction over composite keys:
    pre-filtering `F2` by `π_{K12}(F1)` to obtain `F2'`, and pre-filtering `F3`
    by `π_{K23}(F2')` to obtain `F3'`, preserves the exact 3-way join output
    (including multiplicities and row order). -/
theorem CompositeKeySemiJoinChainReduction_sound
    {A B C K12 K23 : Type} [DecidableEq K12] [DecidableEq K23]
    (k1 : A → K12) (k2 : B → K12) (j2 : B → K23) (j3 : C → K23)
    (f1 : List A) (f2 : List B) (f3 : List C) :
    let keys12 := f1.map k1
    let f2' := f2.filter (fun b => decide (k2 b ∈ keys12))
    let keys23 := f2'.map j2
    let f3' := f3.filter (fun c => decide (j3 c ∈ keys23))
    chainJoin3 k1 k2 j2 j3 f1 f2' f3' = chainJoin3 k1 k2 j2 j3 f1 f2 f3 := by
  dsimp only [chainJoin3]
  apply flatMap_congr_mem
  intro a ha
  have h2 :
      (f2.filter (fun b => decide (k2 b ∈ f1.map k1))).filter
        (fun b => decide (k1 a = k2 b)) =
      f2.filter (fun b => decide (k1 a = k2 b)) := by
    apply filter_filter_of_imp
    intro b hb
    simp only [decide_eq_true_eq] at hb ⊢
    rw [← hb]
    exact List.mem_map_of_mem ha
  rw [h2]
  apply flatMap_congr_mem
  intro b hb
  simp only [List.mem_filter, decide_eq_true_eq] at hb
  have hb_in_f2' : b ∈ f2.filter (fun b' => decide (k2 b' ∈ f1.map k1)) := by
    simp only [List.mem_filter, decide_eq_true_eq]
    refine ⟨hb.1, ?_⟩
    rw [← hb.2]
    exact List.mem_map_of_mem ha
  have h3 :
      (f3.filter (fun c => decide (j3 c ∈ (f2.filter (fun b' => decide (k2 b' ∈ f1.map k1))).map j2))).filter
        (fun c => decide (j2 b = j3 c)) =
      f3.filter (fun c => decide (j2 b = j3 c)) := by
    apply filter_filter_of_imp
    intro c hc
    simp only [decide_eq_true_eq] at hc ⊢
    rw [← hc]
    exact List.mem_map_of_mem hb_in_f2'
  rw [h3]

/-! ## 5. Left-Outer-Join Right-Side Key Filter Pushdown (TPC-DS Q75) -/

/-- Exact SQL `LEFT OUTER JOIN` over key functions `kL`, `kR`: emits
    `(a, some b)` for every matching `b ∈ R`, and `(a, none)` when no `b ∈ R`
    matches `kL a`. -/
def leftOuterJoin {A B K : Type} [DecidableEq K]
    (kL : A → K) (kR : B → K) (L : List A) (R : List B) : List (A × Option B) :=
  L.flatMap fun a =>
    let matchesR := R.filter (fun b => decide (kL a = kR b))
    if matchesR.isEmpty then [(a, none)]
    else matchesR.map (fun b => (a, some b))

/-- If every row in the left input `L` satisfies a key predicate `p (kL a)`
    (e.g., `i_category = 'Sports'` already restricted `item_sk` on the left
    sales stream in TPC-DS Q75), then pushing `p (kR b)` down onto the right
    table (`catalog_returns` / `store_returns` / `web_returns`) before the
    `LEFT OUTER JOIN` preserves every matched AND every NULL-padded row. -/
theorem LeftJoinRightSideKeyFilterPushdown_sound
    {A B K : Type} [DecidableEq K]
    (kL : A → K) (kR : B → K) (p : K → Bool)
    (L : List A) (R : List B)
    (hL : ∀ a ∈ L, p (kL a) = true) :
    leftOuterJoin kL kR L (R.filter (fun b => p (kR b))) =
      leftOuterJoin kL kR L R := by
  dsimp only [leftOuterJoin]
  apply flatMap_congr_mem
  intro a ha
  have hmatch :
      (R.filter (fun b => p (kR b))).filter (fun b => decide (kL a = kR b)) =
      R.filter (fun b => decide (kL a = kR b)) := by
    apply filter_filter_of_imp
    intro b hb
    simp only [decide_eq_true_eq] at hb
    rw [← hb]
    exact hL a ha
  rw [hmatch]

/-! ## 6. Upper-Bound Sketch Pruning for `HAVING COUNT(*) > T` (ClickBench Q28 / Q32) -/

/-- Exact frequency of key `k` in a stream `xs`. -/
def exactFreq {K : Type} [DecidableEq K] (xs : List K) (k : K) : Nat :=
  (xs.filter (fun x => decide (x = k))).length

/-- Two-pass pruned frequency: first discard any row whose key has an upper
    bound `ub k ≤ threshold` (e.g. from a Count-Min or 4-bit saturating hash
    sketch), then count exact frequencies on the surviving candidate stream. -/
def prunedFreq {K : Type} [DecidableEq K]
    (ub : K → Nat) (threshold : Nat) (xs : List K) (k : K) : Nat :=
  exactFreq (xs.filter (fun x => decide (threshold < ub x))) k

private theorem filter_filter_length_le {α : Type} (xs : List α) (p q : α → Bool) :
    ((xs.filter p).filter q).length ≤ (xs.filter q).length := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.filter_cons]
      cases hp : p x <;> cases hq : q x
      · simp only [Bool.false_eq_true, ite_false]
        exact ih
      · simp only [Bool.false_eq_true, ite_false, ite_true, List.length_cons]
        omega
      · simp only [ite_true, Bool.false_eq_true, ite_false, List.filter_cons, hq]
        exact ih
      · simp only [ite_true, List.filter_cons, hq, List.length_cons]
        omega

/-- For every key `k` that passes `HAVING COUNT(*) > threshold`, any sound
    frequency upper-bound oracle `∀ k, exactFreq xs k ≤ ub k` preserves both
    membership in the `HAVING` winner set and the exact frequency. -/
theorem FrequencyUpperBoundHavingPrune_sound {K : Type} [DecidableEq K]
    (ub : K → Nat) (threshold : Nat) (xs : List K)
    (hub : ∀ k, exactFreq xs k ≤ ub k) (k : K) :
    (if threshold < prunedFreq ub threshold xs k then
       some (prunedFreq ub threshold xs k)
     else none) =
    (if threshold < exactFreq xs k then
       some (exactFreq xs k)
     else none) := by
  by_cases hgt : threshold < exactFreq xs k
  · have hub_k : threshold < ub k := Nat.lt_of_lt_of_le hgt (hub k)
    have hfilt :
        (xs.filter (fun x => decide (threshold < ub x))).filter (fun x => decide (x = k)) =
        xs.filter (fun x => decide (x = k)) := by
      apply filter_filter_of_imp
      intro x hx
      simp only [decide_eq_true_eq] at hx ⊢
      rw [hx]
      exact hub_k
    have heq : prunedFreq ub threshold xs k = exactFreq xs k := by
      simp only [prunedFreq, exactFreq, hfilt]
    simp [heq, hgt]
  · have hle : prunedFreq ub threshold xs k ≤ exactFreq xs k :=
      filter_filter_length_le xs (fun x => decide (threshold < ub x)) (fun x => decide (x = k))
    have hnot : ¬ (threshold < prunedFreq ub threshold xs k) := by omega
    simp [hgt, hnot]

end TinylambOpt.ResearchProofs.SemijoinFiltersAndGroupThresholds

-- C++ references to this model or its proof.
-- executor/detail/build_input.cpp
-- executor/detail/typed_stream_aggregate_direct.cpp
-- executor/relational_group_thresholds.cpp
-- executor/relational_ordered_limits.cpp
-- executor/relational_regression_test_part3.cpp
-- executor/relational_regression_test_part8.cpp
-- query/select_rewrites.cpp
-- query/select_rewrites_test.cpp
