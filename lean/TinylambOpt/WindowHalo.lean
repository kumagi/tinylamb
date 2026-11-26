import TinylambOpt.SetJoinAndScalarRewrites

/-!
# ROWS-frame halo cutting (`executor/detail/window_eval.cpp`)

The reference evaluator walks one partition in its stable total order (the
snapshot is fixed) and computes every window value from the *values* of the
rows in the ROWS frame `p-a .. p+b`.  The optimized plan keeps only the halo
slice `[i-a, min n (j+b))` of the partition and the output positions `[i,j)`.

Rows carry their original 0-based position, which is exactly the `base`
offset the physical window operator receives when it is run on the slice;
hence "local position + offset = original position".  Under this model the
frame of every surviving output row is *literally the same list of original
row numbers*, so any aggregate that depends only on frame values (SUM, AVG,
MIN, MAX, COUNT, ...) is preserved.  Position-dependent functions such as
ROW_NUMBER are excluded by the shape of `agg`.

C++ checks: `WindowRuntime::ResolveFrame` in `executor/detail/window_eval.cpp`
validates the constant non-negative ROWS offsets and clamps the frame to the
partition; the planner rewrite supplying `i, j` must use the same partition
order and snapshot (`plan/cascades.cpp`, `plan/optimizer.cpp`).
-/

namespace TinylambOpt.WindowHalo

/-- A partition row together with its position in the stable total order. -/
abbrev IndexedRow (Row : Type) := Nat × Row

/-- The snapshot of one partition: rows paired with positions `0 .. n-1`. -/
def indexedPartition {Row : Type} (R : List Row) : List (IndexedRow Row) :=
  (List.range R.length).zip R

private theorem mem_indexed_pos {Row : Type} (R : List Row) :
    ∀ x ∈ indexedPartition R, x.1 < R.length := by
  induction R with
  | nil => simp [indexedPartition]
  | cons r rs ih =>
      intro x hx
      rw [indexedPartition, List.length_cons, List.range_succ_eq_map,
          List.zip_cons_cons, List.zip_map_left] at hx
      rcases List.mem_cons.mp hx with h | h
      · rcases h with rfl
        simp
      · rcases List.mem_map.mp h with ⟨⟨yi, yr⟩, hy, rfl⟩
        have hypos : yi < rs.length := ih (yi, yr) (by simpa [indexedPartition] using hy)
        simp only [Prod.map, id_eq, List.length_cons]
        omega

/-- The frame of row `p`: positions `[p-a, p+b]` clamped by the partition
    boundary (clamping is automatic because only present rows are filtered). -/
def frameRows {Row : Type} (a b p : Nat) (T : List (IndexedRow Row)) : List Row :=
  (T.filter (fun q => p - a ≤ q.1 ∧ q.1 ≤ p + b)).map (fun q => q.2)

/-- Reference / sliced window evaluation: every row is annotated with the
    aggregate of the values in its frame. -/
def evalWindow {Row V O : Type} (a b : Nat) (valOf : Row → V) (agg : List V → O)
    (T : List (IndexedRow Row)) : List (Nat × Row × O) :=
  T.map (fun q => (q.1, q.2, agg ((frameRows a b q.1 T).map valOf)))

/-- Helper: filtering a map whose predicate only inspects the source. -/
private theorem evalWindow_filter {Row V O : Type} (a b i j : Nat)
    (valOf : Row → V) (agg : List V → O) (T : List (IndexedRow Row)) :
    (evalWindow a b valOf agg T).filter (fun x => i ≤ x.1 ∧ x.1 < j) =
      (T.filter (fun q => i ≤ q.1 ∧ q.1 < j)).map
        (fun q => (q.1, q.2, agg ((frameRows a b q.1 T).map valOf))) := by
  unfold evalWindow
  rw [List.filter_map]
  simp [Function.comp_def]

/-- If `pout` implies `pin`, filtering by both is the same as filtering by
    `pout` alone. -/
private theorem filter_superset {α : Type} (T : List α) (pin pout : α → Bool)
    (h : ∀ x ∈ T, pout x = true → pin x = true) :
    (T.filter pin).filter pout = T.filter pout := by
  rw [List.filter_filter]
  apply List.filter_congr
  intro x hx
  by_cases hp : pout x = true
  · have hi : pin x = true := h x hx hp
    simp [hp, hi]
  · have hf : pout x = false := Bool.eq_false_iff.mpr hp
    simp [hf]

/-- ✅ **`window_halo_sound`**: for output positions `[i,j)` and a bounded
    `ROWS BETWEEN a PRECEDING AND b FOLLOWING` frame, replacing the partition
    by the halo slice `[i-a, min n (j+b))` leaves every emitted `(row, value)`
    unchanged.  No extra hypothesis is needed once the rows are the same
    snapshot in the same stable order: for `p ∈ [i,j)` the frame positions
    satisfy `i-a ≤ p-a ≤ q ≤ p+b < j+b` and `q < n`, hence they all survive
    the halo cut.  Empty output, clipped head/tail frames and partition edges
    are covered by the arithmetic. -/
theorem window_halo_sound {Row V O : Type} (a b i j : Nat)
    (R : List Row) (valOf : Row → V) (agg : List V → O) :
    let T := indexedPartition R
    let s := i - a
    let e := min R.length (j + b)
    let H := T.filter (fun q => s ≤ q.1 ∧ q.1 < e)
    (evalWindow a b valOf agg T).filter (fun x => i ≤ x.1 ∧ x.1 < j) =
      (evalWindow a b valOf agg H).filter (fun x => i ≤ x.1 ∧ x.1 < j) := by
  dsimp only
  let T := indexedPartition R
  let s := i - a
  let e := min R.length (j + b)
  let inHalo : IndexedRow Row → Prop := fun q => s ≤ q.1 ∧ q.1 < e
  let outPred : IndexedRow Row → Prop := fun q => i ≤ q.1 ∧ q.1 < j
  let H := T.filter (fun q => inHalo q)
  have hfilter : H.filter (fun q => outPred q) = T.filter (fun q => outPred q) := by
    dsimp [H]
    apply filter_superset T (fun q => decide (inHalo q)) (fun q => decide (outPred q))
    intro q hqT hqout
    have hqpos : q.1 < R.length := by
      exact mem_indexed_pos R q (by simpa [T] using hqT)
    have hqout' : i ≤ q.1 ∧ q.1 < j := of_decide_eq_true hqout
    have hz : s ≤ q.1 ∧ q.1 < e := by
      dsimp [s, e]
      constructor <;> omega
    simpa [inHalo] using hz
  have hframe : ∀ q ∈ T, i ≤ q.1 ∧ q.1 < j →
      frameRows a b q.1 H = frameRows a b q.1 T := by
    intro q hqT hqout
    dsimp [frameRows]
    apply congrArg (fun L : List (IndexedRow Row) => L.map (fun z => z.2))
    dsimp only [H]
    apply filter_superset T (fun z => decide (inHalo z))
      (fun z => decide (q.1 - a ≤ z.1 ∧ z.1 ≤ q.1 + b))
    intro z hzT hzframe
    have hzpos : z.1 < R.length := by
      exact mem_indexed_pos R z (by simpa [T] using hzT)
    have hqout' : i ≤ q.1 ∧ q.1 < j := hqout
    have hzframe' : q.1 - a ≤ z.1 ∧ z.1 ≤ q.1 + b := of_decide_eq_true hzframe
    have hz : s ≤ z.1 ∧ z.1 < e := by
      dsimp [s, e]
      constructor <;> omega
    simpa [inHalo] using hz
  rw [evalWindow_filter a b i j valOf agg T,
      evalWindow_filter a b i j valOf agg H]
  rw [hfilter]
  apply List.map_congr_left
  intro q hq
  have hqT : q ∈ T := (List.mem_filter.mp hq).1
  have hqout : i ≤ q.1 ∧ q.1 < j := by
    simpa using (List.mem_filter.mp hq).2
  have hf := hframe q hqT hqout
  simp [hf]

end TinylambOpt.WindowHalo

-- C++ references to this model or its proof.
-- executor/detail/window_eval.cpp
-- plan/cascades.cpp
