import TinylambOpt.ResearchGoals.SinglePassMultiConsumerFactScan

/-!
Proved

- `Goal` — Multi-consumer single-pass fact scan sharing (TPC-DS Q14 `cross_items` +
  `avg_sales` + branches, Q44 dual-RANK `store_sales`, Q65 `sb`+`sc` `store_sales`):
  dispatching one scan with union predicate `p1 || p2 || p3` across heterogeneous
  consumers produces identical outputs while bounding row reads to `rows.length` vs
  `3 * rows.length`.

The reference model is defined in
`TinylambOpt.ResearchGoals.SinglePassMultiConsumerFactScan`.
-/

namespace TinylambOpt.ResearchProofs.SinglePassMultiConsumerFactScan

open TinylambOpt.ResearchGoals.SinglePassMultiConsumerFactScan

/-- The single-pass dispatch used by `optSinglePass`. -/
private def dispatch (out : ThreeOutputs) (r : Row) : ThreeOutputs :=
  { distinctKeys := stepC1 out.distinctKeys r
    globalSumCnt := stepC2 out.globalSumCnt r
    groupedSumCnt := stepC3 out.groupedSumCnt r
    reads := out.reads }

/-- If `f` is the identity on rows rejected by `p`, then filtering with the
    weaker predicate `u` (which accepts everything `p` accepts) before folding
    yields the same result as filtering with `p`. -/
private theorem foldl_filter_of_imp {α β : Type}
    (p u : α → Bool) (f : β → α → β)
    (h_id : ∀ z x, p x = false → f z x = z)
    (hpu : ∀ x, p x = true → u x = true) :
    ∀ (xs : List α) (init : β),
      (xs.filter u).foldl f init = (xs.filter p).foldl f init := by
  intro xs
  induction xs with
  | nil => intro init; rfl
  | cons a xs ih =>
      intro init
      by_cases hpa : p a = true
      · have hua : u a = true := hpu a hpa
        simpa [hpa, hua] using ih (f init a)
      · have hpaf : p a = false := by cases hp : p a <;> simp_all
        by_cases hua : u a = true
        · calc
            List.foldl f init ((a :: xs).filter u) = List.foldl f (f init a) (xs.filter u) := by simp [hua]
            _ = List.foldl f init (xs.filter u) := by rw [h_id init a hpaf]
            _ = List.foldl f init (xs.filter p) := ih init
            _ = List.foldl f init ((a :: xs).filter p) := by simp [hpaf]
        · calc
            List.foldl f init ((a :: xs).filter u) = List.foldl f init (xs.filter u) := by simp [hua]
            _ = List.foldl f init (xs.filter p) := ih init
            _ = List.foldl f init ((a :: xs).filter p) := by simp [hpaf]

private theorem stepC1_of_not_p1 (xs : List Int) (r : Row) (h : r.p1 = false) :
    stepC1 xs r = xs := by
  simp [stepC1, h]

private theorem stepC2_of_not_p2 (sc : Int × Nat) (r : Row) (h : r.p2 = false) :
    stepC2 sc r = sc := by
  simp [stepC2, h]

private theorem stepC3_of_not_p3 (m : List (Int × (Int × Nat))) (r : Row)
    (h : r.p3 = false) : stepC3 m r = m := by
  simp [stepC3, h]

/-- Each field of the dispatched fold is exactly the fold of the corresponding
    consumer step over the same list. -/
private theorem foldl_dispatch_distinctKeys (xs : List Row) (init : ThreeOutputs) :
    (xs.foldl dispatch init).distinctKeys = xs.foldl stepC1 init.distinctKeys := by
  induction xs generalizing init with
  | nil => rfl
  | cons a xs ih => simp [dispatch, ih]

private theorem foldl_dispatch_globalSumCnt (xs : List Row) (init : ThreeOutputs) :
    (xs.foldl dispatch init).globalSumCnt = xs.foldl stepC2 init.globalSumCnt := by
  induction xs generalizing init with
  | nil => rfl
  | cons a xs ih => simp [dispatch, ih]

private theorem foldl_dispatch_groupedSumCnt (xs : List Row) (init : ThreeOutputs) :
    (xs.foldl dispatch init).groupedSumCnt = xs.foldl stepC3 init.groupedSumCnt := by
  induction xs generalizing init with
  | nil => rfl
  | cons a xs ih => simp [dispatch, ih]

private theorem foldl_dispatch_reads (xs : List Row) (init : ThreeOutputs) :
    (xs.foldl dispatch init).reads = init.reads := by
  induction xs generalizing init with
  | nil => rfl
  | cons a xs ih => simp [dispatch, ih]

private theorem opt_distinct (rows : List Row) :
    (optSinglePass rows).distinctKeys = (rows.filter Row.p1).foldl stepC1 [] := by
  unfold optSinglePass
  change ((rows.filter (fun r : Row => r.p1 || r.p2 || r.p3)).foldl dispatch
    ({ reads := rows.length } : ThreeOutputs)).distinctKeys =
      (rows.filter Row.p1).foldl stepC1 []
  rw [foldl_dispatch_distinctKeys]
  exact foldl_filter_of_imp Row.p1 (fun r : Row => r.p1 || r.p2 || r.p3)
    stepC1 stepC1_of_not_p1 (by intro r hr; simp [hr]) rows []

private theorem opt_global (rows : List Row) :
    (optSinglePass rows).globalSumCnt = (rows.filter Row.p2).foldl stepC2 (0, 0) := by
  unfold optSinglePass
  change ((rows.filter (fun r : Row => r.p1 || r.p2 || r.p3)).foldl dispatch
    ({ reads := rows.length } : ThreeOutputs)).globalSumCnt =
      (rows.filter Row.p2).foldl stepC2 (0, 0)
  rw [foldl_dispatch_globalSumCnt]
  exact foldl_filter_of_imp Row.p2 (fun r : Row => r.p1 || r.p2 || r.p3)
    stepC2 stepC2_of_not_p2 (by intro r hr; simp [hr]) rows (0, 0)

private theorem opt_grouped (rows : List Row) :
    (optSinglePass rows).groupedSumCnt = (rows.filter Row.p3).foldl stepC3 [] := by
  unfold optSinglePass
  change ((rows.filter (fun r : Row => r.p1 || r.p2 || r.p3)).foldl dispatch
    ({ reads := rows.length } : ThreeOutputs)).groupedSumCnt =
      (rows.filter Row.p3).foldl stepC3 []
  rw [foldl_dispatch_groupedSumCnt]
  exact foldl_filter_of_imp Row.p3 (fun r : Row => r.p1 || r.p2 || r.p3)
    stepC3 stepC3_of_not_p3 (by intro r hr; simp [hr]) rows []

private theorem opt_reads (rows : List Row) : (optSinglePass rows).reads = rows.length := by
  unfold optSinglePass
  change ((rows.filter (fun r : Row => r.p1 || r.p2 || r.p3)).foldl dispatch
    ({ reads := rows.length } : ThreeOutputs)).reads = rows.length
  rw [foldl_dispatch_reads]

theorem sound : TinylambOpt.ResearchGoals.SinglePassMultiConsumerFactScan.Goal := by
  intro rows
  dsimp only
  rw [opt_distinct, opt_global, opt_grouped]
  simp [refThreePass, opt_reads]
  omega

end TinylambOpt.ResearchProofs.SinglePassMultiConsumerFactScan
