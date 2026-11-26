import TinylambOpt.ResearchGoals.CorrelatedAggPartitionKeyFilterPushdown

/-!
Proved

- `Goal` — Correlated partition-average filter pushdown vs semi-join partition pruning
  (TPC-DS Q01 `s_state='TN'` on `ctr_store_sk`, Q30/Q81 `ca_state='GA'` on
  `c_customer_sk`, Q65): (1) pushing a `partKey`-only predicate into the CTE
  preserves exact averages, (2) for an `entityKey` predicate, pruning unreferenced
  `partKey`s (while retaining all entity rows in active partitions) preserves exact
  averages, and (3) naively pushing an `entityKey` predicate changes partition
  averages.

The reference model is defined in
`TinylambOpt.ResearchGoals.CorrelatedAggPartitionKeyFilterPushdown`.
-/

namespace TinylambOpt.ResearchProofs.CorrelatedAggPartitionKeyFilterPushdown

open TinylambOpt.ResearchGoals.CorrelatedAggPartitionKeyFilterPushdown

/-- One step of the partition `(SUM, COUNT)` fold used by `partSumCnt`. -/
private def aggStep (pk : Int) (acc : Int × Nat) (r : CteRow) : Int × Nat :=
  if r.partKey == pk then (acc.1 + r.val, acc.2 + 1) else acc

/-- Filtering by a predicate that is *true on this partition key* does not
    change the partition aggregate: rows of other partitions contribute
    nothing to the fold, and every row of this partition survives. -/
private theorem filter_foldl_aggStep
    (rows : List CteRow) (f : Int → Bool) (pk : Int) (hf : f pk = true) :
    ∀ acc : Int × Nat,
      (rows.filter (fun r => f r.partKey)).foldl (aggStep pk) acc =
        rows.foldl (aggStep pk) acc := by
  induction rows with
  | nil => intro acc; rfl
  | cons r rows ih =>
      intro acc
      by_cases hr : (r.partKey == pk)
      · have hr_eq : r.partKey = pk := by simpa using hr
        have hfr : f r.partKey = true := by simpa [hr_eq] using hf
        rw [List.filter_cons_of_pos (p := fun x : CteRow => f x.partKey) hfr]
        simp only [List.foldl_cons, aggStep]
        rw [ite_eq_left hr]
        exact ih (acc.1 + r.val, acc.2 + 1)
      · by_cases hfr : f r.partKey = true
        · rw [List.filter_cons_of_pos (p := fun x : CteRow => f x.partKey) hfr]
          simp only [List.foldl_cons, aggStep]
          rw [ite_eq_right hr]
          simpa [aggStep, hr] using ih acc
        · rw [List.filter_cons_of_neg (p := fun x : CteRow => f x.partKey) hfr]
          simpa [aggStep, hr] using ih acc

private theorem partSumCnt_filter_partKey
    (rows : List CteRow) (f : Int → Bool) (pk : Int) (hf : f pk = true) :
    partSumCnt (rows.filter (fun r => f r.partKey)) pk = partSumCnt rows pk := by
  exact filter_foldl_aggStep rows f pk hf (0, 0)

private theorem aboveThreshold_filter_partKey
    (rows : List CteRow) (f : Int → Bool) (num den : Nat) (r : CteRow)
    (hf : f r.partKey = true) :
    aboveThreshold (rows.filter (fun x => f x.partKey)) num den r =
      aboveThreshold rows num den r := by
  unfold aboveThreshold
  rw [partSumCnt_filter_partKey rows f r.partKey hf]

private theorem filter_congr_bool {α : Type} (l : List α) (p q : α → Bool)
    (h : ∀ x ∈ l, p x = q x) : l.filter p = l.filter q := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      have hx := h x (by simp)
      have hxs : ∀ y ∈ xs, p y = q y := fun y hy => h y (by simp [hy])
      rw [List.filter_cons, List.filter_cons, hx]
      simp [ih hxs]

/-- A row that passes `pEntity` lives in an active partition: its `partKey`
    appears in the mapped list of entity-filtered rows. -/
private theorem activeParts_contains_of_mem
    (rows : List CteRow) (pEntity : Int → Bool) (r : CteRow)
    (hr : r ∈ rows) (hp : pEntity r.entityKey = true) :
    ((rows.filter (fun x => pEntity x.entityKey)).map CteRow.partKey).contains r.partKey = true := by
  induction rows with
  | nil => simp at hr
  | cons x xs ih =>
      simp only [List.mem_cons] at hr
      rcases hr with hx | hxs
      · subst hx
        rw [List.filter_cons_of_pos (p := fun y : CteRow => pEntity y.entityKey) hp]
        rw [List.map_cons, List.contains_cons]
        simp
      · by_cases hpx : pEntity x.entityKey = true
        · rw [List.filter_cons_of_pos (p := fun y : CteRow => pEntity y.entityKey) hpx]
          rw [List.map_cons, List.contains_cons]
          by_cases hbeq : (r.partKey == x.partKey)
          · simp [hbeq]
          · simpa [hbeq] using ih hxs
        · rw [List.filter_cons_of_neg (p := fun y : CteRow => pEntity y.entityKey) hpx]
          exact ih hxs

/-- If every row accepted by `outer` has an accepted partition key, filtering
    the partition first preserves the reference correlated query. -/
private theorem partition_filter_query_eq
    (rows : List CteRow) (f : Int → Bool) (outer : CteRow → Bool) (num den : Nat)
    (himpl : ∀ r ∈ rows, outer r = true → f r.partKey = true) :
    (rows.filter (fun r => f r.partKey)).filter
        (fun r => outer r && aboveThreshold (rows.filter (fun x => f x.partKey)) num den r)
      = rows.filter (fun r => outer r && aboveThreshold rows num den r) := by
  rw [List.filter_filter]
  apply filter_congr_bool
  intro r hr
  by_cases ho : outer r = true
  · have hf : f r.partKey = true := himpl r hr ho
    rw [aboveThreshold_filter_partKey rows f num den r hf]
    simp [ho, hf]
  · simp [ho]

theorem sound : Goal := by
  constructor
  · intro rows pPart num den
    unfold optPushPartFilter refQuery
    rw [List.filter_filter]
    apply filter_congr_bool
    intro r hr
    by_cases hp : pPart r.partKey = true
    · rw [aboveThreshold_filter_partKey rows pPart num den r hp]
      simp [hp]
    · simp [hp]
  constructor
  · intro rows pEntity num den
    unfold optSemiJoinActiveParts refQuery
    let activeParts : List Int :=
      (rows.filter (fun r => pEntity r.entityKey)).map CteRow.partKey
    simpa [activeParts] using
      (partition_filter_query_eq rows (fun pk => activeParts.contains pk)
        (fun r : CteRow => pEntity r.entityKey) num den
        (fun r hr he => activeParts_contains_of_mem rows pEntity r hr he))
  · let rows : List CteRow :=
      [ { partKey := 1, entityKey := 10, val := 100 }
      , { partKey := 1, entityKey := 20, val := 200 }
      , { partKey := 2, entityKey := 20, val := 300 }
      ]
    let pEntity : Int → Bool := fun ek => ek == 10
    refine ⟨rows, pEntity, 8, 10, ?_⟩
    decide

end TinylambOpt.ResearchProofs.CorrelatedAggPartitionKeyFilterPushdown
