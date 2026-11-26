import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.KeyRoutedJoinStates.Goal_SegmentOrderClosureBeforeLineProbe

open TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates


/-! The eligible-customer closure of orders is exactly the set of order keys
    with nonzero join weight. Filtering line probes by that closure therefore
    drops only probes with zero output multiplicity and preserves the join sum. -/

open TinylambOpt.ResearchGoals.QueryRewrites

/-- A key with a nonzero eligible join weight must occur in at least one order
    whose customer is eligible. -/
theorem nonzeroWeight_implies_support (customers : List Nat) (orders : List (Nat × Nat)) (l : Nat)
    (h : ((forKey orders l).map customers.count).sum ≠ 0) :
    l ∈ (orders.filter (fun r => customers.contains r.2)).map Prod.fst := by
  induction orders with
  | nil => simp [forKey] at h
  | cons o os ih =>
      by_cases hk : o.1 == l
      · have hkeq : o.1 = l := beq_iff_eq.mp hk
        by_cases hc : customers.contains o.2 = true
        · apply List.mem_map.mpr
          refine ⟨o, ?_, hkeq⟩
          exact (List.mem_filter).mpr ⟨by simp, hc⟩
        · have hcnot : o.2 ∉ customers := by
            intro hm
            exact hc ((List.contains_iff_mem).mpr hm)
          have hcnt : customers.count o.2 = 0 := (List.count_eq_zero).mpr hcnot
          have htail : ((forKey os l).map customers.count).sum ≠ 0 := by
            simpa [forKey, hk, hcnt] using h
          rcases (List.mem_map.mp (ih htail)) with ⟨a, ha, hal⟩
          have ha' : a ∈ os.filter (fun r => decide (r.2 ∈ customers)) := by
            simpa [List.contains_iff_mem] using ha
          apply List.mem_map.mpr
          refine ⟨a, ?_, hal⟩
          rw [List.filter_cons]
          simp [hcnot, ha']
      · have htail : ((forKey os l).map customers.count).sum ≠ 0 := by
          simpa [forKey, hk] using h
        rcases (List.mem_map.mp (ih htail)) with ⟨a, ha, hal⟩
        have ha' : a ∈ os.filter (fun r => decide (r.2 ∈ customers)) := by
          simpa [List.contains_iff_mem] using ha
        apply List.mem_map.mpr
        refine ⟨a, ?_, hal⟩
        rw [List.filter_cons]
        by_cases hmemO : o.2 ∈ customers
        · simp [hmemO, ha']
        · simp [hmemO, ha']

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.SegmentOrderClosureBeforeLineProbe.Goal := by
  intro customers orders lines
  dsimp only
  let weight := fun order => ((forKey orders order).map customers.count).sum
  let support := (orders.filter (fun r => customers.contains r.2)).map Prod.fst
  change ((lines.filter support.contains).map weight).sum = (lines.map weight).sum
  induction lines with
  | nil => simp
  | cons l ls ih =>
      rw [List.filter_cons]
      by_cases h : support.contains l = true
      · have hmem : l ∈ support := (List.contains_iff_mem).mp h
        simp [hmem, ih]
      · have hnot : l ∉ support := by
          intro hm
          exact h ((List.contains_iff_mem).mpr hm)
        have hw : weight l = 0 := by
          dsimp [weight]
          by_cases hz : ((forKey orders l).map customers.count).sum = 0
          · exact hz
          · exact False.elim (hnot (nonzeroWeight_implies_support customers orders l hz))
        simp [hnot, hw, ih]
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.KeyRoutedJoinStates.Goal_SegmentOrderClosureBeforeLineProbe
