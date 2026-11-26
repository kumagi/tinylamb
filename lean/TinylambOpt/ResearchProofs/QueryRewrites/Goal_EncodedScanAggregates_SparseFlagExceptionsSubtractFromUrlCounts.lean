import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

set_option linter.unusedSimpArgs false
set_option linter.deprecated false



/-!
# SparseFlagExceptionsSubtractFromUrlCounts

For a fixed URL, rows carrying either exception flag form a subset of all rows
for that URL. Therefore subtracting the union of the two flagged sets from the
URL count leaves exactly the rows with both flags cleared. The executor may
maintain one URL counter and subtract sparse exception counters instead of
keeping a separate survivor counter.
-/


namespace TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_SparseFlagExceptionsSubtractFromUrlCounts

open TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates



set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.QueryRewrites

/-- The flagged rows for a URL are a submultiset of that URL's rows. -/
theorem exception_filter_le (rs : List (String × Bool × Bool)) (u : String) :
    (rs.filter (fun x => x.fst == u && (x.2.fst || x.2.snd))).length ≤ (rs.map Prod.fst).count u := by
  induction rs with
  | nil => simp
  | cons r rs ih =>
      rcases r with ⟨v, a, b⟩
      simp at ih ⊢
      by_cases hv : v == u
      · have hvu : v = u := beq_iff_eq.mp hv
        subst v
        by_cases ha : a <;> by_cases hb : b <;> simp [ha, hb] at ih ⊢ <;> omega
      · have hvu : v ≠ u := by intro h; apply hv; simp [h]
        simp [hvu] at ih ⊢
        exact ih

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.SparseFlagExceptionsSubtractFromUrlCounts.Goal := by
  intro rs u
  induction rs with
  | nil => simp [countWhere]
  | cons r rs ih =>
      rcases r with ⟨v, a, b⟩
      simp [countWhere] at ih ⊢
      by_cases hv : v == u
      · have hvu : v = u := beq_iff_eq.mp hv
        subst v
        by_cases ha : a <;> by_cases hb : b <;> simp [ha, hb] at ih ⊢
        · exact ih
        · exact ih
        · exact ih
        · have hle : (rs.filter (fun x => x.fst == u && (x.2.fst || x.2.snd))).length ≤
              (rs.map Prod.fst).count u := exception_filter_le rs u
          rw [Nat.succ_sub hle, ih]
      · have hvu : v ≠ u := by intro h; apply hv; simp [h]
        simp [hvu] at ih ⊢
        exact ih
end TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_SparseFlagExceptionsSubtractFromUrlCounts
