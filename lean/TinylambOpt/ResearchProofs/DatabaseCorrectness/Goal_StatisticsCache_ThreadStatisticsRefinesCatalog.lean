import TinylambOpt.ResearchGoals.DatabaseCorrectness.StatisticsCache

/-! Thread-local statistics lookup agrees with the catalog oracle.

The cache is only reused for an identical `(owner, epoch, snapshot)` view; a
cache hit carries a value previously justified by `faithful`, and a miss is
filled from the oracle. The next cache therefore remains faithful, so SQL
execution may reuse or refill entries without changing the answer a reader of
the same view would obtain from the catalog. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.StatisticsCache_ThreadStatisticsRefinesCatalog

open TinylambOpt.ResearchGoals.DatabaseCorrectness.StatisticsCache

theorem sound : ThreadStatisticsRefinesCatalog.Goal := by
  intro oracle c view key hf
  dsimp [lookup]
  by_cases hs : c.view == view
  · rw [ite_eq_left hs]
    have hview : c.view = view := beq_iff_eq.mp hs
    cases hlk : c.entries.lookup key with
    | some hit =>
        simp [hlk]
        constructor
        · have h0 := hf key hit hlk
          simpa [hview] using h0.symm
        · intro k v hv
          have h0 := hf k v hv
          simpa [hview] using h0
    | none =>
        cases hork : oracle view key with
        | none =>
            simp [hork]
            intro k v hv
            have h0 := hf k v hv
            simpa [hview] using h0
        | some newv =>
            simp [hork]
            intro k' v' hv
            by_cases hkeq : k' = key
            · subst k'
              simp at hv
              rcases hv with hv'
              subst v'
              exact hork
            · have hbeq : (k' == key) = false := by simp [hkeq]
              have hvtail : c.entries.lookup k' = some v' := by
                rw [List.lookup_cons, hbeq] at hv
                exact hv
              have h0 := hf k' v' hvtail
              simpa [hview] using h0
  · rw [ite_eq_right hs]
    simp only [List.lookup_nil]
    cases hork : oracle view key with
    | none =>
        simp [hork]
        intro k v hv
        simp at hv
    | some newv =>
        simp [hork]
        intro k v' hv
        simp at hv
        rcases hv with ⟨rfl, rfl⟩
        exact hork

end TinylambOpt.ResearchProofs.DatabaseCorrectness.StatisticsCache_ThreadStatisticsRefinesCatalog
