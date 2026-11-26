import TinylambOpt.ResearchGoals.DatabaseCorrectness.StatisticsCache

/-! A failed catalog fetch can be retried on the same thread.

The failed lookup leaves a cache pinned to the reader's view with no entry for
the requested key. A later successful oracle answer therefore populates that
key instead of being shadowed by a cached absence. SQL execution may retry a
statistics fetch after a transient catalog failure without changing results. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.StatisticsCache_CatalogLookupFailureCanRetry

open TinylambOpt.ResearchGoals.DatabaseCorrectness.StatisticsCache

theorem sound : CatalogLookupFailureCanRetry.Goal := by
  intro before after c view key value hfail hsuccess
  dsimp [lookup] at hfail ⊢
  by_cases hs : (c.view == view) = true
  · rw [ite_eq_left hs] at hfail ⊢
    cases hlk : c.entries.lookup key with
    | some hit =>
        simp [hlk] at hfail
    | none =>
        cases hbefore : before view key with
        | none =>
            simp [hlk, hbefore, hsuccess]
        | some v =>
            simp [hlk, hbefore] at hfail
  · rw [ite_eq_right hs] at hfail ⊢
    simp at hfail ⊢
    cases hbefore : before view key with
    | none =>
        simp [hbefore]
        simp [hs, hsuccess]
    | some v =>
        simp [hbefore] at hfail

end TinylambOpt.ResearchProofs.DatabaseCorrectness.StatisticsCache_CatalogLookupFailureCanRetry
