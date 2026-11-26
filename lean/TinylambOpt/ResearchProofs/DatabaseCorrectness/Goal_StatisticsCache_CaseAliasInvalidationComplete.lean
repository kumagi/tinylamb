import TinylambOpt.ResearchGoals.DatabaseCorrectness.StatisticsCache

/-! Case-alias invalidation removes every matching table-name binding.

`purge` filters out entries whose lower-cased key matches the DDL target, so
an alias with the same lower-cased spelling cannot survive. SQL execution may
invalidate DDL targets case-insensitively, including duplicate context-cache
entries, without leaving a stale statistics binding. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.StatisticsCache_CaseAliasInvalidationComplete

open TinylambOpt.ResearchGoals.DatabaseCorrectness.StatisticsCache

theorem sound : CaseAliasInvalidationComplete.Goal := by
  intro xs n alias h
  cases hlk : (purge xs n).lookup alias with
  | none => rfl
  | some v =>
      exfalso
      rcases (List.lookup_eq_some_iff.mp hlk) with ⟨l1, l2, heq, _⟩
      have hmem : (alias, v) ∈ purge xs n := by
        rw [heq]
        simp
      rw [purge, List.mem_filter] at hmem
      have hfalse : (alias.toLower != n.toLower) = true := hmem.2
      simp [h] at hfalse

end TinylambOpt.ResearchProofs.DatabaseCorrectness.StatisticsCache_CaseAliasInvalidationComplete
