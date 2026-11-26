import TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies

namespace TinylambOpt.ResearchProofs.ResourcePolicies.CatalogDependencies.Goal_UnusedAppendSignatureReuse

open TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies


/-!
Appending a column whose name is not referenced by a query cannot change any
name lookup in that query's projection signature. This is the catalog condition
under which a prepared binder result may be reused after a suffix DDL append.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.UnusedAppendSignatureReuse.Goal := by
  intro s names c hc
  unfold TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.signature
  apply List.map_congr_left
  intro n hn
  have hne : (c.name == n) = false := by
    by_cases h : c.name = n
    · subst h
      exact (hc hn).elim
    · simp [h]
  simp [TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.bind, List.find?_append, hne]
end TinylambOpt.ResearchProofs.ResourcePolicies.CatalogDependencies.Goal_UnusedAppendSignatureReuse
