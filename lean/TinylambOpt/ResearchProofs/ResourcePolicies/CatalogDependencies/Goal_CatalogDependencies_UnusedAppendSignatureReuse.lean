import TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CatalogDependencies_UnusedAppendSignatureReuse
open TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies
open TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.UnusedAppendSignatureReuse
theorem sound : Goal := by
  intro s names c hc
  unfold signature
  apply List.map_congr_left
  intro n hn
  have hne : (c.name == n) = false := by
    by_cases h : c.name = n
    · subst h; exact (hc hn).elim
    · simp [h]
  simp [TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.bind, List.find?_append, hne]
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CatalogDependencies_UnusedAppendSignatureReuse
