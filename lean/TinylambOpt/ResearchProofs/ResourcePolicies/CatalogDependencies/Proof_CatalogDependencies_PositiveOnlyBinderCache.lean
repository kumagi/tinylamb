import TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_CatalogDependencies_PositiveOnlyBinderCache
open TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies
open TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.PositiveOnlyBinderCache
theorem refute : ¬ Goal := by
  intro h
  let c : Column := ⟨0, 0, 0⟩
  have := h ([] : Schema) [c] [0] (by intro n hn hsome; simp [TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.bind] at hsome)
  simp [signature, TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.bind, c] at this
end Nov26.Proof_CatalogDependencies_PositiveOnlyBinderCache
