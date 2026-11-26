import TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CatalogDependencies.Goal_WildcardProjectionSignature

open TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies


/-!
Filter dependency signatures say nothing about wildcard output columns: two
catalogs with identical predicate bindings may have different physical column
sets, so `SELECT *` must not share a prepared projection cache entry merely
because its filters match.
-/

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.WildcardProjectionSignature.Goal := by
  intro h
  have hEq := h [] [⟨1, 10, 0⟩] [] (by
    simp [TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.signature])
  simp at hEq
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CatalogDependencies.Goal_WildcardProjectionSignature
