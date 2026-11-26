import TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies

namespace TinylambOpt.ResearchProofs.ResourcePolicies.CatalogDependencies.Goal_PositiveOnlyBinderCache

open TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies


/-!
Name-resolution caching cannot ignore names that were missing at prepare time:
a fresh catalog can introduce a binding for a name that previously failed to
resolve. The counterexample keeps every prepared successful binding stable and
still changes the signature, so negative dependencies must also be checked
before reusing a binder cache entry.
-/

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.PositiveOnlyBinderCache.Goal := by
  intro h
  have hEq := h [] [⟨1, 10, 0⟩] [1] (by
    intro n hn hsome
    simp [TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.bind] at hsome)
  simp [TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.signature,
    TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.bind] at hEq
end TinylambOpt.ResearchProofs.ResourcePolicies.CatalogDependencies.Goal_PositiveOnlyBinderCache
