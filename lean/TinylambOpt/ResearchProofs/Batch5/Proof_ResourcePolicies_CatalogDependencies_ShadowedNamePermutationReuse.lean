import TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CatalogDependencies.Goal_ShadowedNamePermutationReuse

open TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies


/-!
Because lookups use the first visible binding, permuting two catalog columns
that share a name changes the resolved column even though the catalog contains
exactly the same columns. A permutation invariant cannot justify binder reuse.
-/

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.ShadowedNamePermutationReuse.Goal := by
  intro h
  have hEq := h [⟨1, 10, 0⟩, ⟨1, 20, 0⟩] [⟨1, 20, 0⟩, ⟨1, 10, 0⟩] [1] (by
    exact List.Perm.swap _ _ _)
  simp [TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.signature,
    TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.bind] at hEq
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CatalogDependencies.Goal_ShadowedNamePermutationReuse
