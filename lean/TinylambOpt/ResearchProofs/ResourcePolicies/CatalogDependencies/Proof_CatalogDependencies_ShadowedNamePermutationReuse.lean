import TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_CatalogDependencies_ShadowedNamePermutationReuse
open TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies
open TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.ShadowedNamePermutationReuse
theorem refute : ¬ Goal := by
  intro h
  let c1 : Column := ⟨0, 1, 0⟩
  let c2 : Column := ⟨0, 2, 0⟩
  have hp : ([c1, c2] : Schema).Perm [c2, c1] := by
    simpa using (List.Perm.swap c2 c1 ([] : Schema))
  have := h [c1, c2] [c2, c1] [0] hp
  simp [signature, TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.bind, c1, c2] at this
end Nov26.Proof_CatalogDependencies_ShadowedNamePermutationReuse
