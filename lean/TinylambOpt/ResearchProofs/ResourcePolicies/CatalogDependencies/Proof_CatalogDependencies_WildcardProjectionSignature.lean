import TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_CatalogDependencies_WildcardProjectionSignature
open TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies
open TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.WildcardProjectionSignature
theorem refute : ¬ Goal := by
  intro h
  let c1 : Column := ⟨0, 1, 0⟩
  let c2 : Column := ⟨0, 2, 0⟩
  have hsig : signature [c1] [1] = signature [c2] [1] := by
    simp [signature, TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.bind, c1, c2]
  have := h [c1] [c2] [1] hsig
  simp [c1, c2] at this
end Nov26.Proof_CatalogDependencies_WildcardProjectionSignature
