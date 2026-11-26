import TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_CatalogDependencies_SignatureImpliesOrdinalReuse
open TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies
open TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.SignatureImpliesOrdinalReuse
theorem refute : ¬ Goal := by
  intro h
  let c : Column := ⟨0, 0, 0⟩
  let d : Column := ⟨1, 1, 0⟩
  let old : Schema := [c, d]
  let fresh : Schema := [d, c]
  have hsig : signature old [0] = signature fresh [0] := by
    native_decide
  have := h old fresh [0] hsig 0 (by simp)
  revert this
  native_decide
end Nov26.Proof_CatalogDependencies_SignatureImpliesOrdinalReuse
