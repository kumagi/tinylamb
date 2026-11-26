import TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies

namespace TinylambOpt.ResearchProofs.ResourcePolicies.CatalogDependencies.Goal_SignatureImpliesOrdinalReuse

open TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies


/-!
Equal name bindings do not determine physical ordinals: inserting an unrelated
column before a bound column shifts its row offset while leaving the signature
of the queried names unchanged. Ordinal-specialized executors therefore need a
schema-position check in addition to a signature check.
-/

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.SignatureImpliesOrdinalReuse.Goal := by
  intro h
  have hEq := h [⟨1, 10, 0⟩] [⟨2, 20, 0⟩, ⟨1, 10, 0⟩] [1] (by
    simp [TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.signature,
      TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.bind])
  have hOrd := hEq 1 (by simp)
  simp [TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.bind] at hOrd
  have hidx : List.idxOf (⟨1, 10, 0⟩ : TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.Column)
      [⟨2, 20, 0⟩, ⟨1, 10, 0⟩] = 1 := by decide
  omega
end TinylambOpt.ResearchProofs.ResourcePolicies.CatalogDependencies.Goal_SignatureImpliesOrdinalReuse
