import TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_CatalogDependencies_DistinctBindingChecks
open TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies
open TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.DistinctBindingChecks
theorem sound : Goal := by
  intro schema names
  dsimp
  apply List.map_congr_left
  intro n hn
  have hmem : n ∈ names.eraseDups := (List.mem_eraseDups).mpr hn
  have hidx : names.eraseDups.idxOf n < (signature schema names.eraseDups).length := by
    have h1 : names.eraseDups.idxOf n < names.eraseDups.length := List.idxOf_lt_length_of_mem hmem
    simpa [signature] using h1
  have hget : (signature schema names.eraseDups)[names.eraseDups.idxOf n]? = some (TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.bind schema n) := by
    rw [List.getElem?_eq_getElem hidx]
    simp [signature, List.getElem_map, List.getElem_idxOf (List.idxOf_lt_length_of_mem hmem)]
  simp [hget]
end Nov26.Proof_CatalogDependencies_DistinctBindingChecks
