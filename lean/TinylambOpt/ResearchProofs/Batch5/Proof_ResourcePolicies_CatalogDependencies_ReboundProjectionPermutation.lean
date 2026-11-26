import TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CatalogDependencies.Goal_ReboundProjectionPermutation

open TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies


/-!
Equal signature bindings determine equal column values when values are keyed by stable column identity, even if physical ordinals differ; rebinding by identity therefore preserves projected values.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
open TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies
open TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.ReboundProjectionPermutation
theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.ReboundProjectionPermutation.Goal := by
  intro old fresh names value hsig
  unfold project signature at *
  apply List.map_congr_left
  intro n hn
  have hbind : TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.bind old n = TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.bind fresh n := by
    have hidx : names.idxOf n < names.length := List.idxOf_lt_length_of_mem hn
    have := congrArg (fun l => l[names.idxOf n]?) hsig
    simp only [List.getElem?_map, List.getElem?_eq_getElem hidx, List.getElem_idxOf hidx] at this
    simpa using this
  simp [hbind]
  cases h : TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.bind fresh n with
  | none => simp
  | some c =>
    simp
    have hmemOld : c ∈ old := by
      have : TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.bind old n = some c := by rw [hbind, h]
      unfold TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.bind at this
      exact List.mem_of_find?_eq_some this
    have hmemFresh : c ∈ fresh := by
      unfold TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.bind at h
      exact List.mem_of_find?_eq_some h
    have hidxOld : old.idxOf c < old.length := List.idxOf_lt_length_of_mem hmemOld
    have hidxFresh : fresh.idxOf c < fresh.length := List.idxOf_lt_length_of_mem hmemFresh
    rw [List.getElem?_eq_getElem (by simpa using hidxOld), List.getElem?_eq_getElem (by simpa using hidxFresh)]
    simp [List.getElem_map, List.getElem_idxOf hidxOld, List.getElem_idxOf hidxFresh]
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CatalogDependencies.Goal_ReboundProjectionPermutation
