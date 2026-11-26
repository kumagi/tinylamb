import TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CatalogDependencies.Goal_AppendOnlyOrdinalProjection

open TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies


/-!
Appending unrelated catalog columns preserves first-match bindings and their physical ordinals, so an ordinal-specialized projection may reuse its offsets when the old names all resolved and the row prefix is unchanged.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
open TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies
open TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.AppendOnlyOrdinalProjection
theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.AppendOnlyOrdinalProjection.Goal := by
  intro schema suffix names row added hlen hall
  unfold project
  apply List.map_congr_left
  intro n hn
  have hsome : (TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.bind schema n).isSome = true := hall n hn
  cases hfind : TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.bind schema n with
  | none => simp [hfind] at hsome
  | some c =>
    have hmem : c ∈ schema := by
      unfold TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.bind at hfind
      exact List.mem_of_find?_eq_some hfind
    have hbindApp : TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.bind (schema ++ suffix) n = some c := by
      unfold TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.bind at hfind ⊢
      rw [List.find?_append]
      simp [hfind]
    simp [hbindApp]
    have hidx : (schema ++ suffix).idxOf c = schema.idxOf c := by
      clear hlen hall names added row n hn hsome hfind hbindApp
      induction schema generalizing c with
      | nil => cases hmem
      | cons hd tl ih =>
        simp only [List.cons_append, List.idxOf_cons]
        by_cases hhd : hd = c
        · simp [hhd]
        · have hmemtl : c ∈ tl := by
            have : c = hd ∨ c ∈ tl := by simpa using hmem
            rcases this with rfl | hm
            · exact (hhd rfl).elim
            · exact hm
          simp [hhd, ih c hmemtl]
    have hidxLt : schema.idxOf c < row.length := by
      have := List.idxOf_lt_length_of_mem hmem
      omega
    rw [hidx, List.getElem?_append_left hidxLt]
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CatalogDependencies.Goal_AppendOnlyOrdinalProjection
