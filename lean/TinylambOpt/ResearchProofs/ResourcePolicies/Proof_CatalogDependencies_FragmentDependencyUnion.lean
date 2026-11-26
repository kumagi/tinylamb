import TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies

namespace TinylambOpt.ResearchProofs.ResourcePolicies.CatalogDependencies.Goal_FragmentDependencyUnion

open TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies


/-!
Per-fragment dependency equality composes through the deduplicated union: every name in the union comes from one fragment, so fragment-local binder checks are equivalent to one global check.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
open TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies
open TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.FragmentDependencyUnion

private theorem bind_eq_of_signature_eq {old fresh : Schema} {names : List Nat}
    (h : signature old names = signature fresh names) :
    ∀ n ∈ names, TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.bind old n = TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.bind fresh n := by
  intro n hn
  have hidx : names.idxOf n < names.length := List.idxOf_lt_length_of_mem hn
  have := congrArg (fun l => l[names.idxOf n]?) h
  simp only [signature, List.getElem?_map, List.getElem?_eq_getElem hidx, List.getElem_idxOf hidx] at this
  simpa using this

private theorem signature_eq_of_bind_eq {old fresh : Schema} {names : List Nat}
    (h : ∀ n ∈ names, TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.bind old n = TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.bind fresh n) :
    signature old names = signature fresh names := by
  unfold signature
  apply List.map_congr_left
  intro n hn
  exact h n hn

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.FragmentDependencyUnion.Goal := by
  intro old fresh a b
  constructor
  · intro h
    have hall : ∀ n ∈ (a ++ b).eraseDups, TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.bind old n = TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies.bind fresh n :=
      bind_eq_of_signature_eq h
    constructor
    · apply signature_eq_of_bind_eq
      intro n hn
      apply hall
      exact (List.mem_eraseDups).2 (List.mem_append_left b hn)
    · apply signature_eq_of_bind_eq
      intro n hn
      apply hall
      exact (List.mem_eraseDups).2 (List.mem_append_right a hn)
  · intro h
    rcases h with ⟨ha, hb⟩
    apply signature_eq_of_bind_eq
    intro n hn
    have hn' : n ∈ a ++ b := (List.mem_eraseDups).1 hn
    rcases List.mem_append.1 hn' with hna | hnb
    · exact bind_eq_of_signature_eq ha n hna
    · exact bind_eq_of_signature_eq hb n hnb
end TinylambOpt.ResearchProofs.ResourcePolicies.CatalogDependencies.Goal_FragmentDependencyUnion
