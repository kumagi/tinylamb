import TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary

namespace TinylambOpt.ResearchProofs.ResourcePolicies.SharedVocabulary.VocabularyPredicateSelection

open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary

theorem sound : VocabularyPredicateSelection.Goal := by
  intro xs p
  simp only [ids, List.map_map]
  apply List.map_congr_left
  intro x hx
  have hxv : x ∈ vocab xs := List.mem_eraseDups.mpr hx
  have hidx : (vocab xs).idxOf x < (vocab xs).length := List.idxOf_lt_length_of_mem hxv
  simp only [Function.comp_apply, List.getElem?_map, List.getElem?_eq_getElem hidx,
    List.getElem_idxOf hidx, Option.map_some, Option.getD_some]

end TinylambOpt.ResearchProofs.ResourcePolicies.SharedVocabulary.VocabularyPredicateSelection
