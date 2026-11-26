import TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SharedVocabulary.Goal_ComposedDictionaryRemap

open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary


/-! Scratch proof for the ComposedDictionaryRemap resource-policy goal. It records the
relationship between dictionary ids and SQL-visible scan results. -/


open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary.ComposedDictionaryRemap.Goal := by
  intro xs others
  simp only [ids, List.map_map]
  apply List.map_congr_left
  intro x hx
  simp only [Function.comp_apply]
  have hxv : x ∈ vocab xs := List.mem_eraseDups.mpr hx
  have hidx : (vocab xs).idxOf x < (vocab xs).length := List.idxOf_lt_length_of_mem hxv
  rw [List.getElem?_map, List.getElem?_eq_getElem hidx]
  simp [List.getElem_idxOf hidx]
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SharedVocabulary.Goal_ComposedDictionaryRemap
