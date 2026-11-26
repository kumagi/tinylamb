import TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary

/-!
`ComposedDictionaryRemap`: a row-group dictionary need not survive compaction after
its ids have been translated once into the shared dictionary.  Remapping every local
id through the shared numbering of the local dictionary equals taking the shared id
of the row directly, so the per-group dictionary and its indirection table can be
dropped after the rewrite.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.SharedVocabulary_ComposedDictionaryRemap
open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary
open TinylambOpt.ResearchGoals.Compression.Wire

theorem sound : ComposedDictionaryRemap.Goal := by
  intro xs others
  simp only [ids, List.map_map]
  apply List.map_congr_left
  intro x hx
  simp only [Function.comp_apply]
  have hmem : x ∈ vocab xs := List.mem_eraseDups.mpr hx
  have hidx : (vocab xs).idxOf x < (vocab xs).length := List.idxOf_lt_length_of_mem hmem
  have hidx' : (vocab xs).idxOf x < ((vocab xs).map (vocab (others ++ xs)).idxOf).length := by
    rw [List.length_map]
    exact hidx
  rw [List.getElem?_eq_getElem hidx', List.getElem_map, List.getElem_idxOf hidx]

end TinylambOpt.ResearchProofs.ResourcePolicies.SharedVocabulary_ComposedDictionaryRemap
