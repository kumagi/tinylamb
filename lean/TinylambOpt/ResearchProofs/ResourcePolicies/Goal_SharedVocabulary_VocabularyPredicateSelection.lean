import TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary
/-!
Registers the checked result for `SharedVocabulary.VocabularyPredicateSelection.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/

/-!
`VocabularyPredicateSelection`: a predicate evaluated once per dictionary entry
drives a selection vector over the whole row stream: looking the per-entry result up
by the row's id returns exactly the predicate applied to the row's symbol, for
arbitrary repeating byte strings.  Evaluation can therefore be memoized at
dictionary granularity.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SharedVocabulary_VocabularyPredicateSelection
open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary
open TinylambOpt.ResearchGoals.Compression.Wire

theorem sound : VocabularyPredicateSelection.Goal := by
  intro xs p
  simp only [ids, List.map_map]
  apply List.map_congr_left
  intro x hx
  simp only [Function.comp_apply]
  have hmem : x ∈ vocab xs := List.mem_eraseDups.mpr hx
  have hidx : (vocab xs).idxOf x < (vocab xs).length := List.idxOf_lt_length_of_mem hmem
  have hidx' : (vocab xs).idxOf x < ((vocab xs).map p).length := by
    rw [List.length_map]
    exact hidx
  rw [List.getElem?_eq_getElem hidx', List.getElem_map, List.getElem_idxOf hidx]
  rfl

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SharedVocabulary_VocabularyPredicateSelection
