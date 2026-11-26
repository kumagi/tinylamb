import TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary

/-!
`DictionaryInsertLocalReprice`: inserting one entry into a front-coded dictionary
only re-prices that entry and its immediate successor.  The telescoping sum `chain`
splits at the insertion point, so after cancelling the common `chain next tail` and
`cost prev next` terms the identity is just commutativity of addition.  A dictionary
builder may evaluate the benefit of an insertion from three local costs alone.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.SharedVocabulary_DictionaryInsertLocalReprice
open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary
open TinylambOpt.ResearchGoals.Compression.Wire

theorem sound : DictionaryInsertLocalReprice.Goal := by
  intro prev entry next tail
  simp only [chain, cost, front]
  omega

end TinylambOpt.ResearchProofs.ResourcePolicies.SharedVocabulary_DictionaryInsertLocalReprice
