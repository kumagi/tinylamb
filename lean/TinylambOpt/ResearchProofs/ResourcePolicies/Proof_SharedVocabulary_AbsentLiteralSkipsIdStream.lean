import TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary

/-!
`AbsentLiteralSkipsIdStream`: if an equality literal is absent from the shared
vocabulary, no row can match it, so the scan can skip the entire id stream.
The proof turns "some filtered row survives" into membership in the vocabulary.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.SharedVocabulary_AbsentLiteralSkipsIdStream
open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary
open TinylambOpt.ResearchGoals.Compression.Wire

theorem sound : AbsentLiteralSkipsIdStream.Goal := by
  intro xs lit h
  apply List.eq_nil_iff_forall_not_mem.mpr
  intro x hx
  obtain ⟨hin, hbeq⟩ := List.mem_filter.mp hx
  have hx' : lit ∈ xs := by
    have hxeq : x = lit := by simpa using hbeq
    exact hxeq ▸ hin
  exact h (List.mem_eraseDups.mpr hx')

end TinylambOpt.ResearchProofs.ResourcePolicies.SharedVocabulary_AbsentLiteralSkipsIdStream
