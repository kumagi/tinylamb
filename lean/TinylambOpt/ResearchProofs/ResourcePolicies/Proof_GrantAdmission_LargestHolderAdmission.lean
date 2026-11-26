import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.GrantAdmission_LargestHolderAdmission
open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

/-- Refuted: releasing the largest holder first is not always optimal once
scratch demands are heterogeneous. With `jobs = [⟨held 10, scratch 8⟩, ⟨held 5,
scratch 3⟩]` the held-descending merge-sort order is the list itself and needs
`headroom = max 8 (max 3 0 - 10) = 8` initial free bytes; but the permutation
`[⟨held 5, scratch 3⟩, ⟨held 10, scratch 8⟩]` needs only `max 3 (8 - 5) = 3`.
Since `8 ≤ 3` is false, the heuristic overshoots an achievable order. -/
theorem refute : ¬ LargestHolderAdmission.Goal := by
  intro h
  have c := h [⟨10, 8⟩, ⟨5, 3⟩] [⟨5, 3⟩, ⟨10, 8⟩] (List.Perm.swap _ _ [])
  rw [List.mergeSort_pair, if_pos (by decide)] at c
  exact absurd c (by decide)

end TinylambOpt.ResearchProofs.ResourcePolicies.GrantAdmission_LargestHolderAdmission
