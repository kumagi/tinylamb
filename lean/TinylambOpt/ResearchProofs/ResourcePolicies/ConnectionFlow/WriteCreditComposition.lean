import TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.ConnectionFlow_WriteCreditComposition
open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

/-- Coalescing two socket-progress callbacks (`remaining b` after `remaining a`)
is the same as a single callback with the summed credit `remaining (a+b)`, so the
server may batch write notifications without changing the unwritten reply. -/
theorem sound : WriteCreditComposition.Goal := by
  intro fs
  induction fs with
  | nil => intro a b; simp [remaining]
  | cons f fs ih =>
    intro a b
    simp only [remaining]
    by_cases ha : a < f.length
    · rw [if_pos ha]
      simp only [remaining]
      by_cases hb : b < (f.drop a).length
      · rw [if_pos hb, if_pos (by rw [List.length_drop] at hb; omega)]
        rw [← List.drop_drop]
      · rw [if_neg hb, if_neg (by rw [List.length_drop] at hb; omega)]
        rw [List.length_drop]
        rw [show b - (f.length - a) = a + b - f.length by omega]
    · rw [if_neg ha, ih, if_neg (by omega)]
      rw [show (a - f.length) + b = a + b - f.length by omega]

end TinylambOpt.ResearchProofs.ResourcePolicies.ConnectionFlow_WriteCreditComposition
