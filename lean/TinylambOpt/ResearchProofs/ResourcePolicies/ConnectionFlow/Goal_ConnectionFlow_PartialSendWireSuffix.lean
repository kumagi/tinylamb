import TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ConnectionFlow_PartialSendWireSuffix
open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

/-- A partial write of `n` bytes leaves exactly the last `total - n` bytes of the
serialized frame stream: retiring `remaining n fs` as a wire suffix preserves the
byte content `fs.flatten.drop n`, so SQL servers may free already-acked frames
without changing the unwritten reply. -/
theorem sound : PartialSendWireSuffix.Goal := by
  intro fs
  induction fs with
  | nil => intro n; simp [remaining]
  | cons f fs ih =>
    intro n
    simp only [remaining, List.flatten_cons]
    by_cases hn : n < f.length
    · rw [if_pos hn]
      simp only [List.flatten_cons]
      rw [List.drop_append, Nat.sub_eq_zero_of_le (Nat.le_of_lt hn), List.drop_zero]
    · rw [if_neg hn, ih, List.drop_append,
        List.drop_eq_nil_of_le (Nat.le_of_not_lt hn), List.nil_append]

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ConnectionFlow_PartialSendWireSuffix
