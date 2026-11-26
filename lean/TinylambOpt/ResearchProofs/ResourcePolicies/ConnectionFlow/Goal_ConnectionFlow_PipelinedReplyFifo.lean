import TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ConnectionFlow_PipelinedReplyFifo
open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

/-- Private restatement of `PartialSendWireSuffix`: a partial write of `n` bytes
trims exactly `n` bytes from the flattened frame stream. Used below to reason
about pipelined replies. -/
private theorem rem_flatten : ∀ (fs : Frames) (n : Nat),
    (remaining n fs).flatten = fs.flatten.drop n := by
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

/-- Appending a pipelined SQL reply `b` behind a partially written reply `a`
preserves FIFO order: as long as only `n ≤ |a|` bytes have been sent, the
unwritten stream is the rest of `a` followed by all of `b` (including its
ReadyForQuery suffix). -/
theorem sound : PipelinedReplyFifo.Goal := by
  intro a b n hn
  rw [rem_flatten (a ++ b) n, rem_flatten a n, List.flatten_append, List.drop_append]
  rw [Nat.sub_eq_zero_of_le hn, List.drop_zero]

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ConnectionFlow_PipelinedReplyFifo
