import TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.ConnectionFlow.Goal_PipelinedReplyFifo

open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow


/-! FIFO preservation for pipelined replies: an outstanding write of `n` bytes
into the first batch `a` does not reorder a later reply batch `b`, so the
unwritten bytes are the suffix of `a` followed by all of `b` (including its
ReadyForQuery control frame). -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace PrfProof

open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

/-- Reuse the byte-accurate partial-send invariant of the wire-suffix model. -/
theorem remaining_flatten : ∀ (fs : Frames) (n : Nat),
    (remaining n fs).flatten = fs.flatten.drop n
  | [], _ => by simp [remaining]
  | f :: fs, n => by
      by_cases h : n < f.length
      · rw [remaining, ite_eq_left_iff.mpr (fun hn => (hn h).elim)]
        simp only [List.flatten_cons]
        rw [List.drop_append_of_le_length (Nat.le_of_lt h)]
      · rw [remaining, ite_eq_right_iff.mpr (fun hp => (h hp).elim), remaining_flatten]
        simp only [List.flatten_cons]
        rw [List.drop_append]
        have hle : f.length ≤ n := Nat.le_of_not_gt h
        simp [hle]

end PrfProof

open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow.PipelinedReplyFifo.Goal := by
  intro a b n hn
  rw [PrfProof.remaining_flatten, PrfProof.remaining_flatten]
  simp only [List.flatten_append]
  rw [List.drop_append_of_le_length hn]
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.ConnectionFlow.Goal_PipelinedReplyFifo
