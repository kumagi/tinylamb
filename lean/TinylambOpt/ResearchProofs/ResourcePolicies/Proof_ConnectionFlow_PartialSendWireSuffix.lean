import TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

namespace TinylambOpt.ResearchProofs.ResourcePolicies.ConnectionFlow.Goal_PartialSendWireSuffix

open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow


/-! Soundness of the partial-send wire suffix model: a socket acknowledgement of
`n` bytes retires complete reply frames and trims exactly one frame, so the
remaining frames flatten to the original byte stream with those `n` bytes
removed. No fairness or socket-service bound is needed. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace PswsProof

open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

/-- Inductive invariant: `remaining` consumes exactly the first `n` flattened
bytes, whether the cut lands inside a frame or on a frame boundary. -/
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

end PswsProof

open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow.PartialSendWireSuffix.Goal :=
  PswsProof.remaining_flatten
end TinylambOpt.ResearchProofs.ResourcePolicies.ConnectionFlow.Goal_PartialSendWireSuffix
