import TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

/-! A vector group may hand off to the next group only when its own prefix has
neither a match nor a vacancy. The scalar probe satisfies the stated
continuation law, so the hand-off condition is exact and preserves the returned
payload. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.ControlByteProbe.Goal_ProbeContinuationAtVectorEdge

open TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

theorem sound : ProbeContinuationAtVectorEdge.Goal := by
  intro a b k
  induction a with
  | nil => simp [probe, untilEmpty]
  | cons s a ih =>
      unfold probe at ih ⊢
      cases s with
      | empty =>
          simp [untilEmpty]
      | tomb =>
          simpa [untilEmpty, value, List.findSome?_cons] using ih
      | live t kt v =>
          cases hval : value k (.live t kt v) with
          | none => simpa [untilEmpty, hval] using ih
          | some x => simp [untilEmpty, hval]

end TinylambOpt.ResearchProofs.JitKernels.ControlByteProbe.Goal_ProbeContinuationAtVectorEdge
