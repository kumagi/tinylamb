import TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

/-! Tombstones never return a payload and are not vacancies, so deleting them
before the prefix scan leaves both the scan prefix and the first exact match
unchanged. SQL may compact deleted lanes in registers without changing the
looked-up value. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.ControlByteProbe.Goal_TombstoneErasureInRegister

open TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

theorem sound : TombstoneErasureInRegister.Goal := by
  intro xs k
  induction xs with
  | nil => simp [probe, untilEmpty]
  | cons s xs ih =>
      unfold probe at ih ⊢
      cases s with
      | empty => simp [untilEmpty]
      | tomb => simpa [untilEmpty, value, List.findSome?_cons] using ih
      | live t kt v =>
          unfold untilEmpty at ih
          cases hval : value k (.live t kt v) with
          | none => simpa [untilEmpty, hval, List.findSome?_cons] using ih
          | some val => simp [untilEmpty, hval]

end TinylambOpt.ResearchProofs.JitKernels.ControlByteProbe.Goal_TombstoneErasureInRegister
