import TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

/-! A tombstone is not a vacant terminator. A probe of an empty prefix fails,
yet a match placed after the tombstone is still found, so the assumption does
not license stopping the search at the tombstone. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.ControlByteProbe.Goal_DeletedSlotAsProbeFence

open TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

theorem refute : ¬ DeletedSlotAsProbeFence.Goal := by
  intro h
  have hw := h [] [.live 1 9 7] 9
  simp [probe, untilEmpty, value] at hw

end TinylambOpt.ResearchProofs.JitKernels.ControlByteProbe.Goal_DeletedSlotAsProbeFence
