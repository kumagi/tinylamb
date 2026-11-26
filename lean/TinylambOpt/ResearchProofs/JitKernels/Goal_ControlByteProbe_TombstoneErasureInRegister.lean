import TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ControlByteProbe_TombstoneErasureInRegister

open TinylambOpt.ResearchGoals.JitKernels

theorem sound : TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe.TombstoneErasureInRegister.Goal := by
  intro xs k
  let p : ControlByteProbe.Slot → Bool := fun x => x != .tomb
  have htake : ControlByteProbe.untilEmpty (xs.filter p) = (ControlByteProbe.untilEmpty xs).filter p := by
    induction xs with
    | nil => simp [ControlByteProbe.untilEmpty]
    | cons x xs ih =>
        cases x
        · simp [ControlByteProbe.untilEmpty, p]
        · simpa [ControlByteProbe.untilEmpty, p] using ih
        · simpa [ControlByteProbe.untilEmpty, p] using ih
  have hfind_all : ∀ ys : List ControlByteProbe.Slot,
      List.findSome? (ControlByteProbe.value k) (ys.filter p) =
        List.findSome? (ControlByteProbe.value k) ys := by
    intro ys
    induction ys with
    | nil => simp [List.findSome?]
    | cons x ys ih =>
        cases x <;> simp [List.findSome?, ControlByteProbe.value, p, ih]
  have hfind := hfind_all (ControlByteProbe.untilEmpty xs)
  simpa [ControlByteProbe.probe] using congrArg (List.findSome? (ControlByteProbe.value k)) htake |>.trans hfind

end TinylambOpt.ResearchProofs.JitKernels.Goal_ControlByteProbe_TombstoneErasureInRegister
