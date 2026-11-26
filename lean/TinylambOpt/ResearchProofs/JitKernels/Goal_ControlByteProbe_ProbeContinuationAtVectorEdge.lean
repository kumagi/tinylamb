import TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ControlByteProbe_ProbeContinuationAtVectorEdge

theorem sound : TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe.ProbeContinuationAtVectorEdge.Goal := by
  have hu : ∀ (a b : List TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe.Slot),
      TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe.untilEmpty (a ++ b) =
        TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe.untilEmpty a ++
          (if a.contains .empty then [] else
            TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe.untilEmpty b) := by
    intro a
    induction a with
    | nil => intro b; simp [TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe.untilEmpty]
    | cons x xs ih =>
        intro b
        cases x <;>
          simp [TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe.untilEmpty] <;>
          simpa [TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe.untilEmpty] using ih b
  intro a b k
  unfold TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe.probe
  rw [hu]
  by_cases h : a.contains .empty
  · have hp : TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe.Slot.empty ∈ a := by simpa using h
    simp [hp]
  · have hp : TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe.Slot.empty ∉ a := by simpa using h
    simp [hp]

end TinylambOpt.ResearchProofs.JitKernels.Goal_ControlByteProbe_ProbeContinuationAtVectorEdge
