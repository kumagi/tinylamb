import TinylambOpt.ResearchGoals.JitKernels.InListLowering

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_InListLowering_NullEntryFilterElision

theorem sound : TinylambOpt.ResearchGoals.JitKernels.InListLowering.NullEntryFilterElision.Goal := by
  intro xs
  induction xs with
  | nil =>
      intro v
      simp [TinylambOpt.ResearchGoals.JitKernels.InListLowering.keep,
        TinylambOpt.ResearchGoals.JitKernels.InListLowering.test]
  | cons y ys ih =>
      cases y with
      | none =>
          intro v
          cases v with
          | none =>
              simp [TinylambOpt.ResearchGoals.JitKernels.InListLowering.keep,
                TinylambOpt.ResearchGoals.JitKernels.InListLowering.test]
          | some val =>
              by_cases hmem : some val ∈ ys
              · have hna : ¬ (∀ a ∈ ys, a = none) := by
                  intro hall
                  have := hall (some val) hmem
                  simp at this
                simp [TinylambOpt.ResearchGoals.JitKernels.InListLowering.keep,
                  TinylambOpt.ResearchGoals.JitKernels.InListLowering.test, hmem, hna]
              · simp [TinylambOpt.ResearchGoals.JitKernels.InListLowering.keep,
                  TinylambOpt.ResearchGoals.JitKernels.InListLowering.test, hmem]
      | some z =>
          intro v
          cases v with
          | none =>
              simp [TinylambOpt.ResearchGoals.JitKernels.InListLowering.keep,
                TinylambOpt.ResearchGoals.JitKernels.InListLowering.test]
          | some val =>
              by_cases hz : z = val
              · subst z
                simp [TinylambOpt.ResearchGoals.JitKernels.InListLowering.keep,
                  TinylambOpt.ResearchGoals.JitKernels.InListLowering.test]
              · have hz' : val ≠ z := fun h => hz h.symm
                by_cases hmem : some val ∈ ys <;>
                  simp [TinylambOpt.ResearchGoals.JitKernels.InListLowering.keep,
                    TinylambOpt.ResearchGoals.JitKernels.InListLowering.test, hz, hz', hmem]

end TinylambOpt.ResearchProofs.JitKernels.Goal_InListLowering_NullEntryFilterElision
