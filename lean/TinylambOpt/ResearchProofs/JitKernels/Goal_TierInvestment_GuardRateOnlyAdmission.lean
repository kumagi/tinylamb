import TinylambOpt.ResearchGoals.JitKernels.TierInvestment

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_TierInvestment_GuardRateOnlyAdmission

/-- Two traces with the same length, row total and passing-batch count can put
those rows on opposite sides of the guard, changing the compiled bill. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.TierInvestment.GuardRateOnlyAdmission.Goal := by
  intro h
  let t : TinylambOpt.ResearchGoals.JitKernels.TierInvestment.Tariff := ⟨0, 0, 1, 2, 0⟩
  let a : List TinylambOpt.ResearchGoals.JitKernels.TierInvestment.Batch :=
    [⟨10, true⟩, ⟨0, false⟩]
  let b : List TinylambOpt.ResearchGoals.JitKernels.TierInvestment.Batch :=
    [⟨0, true⟩, ⟨10, false⟩]
  have c := h t a b (by native_decide) (by native_decide) (by native_decide)
  have bad :
      TinylambOpt.ResearchGoals.JitKernels.TierInvestment.bill t a 0 ≠
        TinylambOpt.ResearchGoals.JitKernels.TierInvestment.bill t b 0 := by
    native_decide
  exact bad c

end TinylambOpt.ResearchProofs.JitKernels.Goal_TierInvestment_GuardRateOnlyAdmission
