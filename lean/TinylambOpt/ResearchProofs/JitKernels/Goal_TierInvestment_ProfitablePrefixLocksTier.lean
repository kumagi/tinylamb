import TinylambOpt.ResearchGoals.JitKernels.TierInvestment

/-! A profitable prefix does not lock in tier-up: a later failing batch with
    a large replay fee and no rows can make the compiled suffix more expensive. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_TierInvestment_ProfitablePrefixLocksTier

open TinylambOpt.ResearchGoals.JitKernels.TierInvestment

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.TierInvestment.ProfitablePrefixLocksTier.Goal := by
  intro h
  let t : Tariff := { compile := 0, guardFee := 0, slow := 1, fast := 0, replay := 100 }
  let a : List Batch := [{ rows := 1, passes := true }]
  let b : List Batch := [{ rows := 0, passes := false }]
  have hprofit : bill t a 0 < bill t a a.length := by native_decide
  have hh := h t a b hprofit
  have hne : ¬ bill t (a++b) 0 ≤ bill t (a++b) (a++b).length := by native_decide
  exact hne hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_TierInvestment_ProfitablePrefixLocksTier
