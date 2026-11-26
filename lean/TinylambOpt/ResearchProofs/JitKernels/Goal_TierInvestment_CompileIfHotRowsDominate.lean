import TinylambOpt.ResearchGoals.JitKernels.TierInvestment

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_TierInvestment_CompileIfHotRowsDominate

open TinylambOpt.ResearchGoals.JitKernels.TierInvestment

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.TierInvestment.CompileIfHotRowsDominate.Goal := by
  intro h
  let t : Tariff := ⟨1, 100, 2, 1, 0⟩
  let xs : List Batch := [⟨2, true⟩, ⟨1, false⟩]
  have hfast : t.fast < t.slow := by native_decide
  have hcomp : t.compile ≤ (xs.map Batch.rows).sum := by native_decide
  have hhot : (xs.filter Batch.passes |>.map Batch.rows).sum * 2 > (xs.map Batch.rows).sum := by
    native_decide
  have hres := h t xs hfast hcomp hhot
  have hbad : ¬ bill t xs 0 ≤ bill t xs xs.length := by native_decide
  exact hbad hres

end TinylambOpt.ResearchProofs.JitKernels.Goal_TierInvestment_CompileIfHotRowsDominate
