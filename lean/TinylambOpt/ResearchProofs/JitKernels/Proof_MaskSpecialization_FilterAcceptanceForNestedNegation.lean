import TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization

/-! Independent proof record for the MaskSpecialization.FilterAcceptanceForNestedNegation research goal: the proposed rewrite is unsound and this file exhibits a counterexample. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace Nov26.Proof_MaskSpecialization_FilterAcceptanceForNestedNegation
open TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization
open TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization.FilterAcceptanceForNestedNegation
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization
open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA

theorem refute : ¬ Goal := by
  intro h
  have hh := h [] (.lit .null) (.lit .no)
  simp [answer, run, TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA.flip] at hh
  have hn : (V.null == V.yes) = false := by decide
  have hno : (V.no == V.yes) = false := by decide
  have hy : (V.yes == V.yes) = true := by decide
  simp [hn, hno, hy] at hh

end Nov26.Proof_MaskSpecialization_FilterAcceptanceForNestedNegation
