import TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization

/-! Independent proof record for the MaskSpecialization.TraceSetAsResultCacheKey research goal: the proposed rewrite is unsound and this file exhibits a counterexample. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace Nov26.Proof_MaskSpecialization_TraceSetAsResultCacheKey
open TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization
open TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization.TraceSetAsResultCacheKey
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization
open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA

theorem refute : ¬ Goal := by
  intro h
  have hh := h [.yes] [.no] (.arg 0)
  simp [answer, reads, run] at hh

end Nov26.Proof_MaskSpecialization_TraceSetAsResultCacheKey
