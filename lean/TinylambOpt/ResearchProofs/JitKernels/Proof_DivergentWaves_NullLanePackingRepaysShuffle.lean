import TinylambOpt.ResearchGoals.JitKernels.DivergentWaves
/-! Certificate for `TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.NullLanePackingRepaysShuffle.Goal`. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! Refutation: compacting zero-work lanes does not pay for a shuffle per packet.
With a single zero lane the original wave already costs no work, while the
shuffle budget is one, so the claimed inequality fails. -/

namespace Nov26.Proof_DivergentWaves_NullLanePackingRepaysShuffle

open TinylambOpt.ResearchGoals.JitKernels.DivergentWaves
open TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.NullLanePackingRepaysShuffle

theorem refute : ¬ Goal := by
  intro h
  have hbad := h [0] 1 (by omega) (by simp)
  have hc : ¬ ticks 1 ([0].filter (fun x => x != 0)) + (waves 1 [0]).length ≤ ticks 1 [0] := by
    native_decide
  exact hc hbad

end Nov26.Proof_DivergentWaves_NullLanePackingRepaysShuffle
