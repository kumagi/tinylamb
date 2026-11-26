import TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

/-! Independent proof record for the LiteralSkip.SparseAnchorCostChoice research goal: the proposed rewrite is unsound and this file exhibits a counterexample. -/

/-!
Whole-haystack byte frequencies do not order the number of position-valid
anchor candidates: each anchor only sees a window of starts of length
`s.length + 1 - p.length`. With needle `[0,1,0,0]` and haystack `[0,0,1,1,1]`,
byte 0 occurs twice and byte 1 three times, yet the first byte yields two
candidates and the second only one.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace Nov26.Proof_LiteralSkip_SparseAnchorCostChoice
open TinylambOpt.ResearchGoals.JitKernels.LiteralSkip
open TinylambOpt.ResearchGoals.JitKernels.LiteralSkip.SparseAnchorCostChoice
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

theorem refute : ¬ Goal := by
  intro h
  have t := h ([0, 1, 0, 0] : B) ([0, 0, 1, 1, 1] : B) 0 1 0 1
    (by decide) (by decide) (by decide)
  have hbad : ¬ ((cand ([0, 1, 0, 0] : B) ([0, 0, 1, 1, 1] : B) 0).length ≤
      (cand ([0, 1, 0, 0] : B) ([0, 0, 1, 1, 1] : B) 1).length) := by
    decide
  exact hbad t

end Nov26.Proof_LiteralSkip_SparseAnchorCostChoice
