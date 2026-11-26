import TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

/-!
Refutes `MinMaxTieKeepsLeftBits.Goal`: replacing the left-biased update
`if a ≤ b then a else b` by the strict test `if a < b then a else b` changes
observable bits on the `a = b` tie when the two sides are the two signed
zeros. With `a = +0.0` and `b = -0.0` (both finite): IEEE says `+0.0 ≤ -0.0`
is true, so the left-biased min keeps `a = +0.0`; but `+0.0 < -0.0` is false,
so the right-biased select returns `b = -0.0`. The bit patterns differ; all
witness terms are closed ground expressions checked by kernel `decide`.
Vectorized MIN updates must keep the non-strict comparison if lane provenance
is observable through bit-exact output.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.JitKernels.FloatingReduction_MinMaxTieKeepsLeftBits
open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

private theorem left_form :
    (if (0 : Float) ≤ (-0.0) then (0 : Float) else (-0.0)) = (0 : Float) := by decide
private theorem right_form :
    (if (0 : Float) < (-0.0) then (0 : Float) else (-0.0)) = (-0.0 : Float) := by decide
private theorem h' : (0 : Float).toBits ≠ (-0.0 : Float).toBits := by decide

theorem refute : ¬ MinMaxTieKeepsLeftBits.Goal := by
  intro h
  have h0 : (0 : Float).isFinite = true := by decide
  have key : same (if (0 : Float) ≤ (-0.0) then (0 : Float) else (-0.0))
                 (if (0 : Float) < (-0.0) then (0 : Float) else (-0.0)) :=
    h 0 (-0.0) h0 h0
  rw [left_form, right_form] at key
  exact h' key

end TinylambOpt.ResearchProofs.JitKernels.FloatingReduction_MinMaxTieKeepsLeftBits
