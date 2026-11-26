import TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

/-!
Refutes `AbsentLaneAsPositiveZero.Goal`: padding masked-off vector lanes with
positive zero before an ordered fold is NOT always bit-identical to skipping
the absent lanes, because each pad adds one extra IEEE addition. Take
`s = -0.0` and `xs = [none]`: the padded total is `(-0.0) + (+0.0) = +0.0`
(the round-to-nearest-even sum of opposite-signed zeros is `+0.0`), while the
live total leaves the seed untouched at `-0.0`. The two differ in the sign
bit, so `same` fails; all witness terms are closed ground expressions checked
by kernel `decide`. SQL execution may only elide inactive lanes when the seed
is known to be non-negative zero or the fold has a neutral first step.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.JitKernels.FloatingReduction_AbsentLaneAsPositiveZero
open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

theorem refute : ¬ AbsentLaneAsPositiveZero.Goal := by
  intro h
  have h' : (total (-0.0 : Float) ((([none] : List (Option Float)).map (fun v => v.getD 0)))).toBits
            ≠ (total (-0.0 : Float) (live [none])).toBits := by decide
  exact h' (h (-0.0) [none] (show (-0.0 : Float).isFinite = true from by decide))

end TinylambOpt.ResearchProofs.JitKernels.FloatingReduction_AbsentLaneAsPositiveZero
