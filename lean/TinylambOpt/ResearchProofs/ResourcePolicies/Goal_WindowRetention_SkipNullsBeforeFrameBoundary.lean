import TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention
/-!
Registers the checked result for `WindowRetention.SkipNullsBeforeFrameBoundary.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/

/-!
Refutes `SkipNullsBeforeFrameBoundary`: the physical-row frame counts NULL rows against
the width, so a null-stripped halo that keeps `w` non-NULL rows can expose more non-NULL
inputs than the physical frame does. With old = [some 0, none], fresh = [none], w = 1,
j = 0 the physical frame is [none, none] (SUM over no non-NULL inputs, i.e. NULL), while
the halo frame is [some 0, none] whose SUM is 0.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WindowRetention_SkipNullsBeforeFrameBoundary
open TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention
open TinylambOpt.ResearchGoals.QueryRewrites

theorem refute : ¬ SkipNullsBeforeFrameBoundary.Goal := by
  intro h
  have key : nsum (frame ([some 0, none] ++ [none] : List (Option Int)) 1 2) =
      nsum (frame
        (tailRows (([some 0, none] : List (Option Int)).filter Option.isSome) 1 ++ [none]) 1
        (tailRows (([some 0, none] : List (Option Int)).filter Option.isSome) 1).length) →
      False := by
    decide
  exact key (h [some 0, none] [none] 1 0 (by decide))

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WindowRetention_SkipNullsBeforeFrameBoundary
