import TinylambOpt.ResearchGoals.JitKernels.InListLowering

/-!
Booleanizing a three-valued disjunction to a conjunction of negated
acceptances is NOT sound: `test [NULL] (some 0)` is `none` (unknown, not
accepted) and `test [] (some 0)` is `some false`, so their `disj` is `none`,
whose comparison with `some false` is `false` — yet both individual `keep`
acceptances are `false`, so `!keep a && !keep b` is `true`. The unknown
propagated by the NULL-bearing side is lost by per-side booleanization; the
closed ground equation is refuted by `decide`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.InListLowering_CompositeAntiPredicateBooleanization
open TinylambOpt.ResearchGoals.JitKernels.InListLowering

theorem refute : ¬ CompositeAntiPredicateBooleanization.Goal := by
  intro h
  exact absurd (h [none] [] (some 0)) (by decide)

end TinylambOpt.ResearchProofs.JitKernels.InListLowering_CompositeAntiPredicateBooleanization
