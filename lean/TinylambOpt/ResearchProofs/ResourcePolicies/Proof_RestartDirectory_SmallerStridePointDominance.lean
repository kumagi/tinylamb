import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
An arbitrary smaller stride does not dominate a larger one for point lookups.

Refutation (`SmallerStridePointDominance.Goal`): halving the restart stride was
conjectured to never increase the distance from a row to its restart anchor. But
dominance only holds when the strides align: at row `i = 5`, stride `a = 2`
anchors at row `4` (distance 1) while stride `b = 5` anchors at row `5` itself
(distance 0). So `i - anchor a i ≤ i - anchor b i` fails — a page reader that
assumes finer-is-always-better must restrict to strides dividing the coarse one.
-/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory_SmallerStridePointDominance
open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory

theorem refute : ¬ SmallerStridePointDominance.Goal := by
  intro h
  have hbad := h 2 5 5 (by decide) (by decide)
  exact absurd hbad (by decide)

end TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory_SmallerStridePointDominance

