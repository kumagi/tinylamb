import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Refuted by counterexample

- `PeakCount.DuplicateLoss` — `∀ x, measure (x.flatMap (repeated 2)) = measure x`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PeakCount_DuplicateLoss

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PeakCount

abbrev M := TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PeakCount.measure
abbrev rep := TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.repeated

-- Concrete counterexample: `[0,1,0]` has exactly one strict peak (the `1`),
-- but duplicating every row gives `[0,0,1,1,0,0]`, whose maximum is a plateau
-- with no row strictly higher than both neighbours.
#eval M [0, 1, 0]                   -- 1
#eval M ([0, 1, 0].flatMap (rep 2)) -- 0

theorem disproof_PeakCount_DuplicateLoss : ¬ PeakCount.DuplicateLoss.Goal := by
  intro h
  have hbad : ¬ M ([0, 1, 0].flatMap (rep 2)) = M [0, 1, 0] := by native_decide
  exact hbad (h [0, 1, 0])

end TinylambOpt.ResearchProofs.Atlas.Goal_PeakCount_DuplicateLoss
