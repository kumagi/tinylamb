import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_QuadraticGap_GapWitness

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.QuadraticGap

-- Exhaustive small-grid search (0 <= lo,hi < 6), filtered by the hypothesis
-- a.hi < b.lo, found e.g. (al, ah, bl, bh) = (4, 1, 2, 2).
#eval (kernel.bounds 0 ⟨4, 1⟩ ⟨2, 2⟩, kernel.score 0 (1, 2))
#guard decide (1 < 2)
#guard (kernel.bounds 0 ⟨4, 1⟩ ⟨2, 2⟩).lo != kernel.score 0 (1, 2)

-- a = ⟨4,1⟩, b = ⟨2,2⟩ satisfies 1 < 2, but
-- gapMin = max (4-2) (2-1) = 2, so bounds.lo = 2^2 + 0 = 4,
-- while score 0 (1,2) = ((1-2)+(2-1))^2 + 0 = 1.
theorem disproof_GapWitness : ¬ QuadraticGap.GapWitness.Goal := by
  intro h
  have hbad := h 0 ⟨4, 1⟩ ⟨2, 2⟩ (by decide)
  have hneq : (kernel.bounds 0 ⟨4, 1⟩ ⟨2, 2⟩).lo ≠ kernel.score 0 (1, 2) := by
    native_decide
  exact hneq hbad

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_QuadraticGap_GapWitness
