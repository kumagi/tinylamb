import TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Common_SeekIntervals
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Cores
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Specifics

/-!
An index range plan must cover every qualifying key and preserve any gaps excluded by
the SQL predicate.

Refuted by counterexample

- `TwoLiterals.UseHull` — Can multiple seeks be replaced by one seek with no residual
  filter?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Goal_TwoLiterals_UseHull

open TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Common
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Cores
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Specifics

theorem refute : ¬ TwoLiterals.UseHull.Goal := by
  intro h
  have hh := h ⟨3,0,2⟩ 1
  have hf : ¬ (inside (hull (ranges TwoLiterals.model ⟨3,0,2⟩)) 1 =
      (1 < 3 && TwoLiterals.model.test ⟨3,0,2⟩ 1)) := by native_decide
  exact hf hh

end TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Goal_TwoLiterals_UseHull
