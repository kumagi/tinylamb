import TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals

/-!
An index range plan must cover every qualifying key and preserve any gaps excluded by
the SQL predicate.

Refuted by counterexample

- `Residue.SeekMinimum` — The emitted seek count equals the number of maximal
  contiguous true regions.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Goal_Residue_SeekMinimum

open TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals

theorem refute : ¬ Residue.SeekMinimum.Goal := by
  intro h
  have hh := h ⟨2,0,0⟩
  have hf : ¬ ((ranges Residue.model ⟨2,0,0⟩).length =
      runs ((List.range 2).map (Residue.model.test ⟨2,0,0⟩))) := by native_decide
  exact hf hh

end TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Goal_Residue_SeekMinimum
