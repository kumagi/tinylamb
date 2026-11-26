import TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Goal_TwoLiterals_SeekMinimum

open TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals

theorem refute : ¬ TwoLiterals.SeekMinimum.Goal := by
  intro h
  have hh := h ⟨2,0,1⟩
  have hf : ¬ ((ranges TwoLiterals.model ⟨2,0,1⟩).length =
      runs ((List.range 2).map (TwoLiterals.model.test ⟨2,0,1⟩))) := by native_decide
  exact hf hh

end TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Goal_TwoLiterals_SeekMinimum
