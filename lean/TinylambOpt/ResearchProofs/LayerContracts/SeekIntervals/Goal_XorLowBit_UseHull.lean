import TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Common_SeekIntervals
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Cores
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Specifics

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Goal_XorLowBit_UseHull

open TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Common
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Cores
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Specifics

theorem refute : ¬ XorLowBit.UseHull.Goal := by
  intro h
  have hh := h ⟨4,0,2⟩ 2
  have hf : ¬ (inside (hull (ranges XorLowBit.model ⟨4,0,2⟩)) 2 =
      (2 < 4 && XorLowBit.model.test ⟨4,0,2⟩ 2)) := by native_decide
  exact hf hh

end TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Goal_XorLowBit_UseHull
