import TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Common_SeekIntervals
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Cores
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Specifics

/-!
An index range plan must cover every qualifying key and preserve any gaps excluded by
the SQL predicate.

Proved

- `QuotientBand.UseHull` — Can multiple seeks be replaced by one seek with no residual
  filter?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Goal_QuotientBand_UseHull

open TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Common
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Cores
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Specifics

theorem sound : QuotientBand.UseHull.Goal := by
  exact Common.useHull_of_exact_single QuotientBand.model Cores.quotientBand_exact Cores.quotientBand_single

end TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Goal_QuotientBand_UseHull
