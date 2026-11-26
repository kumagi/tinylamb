import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Common_CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.YearsCommon

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_HalfMonth

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges.HalfMonth
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges

theorem h0 : model.start 0 = 0 := by
  change monthStart 0 + 15*0 = 0
  rw [monthStart_zero]
theorem hbins : 0 < model.bins := by native_decide
theorem hcyc : cycleShift model := by
  intro i
  change monthStart ((i+9600)/2) + 15*((i+9600)%2) = monthStart (i/2) + 15*(i%2) + 146097
  have hdiv : (i+9600)/2 = i/2 + 4800 := by omega
  have hmod : (i+9600)%2 = i%2 := by omega
  rw [hdiv, hmod, monthStart_add_4800]
  omega
theorem hmono : ∀ i, i < model.bins → model.start i < model.start (i+1) := by
  intro i hi
  change monthStart (i/2) + 15*(i%2) < monthStart ((i+1)/2) + 15*((i+1)%2)
  by_cases he : i%2 = 0
  · have hdiv : (i+1)/2 = i/2 := by omega
    have hmod : (i+1)%2 = 1 := by omega
    rw [hdiv, hmod, he]
    omega
  · have hodd : i%2 = 1 := by omega
    have hdiv : (i+1)/2 = i/2 + 1 := by omega
    have hmod : (i+1)%2 = 0 := by omega
    rw [hdiv, hmod, hodd]
    have hg := monthStart_gap (i/2)
    omega

theorem locateP : Locate.Goal := gen_locate model h0 hcyc hbins hmono
theorem predicateRangeP : PredicateRange.Goal := gen_predicateRange model h0 hcyc hbins hmono
theorem predecessorP : Predecessor.Goal := gen_predecessor model h0 hcyc hbins hmono
theorem cyclePopulationP : CyclePopulation.Goal := gen_cyclePopulation model h0 hcyc hbins hmono
theorem coverBlocksP : CoverBlocks.Goal := gen_coverBlocks model h0 hcyc hbins hmono

end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_HalfMonth
