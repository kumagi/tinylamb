import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Common_CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.YearsCommon

/-!
Date and timestamp predicates can use calendar bucket boundaries and range seeks where
the modeled endpoints agree.

Proved

- `Quarter.CoverBlocks` — Minimal bucket interval covering every day of a nonempty page
  range.
- `Quarter.CyclePopulation` — Analytical cardinality of a UNION of consecutive whole
  buckets.
- `Quarter.Locate` — Cycle reduction and binary search identify the unique half-open
  interval.
- `Quarter.Predecessor` — On the day before a boundary, the previous bucket must win.
- `Quarter.PredicateRange` — Replace a truncated-date equality filter with two raw date
  comparisons.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_Quarter

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges.Quarter
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges

theorem h0 : model.start 0 = 0 := by
  change monthStart 0 = 0
  exact monthStart_zero
theorem hbins : 0 < model.bins := by native_decide
theorem hcyc : cycleShift model := by
  intro i
  change monthStart (3*(i+1600)) = monthStart (3*i) + 146097
  rw [show 3*(i+1600) = 3*i+4800 by omega]
  exact monthStart_add_4800 (3*i)
theorem hmono : ∀ i, i < model.bins → model.start i < model.start (i+1) := by
  intro i hi
  change monthStart (3*i) < monthStart (3*(i+1))
  apply monthStart_strict
  omega

theorem locateP : Locate.Goal := gen_locate model h0 hcyc hbins hmono
theorem predicateRangeP : PredicateRange.Goal := gen_predicateRange model h0 hcyc hbins hmono
theorem predecessorP : Predecessor.Goal := gen_predecessor model h0 hcyc hbins hmono
theorem cyclePopulationP : CyclePopulation.Goal := gen_cyclePopulation model h0 hcyc hbins hmono
theorem coverBlocksP : CoverBlocks.Goal := gen_coverBlocks model h0 hcyc hbins hmono

end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_Quarter
