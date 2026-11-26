from pathlib import Path

D = Path('TinylambOpt/ResearchProofs/LayerContracts/CalendarRanges')

HEAD = '''import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Common_CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.YearsCommon

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_{M}

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges.{M}
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges

'''

TAIL = '''
theorem locateP : Locate.Goal := gen_locate model h0 hcyc hbins hmono
theorem predicateRangeP : PredicateRange.Goal := gen_predicateRange model h0 hcyc hbins hmono
theorem predecessorP : Predecessor.Goal := gen_predecessor model h0 hcyc hbins hmono
theorem cyclePopulationP : CyclePopulation.Goal := gen_cyclePopulation model h0 hcyc hbins hmono
theorem coverBlocksP : CoverBlocks.Goal := gen_coverBlocks model h0 hcyc hbins hmono

end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_{M}
'''

FACTS = {}

SIMPLE = {
 'BusinessDay': '''theorem h0 : model.start 0 = 0 := by native_decide
theorem hbins : 0 < model.bins := by native_decide
theorem hcyc : cycleShift model := by
  intro i
  unfold model
  simp
  omega
theorem hmono : ∀ i, i < model.bins → model.start i < model.start (i+1) := by
  intro i hi
  unfold model
  simp
  omega
''',
 'Retail445': '''theorem h0 : model.start 0 = 0 := by native_decide
theorem hbins : 0 < model.bins := by native_decide
theorem hcyc : cycleShift model := by
  intro i
  unfold model
  simp
  omega
theorem hmono : ∀ i, i < model.bins → model.start i < model.start (i+1) := by
  intro i hi
  unfold model
  simp
  omega
''',
 'Payroll': '''theorem h0 : model.start 0 = 0 := by native_decide
theorem hbins : 0 < model.bins := by native_decide
theorem hcyc : cycleShift model := by
  intro i
  unfold model
  simp
  omega
theorem hmono : ∀ i, i < model.bins → model.start i < model.start (i+1) := by
  intro i hi
  unfold model
  simp
  omega
''',
 'CivilYear': '''theorem h0 : model.start 0 = 0 := by
  unfold model years
  simp
theorem hbins : 0 < model.bins := by native_decide
theorem hcyc : cycleShift model := by
  intro i
  change years (i+400) = years i + 146097
  simpa using (years400 i 1)
theorem hmono : ∀ i, i < model.bins → model.start i < model.start (i+1) := by
  intro i hi
  change years i < years (i+1)
  exact years_strict i (i+1) (by omega)
''',
 'Month': '''theorem h0 : model.start 0 = 0 := monthStart_zero
theorem hbins : 0 < model.bins := by native_decide
theorem hcyc : cycleShift model := by
  intro i
  unfold Month.model
  simp
  exact monthStart_add_4800 i
theorem hmono : ∀ i, i < model.bins → model.start i < model.start (i+1) := by
  intro i hi
  unfold Month.model
  simp
  exact monthStart_succ i
''',
 'Quarter': '''theorem h0 : model.start 0 = 0 := by
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
''',
 'FiscalApril': '''theorem h0 : model.start 0 = 0 := by
  change monthStart 3 - monthStart 3 = 0
  omega
theorem hbins : 0 < model.bins := by native_decide
theorem hcyc : cycleShift model := by
  intro i
  change monthStart (12*(i+400)+3) - monthStart 3 = (monthStart (12*i+3) - monthStart 3) + 146097
  rw [show 12*(i+400)+3 = (12*i+3)+4800 by omega]
  rw [monthStart_add_4800]
  have hle := monthStart_mono 3 (12*i+3) (by omega)
  omega
theorem hmono : ∀ i, i < model.bins → model.start i < model.start (i+1) := by
  intro i hi
  change monthStart (12*i+3) - monthStart 3 < monthStart (12*(i+1)+3) - monthStart 3
  have hlt := monthStart_strict (12*i+3) (12*(i+1)+3) (by omega)
  have hle := monthStart_mono 3 (12*i+3) (by omega)
  omega
''',
 'HalfMonth': '''theorem h0 : model.start 0 = 0 := by
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
''',
 'Decade': '''theorem h0 : model.start 0 = 0 := by
  change years 0 = 0
  unfold years
  simp
theorem hbins : 0 < model.bins := by native_decide
theorem hcyc : cycleShift model := by
  intro i
  change years (10*(i+40)) = years (10*i) + 146097
  rw [show 10*(i+40) = 10*i+400 by omega]
  simpa using (years400 (10*i) 1)
theorem hmono : ∀ i, i < model.bins → model.start i < model.start (i+1) := by
  intro i hi
  change years (10*i) < years (10*(i+1))
  apply years_strict
  omega
''',
 'IsoWeekYear': '''theorem h0 : model.start 0 = 0 := by
  change (years 0 + 3)/7*7 = 0
  unfold years
  simp
theorem hbins : 0 < model.bins := by native_decide
theorem hcyc : cycleShift model := by
  intro i
  change (years (i+400)+3)/7*7 = (years i+3)/7*7 + 146097
  rw [years400 i 1]
  have hc : 146097 = 7*20871 := by omega
  rw [hc]
  omega
theorem hmono : ∀ i, i < model.bins → model.start i < model.start (i+1) := by
  intro i hi
  change (years (i+1)+3)/7*7 > (years i+3)/7*7
  have hd := years_diff_bounds i
  omega
''',
}

for m, body in SIMPLE.items():
    (D / ('Facts_%s.lean' % m)).write_text(HEAD.format(M=m) + body + TAIL.format(M=m))
    print('wrote', m)
