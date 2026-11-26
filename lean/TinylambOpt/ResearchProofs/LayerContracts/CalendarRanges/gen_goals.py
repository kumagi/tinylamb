from pathlib import Path

D = Path('TinylambOpt/ResearchProofs/LayerContracts/CalendarRanges')
MODELS = ['Month','Quarter','CivilYear','FiscalApril','HalfMonth','BusinessDay',
          'Retail445','Decade','IsoWeekYear','Payroll']
GENERIC = ['CycleShift','Locate','PredicateRange','Predecessor',
           'CyclePopulation','ConstantWidth','AverageDivision','CoverBlocks']

TEMPLATE = '''import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Common_CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.YearsCommon
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_{M}

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_{M}_{P}

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges.{M}
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_{M}

{line}

end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_{M}_{P}
'''

GENERIC_PROOF = {
 'CycleShift': 'theorem sound : {M}.CycleShift.Goal := hcyc\n',
 'Locate': 'theorem sound : {M}.Locate.Goal := locateP\n',
 'PredicateRange': 'theorem sound : {M}.PredicateRange.Goal := predicateRangeP\n',
 'Predecessor': 'theorem sound : {M}.Predecessor.Goal := predecessorP\n',
 'CyclePopulation': 'theorem sound : {M}.CyclePopulation.Goal := cyclePopulationP\n',
 'CoverBlocks': 'theorem sound : {M}.CoverBlocks.Goal := coverBlocksP\n',
}

# model: witness i, span0, start(i);  d, fast d, d*bins/days
WITNESS = {
 'Month':       (2,31,59, 59,2,1),
 'Quarter':     (4,90,365, 90,1,0),
 'CivilYear':   (4,365,1461, 365,1,0),
 'FiscalApril': (4,365,1461, 365,1,0),
 'HalfMonth':   (2,15,31, 15,1,0),
 'BusinessDay': (5,1,7, 5,4,3),
 'Retail445':   (3,28,91, 28,1,0),
 'Decade':      (2,3652,7305, 3652,1,0),
 'IsoWeekYear': (4,364,1463, 364,1,0),
 'Payroll':     (2,13,28, 13,1,0),
}

def cw_proof(m):
    i,s,v,_,_,_ = WITNESS[m]
    return f'''theorem refute : ¬ {m}.ConstantWidth.Goal := by
  intro h
  have hh := h {i}
  have hs : span {m}.model 0 = {s} := by native_decide
  have hv : {m}.model.start {i} = {v} := by native_decide
  omega
'''

def avg_proof(m):
    _,_,_,d,f,r = WITNESS[m]
    return f'''theorem refute : ¬ {m}.AverageDivision.Goal := by
  intro h
  have hh := h {d}
  have hf : fast {m}.model {d} = {f} := by native_decide
  have hd : {d} * {m}.model.bins / {m}.model.days = {r} := by native_decide
  omega
'''

SPECIFIC = {}

SPECIFIC['Month'] = {
 'February': '''theorem sound : Month.February.Goal := by
  intro y
  have hm1 := monthStart_of y 1 (by omega : 1 < 12)
  have hm2 := monthStart_of y 2 (by omega : 2 < 12)
  have hm1' : monthStart (12*y+1) = years y + 31 := by rw [hm1]; simp
  have hm2' : monthStart (12*y+2) = years y + 59 + (if years (y+1)-years y == 366 then 1 else 0) := by rw [hm2]; simp
  unfold Month.model span
  change monthStart (12*y+1+1) - monthStart (12*y+1) = years (y+1)-years y-337
  rw [show 12*y+1+1 = 12*y+2 by omega]
  rw [hm1', hm2']
  rw [years_diff_eq y]
  by_cases hleap : years (y+1)-years y == 366 <;> simp [hleap] <;> omega
''',
 'QuarterOrigin': '''theorem sound : Month.QuarterOrigin.Goal := by
  intro d q
  exact gen_thirdBucket Month.model h0 hcyc hbins hmono q d
''',
}

SPECIFIC['Quarter'] = {
 'FirstQuarter': '''theorem sound : Quarter.FirstQuarter.Goal := by
  intro y
  have hm0 := monthStart_of y 0 (by omega : 0 < 12)
  have hm3 := monthStart_of y 3 (by omega : 3 < 12)
  have hm0' : monthStart (12*y+0) = years y := by rw [hm0]; simp
  have hm3' : monthStart (12*y+3) = years y + 90 + (if years (y+1)-years y == 366 then 1 else 0) := by rw [hm3]; simp
  unfold Quarter.model span
  change monthStart (3*(4*y+1)) - monthStart (3*(4*y)) = years (y+1)-years y-275
  rw [show 3*(4*y+1)=12*y+3 by omega, show 3*(4*y)=12*y+0 by omega]
  rw [hm0', hm3']
  rw [years_diff_eq y]
  by_cases hleap : years (y+1)-years y == 366 <;> simp [hleap] <;> omega
''',
 'WithinYear': '''theorem sound : Quarter.WithinYear.Goal := by
  intro y
  have hm9 := monthStart_of y 9 (by omega : 9 < 12)
  have hm9' : monthStart (12*y+9) = years y + 273 + (if years (y+1)-years y == 366 then 1 else 0) := by rw [hm9]; simp
  have hy' : years (y+1) = years y + 365 + (if years (y+1)-years y == 366 then 1 else 0) := by
    have hd := years_diff_eq y
    omega
  unfold Quarter.model
  change monthStart (3*(4*y+3)) < years (y+1)
  rw [show 3*(4*y+3)=12*y+9 by omega]
  rw [hm9']
  by_cases hleap : years (y+1)-years y == 366
  · have hd := beq_iff_eq.mp hleap
    simp [hleap]
    omega
  · have hd : years (y+1)-years y ≠ 366 := by intro hh; exact hleap (beq_iff_eq.mpr hh)
    have hb := years_diff_bounds y
    simp [hleap]
    omega
''',
}

SPECIFIC['CivilYear'] = {
 'CenturyException': '''theorem sound : CivilYear.CenturyException.Goal := by
  intro c hc
  unfold CivilYear.model span
  change years (100*(c+1)) - years (100*(c+1)-1) = 365
  have hidx : 100*(c+1)-1 = 100*c+99 := by omega
  rw [hidx]
  unfold years
  omega
''',
 'LeapCentury': '''theorem sound : CivilYear.LeapCentury.Goal := by
  intro c
  unfold CivilYear.model span
  change years (400*(c+1)) - years (400*(c+1)-1) = 366
  have hidx : 400*(c+1)-1 = 400*c+399 := by omega
  rw [hidx]
  unfold years
  omega
''',
}

SPECIFIC['FiscalApril'] = {
 'LeapAhead': '''theorem sound : FiscalApril.LeapAhead.Goal := by
  intro y
  unfold FiscalApril.model span
  change (monthStart (12*(y+1)+3) - monthStart 3) - (monthStart (12*y+3) - monthStart 3) = years (y+1+1)-years (y+1)
  have hf0 := fiscalStart y
  have hf1 := fiscalStart (y+1)
  rw [hf1, hf0]
  have hyd := years_diff_eq y
  have hyd1 := years_diff_eq (y+1)
  by_cases h0 : years (y+1) - years y == 366
  · have hd0 : years (y+1) - years y = 366 := beq_iff_eq.mp h0
    by_cases h1 : years (y+1+1) - years (y+1) == 366
    · have hd1 : years (y+1+1) - years (y+1) = 366 := beq_iff_eq.mp h1
      simp [h0, h1] at *
      omega
    · have hd1 : years (y+1+1) - years (y+1) ≠ 366 := by intro hh; exact h1 (beq_iff_eq.mpr hh)
      simp [h0, h1] at *
      omega
  · have hd0 : years (y+1) - years y ≠ 366 := by intro hh; exact h0 (beq_iff_eq.mpr hh)
    by_cases h1 : years (y+1+1) - years (y+1) == 366
    · have hd1 : years (y+1+1) - years (y+1) = 366 := beq_iff_eq.mp h1
      simp [h0, h1] at *
      omega
    · have hd1 : years (y+1+1) - years (y+1) ≠ 366 := by intro hh; exact h1 (beq_iff_eq.mpr hh)
      simp [h0, h1] at *
      omega
''',
 'AprilEdge': '''theorem sound : FiscalApril.AprilEdge.Goal := by
  intro y
  have hstrict := start_strict_mono FiscalApril.model h0 hcyc hbins hmono y
  have hs : FiscalApril.model.start y ≤ FiscalApril.model.start y ∧
      FiscalApril.model.start y < FiscalApril.model.start (y+1) := by
    exact ⟨by omega, hstrict⟩
  exact predicateRangeP (FiscalApril.model.start y) y |>.mpr hs
''',
}

SPECIFIC['HalfMonth'] = {
 'EarlyHalf': '''theorem sound : HalfMonth.EarlyHalf.Goal := by
  intro m
  unfold HalfMonth.model span
  change (monthStart ((2*m+1)/2) + 15*((2*m+1)%2)) - (monthStart ((2*m)/2) + 15*((2*m)%2)) = 15
  have h1 : (2*m)/2 = m := by omega
  have h2 : (2*m)%2 = 0 := by omega
  have h3 : (2*m+1)/2 = m := by omega
  have h4 : (2*m+1)%2 = 1 := by omega
  rw [h1, h2, h3, h4]
  omega
''',
 'LateHalf': '''theorem sound : HalfMonth.LateHalf.Goal := by
  intro m
  unfold HalfMonth.model span
  change (monthStart ((2*m+2)/2) + 15*((2*m+2)%2)) - (monthStart ((2*m+1)/2) + 15*((2*m+1)%2)) =
    monthStart (m+1) - monthStart m - 15
  have h1 : (2*m+2)/2 = m+1 := by omega
  have h2 : (2*m+2)%2 = 0 := by omega
  have h3 : (2*m+1)/2 = m := by omega
  have h4 : (2*m+1)%2 = 1 := by omega
  rw [h1, h2, h3, h4]
  have hg := monthStart_gap m
  omega
''',
}

SPECIFIC['BusinessDay'] = {
 'Weekend': '''theorem sound : BusinessDay.Weekend.Goal := by
  intro w
  have hs5 : BusinessDay.model.start (5*w+4) ≤ 7*w+5 ∧ 7*w+5 < BusinessDay.model.start (5*w+4+1) := by
    unfold BusinessDay.model
    simp
    omega
  have hs6 : BusinessDay.model.start (5*w+4) ≤ 7*w+6 ∧ 7*w+6 < BusinessDay.model.start (5*w+4+1) := by
    unfold BusinessDay.model
    simp
    omega
  have h5 := predicateRangeP (7*w+5) (5*w+4) |>.mpr hs5
  have h6 := predicateRangeP (7*w+6) (5*w+4) |>.mpr hs6
  omega
''',
 'Monday': '''theorem sound : BusinessDay.Monday.Goal := by
  intro w
  have hs : BusinessDay.model.start (5*w) ≤ 7*w ∧ 7*w < BusinessDay.model.start (5*w+1) := by
    unfold BusinessDay.model
    simp
    omega
  exact predicateRangeP (7*w) (5*w) |>.mpr hs
''',
}

SPECIFIC['Retail445'] = {
 'FifthWeek': '''theorem sound : Retail445.FifthWeek.Goal := by
  intro q
  unfold Retail445.model span
  change (3*q+2+1)/3*91 + 28*((3*q+2+1)%3) - ((3*q+2)/3*91 + 28*((3*q+2)%3)) = 35
  have h1 : (3*q+2)/3 = q := by omega
  have h2 : (3*q+2)%3 = 2 := by omega
  have h3 : (3*q+2+1)/3 = q+1 := by omega
  have h4 : (3*q+2+1)%3 = 0 := by omega
  rw [h1, h2, h3, h4]
  omega
''',
 'FourthWeek': '''theorem sound : Retail445.FourthWeek.Goal := by
  intro q
  have hs : Retail445.model.start (3*q+1) ≤ 91*q+55 ∧ 91*q+55 < Retail445.model.start (3*q+1+1) := by
    unfold Retail445.model
    simp
    omega
  exact predicateRangeP (91*q+55) (3*q+1) |>.mpr hs
''',
}

SPECIFIC['Decade'] = {
 'CenturyWidth': '''theorem sound : Decade.CenturyWidth.Goal := by
  intro c
  unfold Decade.model
  change years (10*(10*c)) = years (100*c)
  congr 1
  omega
''',
 'DecadeDays': '''theorem sound : Decade.DecadeDays.Goal := by
  intro i
  let q := i / 40
  let k := i % 40
  have hk : k < 40 := by dsimp [k]; exact Nat.mod_lt i (by omega : 0 < 40)
  have hi : i = 40*q + k := by
    dsimp [q, k]
    simpa [Nat.mul_comm] using (Nat.div_add_mod i 40).symm
  have hkcase : k = 0 ∨ k = 1 ∨ k = 2 ∨ k = 3 ∨ k = 4 ∨ k = 5 ∨ k = 6 ∨ k = 7 ∨
      k = 8 ∨ k = 9 ∨ k = 10 ∨ k = 11 ∨ k = 12 ∨ k = 13 ∨ k = 14 ∨ k = 15 ∨
      k = 16 ∨ k = 17 ∨ k = 18 ∨ k = 19 ∨ k = 20 ∨ k = 21 ∨ k = 22 ∨ k = 23 ∨
      k = 24 ∨ k = 25 ∨ k = 26 ∨ k = 27 ∨ k = 28 ∨ k = 29 ∨ k = 30 ∨ k = 31 ∨
      k = 32 ∨ k = 33 ∨ k = 34 ∨ k = 35 ∨ k = 36 ∨ k = 37 ∨ k = 38 ∨ k = 39 := by omega
  have hspan : span Decade.model i = span Decade.model k := by
    unfold Decade.model span
    change years (10*(i+1)) - years (10*i) = years (10*(k+1)) - years (10*k)
    have h10i : 10*i = 400*q + 10*k := by dsimp [q, k] at *; omega
    have h10i1 : 10*(i+1) = 400*q + (10*k+10) := by omega
    rw [h10i, h10i1]
    have hA := years400 (10*k) q
    have hB := years400 (10*k+10) q
    have hA' : years (400*q+10*k) = years (10*k) + 146097*q := by
      rw [Nat.add_comm]
      exact hA
    have hB' : years (400*q+(10*k+10)) = years (10*k+10) + 146097*q := by
      rw [Nat.add_comm]
      exact hB
    rw [hA', hB']
    rw [show 10*(k+1) = 10*k+10 by omega]
    rw [Nat.add_sub_add_right]
  rw [hspan]
  rcases hkcase with ''' + ' | '.join('hk%d'%i for i in range(40)) + '\n' +
  ''.join('  · rw [hk%d]\n    native_decide\n' % i for i in range(40))
}

SPECIFIC['IsoWeekYear'] = {
 'WholeWeeks': '''theorem sound : IsoWeekYear.WholeWeeks.Goal := by
  intro i
  unfold IsoWeekYear.model span
  change ((years (i+1)+3)/7*7 - (years i+3)/7*7 = 364 ∨
          (years (i+1)+3)/7*7 - (years i+3)/7*7 = 371)
  have hd := years_diff_bounds i
  omega
''',
 'JanuaryFourth': '''theorem sound : IsoWeekYear.JanuaryFourth.Goal := by
  intro y
  apply predicateRangeP (years y+3) y |>.mpr
  constructor
  · change (years y+3)/7*7 ≤ years y+3
    omega
  · change years y+3 < (years (y+1)+3)/7*7
    have hd := years_diff_bounds y
    omega
''',
}

SPECIFIC['Payroll'] = {
 'ShortPeriod': '''theorem sound : Payroll.ShortPeriod.Goal := by
  intro n
  unfold Payroll.model span
  change (2*n+1)/2*28 + 13*((2*n+1)%2) - ((2*n)/2*28 + 13*((2*n)%2)) = 13
  have h1 : (2*n+1)/2 = n := by omega
  have h2 : (2*n+1)%2 = 1 := by omega
  have h3 : (2*n)/2 = n := by omega
  have h4 : (2*n)%2 = 0 := by omega
  rw [h1, h2, h3, h4]
  omega
''',
 'OddBoundary': '''theorem sound : Payroll.OddBoundary.Goal := by
  intro n
  have hs : Payroll.model.start (2*n+1) ≤ 28*n+13 ∧ 28*n+13 < Payroll.model.start (2*n+1+1) := by
    unfold Payroll.model
    simp
    omega
  exact predicateRangeP (28*n+13) (2*n+1) |>.mpr hs
''',
}

count = 0
for m in MODELS:
    for p in GENERIC:
        if p in GENERIC_PROOF:
            line = GENERIC_PROOF[p].format(M=m)
        elif p == 'ConstantWidth':
            line = cw_proof(m)
        else:
            line = avg_proof(m)
        path = D / ('Goal_%s_%s.lean' % (m, p))
        path.write_text(TEMPLATE.format(M=m, P=p, line=line))
        count += 1
    for p, line in SPECIFIC[m].items():
        path = D / ('Goal_%s_%s.lean' % (m, p))
        path.write_text(TEMPLATE.format(M=m, P=p, line=line))
        count += 1
print('generated', count)

imports = ['import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Common_CalendarRanges',
           'import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.YearsCommon']
for m in MODELS:
    imports.append('import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_%s' % m)
for m in MODELS:
    for p in GENERIC + list(SPECIFIC[m]):
        imports.append('import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_%s_%s' % (m, p))
(D.parent / 'CalendarRanges_All.lean').write_text('-- Proof aggregation for LayerContracts.CalendarRanges\n' + '\n'.join(imports) + '\n')
print('wrote All')
