import Std

/-!
Date and timestamp predicates can use calendar bucket boundaries and range seeks where
the modeled endpoints agree.
-/

namespace TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges

/-! Calendar bucket boundaries indexed from epoch zero. Day numbers and period ordinals
are nonnegative. Gregorian epoch is 0001-01-01. Fixed business calendars specify
their own epoch in the model. No time zone conversion is implicit. -/
def years (y : Nat) := 365*y + y/4 - y/100 + y/400
def monthStart (n : Nat) :=
  let y := n/12
  let m := n%12
  let offsets : List Nat := [0,31,59,90,120,151,181,212,243,273,304,334]
  years y + offsets[m]! + (if 2 ≤ m && years (y+1) - years y == 366 then 1 else 0)
structure Model where
  start : Nat → Nat
  bins : Nat
  days : Nat

/-- Search only the candidate ordinals up to the given limit, including both ends. -/
def seek (m : Model) (d last : Nat) :=
  Id.run do
    let mut lo := 0
    let mut hi := last+1
    for _ in [:hi.log2+1] do
      let mid := (lo+hi)/2
      if m.start mid ≤ d then lo := mid else hi := mid
    return lo
def fast (m : Model) (d : Nat) := d/m.days*m.bins + seek m (d%m.days) (m.bins-1)
def span (m : Model) (i : Nat) := m.start (i+1) - m.start i

/-- Translate a bucket boundary by one calendar cycle. -/
def cycleShift (m : Model) : Prop := ∀ i, m.start (i+m.bins) = m.start i + m.days

/-- Cycle reduction and binary search identify the unique half-open interval. -/
def locate (m : Model) : Prop := ∀ d, let i := fast m d
  m.start i ≤ d ∧ d < m.start (i+1)

/-- Replace a truncated-date equality filter with two raw date comparisons. -/
def predicateRange (m : Model) : Prop := ∀ d i, fast m d = i ↔ m.start i ≤ d ∧ d < m.start (i+1)

/-- On the day before a boundary, the previous bucket must win. -/
def predecessor (m : Model) : Prop := ∀ i, fast m (m.start (i+1)-1) = i

/-- Analytical cardinality of a UNION of consecutive whole buckets. -/
def cyclePopulation (m : Model) : Prop := ∀ i n, ((List.range n).map (fun j => span m (i+j))).sum = m.start (i+n) - m.start i

/-- Can the JIT use a constant width learned from the first bucket? -/
def constantWidth (m : Model) : Prop := ∀ i, m.start i = i * span m 0

/-- Can the optimizer replace calendar lookup by scaling with the mean width? -/
def averageDivision (m : Model) : Prop := ∀ d, fast m d = d*m.bins/m.days

/-- Minimal bucket interval covering every day of a nonempty page range. -/
def coverBlocks (m : Model) : Prop := ∀ lo hi i, lo < hi →
  (fast m lo ≤ i ∧ i ≤ fast m (hi-1) ↔ ∃ d, lo ≤ d ∧ d < hi ∧ fast m d = i)

namespace Month

/-- Gregorian calendar months. -/
def model : Model :=
  ⟨monthStart, 4800, 146097⟩

def CycleShift.Goal : Prop := cycleShift model
def Locate.Goal : Prop := locate model
def PredicateRange.Goal : Prop := predicateRange model
def Predecessor.Goal : Prop := predecessor model
def CyclePopulation.Goal : Prop := cyclePopulation model
def ConstantWidth.Goal : Prop := constantWidth model
def AverageDivision.Goal : Prop := averageDivision model
def CoverBlocks.Goal : Prop := coverBlocks model

def February.Goal : Prop := ∀ y, span model (12*y+1) = years (y+1)-years y-337

def QuarterOrigin.Goal : Prop := ∀ d q, fast model d / 3 = q ↔
  monthStart (3*q) ≤ d ∧ d < monthStart (3*q+3)

end Month

namespace Quarter

/-- Quarter buckets starting in January. -/
def model : Model :=
  ⟨fun i => monthStart (3*i), 1600, 146097⟩

def CycleShift.Goal : Prop := cycleShift model
def Locate.Goal : Prop := locate model
def PredicateRange.Goal : Prop := predicateRange model
def Predecessor.Goal : Prop := predecessor model
def CyclePopulation.Goal : Prop := cyclePopulation model
def ConstantWidth.Goal : Prop := constantWidth model
def AverageDivision.Goal : Prop := averageDivision model
def CoverBlocks.Goal : Prop := coverBlocks model

def FirstQuarter.Goal : Prop := ∀ y, span model (4*y) = years (y+1)-years y-275

def WithinYear.Goal : Prop := ∀ y, model.start (4*y+3) < years (y+1)

end Quarter

namespace CivilYear

/-- Gregorian years with century exceptions. -/
def model : Model :=
  ⟨years, 400, 146097⟩

def CycleShift.Goal : Prop := cycleShift model
def Locate.Goal : Prop := locate model
def PredicateRange.Goal : Prop := predicateRange model
def Predecessor.Goal : Prop := predecessor model
def CyclePopulation.Goal : Prop := cyclePopulation model
def ConstantWidth.Goal : Prop := constantWidth model
def AverageDivision.Goal : Prop := averageDivision model
def CoverBlocks.Goal : Prop := coverBlocks model

def CenturyException.Goal : Prop := ∀ c, (c+1)%4 ≠ 0 → span model (100*(c+1)-1) = 365

def LeapCentury.Goal : Prop := ∀ c, span model (400*(c+1)-1) = 366

end CivilYear

namespace FiscalApril

/-- Fiscal years beginning April 1, day zero rebased to the first April. -/
def model : Model :=
  ⟨fun y => monthStart (12*y+3) - monthStart 3, 400, 146097⟩

def CycleShift.Goal : Prop := cycleShift model
def Locate.Goal : Prop := locate model
def PredicateRange.Goal : Prop := predicateRange model
def Predecessor.Goal : Prop := predecessor model
def CyclePopulation.Goal : Prop := cyclePopulation model
def ConstantWidth.Goal : Prop := constantWidth model
def AverageDivision.Goal : Prop := averageDivision model
def CoverBlocks.Goal : Prop := coverBlocks model

def LeapAhead.Goal : Prop := ∀ y, span model y = years (y+2) - years (y+1)

def AprilEdge.Goal : Prop := ∀ y, fast model (monthStart (12*y+3)-monthStart 3) = y

end FiscalApril

namespace HalfMonth

/-- A split at day 16 within each month. -/
def model : Model :=
  ⟨fun i => monthStart (i/2) + 15*(i%2), 9600, 146097⟩

def CycleShift.Goal : Prop := cycleShift model
def Locate.Goal : Prop := locate model
def PredicateRange.Goal : Prop := predicateRange model
def Predecessor.Goal : Prop := predecessor model
def CyclePopulation.Goal : Prop := cyclePopulation model
def ConstantWidth.Goal : Prop := constantWidth model
def AverageDivision.Goal : Prop := averageDivision model
def CoverBlocks.Goal : Prop := coverBlocks model

def EarlyHalf.Goal : Prop := ∀ m, span model (2*m) = 15

def LateHalf.Goal : Prop := ∀ m, span model (2*m+1) = monthStart (m+1)-monthStart m-15

end HalfMonth

namespace BusinessDay

/-- Five trading days per week; the Friday bucket extends through Sunday. -/
def model : Model :=
  ⟨fun i => i/5*7+i%5, 5, 7⟩

def CycleShift.Goal : Prop := cycleShift model
def Locate.Goal : Prop := locate model
def PredicateRange.Goal : Prop := predicateRange model
def Predecessor.Goal : Prop := predecessor model
def CyclePopulation.Goal : Prop := cyclePopulation model
def ConstantWidth.Goal : Prop := constantWidth model
def AverageDivision.Goal : Prop := averageDivision model
def CoverBlocks.Goal : Prop := coverBlocks model

def Weekend.Goal : Prop := ∀ w, fast model (7*w+5) = fast model (7*w+6)

def Monday.Goal : Prop := ∀ w, fast model (7*w) = 5*w

end BusinessDay

namespace Retail445

/-- Four, four and five weeks in a 13-week retail quarter. -/
def model : Model :=
  ⟨fun i => i/3*91 + 28*(i%3), 3, 91⟩

def CycleShift.Goal : Prop := cycleShift model
def Locate.Goal : Prop := locate model
def PredicateRange.Goal : Prop := predicateRange model
def Predecessor.Goal : Prop := predecessor model
def CyclePopulation.Goal : Prop := cyclePopulation model
def ConstantWidth.Goal : Prop := constantWidth model
def AverageDivision.Goal : Prop := averageDivision model
def CoverBlocks.Goal : Prop := coverBlocks model

def FifthWeek.Goal : Prop := ∀ q, span model (3*q+2) = 35

def FourthWeek.Goal : Prop := ∀ q, fast model (91*q+55) = 3*q+1

end Retail445

namespace Decade

/-- Ten civil years per partition. -/
def model : Model :=
  ⟨fun i => years (10*i), 40, 146097⟩

def CycleShift.Goal : Prop := cycleShift model
def Locate.Goal : Prop := locate model
def PredicateRange.Goal : Prop := predicateRange model
def Predecessor.Goal : Prop := predecessor model
def CyclePopulation.Goal : Prop := cyclePopulation model
def ConstantWidth.Goal : Prop := constantWidth model
def AverageDivision.Goal : Prop := averageDivision model
def CoverBlocks.Goal : Prop := coverBlocks model

def CenturyWidth.Goal : Prop := ∀ c, model.start (10*c) = years (100*c)

def DecadeDays.Goal : Prop := ∀ i, 3651 ≤ span model i ∧ span model i ≤ 3653

end Decade

namespace IsoWeekYear

/-- Years starting on the Monday of the week containing January 4 (epoch Monday). -/
def model : Model :=
  ⟨fun y => (years y + 3)/7*7, 400, 146097⟩

def CycleShift.Goal : Prop := cycleShift model
def Locate.Goal : Prop := locate model
def PredicateRange.Goal : Prop := predicateRange model
def Predecessor.Goal : Prop := predecessor model
def CyclePopulation.Goal : Prop := cyclePopulation model
def ConstantWidth.Goal : Prop := constantWidth model
def AverageDivision.Goal : Prop := averageDivision model
def CoverBlocks.Goal : Prop := coverBlocks model

def WholeWeeks.Goal : Prop := ∀ i, span model i = 364 ∨ span model i = 371

def JanuaryFourth.Goal : Prop := ∀ y, fast model (years y+3) = y

end IsoWeekYear

namespace Payroll

/-- Alternating 13-day and 15-day pay periods in a 28-day roster. -/
def model : Model :=
  ⟨fun i => i/2*28 + 13*(i%2), 2, 28⟩

def CycleShift.Goal : Prop := cycleShift model
def Locate.Goal : Prop := locate model
def PredicateRange.Goal : Prop := predicateRange model
def Predecessor.Goal : Prop := predecessor model
def CyclePopulation.Goal : Prop := cyclePopulation model
def ConstantWidth.Goal : Prop := constantWidth model
def AverageDivision.Goal : Prop := averageDivision model
def CoverBlocks.Goal : Prop := coverBlocks model

def ShortPeriod.Goal : Prop := ∀ n, span model (2*n) = 13

def OddBoundary.Goal : Prop := ∀ n, fast model (28*n+13) = 2*n+1

end Payroll

#guard fast BusinessDay.model 6 == 4
#guard fast Month.model 31 == 1
#guard fast HalfMonth.model 15 == 1

end TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
