import Std

/-! Generate fixed-offset decimal gathers for canonical nineteen-byte civil text. This layout model uses day ordinals and minute-of-day arithmetic for timezone adjustment; it is not a timezone or date parser. -/
namespace TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

abbrev B := List Nat
def pair (s : B) (i : Nat) := do
  let a ← s[i]?
  let b ← s[i+1]?
  if 48 ≤ a && a ≤ 57 && 48 ≤ b && b ≤ 57 then some (10*(a-48)+(b-48)) else none
def utcMinute (day hour minute : Int) (offset : Int) := day*1440+hour*60+minute-offset
def utcHour (day hour minute offset : Int) := (utcMinute day hour minute offset / 60)%24
def part (s : B) (start limit off : Nat) :=
  if start+off+2 ≤ limit then pair s (start+off) else none

/-- Read two decimal digits from the shared text buffer at a known offset without slicing the row. -/
def FixedOffsetRowSlice.Goal : Prop := ∀ s start finish off, start ≤ finish → finish ≤ s.length →
  part s start finish off = pair ((s.drop start).take (finish-start)) off

/-- A two-byte bounds guard for each date component licenses an unmasked four-byte gather. -/
def FourByteGatherFromPairGuard.Goal : Prop := ∀ start limit off : Nat, start+off+2 ≤ limit → start+off+4 ≤ limit

/-- A 19-byte canonical row permits a single four-byte gather at both hour and minute offsets, including the final row. -/
def CanonicalHourMinuteWideGather.Goal : Prop := ∀ start limit off : Nat, start+19 ≤ limit → off ∈ [11,14] → start+off+4 ≤ limit

/-- After validating the first row's hour digits, equal row lengths suffice to elide per-row digit checks. -/
def FirstRowFormatCertificate.Goal : Prop := ∀ (a b : B), a.length = 19 → b.length = a.length → (pair a 11).isSome → (pair b 11).isSome

/-- For canonical digits, two lane extractions after subtracting ASCII zero from a packed 16-bit word decode the same component. -/
def PairPackedWordSubtraction.Goal : Prop := ∀ (a b : Nat), 48 ≤ a → a ≤ 57 → 48 ≤ b → b ≤ 57 →
  let word := (a+256*b)-0x3030
  pair [a,b] 0 = some (10*(word%256)+word/256)

/-- Grouping by the original civil hour matches grouping by UTC hour for rows that share one arbitrary minute offset. -/
def OffsetHashGrouping.Goal : Prop := ∀ d h m d' h' m' z, 0 ≤ h → h < 24 → 0 ≤ h' → h' < 24 → 0 ≤ m → m < 60 →
  0 ≤ m' → m' < 60 → (h = h' ↔ utcHour d h m z = utcHour d' h' m' z)

/-- Specialize a constant whole-hour displacement as one cyclic rotation of the 24 hour group IDs. -/
def WholeHourOffsetKeyRotation.Goal : Prop := ∀ d h m z, 0 ≤ m → m < 60 → utcHour d h m (60*z) = (h-z)%24

/-- Truncate a local timestamp to its day before subtracting the timezone displacement for a UTC day key. -/
def ShiftBeforeDayFloor.Goal : Prop := ∀ d h m z, 0 ≤ h → h < 24 → 0 ≤ m → m < 60 →
  utcMinute d h m z / 1440 = (d*1440-z)/1440

end TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers
