import TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

/-!
This module audits fixed-offset gathers from canonical civil-text rows and the
arithmetic used to derive UTC grouping keys. It proves the bounded slice,
canonical wide-gather, packed-digit, and whole-hour rotation goals, allowing SQL
execution to use those equivalent representations without changing the modeled
field or key. It also refutes four unsafe execution changes: widening a gather
from only a two-byte guard, certifying later rows from one row's digits,
identifying civil-hour groups with arbitrary-offset UTC-hour groups, and
flooring the local day before applying a displacement.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Proof_CivilTextGathers

open TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

/-- A bounded row slice and the shared text buffer decode the same pair at a
fixed offset. SQL execution may replace the sliced read with an absolute read
when `start ≤ finish` and the row end is within the buffer. -/
theorem fixedOffsetRowSlice : FixedOffsetRowSlice.Goal := by
  intro s start finish off hstart hfinish
  unfold part pair
  by_cases hguard : start + off + 2 ≤ finish
  · simp [hguard]
    have hlen : off + 2 ≤ finish - start := by omega
    have hoff : off < finish - start := by omega
    have hoff1 : off + 1 < finish - start := by omega
    simp [List.getElem?_drop, hoff, hoff1]
    congr 2 <;> omega
  · simp [hguard]
    have hlen : finish - start < off + 2 := by omega
    by_cases hoff : off < finish - start
    · have hoff1 : ¬ off + 1 < finish - start := by omega
      simp [List.getElem?_take, hoff1]
    · simp [List.getElem?_take, hoff]

/-- A guard covering only two bytes does not imply that four bytes are in
bounds. The limit `2` after start `0` refutes widening the gather in SQL
execution without an additional bounds certificate. -/
theorem fourByteGatherFromPairGuard_refuted : ¬ FourByteGatherFromPairGuard.Goal := by
  intro h
  have bad := h 0 2 0 (by omega)
  omega

/-- A canonical nineteen-byte row has enough slack for a four-byte gather at
either component offset. SQL execution may use the wide gather at the final
row because offsets `11` and `14` both end before the certified row limit. -/
theorem canonicalHourMinuteWideGather : CanonicalHourMinuteWideGather.Goal := by
  intro start limit off hstart hoff
  simp only [List.mem_cons, List.mem_nil_iff, or_false] at hoff
  rcases hoff with rfl | rfl <;> omega

/-- Equal row lengths do not transfer a digit validation between rows. A
nineteen-byte row of ASCII zeroes and a same-length row of ASCII `a` characters
refute eliding later per-row hour checks in SQL execution. -/
theorem firstRowFormatCertificate_refuted : ¬ FirstRowFormatCertificate.Goal := by
  intro h
  have ha : (pair (List.replicate 19 48) 11).isSome := by
    simp [pair]
  have bad := h (List.replicate 19 48) (List.replicate 19 97) (by simp) (by simp) ha
  simp [pair] at bad

/-- Packed ASCII subtraction separates the two validated decimal digits, so
lane extraction returns exactly the original `pair` value. SQL execution may
replace the two scalar digit reads with this packed-word computation. -/
theorem pairPackedWordSubtraction : PairPackedWordSubtraction.Goal := by
  intro a b ha ha' hb hb'
  dsimp
  have hword : a + 256 * b - 0x3030 = (a - 48) + 256 * (b - 48) := by omega
  rw [hword]
  simp [pair, Nat.add_mul_div_right, Nat.add_mul_mod_self_left]
  omega

/-- An arbitrary minute displacement can make distinct civil hours share a UTC
hour. With offset `-1`, local `00:59` and `01:00` both map to UTC hour `1`,
refuting interchange of the two SQL grouping keys. -/
theorem offsetHashGrouping_refuted : ¬ OffsetHashGrouping.Goal := by
  intro h
  have bad := h 0 0 59 0 1 0 (-1) (by omega) (by omega) (by omega) (by omega) (by omega) (by omega)
  norm_num [utcHour, utcMinute] at bad

/-- A whole-hour displacement rotates the UTC hour key by a constant modulo
`24`; a valid residual minute cannot cross another hour boundary. SQL execution
may precompute this cyclic rotation instead of applying the displacement to
each row's key. -/
theorem wholeHourOffsetKeyRotation : WholeHourOffsetKeyRotation.Goal := by
  intro d h m z hm hm'
  unfold utcHour utcMinute
  omega

/-- Flooring after subtracting a displacement is not equivalent to flooring the
local day first. At local `23:59` with offset `-1`, the UTC day is `1` while the
proposed early floor remains `0`, refuting that SQL day-key rewrite. -/
theorem shiftBeforeDayFloor_refuted : ¬ ShiftBeforeDayFloor.Goal := by
  intro h
  have bad := h 0 23 59 (-1) (by omega) (by omega) (by omega) (by omega)
  norm_num [utcMinute] at bad

end TinylambOpt.ResearchProofs.JitKernels.Proof_CivilTextGathers
