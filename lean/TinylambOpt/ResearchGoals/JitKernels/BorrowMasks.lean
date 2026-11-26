import TinylambOpt.ResearchGoals.SwarDigitBallot

/-! Word-at-a-time byte tests for literal, delimiter and tag lookup. Distinguish an existential hit, an exact per-lane mask, and the position of the earliest hit on little-endian input. -/
namespace TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

open TinylambOpt.ResearchGoals.SwarDigitBallot
def ones : W := 0x0101010101010101
def fast (x : W) := (x - ones) &&& ~~~x &&& high
def lane (x : W) (i : Nat) := (x.extractLsb' (8*i) 8).toNat
def marked (x : W) := (List.range 8).filter (fun i => x.getLsbD (8*i+7))
def repeatByte (b : BitVec 8) : W := BitVec.ofNat 64 b.toNat * ones

/-- The cheaper subtract-and-mask test suffices to bypass eight scalar equality checks on a miss. -/
def BorrowAnyByteHit.Goal : Prop := ∀ x, (fast x == 0) = (zeroBytes x == 0)

/-- Use the subtract mask directly as a stable multi-match compaction ballot. -/
def BorrowBallotForCompaction.Goal : Prop := ∀ x, marked (fast x) = marked (zeroBytes x)

/-- Use one trailing-zero search to find the first equality without normalizing the remaining mask. -/
def BorrowEarliestHit.Goal : Prop := ∀ x, (marked (fast x)).head? = (marked (zeroBytes x)).head?

/-- Choose the highest subtract-mask bit to implement reverse delimiter search. -/
def BorrowReverseDelimiter.Goal : Prop := ∀ x, (marked (fast x)).getLast? = (marked (zeroBytes x)).getLast?

/-- Compute the number of delimiters before a LIMIT boundary using popcount on the subtract mask. -/
def BorrowRankBeforeLimit.Goal : Prop := ∀ x n, n ≤ 8 → ((marked (fast x)).filter (· < n)).length = ((marked (zeroBytes x)).filter (· < n)).length

/-- Use the cheaper exact lane mask when the input alphabet excludes the byte that can continue a borrow. -/
def BorrowImpossibleNeighbor.Goal : Prop := ∀ x, (∀ i < 8, lane x i ≠ 1) → fast x = zeroBytes x

/-- Locate the first literal byte using a replicated needle, XOR and one word search. -/
def NeedleXorFirstMatch.Goal : Prop := ∀ x b, (marked (fast (x ^^^ repeatByte b))).head? = (List.range 8).find? (fun i => lane x i == b.toNat)

/-- Masking the subtract result after an inactive prefix is enough to recover the first true match in the suffix. -/
def InactivePrefixBorrowContamination.Goal : Prop := ∀ x start, ((marked (fast x)).filter (start ≤ ·)).head? = ((marked (zeroBytes x)).filter (start ≤ ·)).head?

/-- A no-hit branch for a short tail may ignore every mask bit beyond the live prefix. -/
def ActivePrefixMissProof.Goal : Prop := ∀ x n, (marked (fast x)).any (· < n) = (marked (zeroBytes x)).any (· < n)

/-- Intersect two shifted subtract masks to find an exact two-byte literal without verifying candidate pairs. -/
def AdjacentDelimiterIntersection.Goal : Prop := ∀ x a b, ((List.range 7).filter (fun i => (marked (fast (x ^^^ repeatByte a))).contains i &&
    (marked (fast (x ^^^ repeatByte b))).contains (i+1))) =
  (List.range 7).filter (fun i => lane x i == a.toNat && lane x (i+1) == b.toNat)

#guard (marked (fast 0x0100)).take 2 == [0,1]

end TinylambOpt.ResearchGoals.JitKernels.BorrowMasks
