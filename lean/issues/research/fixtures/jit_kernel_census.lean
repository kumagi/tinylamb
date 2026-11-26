import TinylambOpt.ResearchGoals.JitKernels.BorrowMasks
import TinylambOpt.ResearchGoals.JitKernels.FloatingReduction
import TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows
import Lean

/-! Reproducible bounded probes of all goals in three kernel models. Each probe
instantiates the declared proposition itself, including its hypotheses. A hit prints
one input invalidating that instance. No general proof or refutation is registered. -/
namespace JitKernelCensus
open Lean Meta Elab Term
open TinylambOpt.ResearchGoals.JitKernels

/-- Instantiate the outer data parameters of an existing Goal and leave its exact
remaining proposition to the decidable evaluator. Renaming or changing a goal's
inputs breaks a probe at elaboration instead of silently testing a copied formula. -/
elab "claim% " g:ident "(" args:term,* ")" : term => do
  let decl ← getConstInfo (← realizeGlobalConstNoOverloadWithInfo g)
  let some body := decl.value? | throwError "expected a defined proposition"
  let mut proposition := body
  for arg in args.getElems do
    let .forallE _ ty rest _ := proposition | throwError "too many goal inputs"
    let input ← elabTermEnsuringType arg (some ty)
    proposition := rest.instantiate1 input
  return proposition

def cross (as : List α) (bs : List β) := as.flatMap (fun a => bs.map (fun b => (a,b)))
def listsUpTo (alphabet : List α) : Nat → List (List α)
  | 0 => [[]]
  | n+1 => [] :: (cross alphabet (listsUpTo alphabet n)).map (fun (a,b) => a::b)

def probe [Repr α] (name : String) (cases : List α) (holds : α → Bool) : IO Unit := do
  if cases.isEmpty then throw (IO.userError s!"empty domain for {name}")
  match cases.find? (fun x => !(holds x)) with
  | some x => IO.println s!"witness {name} {repr x}"
  | none => IO.println s!"clear   {name} ({cases.length} instances)"

section
open BorrowMasks
open TinylambOpt.ResearchGoals.SwarDigitBallot (W)
def bytes : List Nat := [0,1,2,3,0x42,0x7f,0x80,0xff]
def words : List W := (cross bytes (cross bytes bytes)).map (fun (a,b,c) =>
  BitVec.ofNat 64 (0x4242424242000000+a+256*b+65536*c))
def needles := bytes.map (BitVec.ofNat 8)
def borrows : IO Unit := do
  probe "BorrowAnyByteHit" words fun x => decide (claim% BorrowAnyByteHit.Goal (x))
  probe "BorrowBallotForCompaction" words fun x => decide (claim% BorrowBallotForCompaction.Goal (x))
  probe "BorrowEarliestHit" words fun x => decide (claim% BorrowEarliestHit.Goal (x))
  probe "BorrowReverseDelimiter" words fun x => decide (claim% BorrowReverseDelimiter.Goal (x))
  probe "BorrowRankBeforeLimit" (cross words (List.range 9)) fun (x,n) => decide (claim% BorrowRankBeforeLimit.Goal (x,n))
  probe "BorrowImpossibleNeighbor" words fun x => decide (claim% BorrowImpossibleNeighbor.Goal (x))
  probe "NeedleXorFirstMatch" (cross words needles) fun (x,b) => decide (claim% NeedleXorFirstMatch.Goal (x,b))
  probe "InactivePrefixBorrowContamination" (cross words (List.range 9)) fun (x,n) =>
    decide (claim% InactivePrefixBorrowContamination.Goal (x,n))
  probe "ActivePrefixMissProof" (cross words (List.range 9)) fun (x,n) => decide (claim% ActivePrefixMissProof.Goal (x,n))
  probe "AdjacentDelimiterIntersection" (cross words (cross needles needles)) fun (x,a,b) =>
    decide (claim% AdjacentDelimiterIntersection.Goal (x,a,b))
end

section
open ReciprocalWindows
local instance (x d s : Nat) : Decidable (exactAt x d s) := inferInstanceAs (Decidable (guess x d s = x/d))
def divisions : IO Unit := do
  let cases := cross (List.range 33) (cross (List.range 13) (List.range 8))
  probe "ResidueSensitiveNoCorrection" cases fun (x,d,s) => decide (claim% ResidueSensitiveNoCorrection.Goal (x,d,s))
  probe "LastDividendCertifiesRange" cases fun (x,d,s) => decide (claim% LastDividendCertifiesRange.Goal (x,d,s))
  probe "WorstResidueGuard" cases fun (x,d,s) => decide (claim% WorstResidueGuard.Goal (x,d,s))
  probe "LastResidueRepresentative" cases fun (x,d,s) => decide (claim% LastResidueRepresentative.Goal (x,d,s))
  probe "RadixPrecisionMonotonicity" cases fun (x,d,s) => decide (claim% RadixPrecisionMonotonicity.Goal (x,d,s))
  probe "RoundedDownAddOne" cases fun (x,d,s) => decide (claim% RoundedDownAddOne.Goal (x,d,s))
  probe "ResidualFromWrappedLowProduct" (cross cases (List.range 6)) fun ((x,d,s),w) =>
    decide (claim% ResidualFromWrappedLowProduct.Goal (x,d,s,w))
  probe "FixedDivisorPeeling" (cross cases (List.range 4)) fun ((x,d,s),k) =>
    decide (claim% FixedDivisorPeeling.Goal (x,d,k,s))
  let aggregates := cross (listsUpTo (List.range 6) 3) (cross (List.range 7) (List.range 7))
  probe "FusedQuotientSum" aggregates fun (xs,d,r) => decide (claim% FusedQuotientSum.Goal (xs,d,r))
  probe "FixedResidueSumCorrection" aggregates fun (xs,d,r) => decide (claim% FixedResidueSumCorrection.Goal (xs,d,r))
end

section
open FloatingReduction
local instance (a b : Float) : Decidable (same a b) := inferInstanceAs (Decidable (bits a = bits b))
-- Use bit patterns as inputs and in reports so that signed zeros, subnormals and
-- payload changes remain visible even when decimal repr rounds values together.
def patterns : List UInt64 :=
  [0,0x8000000000000000,1,0x8000000000000001,0x0010000000000000,
   0x3ff0000000000000,0x4000000000000000,0x4008000000000000,0x4024000000000000,
   0x4340000000000000,0xbff0000000000000,0xc340000000000000,0x7fefffffffffffff,
   0xffefffffffffffff,0x7ff0000000000000,0xfff0000000000000,0x7ff8000000000001]
abbrev f := Float.ofBits
def floats : IO Unit := do
  let pairs := cross patterns patterns
  let triples := cross patterns pairs
  probe "FinitePairwiseAccumulators" triples fun (a,b,c) => decide (claim% FinitePairwiseAccumulators.Goal (f a,f b,f c))
  let maskedRows := listsUpTo (none :: (patterns.map some)) 2
  probe "AbsentLaneAsPositiveZero" (cross patterns maskedRows) fun (s,xs) =>
    decide (claim% AbsentLaneAsPositiveZero.Goal (f s,xs.map (Option.map f)))
  probe "MultiplyMaskInsteadOfSelect" patterns fun x => decide (claim% MultiplyMaskInsteadOfSelect.Goal (f x))
  let rows := listsUpTo patterns 2
  probe "FirstInfinityFromFinalAccumulator" (cross rows patterns) fun (xs,s) =>
    decide (claim% FirstInfinityFromFinalAccumulator.Goal (xs.map f,f s))
  probe "FiniteOverflowLaneReplay" (cross patterns (cross (listsUpTo patterns 1) rows)) fun (s,a,b) =>
    decide (claim% FiniteOverflowLaneReplay.Goal (f s,a.map f,b.map f))
  probe "CanonicalZeroHashJoin" pairs fun (a,b) => decide (claim% CanonicalZeroHashJoin.Goal (f a,f b))
  probe "GroupedMeanReciprocal" (cross patterns ([1,2,3,5,7,10,255] : List UInt64)) fun (s,n) =>
    decide (claim% GroupedMeanReciprocal.Goal (f s,n))
  probe "SameSignReassociation" triples fun (a,b,c) => decide (claim% SameSignReassociation.Goal (f a,f b,f c))
  probe "ExactBinaryPowerScale" pairs fun (a,b) => decide (claim% ExactBinaryPowerScale.Goal (f a,f b))
  probe "NumericCompareAsSignedBitKey" pairs fun (a,b) => decide (claim% NumericCompareAsSignedBitKey.Goal (f a,f b))
  probe "MinMaxTieKeepsLeftBits" pairs fun (a,b) => decide (claim% MinMaxTieKeepsLeftBits.Goal (f a,f b))
end
end JitKernelCensus

def main : IO Unit := do
  JitKernelCensus.borrows
  JitKernelCensus.divisions
  JitKernelCensus.floats
