import Std

/-! Generated decimal parsers combine groups of ASCII digits before checking signed bounds. Values use exact integers so each delayed check and truncation is explicit. -/
namespace TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

def digits (s : List Nat) := s.all (fun x => 48 ≤ x && x ≤ 57)
def magnitude (s : List Nat) := s.foldl (fun n c => 10*n+(c-48)) 0
def parse (w : Nat) (neg : Bool) (s : List Nat) : Option Int := do
  if s.isEmpty || !digits s then none else
    let v : Int := if neg then -(magnitude s : Int) else (magnitude s : Int)
    if -(2^w : Int) ≤ v && v < (2^w : Int) then some v else none

/-- Parse digit groups independently and concatenate them with one constant multiply-add. -/
def DecimalBlockMultiplyAccumulate.Goal : Prop := ∀ a b, magnitude (a++b) = magnitude a*10^b.length + magnitude b

/-- Positive and negative parsing can use one magnitude cutoff without a special negative endpoint. -/
def SignedMagnitudeSharedLimit.Goal : Prop := ∀ w s, (parse w false s).isSome = (parse w true s).isSome

/-- Reject solely by digit count once the length exceeds that of the largest signed magnitude. -/
def LeadingZeroLengthGuard.Goal : Prop := ∀ w s, digits s → 10^(s.length-1) > 2^w → parse w false s = none

/-- If the full magnitude fits, no decimal append on an earlier digit can have overflowed that magnitude type. -/
def NarrowDecimalPrefixCertificate.Goal : Prop := ∀ s w n, digits s → magnitude s < 2^w → magnitude (s.take n) < 2^w

/-- Omit the high-nibble validation when every low nibble belongs to the decimal digit range. -/
def PackedLowNibblesValidateDigits.Goal : Prop := ∀ (s : List Nat), (∀ c ∈ s, c < 128 ∧ c%16 < 10) → s ≠ [] → digits s

/-- Test a nonnegative decimal append against a bound using a quotient test before the multiply. -/
def ExactChunkOverflowDivision.Goal : Prop := ∀ (a b cap k : Nat), b < 10^k → (a*10^k+b ≤ cap ↔ b ≤ cap ∧ a ≤ (cap-b)/10^k)

/-- Read into a wrapping machine accumulator and validate only its final magnitude. -/
def DeferredWrapThenRangeCheck.Goal : Prop := ∀ s w, digits s → magnitude s % 2^(w+1) < 2^w → s ≠ [] → (parse w false s).isSome

/-- Drop every leading zero before sending a value to the normal nonempty parser. -/
def TrimZerosPreservesAllZeroInput.Goal : Prop := ∀ w neg s, digits s → parse w neg (s.dropWhile (· == 48)) = parse w neg s

/-- A right-zero-padded decimal word can be decoded and rescaled by a constant power of ten. -/
def ShortChunkPaddingCompensation.Goal : Prop := ∀ s n, magnitude (s++List.replicate n 48) / 10^n = magnitude s

/-- Permit the one extra negative value by comparing unsigned magnitude directly with the signed negative limit. -/
def NegativeAccumulatorThreshold.Goal : Prop := ∀ w s, digits s → s ≠ [] → ((parse w true s).isSome = decide (magnitude s ≤ 2^w))

end TinylambOpt.ResearchGoals.JitKernels.DecimalChunks
