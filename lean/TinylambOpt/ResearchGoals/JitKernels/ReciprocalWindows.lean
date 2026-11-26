import Std

/-! Per-range reciprocal synthesis for generated integer division. A multiplier rounded up can cross the next quotient boundary only through a product of its error and the dividend. -/
namespace TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

def magic (d s : Nat) := (2^s+d-1)/d
def guess (x d s : Nat) := x*magic d s / 2^s
def excess (d s : Nat) := d*magic d s - 2^s
def exactAt (x d s : Nat) := guess x d s = x/d

/-- Omit correction exactly when the reciprocal error cannot push this dividend past its next quotient boundary. -/
def ResidueSensitiveNoCorrection.Goal : Prop := ∀ x d s, 0 < d → (exactAt x d s ↔ x*excess d s < (d-x%d)*2^s)

/-- A successful test on the largest possible dividend certifies the multiplier for the full range below it. -/
def LastDividendCertifiesRange.Goal : Prop := ∀ bound d s, 0 < d → exactAt bound d s → ∀ x ≤ bound, exactAt x d s

/-- Generate a single conservative guard using the range maximum regardless of which residues occur. -/
def WorstResidueGuard.Goal : Prop := ∀ bound d s, 0 < d → bound*excess d s < 2^s → ∀ x ≤ bound, exactAt x d s

/-- Check only the largest reachable dividend in each residue class to certify an entire profiled window. -/
def LastResidueRepresentative.Goal : Prop := ∀ bound d s, 0 < d → (∀ x ≤ bound, bound < x+d → exactAt x d s) → ∀ x ≤ bound, exactAt x d s

/-- Increasing reciprocal precision by one bit cannot introduce an overestimate where the earlier kernel was exact. -/
def RadixPrecisionMonotonicity.Goal : Prop := ∀ x d s, 0 < d → exactAt x d s → exactAt x d (s+1)

/-- Replace an exact division by a downward-rounded multiplier and an unconditional increment of the dividend. -/
def RoundedDownAddOne.Goal : Prop := ∀ x d s, 1 < d → x < 2^s → (x+1)*(2^s/d)/2^s = x/d

/-- Validate a quotient candidate using only a narrow remainder, omitting an explicit overestimate comparison. -/
def ResidualFromWrappedLowProduct.Goal : Prop := ∀ x d s w, 0 < d → x < 2^w → (x+2^w-(guess x d s*d)%2^w)%2^w < d → exactAt x d s

/-- Strip powers of two from an unsigned divisor and shift the input before the odd-divisor kernel. -/
def FixedDivisorPeeling.Goal : Prop := ∀ x odd k s, 0 < odd → exactAt (x/2^k) odd s →
  guess (x/2^k) odd s = x/(odd*2^k)

/-- For aggregation, combine dividends before dividing when every vector lane has the same nonzero remainder. -/
def FusedQuotientSum.Goal : Prop := ∀ (xs : List Nat) d r, 0 < d → r < d → (∀ x ∈ xs, x%d = r) → (xs.map (·/d)).sum = xs.sum/d

/-- Compute a quotient aggregate from one wide sum and one correction for the shared remainder class. -/
def FixedResidueSumCorrection.Goal : Prop := ∀ (xs : List Nat) d r, 0 < d → (∀ x ∈ xs, x%d = r) →
  (xs.map (·/d)).sum = (xs.sum-xs.length*r)/d

end TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows
