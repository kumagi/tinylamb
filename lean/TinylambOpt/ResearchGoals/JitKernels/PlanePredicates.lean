import Std

/-! Bit-sliced predicate generation for compressed unsigned lanes. Plane n carries bit n of each residual; all operations here preserve row ordinals and keep comparisons unsigned. -/
namespace TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

def bit (n j : Nat) := n.testBit j
def foldBits (x c w : Nat) := (List.range w).foldl (fun lt j =>
  if bit x j == bit c j then lt else bit c j) false
def planes (xs : List Nat) (j : Nat) := xs.map (bit · j)
def weight (xs : List Nat) (j : Nat) := (planes xs j).count true * 2^j

/-- A least-to-most-significant recurrence settles unsigned less-than directly on the encoded bits. -/
def PlaneComparisonWithoutDecode.Goal : Prop := ∀ x c w, x < 2^w → c < 2^w → foldBits x c w = decide (x < c)

/-- Truncate an out-of-frame threshold to the frame width before generating the ballot. -/
def UnsignedThresholdTruncation.Goal : Prop := ∀ x c w, x < 2^w → foldBits x c w = decide (x < c)

/-- Ignore a plane for comparison when it agrees between the threshold and the residual at every selected row. -/
def CommonPlaneElision.Goal : Prop := ∀ x c j w, bit x j = bit c j →
  foldBits (x ^^^ 2^j) (c ^^^ 2^j) w = foldBits x c w

/-- Infer the actual smallest residual by intersecting the bits set in every lane. -/
def OrFoldForRangeMinimum.Goal : Prop := ∀ x xs, (xs.foldl Nat.land x) = ((x::xs).min?).getD 0

/-- Compute SUM directly from bitplane populations without reconstructing any individual lane. -/
def PlanePopcountSum.Goal : Prop := ∀ xs w, (∀ x ∈ xs, x < 2^w) → ((List.range w).map (weight xs)).sum = xs.sum

/-- Subtract a page base from an inclusive SQL threshold in unsigned arithmetic, including thresholds below base. -/
def BiasedResidualThreshold.Goal : Prop := ∀ (base x c : Nat), (x ≤ c-base ↔ base+x ≤ c)

/-- Flip the sign bit so an unsigned plane comparator can order signed two's-complement lanes. -/
def XorBiasSignedOrder.Goal : Prop := ∀ (a b : BitVec 32), (a.toInt < b.toInt ↔ (a ^^^ 0x80000000).toNat < (b ^^^ 0x80000000).toNat)

/-- Per-column bit populations suffice to determine the product aggregate without intersecting the two ballots. -/
def SharedBitMarginalsDotProduct.Goal : Prop := ∀ (a b c : List Nat), a.length = b.length → b.Perm c →
  (a.zipWith (·*·) b).sum = (a.zipWith (·*·) c).sum

/-- Use the OR of all residual lanes as an exact synopsis of every low-width value appearing in a frame. -/
def DistinctMaskFromOr.Goal : Prop := ∀ (a b : List Nat), a.length = b.length → a.foldl Nat.lor 0 = b.foldl Nat.lor 0 →
  a.eraseDups.Perm b.eraseDups

end TinylambOpt.ResearchGoals.JitKernels.PlanePredicates
