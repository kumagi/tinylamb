import Std

/-! Radix JIT scatter derives destination addresses from bucket bases and lane-local ranks. Cursor words may be narrower than row ordinals; masked lanes own no destination. -/
namespace TinylambOpt.ResearchGoals.JitKernels.RadixCursors

def active (xs : List (Option Nat)) := xs.filterMap id
def rank (xs : List (Option Nat)) (i b : Nat) := (active (xs.take i)).count b
def base (xs : List (Option Nat)) (b : Nat) := (active xs).countP (· < b)
def dst (xs : List (Option Nat)) (i : Nat) : Option Nat := do
  let some b ← xs[i]? | none
  return base xs b + rank xs i b
def addresses (xs : List (Option Nat)) := (List.range xs.length).filterMap (dst xs)

/-- Vector bucket bases plus exclusive same-bucket ranks use each live destination exactly once. -/
def DenseStableScatterPermutation.Goal : Prop := ∀ xs, (addresses xs).Perm (List.range (active xs).length)

/-- One popcount over all preceding live lanes can replace same-bucket rank for a monotone radix stream. -/
def PrefixMaskAsBucketRank.Goal : Prop := ∀ xs i b, (active xs).Pairwise (· ≤ ·) → xs[i]? = some (some b) → rank xs i b = (active (xs.take i)).length

/-- The next vector can share immutable bucket bases when its write cursor includes the actual preceding histogram. -/
def CrossVectorCursorCarry.Goal : Prop := ∀ a b j k, rank (a++b) (a.length+j) k = (active a).count k + rank b j k

/-- Permuting lanes before histograms preserves the set of assigned destinations despite inactive lanes. -/
def SwizzleBucketOrder.Goal : Prop := ∀ a b, List.Perm a b → (addresses a).Perm (addresses b)

/-- Each local counter fits in one byte, so reducing multiple histograms with byte addition needs no carry handling. -/
def ByteHistogramCarryFree.Goal : Prop := ∀ (a b : List (Option Nat)) k, (active a).count k < 256 → (active b).count k < 256 →
  (active (a++b)).count k % 256 = (active (a++b)).count k

/-- Emit a 16-bit bucket-local ordinal with a wide base, including buckets above the 16-bit global boundary. -/
def NarrowOffsetAfterRebase.Goal : Prop := ∀ xs i k, xs[i]? = some (some k) → (active xs).count k ≤ 65536 →
  dst xs i = some (base xs k + rank xs i k % 65536)

/-- Replace invalid lanes by bucket zero before histogramming and discard their stores afterward. -/
def IgnoredNullAsBucketZero.Goal : Prop := ∀ xs i k, xs[i]? = some (some k) →
  dst (xs.map (fun x => some (x.getD 0))) i = dst xs i

/-- For one bucket, scatter addresses are consecutive in the original selection order. -/
def BucketSliceStableOrdinals.Goal : Prop := ∀ xs k, ((List.range xs.length).filter (fun i => xs[i]? == some (some k))).filterMap (dst xs) =
  (List.range ((active xs).count k)).map (base xs k + ·)

/-- Locally radix-sorted chunks can be concatenated directly for a globally bucket-ordered stream. -/
def ChunkLocalBaseThenConcatenate.Goal : Prop := ∀ (a b : List Nat), a.mergeSort ++ b.mergeSort = (a++b).mergeSort

/-- Reserve exactly the number of live rows in all buckets below a cutoff without scanning absent bucket IDs. -/
def SparseHistogramHeadReservation.Goal : Prop := ∀ xs cutoff, base xs cutoff = (((active xs).eraseDups.filter (· < cutoff)).map (fun k => (active xs).count k)).sum

end TinylambOpt.ResearchGoals.JitKernels.RadixCursors
