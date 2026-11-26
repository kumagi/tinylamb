import Std

/-! Byte-weighted external merge trees, pinned child buffers, and skew fallback.
Traffic counts bytes written at internal nodes. Space counts temporary output and
completed children that coexist while evaluating a tree from left to right. -/
namespace TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

inductive Run where
  | source (bytes : Nat)
  | merge (left right : Run)
  deriving Repr, DecidableEq

def size : Run → Nat
  | .source n => n
  | .merge a b => size a + size b

def writes : Run → Nat
  | .source _ => 0
  | .merge a b => writes a + writes b + size a + size b

/-- Source runs already exist. Charge the temporary file bytes allocated since the start. -/
def temp : Run → Nat
  | .source _ => 0
  | .merge a b => size a + size b

def peak : Run → Nat
  | .source _ => 0
  | .merge a b => max (peak a) (max (temp a + peak b) (temp a + temp b + size a + size b))

/-- A join-tree rotation chosen from outer run weights alone reduces bytes rewritten. -/
def WeightGuidedRotation.Goal : Prop := ∀ a b c,
  writes (.merge (.merge a b) c) ≤ writes (.merge a (.merge b c)) ↔ size a ≤ size c

/-- A schedule optimized for total writes also minimizes temporary-file high water for three runs. -/
def RewriteMinimumAlsoMinimizesPeak.Goal : Prop := ∀ a b c,
  writes (.merge (.merge a b) c) ≤ writes (.merge a (.merge b c)) →
  peak (.merge (.merge a b) c) ≤ peak (.merge a (.merge b c))

/-- Use the subtree with the larger excess peak first when only sibling evaluation order can change. -/
def SiblingExcessPeakOrder.Goal : Prop := ∀ a b, peak b - temp b ≤ peak a - temp a →
  peak (.merge a b) ≤ peak (.merge b a)

/-- Streaming the root to a consumer can admit a merge even when the final output file would exceed quota. -/
def StreamedRootSpaceCertificate.Goal : Prop := ∀ a b cap,
  peak a ≤ cap → temp a + peak b ≤ cap → temp a + temp b ≤ cap →
  (peak (.merge a b) > cap ↔ temp a + temp b + size a + size b > cap)

def fanIn (available descriptors writerReserve : Nat) (buffers : List Nat) : Prop :=
  buffers.length + 1 ≤ descriptors ∧ buffers.sum + writerReserve ≤ available

/-- Choosing the cheapest k readers gives a feasibility test with a descriptor held for the writer. -/
def CheapestReadersFanIn.Goal : Prop := ∀ (p q : List Nat) k mem fd out,
  p.Perm q → fanIn mem fd out (q.take k) →
  fanIn mem fd out ((p.mergeSort).take k)

/-- Median-size input buffers suffice to predict fan-in when variable-length keys are stored in the heap. -/
def MedianBufferFanIn.Goal : Prop := ∀ (bs : List Nat) m fd out, bs ≠ [] →
  let median := (bs.mergeSort)[bs.length/2]?.getD 0
  bs.length+1 ≤ fd → bs.length*median+out ≤ m → fanIn m fd out bs

/-- Charge only the branches actually followed by recursive repartition. -/
def shrinking : List Nat → Prop
  | a::b::rest => 5*b ≤ 4*a ∧ shrinking (b::rest)
  | _ => True

def GeometricRepartitionTraffic.Goal : Prop := ∀ a rest,
  shrinking (a::rest) → (a::rest).sum ≤ 5*a

/-- Progress in row count would allow dispensing with a byte-skew fallback for wide rows. -/
def RowShrinkPredictsByteShrink.Goal : Prop := ∀ (kept rest : List Nat),
  5*kept.length ≤ 4*(kept++rest).length → 5*kept.sum ≤ 4*(kept++rest).sum

/-- A rejected split still incurs one write pass; account for it after all accepted reductions. -/
def RejectedSplitWriteAllowance.Goal : Prop := ∀ a rest last,
  shrinking (a::rest) → (a::rest).getLast? = some last → (a::rest).sum + last ≤ 6*a

/-- Tile a single hot-key Cartesian product by output rank to resume after any spill boundary. -/
def rankPairs (right first count : Nat) :=
  (List.range count).map (fun i => ((first+i)/right, (first+i)%right))

def HotKeyResumeCoverage.Goal : Prop := ∀ l r cut, 0 < r → cut ≤ l*r →
  rankPairs r 0 cut ++ rankPairs r cut (l*r-cut) =
    (List.range l).flatMap (fun i => (List.range r).map (fun j => (i,j)))

/-- Page-size skew can make row-count-based rank tiling an unreliable per-tile scratch budget. -/
def RankTileByteBudget.Goal : Prop := ∀ (sizes : List Nat) tile unit,
  sizes.sum ≤ sizes.length*unit → ∀ i, ((sizes.drop i).take tile).sum ≤ tile*unit

#guard writes (.merge (.merge (.source 1) (.source 2)) (.source 8)) == 14

end TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling
