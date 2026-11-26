import Std

/-!
Partitioning for a spillable SQL operator must satisfy the modeled size or resource
bound for its partitions.
-/

namespace TinylambOpt.ResearchGoals.EngineBoundaries.RadixBudget

/-! Binary radix spill using a fixed number of hash bits from high to low.
Rows carry their individual serialized sizes. A child retaining over 80% of
its parent's bytes is handed to a skew handler if it still exceeds budget. -/
structure Row where
  hash : Nat
  bytes : Nat
  deriving BEq, DecidableEq
abbrev Batch := List Row
def size (rs : Batch) := (rs.map Row.bytes).sum
def bucket (bit : Nat) (v : Bool) (rs : Batch) :=
  rs.filter (fun r => r.hash.testBit bit == v)
def stagnant (child parent : Nat) : Bool := 5*child > 4*parent
structure Leaf where
  rows : Batch
  fallback : Bool
  deriving BEq
def leaves (budget : Nat) : Nat → Batch → List Leaf
  | 0, rs => [⟨rs, decide (budget < size rs)⟩]
  | bit+1, rs =>
    if size rs ≤ budget then [⟨rs, false⟩] else
      [false, true].flatMap (fun side =>
        let child := bucket bit side rs
        if child.isEmpty then [] else
        if budget < size child && stagnant (size child) (size rs) then [⟨child, true⟩]
        else leaves budget bit child)

/-- A repartition changes order but preserves the entire input multiset. -/
def LeafBag.Goal : Prop := ∀ b d rs, ((leaves b d rs).flatMap Leaf.rows).Perm rs

/-- No regular leaf silently exceeds the byte grant even with varying row lengths. -/
def GrantHonored.Goal : Prop := ∀ b d rs l, l ∈ leaves b d rs → l.fallback = false →
  size l.rows ≤ b

/-- Equal hash keys land in at most one leaf, preserving their original row order. -/
def PerHashOrder.Goal : Prop := ∀ b d rs h,
  ((leaves b d rs).flatMap Leaf.rows).filter (fun r => r.hash == h) =
    rs.filter (fun r => r.hash == h) ∧
  ((leaves b d rs).filter (fun l => l.rows.any (fun r => r.hash == h))).length ≤ 1

/-- Fallback has a byte-sized witness; a large row count alone cannot trigger it. -/
def FallbackWitness.Goal : Prop := ∀ b d rs l, l ∈ leaves b d rs → l.fallback = true →
  b < size l.rows

/-- At most one child can exceed the 80% threshold in a binary pass. -/
def UniqueHeavyChild.Goal : Prop := ∀ rs bit,
  ¬ (stagnant (size (bucket bit true rs)) (size rs) ∧
     stagnant (size (bucket bit false rs)) (size rs))

/-- A non-stagnant oversized path of d passes has a geometric bound on its bytes. -/
def GeometricProgress.Goal : Prop := ∀ (path : List Nat) n,
  ((n::path).zip path).all (fun (a,b) => !(stagnant b a)) →
  5^path.length * (path.getLast?.getD n) ≤ 4^path.length * n

/-- A heavy first bit need not imply that deeper bits are useless. -/
def StagnationIsPermanent.Goal : Prop := ∀ rs bit b, 0 < bit →
  let child := bucket bit false rs
  b < size child → stagnant (size child) (size rs) →
  b < max (size (bucket (bit-1) true child)) (size (bucket (bit-1) false child))

/-- A single tuple larger than the grant must survive in a fallback leaf. -/
def OversizedTuple.Goal : Prop := ∀ b d rs r, r ∈ rs → b < r.bytes →
  ∃ l ∈ leaves b d rs, l.fallback = true ∧ r ∈ l.rows

/-- Binary partitioning bounds the number of concurrently retained leaf descriptors. -/
def LeafCount.Goal : Prop := ∀ b d rs, (leaves b d rs).length ≤ 2^d

/-- Hot-key fallback pressure can only decrease when the same plan gets a larger grant. -/
def LargerGrant.Goal : Prop := ∀ small large d rs, small ≤ large →
  size (((leaves large d rs).filter Leaf.fallback).flatMap Leaf.rows) ≤
  size (((leaves small d rs).filter Leaf.fallback).flatMap Leaf.rows)

private def skew : Batch := [⟨0,6⟩, ⟨1,6⟩]
#guard (leaves 6 2 skew).map Leaf.fallback == [true]
#guard size (bucket 0 true skew) == 6 && size (bucket 0 false skew) == 6

end TinylambOpt.ResearchGoals.EngineBoundaries.RadixBudget
