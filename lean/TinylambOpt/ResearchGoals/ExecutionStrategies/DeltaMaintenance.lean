import Std
import TinylambOpt.ResearchGoals.OuterJoinCounts

/-!
These contracts describe the state or ordering needed for alternative SQL execution
strategies.
-/

namespace TinylambOpt.ResearchGoals.DeltaMaintenance

/-! Before/after bags at two statement snapshots. Signed differences describe insertions
and deletions. No uniqueness or disjointness between a batch and an old bag is assumed. -/
abbrev Bag := List Int

def freq (xs : Bag) (k : Int) : Int := xs.count k
def delta (xs ys : Bag) (k : Int) := freq ys k - freq xs k

def joinKeys (a b : Bag) : Bag := a.flatMap fun x => b.filter (· == x)

/-- Simultaneous changes to both sides include a mixed product, even on an empty old join. -/
def JoinBatch.Goal : Prop := ∀ a b a' b' k,
  delta (joinKeys a b) (joinKeys a' b') k =
    delta a a' k * freq b k + freq a k * delta b b' k + delta a a' k * delta b b' k

/-- Delta of an equijoin chain. Telescoping alternates old and new side indexes. -/
def ChainBatch.Goal : Prop := ∀ a b c a' b' c' k,
  delta (joinKeys (joinKeys a b) c) (joinKeys (joinKeys a' b') c') k =
    delta a a' k * freq b k * freq c k + freq a' k * delta b b' k * freq c k +
    freq a' k * freq b' k * delta c c' k

/-- DISTINCT changes its emitted row only when the reference count crosses zero. -/
def thresholdChange (n d : Int) : Int :=
  if n == 0 && 0 < n+d then 1 else if 0 < n && n+d == 0 then -1 else 0

def DistinctCrossing.Goal : Prop := ∀ xs ys k,
  delta xs.eraseDups ys.eraseDups k = thresholdChange (freq xs k) (delta xs ys k)

/-- Only right-side zero crossings force replay of old matching left rows for EXISTS. -/
def semi (a b : Bag) := a.filter b.contains

def SemiWitness.Goal : Prop := ∀ a b a' b' k,
  delta (semi a b) (semi a' b') k =
    delta a a' k * (b'.contains k).toNat + freq a k * thresholdChange (freq b k) (delta b b' k)

/-- Apply a right-only edit to a left join; padding disappears on first match and
reappears on last deletion. SQL NULL keys never match. -/
def leftKeys (a b : OuterJoinCounts.Rows) := a.flatMap fun x =>
  let hits := b.filter (OuterJoinCounts.keyEq x)
  List.replicate (max 1 hits.length) x

def OuterPadding.Goal : Prop := ∀ (a b b' : OuterJoinCounts.Rows) k,
  ((leftKeys a b').count k : Int) - (leftKeys a b).count k =
    if k.isNone then 0 else (a.count k : Int) *
      ((max 1 (b'.count k) : Int) - max 1 (b.count k))

/-- One right removal hits at most its previous multiplicity in INTERSECT ALL. -/
def common : Bag → Bag → Bag
  | [], _ => []
  | x::xs, ys => if ys.contains x then x :: common xs (ys.erase x) else common xs ys

def IntersectionDelete.Goal : Prop := ∀ (a b removed : Bag) k,
  let b' := removed.foldl List.erase b
  delta (common a b) (common a b') k =
    -((min (a.count k) (b.count k) - b'.count k : Nat) : Int)

/-- A retained runner-up list repairs top-k after at most d deletions by value.
The boundary deliberately includes duplicates. All d may strike winners. -/
def removeBatch (xs dels : Bag) := dels.foldl List.erase xs

def TopKRepair.Goal : Prop := ∀ (xs dels adds : Bag) k,
  ((removeBatch (xs.mergeSort.take (k+dels.length)) dels ++ adds).mergeSort.take k) =
    ((removeBatch xs dels ++ adds).mergeSort.take k)

/-- Rebuild MIN from the surviving base only when the last occurrence of the old
minimum was removed. Insertions can supply a new smaller minimum in either branch. -/
def repairMin (xs dels adds : Bag) : Option Int :=
  match xs.min? with
  | none => adds.min?
  | some v => if dels.count v < xs.count v then (v::adds).min?
              else (removeBatch xs dels ++ adds).min?

def MinimumRepair.Goal : Prop := ∀ xs dels adds,
  repairMin xs dels adds = (removeBatch xs dels ++ adds).min?

#guard delta (joinKeys [] []) (joinKeys [1,1] [1,1,1]) 1 == 6
#guard delta (joinKeys [2] [2]) (joinKeys [2] []) 2 == -1
#guard thresholdChange 2 (-1) == 0 && thresholdChange 1 (-1) == -1
#guard semi [1,1,2] [2,2] == [2]
#guard leftKeys [none,some 1] [none,none,some 1,some 1] == [none,some 1,some 1]
#guard common [1,1,1,2] [1,1] == [1,1]
#guard removeBatch [1,1,2] [1,1] == [2]
#guard repairMin [1,1,2] [1,1] [] == some 2

end TinylambOpt.ResearchGoals.DeltaMaintenance
