import TinylambOpt.ResearchGoals.Compression.Wire

namespace TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.Compression.Wire

/-! Ordered aggregation states for cached SQL pages and spill runs. The
reference evaluates a whole list; merge functions operate only on summaries.
All contracts quantify over reachable summaries, since arbitrary tuples may
violate a representation invariant. -/
structure Fold (σ : Type) where
  reference : List Nat → σ
  join : σ → σ → σ
def combine (f : Fold σ) (ps : List (List Nat)) :=
  (ps.map f.reference).foldl f.join (f.reference [])
def repeatState (f : Fold σ) (s : σ) (k : Nat) : σ :=
  if h : k=0 then f.reference [] else
    let a := repeatState f s (k/2)
    let sq := f.join a a
    if k%2==0 then sq else f.join sq s
termination_by k
decreasing_by exact Nat.div_lt_self (by omega) (by decide)
inductive Tree where
  | leaf (data : List Nat)
  | node (a b : Tree)
def plain : Tree → List Nat
  | .leaf xs => xs
  | .node a b => plain a ++ plain b
def rootState (f : Fold σ) : Tree → σ
  | .leaf xs => f.reference xs
  | .node a b => f.join (rootState f a) (rootState f b)
def longestZero (xs : List Nat) :=
  peak ((List.range (xs.length+1)).map (fun i => ((xs.drop i).takeWhile (· == 0)).length))
def edges (xs : List Nat) := xs.zip (xs.drop 1)
def optionalJoin (mix : α → α → α) : Option α → Option α → Option α
  | none,b => b
  | a,none => a
  | some a,some b => some (mix a b)

/-- Compute a page boundary correction using only the two serialized states. -/
def stitch (f : Fold σ) : Prop := ∀ a b, f.join (f.reference a) (f.reference b) = f.reference (a++b)

/-- Merge an arbitrary spill tree without visiting the raw rows again. -/
def treeReduce (f : Fold σ) : Prop := ∀ t, rootState f t = f.reference (plain t)

/-- Evaluate grammar repetition by binary powers of the summary. -/
def repeatPower (f : Fold σ) : Prop := ∀ xs n, repeatState f (f.reference xs) n = f.reference (List.replicate n xs).flatten

/-- Use only the two ragged boundaries and whole-page summaries for a window. -/
def frameSlice (f : Fold σ) : Prop := ∀ a (ps : List (List Nat)) z lo hi, lo≤a.length →
  f.join (f.reference (a.drop lo)) (f.join (combine f ps) (f.reference (z.take hi))) =
    f.reference (((a++ps.flatten++z).drop lo).take (a.length-lo+ps.flatten.length+hi))

/-- A saved prefix plus page states yields a scan checkpoint at each boundary. -/
def prefixCheckpoint (f : Fold σ) : Prop := ∀ init (ps : List (List Nat)),
  (ps.map f.reference).scanl f.join (f.reference init) =
    (List.range (ps.length+1)).map (fun i => f.reference (init++(ps.take i).flatten))

/-- Update one cached block using the left and right partial states. -/
def amendLeaf (f : Fold σ) : Prop := ∀ (ps : List (List Nat)) i xs, i<ps.length →
  combine f (ps.set i xs) = f.join (combine f (ps.take i)) (f.join (f.reference xs) (combine f (ps.drop (i+1))))

/-- Consume the lengths on a run-encoded column directly. -/
def runInput (f : Fold σ) : Prop := ∀ (rs : List (Nat × Nat)),
  (rs.map (fun (v,n) => repeatState f (f.reference [v]) n)).foldl f.join (f.reference []) =
    f.reference (rs.flatMap (fun (v,n) => List.replicate n v))

/-- Two neighboring states may be scheduled in reverse if they commute on their reachable values. -/
def reorderNeighbors (f : Fold σ) : Prop := ∀ pre a b suf,
  f.join (f.reference a) (f.reference b) = f.join (f.reference b) (f.reference a) →
  f.reference (pre++a++b++suf)=f.reference (pre++b++a++suf)

namespace LongestHole

/-- Longest zero run with head and tail run lengths. -/
def agg : Fold (Nat × Nat × Nat × Nat) where
  reference := fun xs => (xs.length,(xs.takeWhile (· == 0)).length,(xs.reverse.takeWhile (· == 0)).length,longestZero xs)
  join := fun (n,p,s,a) (m,q,t,b) => (n+m,if p==n then n+q else p,if t==m then m+s else t,max (max a b) (s+q))

def Stitch.Goal : Prop := stitch agg
def TreeReduce.Goal : Prop := treeReduce agg
def RepeatPower.Goal : Prop := repeatPower agg
def FrameSlice.Goal : Prop := frameSlice agg
def PrefixCheckpoint.Goal : Prop := prefixCheckpoint agg
def BridgeRun.Goal : Prop :=
  ∀ (a b : List Nat), let s := (a.reverse.takeWhile (· == 0)).length
  let p := (b.takeWhile (· == 0)).length
  longestZero (a++b)=max (max (longestZero a) (longestZero b)) (s+p)
def AmendLeaf.Goal : Prop := amendLeaf agg
def RunInput.Goal : Prop := runInput agg
def ReorderNeighbors.Goal : Prop := reorderNeighbors agg

def ZeroFill.Goal : Prop := ∀ n, agg.reference (List.replicate n 0) = (n,n,n,n)

end LongestHole

namespace Drawdown

/-- Maximum ordered drop from an earlier high to a later low. -/
def agg : Fold (Option (Nat × Nat × Nat)) where
  reference := fun xs => xs.head?.map (fun _ => (xs.min?.getD 0,peak xs,peak (xs.zipIdx.map (fun (x,i) => x - ((xs.drop i).min?.getD x)))))
  join := optionalJoin (fun (lo,hi,d) (l,h,e) => (min lo l,max hi h,max (max d e) (hi-l)))

def Stitch.Goal : Prop := stitch agg
def TreeReduce.Goal : Prop := treeReduce agg
def RepeatPower.Goal : Prop := repeatPower agg
def FrameSlice.Goal : Prop := frameSlice agg
def PrefixCheckpoint.Goal : Prop := prefixCheckpoint agg
def PeakRecovery.Goal : Prop :=
  ∀ a b, a≠[] → b≠[] → ((agg.reference (a++b)).map (fun r => r.2.2)).getD 0 ≥ peak a - b.min?.getD 0
def AmendLeaf.Goal : Prop := amendLeaf agg
def RunInput.Goal : Prop := runInput agg
def ReorderNeighbors.Goal : Prop := reorderNeighbors agg

def AscendingPage.Goal : Prop := ∀ xs, xs.Pairwise (· ≤ ·) → (agg.reference xs).all (fun e => e.2.2==0)

end Drawdown

namespace MonotoneBreaks

/-- Endpoints and count of descending edges for checking sort reuse. -/
def agg : Fold (Option (Nat × Nat × Nat)) where
  reference := fun xs => xs.head?.map (fun a => (a,xs.getLast?.getD a,(edges xs).countP (fun (x,y) => y<x)))
  join := optionalJoin (fun (a,b,n) (c,d,m) => (a,d,n+m+(decide (c<b)).toNat))

def Stitch.Goal : Prop := stitch agg
def TreeReduce.Goal : Prop := treeReduce agg
def RepeatPower.Goal : Prop := repeatPower agg
def FrameSlice.Goal : Prop := frameSlice agg
def PrefixCheckpoint.Goal : Prop := prefixCheckpoint agg
def ParallelMergeFree.Goal : Prop :=
  ∀ a b, a.Pairwise (· ≤ ·) → b.Pairwise (· ≤ ·) → (∀ x∈a, ∀ y∈b, x≤y) → (agg.reference (a++b)).all (fun v => v.2.2==0)
def AmendLeaf.Goal : Prop := amendLeaf agg
def RunInput.Goal : Prop := runInput agg
def ReorderNeighbors.Goal : Prop := reorderNeighbors agg

def SortingCertificate.Goal : Prop := ∀ xs, ((agg.reference xs).map (fun v => v.2.2)).getD 0 = 0 ↔ xs.Pairwise (· ≤ ·)

end MonotoneBreaks

namespace DeltaBytes

/-- First and last values correct the ULEB size of a predecessor-coded stream. -/
def agg : Fold (Option (Nat × Nat × Nat)) where
  reference := fun xs => xs.head?.map (fun a => (a,xs.getLast?.getD a,((edges xs).map (fun (x,y) => sbytes ((y:Int)-x))).sum))
  join := optionalJoin (fun (a,b,n) (c,d,m) => (a,d,n+m+sbytes ((c:Int)-b)))

def Stitch.Goal : Prop := stitch agg
def TreeReduce.Goal : Prop := treeReduce agg
def RepeatPower.Goal : Prop := repeatPower agg
def FrameSlice.Goal : Prop := frameSlice agg
def PrefixCheckpoint.Goal : Prop := prefixCheckpoint agg
def ReverseResidualCliff.Goal : Prop :=
  ∀ xs, let n:=(xs.length-1); let size:=fun (l : List Nat) => ((agg.reference l).map (fun v => v.2.2)).getD 0; size xs.reverse ≤ size xs+n
def AmendLeaf.Goal : Prop := amendLeaf agg
def RunInput.Goal : Prop := runInput agg
def ReorderNeighbors.Goal : Prop := reorderNeighbors agg

def RepeatedConstant.Goal : Prop := ∀ x n, agg.reference (x::List.replicate n x)=some (x,x,n)

end DeltaBytes

namespace PairSubsequence

/-- Count ordered pairs of marker 1 followed by marker 2. -/
def agg : Fold (Nat × Nat × Nat) where
  reference := fun xs => (xs.count 1,xs.count 2,(xs.zipIdx.map (fun (v,i) => if v==1 then (xs.drop (i+1)).count 2 else 0)).sum)
  join := fun (a,b,c) (x,y,z) => (a+x,b+y,c+z+a*y)

def Stitch.Goal : Prop := stitch agg
def TreeReduce.Goal : Prop := treeReduce agg
def RepeatPower.Goal : Prop := repeatPower agg
def FrameSlice.Goal : Prop := frameSlice agg
def PrefixCheckpoint.Goal : Prop := prefixCheckpoint agg
def ReversalComplement.Goal : Prop :=
  ∀ xs, (agg.reference xs).2.2 + (agg.reference xs.reverse).2.2 = xs.count 1 * xs.count 2
def AmendLeaf.Goal : Prop := amendLeaf agg
def RunInput.Goal : Prop := runInput agg
def ReorderNeighbors.Goal : Prop := reorderNeighbors agg

def BatchedMarkers.Goal : Prop := ∀ a b, agg.reference (List.replicate a 1 ++ List.replicate b 2)=(a,b,a*b)

end PairSubsequence

namespace RollingFingerprint

/-- Carry polynomial multiplier and residue together for subsequence equality screening. -/
def agg : Fold (Nat × Nat) where
  reference := fun xs => (257^xs.length%65521,xs.foldl (fun h x => (257*h+x)%65521) 0)
  join := fun (p,h) (q,j) => (p*q%65521,(h*q+j)%65521)

def Stitch.Goal : Prop := stitch agg
def TreeReduce.Goal : Prop := treeReduce agg
def RepeatPower.Goal : Prop := repeatPower agg
def FrameSlice.Goal : Prop := frameSlice agg
def PrefixCheckpoint.Goal : Prop := prefixCheckpoint agg
def EqualWindowResidues.Goal : Prop :=
  ∀ a b pre z, a.length=b.length → (agg.reference a).2=(agg.reference b).2 → agg.reference (pre++a++z)=agg.reference (pre++b++z)
def AmendLeaf.Goal : Prop := amendLeaf agg
def RunInput.Goal : Prop := runInput agg
def ReorderNeighbors.Goal : Prop := reorderNeighbors agg

def CollisionBoundary.Goal : Prop := agg.reference [0] = agg.reference [65521]

end RollingFingerprint

namespace BalanceFloor

/-- Signed balance and minimum prefix balance for an ordered debit/credit page. -/
def agg : Fold (Int × Int) where
  reference := fun xs =>
    let steps := xs.map (fun (x : Nat) => if x%2 == 0 then ((x/2:Nat):Int) else -((x/2+1:Nat):Int))
    (steps.sum, (steps.scanl (·+·) 0).min?.getD 0)
  join := fun (s,m) (t,n) => (s+t,min m (s+n))

def Stitch.Goal : Prop := stitch agg
def TreeReduce.Goal : Prop := treeReduce agg
def RepeatPower.Goal : Prop := repeatPower agg
def FrameSlice.Goal : Prop := frameSlice agg
def PrefixCheckpoint.Goal : Prop := prefixCheckpoint agg
def ShardAdmission.Goal : Prop :=
  ∀ a b (credit : Int), 0≤credit+(agg.reference (a++b)).2 ↔ 0≤credit+(agg.reference a).2 ∧ 0≤credit+(agg.reference a).1+(agg.reference b).2
def AmendLeaf.Goal : Prop := amendLeaf agg
def RunInput.Goal : Prop := runInput agg
def ReorderNeighbors.Goal : Prop := reorderNeighbors agg

def CreditLimit.Goal : Prop := ∀ xs (credit : Int), 0 ≤ credit+(agg.reference xs).2 ↔ ∀ i≤xs.length, 0≤credit+(agg.reference (xs.take i)).1

end BalanceFloor

namespace BitOccupancy

/-- Observed residue bits plus a count for saturating very small bloom filters. -/
def agg : Fold (Nat × Nat) where
  reference := fun xs => (xs.length,xs.foldl (fun a x => a ||| 2^(x%64)) 0)
  join := fun (n,a) (m,b) => (n+m,a ||| b)

def Stitch.Goal : Prop := stitch agg
def TreeReduce.Goal : Prop := treeReduce agg
def RepeatPower.Goal : Prop := repeatPower agg
def FrameSlice.Goal : Prop := frameSlice agg
def PrefixCheckpoint.Goal : Prop := prefixCheckpoint agg
def FoldInto32.Goal : Prop :=
  ∀ xs k, k<32 → ((agg.reference xs).2.testBit k || (agg.reference xs).2.testBit (k+32)) = xs.any (fun x => x%32==k)
def AmendLeaf.Goal : Prop := amendLeaf agg
def RunInput.Goal : Prop := runInput agg
def ReorderNeighbors.Goal : Prop := reorderNeighbors agg

def NegativeTest.Goal : Prop := ∀ xs x, !(agg.reference xs).2.testBit (x%64) → x ∉ xs

end BitOccupancy

namespace FirstLastChange

/-- Endpoint and total number of value runs for RLE planning. -/
def agg : Fold (Option (Nat × Nat × Nat)) where
  reference := fun xs => xs.head?.map (fun a => (a,xs.getLast?.getD a,1+(edges xs).countP (fun (x,y) => x != y)))
  join := optionalJoin (fun (a,b,n) (c,d,m) => (a,d,n+m-(decide (b=c)).toNat))

def Stitch.Goal : Prop := stitch agg
def TreeReduce.Goal : Prop := treeReduce agg
def RepeatPower.Goal : Prop := repeatPower agg
def FrameSlice.Goal : Prop := frameSlice agg
def PrefixCheckpoint.Goal : Prop := prefixCheckpoint agg
def DuplicateBlock.Goal : Prop :=
  ∀ xs k, xs≠[] → 0<k →
  let n := ((agg.reference xs).map (fun r => r.2.2)).getD 0
  ((agg.reference (List.replicate k xs).flatten).map (fun r => r.2.2)).getD 0 = k*n-(if xs.head?==xs.getLast? then k-1 else 0)
def AmendLeaf.Goal : Prop := amendLeaf agg
def RunInput.Goal : Prop := runInput agg
def ReorderNeighbors.Goal : Prop := reorderNeighbors agg

def ReverseCount.Goal : Prop := ∀ xs, (agg.reference xs.reverse).map (fun r => r.2.2) = (agg.reference xs).map (fun r => r.2.2)

end FirstLastChange

namespace IndexWeighted

/-- Sum of value times its physical ordinal, with a shift at the shard boundary. -/
def agg : Fold (Nat × Nat × Nat) where
  reference := fun xs => (xs.length,xs.sum,(xs.zipIdx.map (fun (v,i) => i*v)).sum)
  join := fun (n,s,a) (m,t,b) => (n+m,s+t,a+b+n*t)

def Stitch.Goal : Prop := stitch agg
def TreeReduce.Goal : Prop := treeReduce agg
def RepeatPower.Goal : Prop := repeatPower agg
def FrameSlice.Goal : Prop := frameSlice agg
def PrefixCheckpoint.Goal : Prop := prefixCheckpoint agg
def ReplaceAt.Goal : Prop :=
  ∀ xs i x, i<xs.length → (agg.reference (xs.set i x)).2.2+i*(xs[i]?.getD 0)=(agg.reference xs).2.2+i*x
def AmendLeaf.Goal : Prop := amendLeaf agg
def RunInput.Goal : Prop := runInput agg
def ReorderNeighbors.Goal : Prop := reorderNeighbors agg

def ReverseMoment.Goal : Prop := ∀ xs, (agg.reference xs.reverse).2.2 + (agg.reference xs).2.2 = (xs.length-1)*xs.sum

end IndexWeighted

#guard (LongestHole.agg.join (LongestHole.agg.reference [1,0,0]) (LongestHole.agg.reference [0,3])).2.2.2 == 3
#guard PairSubsequence.agg.reference [1,2,1,2] == (2,2,3)

end TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
