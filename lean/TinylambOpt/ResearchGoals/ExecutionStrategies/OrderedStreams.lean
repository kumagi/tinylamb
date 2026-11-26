import Std

/-!
These contracts describe the state or ordering needed for alternative SQL execution
strategies.
-/

namespace TinylambOpt.ResearchGoals.OrderedStreams

abbrev Row := Int × Nat  -- ORDER BY key, observable payload
def leKey (a b : Row) := decide (a.1 ≤ b.1)
def sorted (xs : List Row) := xs.Pairwise fun a b => a.1 ≤ b.1

/-- A merge-path diagonal cut. The strict inequality gives the left run priority
on equal keys. Independent workers can start at certified (i,j) boundaries. -/
def cutOK (a b : List Row) (i j : Nat) : Prop :=
  i ≤ a.length ∧ j ≤ b.length ∧
  (∀ x ∈ (a.take i).getLast?, ∀ y ∈ b[j]?, x.1 ≤ y.1) ∧
  (∀ y ∈ (b.take j).getLast?, ∀ x ∈ a[i]?, y.1 < x.1)

-- Search for the first feasible left width. If the last consumed right key
-- belongs after the next left key (or ties it), the left width is too small.
def diagonal (a b : Array Row) (n : Nat) : Nat → Nat → Nat → Nat
  | 0, lo, _ => lo
  | fuel+1, lo, hi =>
    if lo ≥ hi then lo else
      let mid := (lo+hi)/2
      let j := n-mid
      let moreLeft := match b[j-1]?, a[mid]? with
        | some y, some x => j != 0 && x.1 ≤ y.1
        | _, _ => false
      if moreLeft then diagonal a b n fuel (mid+1) hi else diagonal a b n fuel lo mid

def mergeCut (a b : List Row) (n : Nat) :=
  let lo := n-b.length
  let hi := min n a.length
  let i := diagonal a.toArray b.toArray n ((hi-lo).log2+2) lo hi
  (i,n-i)

def MergeDiagonal.Goal : Prop := ∀ a b n, sorted a → sorted b → n ≤ a.length+b.length →
  let (i,j) := mergeCut a b n
  i+j = n ∧ cutOK a b i j ∧
    (∀ i' j', i'+j' = n → cutOK a b i' j' → i' = i ∧ j' = j) ∧
    (List.merge (a.drop i) (b.drop j) leKey) = (List.merge a b leKey).drop n

/-- A staircase cursor over increasing probe keys in a sorted build run never rewinds.
Duplicate probes must see the same first matching build occurrence. -/
def seekMany : List Int → List Row → List (Option Row)
  | [], _ => []
  | k::ks, rows =>
    let rest := rows.dropWhile fun r => r.1 < k
    (rest.head?.filter (fun r => r.1 == k)) :: seekMany ks rest

def MonotoneProbes.Goal : Prop := ∀ ks rows, ks.Pairwise (· ≤ ·) → sorted rows →
  seekMany ks rows = ks.map (fun k => rows.find? fun r => r.1 == k)

/-- Random access to a row in the Cartesian product of a matching run pair:
divide the local offset by the right width, preserving build order and duplicates. -/
abbrev Runs := List (List Nat × List Nat)
def seekPair : Runs → Nat → Option (Nat × Nat)
  | [], _ => none
  | (a,b)::rs, k =>
    if k < a.length*b.length then do
      return (← a[k/b.length]?, ← b[k%b.length]?)
    else seekPair rs (k-a.length*b.length)

def WeightedOffset.Goal : Prop := ∀ (rs : Runs) k,
  seekPair rs k = (rs.flatMap (fun (a,b) => a.flatMap fun x => b.map fun y => (x,y)))[k]?

/-- DESC run reuse for stable ORDER BY ASC. Reverse run order, preserving original
peer order inside each run. A full reversal breaks ties. -/
def ReversePeerRuns.Goal : Prop := ∀ (runs : List (Int × List Nat)),
  (runs.map Prod.fst).Pairwise (· > ·) →
  let expand := fun (rs : List (Int × List Nat)) => rs.flatMap fun (k,vs) => vs.map (k, ·)
  expand runs.reverse = (expand runs).mergeSort leKey

/-- Merge adjacent islands of ordered values with gap at most g. A range predicate
can seek each resulting interval provided the residual membership test is retained. -/
def islands (xs : List Nat) (g : Nat) : List (Nat × Nat) :=
  (xs.foldl (fun acc x => match acc with
    | [] => [(x,x)]
    | (l,r)::rest => if x ≤ r+g+1 then (l,x)::rest else (x,x)::acc) []).reverse

def RangeCoalescing.Goal : Prop := ∀ (keys : List Nat) (g : Nat) (rows : List Nat),
  let spans := islands keys.mergeSort g
  (spans.flatMap fun (lo,hi) => rows.filter fun x => lo ≤ x && x ≤ hi && keys.contains x).Perm
    (rows.filter keys.contains) ∧ spans.Pairwise (fun a b => a.2+g+1 < b.1)

/-- A page of sorted output from several locally sorted runs. For a total unique
row order, row x has global rank equal to the sum of its insertion ranks in the runs. -/
def RankSlice.Goal : Prop := ∀ (runs : List (List Nat)) lo size,
  runs.flatten.Nodup → (∀ r ∈ runs, r.Pairwise (· ≤ ·)) →
  ((runs.flatMap fun r => r.filter fun x =>
    let rank := (runs.map fun s => (s.takeWhile (· < x)).length).sum
    lo ≤ rank && rank < lo+size).mergeSort) = (runs.flatten.mergeSort.drop lo).take size

/-- External merge may emit a whole left block ahead of a right run when its last
key is at most the right minimum. Equal keys are admissible only on this side. -/
def SpliceBlock.Goal : Prop := ∀ (block tail right : List Row),
  sorted block → sorted right →
  (∀ x ∈ block.getLast?, ∀ y ∈ right.head?, x.1 ≤ y.1) →
  List.merge (block++tail) right leKey = block ++ List.merge tail right leKey

/-- Intersection of sorted duplicate runs by two cursors. Emit the lower multiplicity,
advancing one or both cursors; zero-length runs are permitted. -/
abbrev Rle := List (Nat × Nat)
def expand (xs : Rle) := xs.flatMap fun (k,n) => List.replicate n k

def intersectRuns : Nat → Rle → Rle → Rle
  | 0, _, _ | _+1, [], _ | _+1, _, [] => []
  | fuel+1, (x,n)::xs, (y,m)::ys =>
    if x < y then intersectRuns fuel xs ((y,m)::ys)
    else if y < x then intersectRuns fuel ((x,n)::xs) ys
    else (x,min n m)::intersectRuns fuel xs ys

def RunIntersection.Goal : Prop := ∀ (a b : Rle),
  (a.map Prod.fst).Pairwise (· < ·) → (b.map Prod.fst).Pairwise (· < ·) →
  ∀ k, (expand (intersectRuns (a.length+b.length) a b)).count k =
    min ((expand a).count k) ((expand b).count k)

#guard (List.range 5).map (mergeCut [(1,20),(1,30)] [(1,10),(2,40)]) ==
  [(0,0),(1,0),(2,0),(2,1),(2,2)]
#guard seekMany [2,2,3] [(2,8),(2,9),(4,1)] == [some (2,8),some (2,8),none]
#guard seekPair [([1],[]),([1,2],[3,4])] 2 == some (2,3)
#guard ([(2,1),(2,0),(1,8)] : List Row).mergeSort leKey == [(1,8),(2,1),(2,0)]
#guard islands [1,1,3,7] 1 == [(1,3),(7,7)]
#guard ([[1,3],[2,4]].map (fun s => (s.takeWhile (· < 3)).length)).sum == 2
#guard List.merge [(1,7),(3,9)] [(1,6)] leKey == [(1,7),(1,6),(3,9)]
#guard intersectRuns 4 [(2,3),(5,1)] [(1,0),(2,2)] == [(2,2)]

end TinylambOpt.ResearchGoals.OrderedStreams
