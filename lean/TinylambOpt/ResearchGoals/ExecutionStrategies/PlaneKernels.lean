import TinylambOpt.ResearchGoals.BitPlaneSum

namespace TinylambOpt.ResearchGoals.PlaneKernels
open BitPlaneSum (W B pack count)

/-! Sixty-four lanes of unsigned byte dictionary codes or numeric payloads.
The caller intersects SQL validity with the selection mask before using a result. -/
abbrev Planes := Nat → W
def planes (xs : Nat → B) : Planes := fun k => pack fun i => (xs i).getLsbD k
def lanes (m : W) := (List.range 64).filter m.getLsbD

/-- Low-to-high comparison replaces the decision whenever a higher pair differs. -/
def lessMask (a b : Planes) : W :=
  (List.range 8).foldl (fun lt k => (~~~(a k) &&& b k) ||| (~~~(a k ^^^ b k) &&& lt)) 0

def ParallelLess.Goal : Prop := ∀ (x y : Nat → B) (m : W),
  m &&& lessMask (planes x) (planes y) = m &&& pack (fun i => decide (x i < y i))

/-- Ripple carry across planes, never across rows; the outgoing plane signals overflow. -/
def add (a b : Planes) : List W × W :=
  let (rev, carry) := (List.range 8).foldl (fun (out,c) k =>
    let p := a k ^^^ b k
    ((p ^^^ c)::out, (a k &&& b k) ||| (p &&& c))) ([], 0)
  (rev.reverse, carry)

def PlaneAdd.Goal : Prop := ∀ (x y : Nat → B),
  let (bits,c) := add (planes x) (planes y)
  bits = (List.range 8).map (planes fun i => x i + y i) ∧
  c = pack (fun i => decide (256 ≤ (x i).toNat + (y i).toNat))

/-- Retain the minimum's lanes (all ties) by eliminating the high branch when possible. -/
def minMask (m : W) (a : Planes) : W :=
  (List.range 8).reverse.foldl (fun live k =>
    let zeros := live &&& ~~~(a k)
    if zeros == 0 then live else zeros) m

def ArgMinMask.Goal : Prop := ∀ (x : Nat → B) (m : W),
  minMask m (planes x) =
    pack fun i => m.getLsbD i && (lanes m).all fun j => x i ≤ x j

/-- Select an order statistic by popcount at each radix split, including duplicate lanes. -/
def kth (m : W) (a : Planes) (rank : Nat) : Option Nat := Id.run do
  if rank ≥ count m then return none
  let mut live := m
  let mut n := rank
  let mut v := 0
  for k in (List.range 8).reverse do
    let zeros := live &&& ~~~(a k)
    let z := count zeros
    if n < z then live := zeros else
      live := live &&& a k
      n := n-z
      v := v + 2^k
  return some v

def RadixSelect.Goal : Prop := ∀ (x : Nat → B) m k,
  kth m (planes x) k = ((lanes m).map (fun i => (x i).toNat)).mergeSort[k]?

/-- A dot product uses the populations of cross-plane intersections; no lane products. -/
def PlaneDot.Goal : Prop := ∀ (x y : Nat → B) (m : W),
  ((List.range 8).map fun j => ((List.range 8).map fun k =>
    2^(j+k) * count (m &&& planes x j &&& planes y k)).sum).sum =
  ((lanes m).map fun i => (x i).toNat * (y i).toNat).sum

/-- Select the median of three codes per lane using pairwise comparisons and masks.
All compared columns must use codes with the same unsigned total order. -/
def median3 (a b c : Planes) : Planes := fun k =>
  let ab := lessMask a b
  let ac := lessMask a c
  let bc := lessMask b c
  let midA := ab ^^^ ac
  -- With ties, either equal input can represent the answer.
  let midB := (~~~ab) ^^^ bc
  (midA &&& a k) ||| (~~~midA &&& midB &&& b k) ||| (~~~midA &&& ~~~midB &&& c k)

def MedianOfThree.Goal : Prop := ∀ (x y z : Nat → B) k,
  median3 (planes x) (planes y) (planes z) k =
    planes (fun i => max (min (x i) (y i)) (min (max (x i) (y i)) (z i))) k

/-- Distinct bit patterns: split only live prefixes; count leaves rather than decoding rows. -/
def occupied (a : Planes) (m : W) : Nat → Nat
  | 0 => if m == 0 then 0 else 1
  | d+1 => if m == 0 then 0 else
      occupied a (m &&& a d) d + occupied a (m &&& ~~~(a d)) d

def OccupiedCodes.Goal : Prop := ∀ (x : Nat → B) m,
  occupied (planes x) m 8 = (((lanes m).map x).eraseDups).length

/-- Skip a run of identical sorted codes by finding the first lane that differs from
the carried code; bit 64 is the end sentinel when every live lane is equal. -/
def changed (a : Planes) (v : B) : W :=
  (List.range 8).foldl (fun w k => w ||| (if v.getLsbD k then ~~~(a k) else a k)) 0

def RunBoundary.Goal : Prop := ∀ (x : Nat → B) (v : B) (m : W),
  (m &&& changed (planes x) v).ctz.toNat =
    ((lanes m).find? (fun i => x i != v)).getD 64

private def sample (i : Nat) : B := if i == 0 then 255 else 2
#guard (lessMask (planes sample) (planes fun _ => 3) &&& 3) == 2
#guard ((add (planes sample) (planes fun _ => 1)).2 &&& 3) == 1
#guard minMask 7 (planes sample) == 6
#guard kth 7 (planes sample) 1 == some 2
#guard count (7 &&& planes sample 1 &&& planes sample 7) == 1
#guard (median3 (planes sample) (planes fun _ => 2) (planes fun _ => 2) 0 &&& 7) == 0
#guard occupied (planes sample) 7 8 == 2
#guard ((6 : W) &&& changed (planes sample) 2).ctz.toNat == 64

end TinylambOpt.ResearchGoals.PlaneKernels
