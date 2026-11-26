import Std

/-!
These claims compute or compare ranks for paired values. An ordered SQL operator must
preserve the pair ordering and the rank boundaries used in its result.

Modeled claims

- `Goal` — `∀ left right k, selectSum left right k = reference left right k`
-/

namespace TinylambOpt.ResearchGoals.PairRank

abbrev Column := List (Option UInt8)

def reference (left right : Column) (k : Nat) : Option Nat :=
  let sums := (left.filterMap id).flatMap fun x =>
    (right.filterMap id).map fun y => x.toNat + y.toNat
  sums.mergeSort[k]?

-- Ascending left and descending right form a monotone staircase.
def countLE (threshold : Nat) : Nat → List Nat → List Nat → Nat → Nat
  | 0, _, _, _ => 0
  | _+1, [], _, _ | _+1, _, [], _ => 0
  | fuel+1, x::xs, y::ys, width =>
    if x+y ≤ threshold then width + countLE threshold fuel xs (y::ys) width
    else countLE threshold fuel (x::xs) ys (width-1)

def search (count : Nat → Nat) (k : Nat) : Nat → Nat → Nat → Nat
  | 0, lo, _ => lo
  | fuel+1, lo, hi =>
    if lo = hi then lo else
      let mid := (lo+hi)/2
      if k < count mid then search count k fuel lo mid
      else search count k fuel (mid+1) hi

def selectSum (left right : Column) (k : Nat) : Option Nat :=
  let xs := ((left.filterMap id).map UInt8.toNat).mergeSort
  let ys := ((right.filterMap id).map UInt8.toNat).mergeSort.reverse
  if k ≥ xs.length * ys.length then none else
    some (search (fun t => countLE t (xs.length+ys.length) xs ys ys.length) k 9 0 510)

def Goal : Prop := ∀ left right k, selectSum left right k = reference left right k

private def sample (f : Column → Column → Nat → Option Nat) :=
  (List.range 5).map (f [some 255, none, some 1] [some 255, some 0])
example : sample selectSum = [some 1, some 255, some 256, some 510, none] := by cbv
example : sample reference = [some 1, some 255, some 256, some 510, none] := by cbv
example : selectSum [some 0, some 0] [some 0, some 0] 3 = some 0 := by cbv

end TinylambOpt.ResearchGoals.PairRank
