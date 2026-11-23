import Std

namespace TinylambOpt.ResearchGoals.IndexPreimages

/-! Exact preimages for seeking sorted indexes. All arithmetic in generated bounds is
widened. SQL NULL probes are filtered by the caller; these are the non-NULL key domains. -/

/-- Integer division toward zero has a double-width central bucket. d is positive. -/
def truncBounds (d q : Int) :=
  if q < 0 then (q*d-d+1,q*d) else if q == 0 then (1-d,d-1) else (q*d,q*d+d-1)

def TruncBucket.Goal : Prop := ∀ (x d q : Int), 0 < d →
  let (l,r) := truncBounds d q
  x.tdiv d = q ↔ l ≤ x ∧ x ≤ r

/-- Banker rounding for exact fixed-point values. Both half endpoints belong to an
even output and neither belongs to an odd output, including for negative inputs. -/
def nearestEven (n d : Int) :=
  let q := n/d
  let twice := 2*(n%d)
  if twice < d || (twice == d && q%2 == 0) then q else q+1

def RoundedBucket.Goal : Prop := ∀ (n d k : Int), 0 < d →
  (nearestEven n d = k ↔
    (let dist := (2*n-2*k*d).natAbs
     (dist:Int) < d ∨ ((dist:Int) = d ∧ k%2 = 0)))

/-- An invertible modular hash on a bounded dense key domain has a single preimage.
The inverse coefficient is validated once when preparing the plan. -/
def ModularLookup.Goal : Prop := ∀ (m a inv bias x target : Int),
  1 < m → (a*inv)%m = 1 → 0 ≤ x → x < m → 0 ≤ target → target < m →
  ((a*x+bias)%m = target ↔ x = (inv*(target-bias))%m)

/-- A band filter on a negative-slope affine expression flips both inequalities.
Use integer floor endpoints to seek an inclusive range even with negative inputs. -/
def DescendingBand.Goal : Prop := ∀ (x lo hi bias d : Int), 0 < d →
  ((lo ≤ bias-d*x ∧ bias-d*x < hi) ↔ (bias-hi)/d+1 ≤ x ∧ x ≤ (bias-lo)/d)

/-- ABS(checked(x-center)) constrains an index but may throw on a rejected row.
A block guard covers BOTH the subtraction and the asymmetric signed minimum. -/
def checkedDistance (m x center : Int) : Option Int :=
  let v := x-center
  if -m ≤ v && v < m && v != -m then some v.natAbs else none

def CheckedDistanceBand.Goal : Prop := ∀ (rows : List Int) (m l h center rad : Int),
  (∀ x ∈ rows, l ≤ x ∧ x ≤ h) → -m < l-center → h-center < m →
  rows.mapM (fun x => (checkedDistance m x center).map (fun n => decide (n ≤ rad))) =
    some (rows.map fun x => decide (center-rad ≤ x ∧ x ≤ center+rad))

/-- A composite index ordered NULLS LAST ASC then descending integer supports
keyset pagination by two disjoint ranges. Every identical cursor key is excluded. -/
abbrev Key := Option Int × Int

def cmpKey (a b : Key) : Ordering :=
  let first_ := match a.1,b.1 with
    | none,none => Ordering.eq
    | none,some _ => .gt
    | some _,none => .lt
    | some x,some y => compare x y
  if first_ == .eq then compare b.2 a.2 else first_

def CursorRanges.Goal : Prop := ∀ (rows : List Key) (cur : Key),
  let later := rows.filter fun r => match cur.1,r.1 with
    | some x,some y => x < y
    | some _,none => true
    | _,_ => false
  let peers := rows.filter fun r => r.1 == cur.1 && r.2 < cur.2
  rows.Pairwise (fun a b => cmpKey a b ≠ .gt) →
    peers ++ later = rows.filter (fun r => cmpKey cur r == .lt)

/-- Split an annulus predicate on |key-origin| into two disjoint inclusive seeks.
The zero lower radius counts the center only in the right seek. -/
def DistanceRing.Goal : Prop := ∀ (rows : List Int) (o l h : Int), 0 ≤ l →
  let neg := rows.filter fun x => o-h ≤ x && x ≤ o-max 1 l
  let pos := rows.filter fun x => o+l ≤ x && x ≤ o+h
  (neg ++ pos).Perm (rows.filter fun x =>
    let d : Int := (x-o).natAbs
    l ≤ d && d ≤ h)

/-- Morton index: an aligned square is one contiguous interval of interleaved codes.
Both component keys fit in d bits; b low bits of each coordinate are wildcards. -/
def morton (d x y : Nat) := ((List.range d).map fun i =>
  ((x.testBit i).toNat + 2*(y.testBit i).toNat)*4^i).sum

def MortonSquare.Goal : Prop := ∀ d b x y u v, b ≤ d → x < 2^d → y < 2^d →
  u < 2^(d-b) → v < 2^(d-b) →
  let start := morton (d-b) u v * 4^b
  (x/2^b = u ∧ y/2^b = v) ↔ start ≤ morton d x y ∧ morton d x y < start+4^b

/-- An index on day numbers can seek the next occurrence of a periodic weekday.
Euclidean remainder matters for days before the epoch. Empty ranges yield zero hits. -/
def PeriodicSeek.Goal : Prop := ∀ (lo hi period phase : Int), 0 < period → 0 ≤ phase → phase < period →
  let first_ := lo + (phase-lo)%period
  let n := ((hi-1-first_)/period+1).toNat
  (List.range n).map (fun (i : Nat) => first_+(i:Int)*period) =
    (((List.range (hi-lo).toNat).map (fun (i : Nat) => lo+i)).filter fun x => x%period == phase)

#guard truncBounds 3 (-1) == (-5,-3) && truncBounds 3 0 == (-2,2)
#guard nearestEven (-3) 2 == -2 && nearestEven (-1) 2 == 0
#guard ((3:Int)*5)%7 == 1 && (5*((1:Int)-3))%7 == 4
#guard (((0:Int)-4)/3+1) == -1
#guard checkedDistance 128 (-128) 0 == none
#guard cmpKey (some 4,2) (none,9) == .lt && cmpKey (none,2) (none,1) == .lt
#guard ([(-1:Int),0,0,1].filter fun x => x.natAbs == 0) == [0,0]
#guard morton 3 3 4 == 37
#guard ((2-(-3))%7 : Int) == 5

end TinylambOpt.ResearchGoals.IndexPreimages
