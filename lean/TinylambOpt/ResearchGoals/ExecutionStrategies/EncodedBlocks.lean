import Std

/-!
These contracts describe the state or ordering needed for alternative SQL execution
strategies.
-/

namespace TinylambOpt.ResearchGoals.EncodedBlocks

/-! Immutable columns after fixing the visibility mask for a snapshot. Intermediates
use mathematical integers; fixed-width implementations need bounds for intermediates. -/

/-- Patched frame of reference: ordinary lanes compare in the narrow delta domain.
Exceptions *replace* their base code, including when that code falsely qualifies. -/
def PatchedRange.Goal : Prop := ∀ (base lo hi : Int) (deltas : List UInt8)
    (patch : Nat → Option Int) (live : Nat → Bool),
  let ids := List.range deltas.length
  let delta := fun i => ((deltas[i]?.getD 0).toNat : Int)
  (ids.filter fun i => live i && (match patch i with
    | some x => lo ≤ x && x < hi
    | none => lo-base ≤ delta i && delta i < hi-base)) =
  (ids.filter fun i =>
    let x := (patch i).getD (base+delta i)
    live i && lo ≤ x && x < hi)

/-- Rank in a validity bitmap locates a logical row in the compact payload stream.
Out-of-range and SQL NULL both produce none; valid duplicate values remain distinct lanes. -/
def dense {α : Type} (xs : List (Option α)) := xs.filterMap id

def SparseGather.Goal : Prop := ∀ (xs : List (Option Int)) i,
  let valid := xs.map Option.isSome
  (if valid[i]?.getD false then (dense xs)[(valid.take i).count true]? else none) = xs[i]?.join

/-- Two independently RLE-encoded columns can be aggregated without aligning their
run boundaries first: a run pair contributes the length of its positional overlap. -/
abbrev Run := Nat × Int
def unpack (rs : List Run) := rs.flatMap fun (n,x) => List.replicate n x

def spans (rs : List Run) :=
  (rs.foldl (fun (offset,acc) (n,x) => (offset+n,(offset,offset+n,x)::acc)) (0,[])).2.reverse

def OverlapDot.Goal : Prop := ∀ (a b : List Run),
  ((spans a).map fun (l,r,x) => ((spans b).map fun (u,v,y) =>
    ((min r v - max l u : Nat) : Int)*x*y).sum).sum =
    (((unpack a).zip (unpack b)).map (fun (x,y) => x*y)).sum

/-- Arithmetic-progression encoding lets SUM of an arbitrary run slice use its first
term, stride and clipped length. Signed strides and odd products are allowed. -/
def StrideSlice.Goal : Prop := ∀ (a d : Int) (n lo hi : Nat),
  let len := min n hi - lo
  2 * ((((List.range n).take hi).drop lo).map (fun (i : Nat) => a+(i:Int)*d)).sum =
    (len:Int)*(2*(a+(lo:Int)*d)+((len:Int)-1)*d)

/-- For a batch of additive range updates, build a difference array.
Repeated range additions become just two boundary impulses, including reversed/empty ranges. -/
abbrev Edit := Nat × Nat × Int
def impulses (es : List Edit) : Std.HashMap Nat Int :=
  es.foldl (fun h (l,r,d) =>
    if l < r then
      let h := h.insert l (h.getD l 0 + d)
      h.insert r (h.getD r 0 - d)
    else h) {}

def RangeEdits.Goal : Prop := ∀ (xs : List Int) (es : List Edit),
  let h := impulses es
  let adjustments := ((List.range xs.length).scanl (fun s i => s + h.getD i 0) 0).drop 1
  (xs.zipWith (· + ·) adjustments) = xs.zipIdx.map fun (x,i) =>
    x + (es.map fun (l,r,d) => if l ≤ i && i < r then d else 0).sum

/-- Across empty TLCS pages, carry the last nonempty key. Local sorted DISTINCT counts
double-count one value precisely at each equal adjacent nonempty page boundary. -/
def DistinctSeams.Goal : Prop := ∀ (pages : List (List Int)), pages.flatten.Pairwise (· ≤ ·) →
  let ps := pages.filter (fun p => !p.isEmpty)
  ((pages.map (fun p => p.eraseDups.length)).sum -
      (ps.zip (ps.drop 1)).countP (fun (a,b) => a.getLast? == b.head?)) =
    pages.flatten.eraseDups.length

/-- Use a binary search over the directory to map a compacted row ordinal back to its source. -/
def locate (ranks : Array Nat) (wanted : Nat) : Nat → Nat → Nat → Nat
  | 0, lo, _ => lo
  | fuel+1, lo, hi =>
    if lo ≥ hi then lo else
      let mid := (lo+hi)/2
      if ranks[mid+1]?.getD 0 ≤ wanted then locate ranks wanted fuel (mid+1) hi
      else locate ranks wanted fuel lo mid

def SelectDirectory.Goal : Prop := ∀ (mask : List Bool) k,
  let ranks := (mask.scanl (fun n bit => n + bit.toNat) 0).toArray
  let i := locate ranks k (mask.length.log2+2) 0 mask.length
  (if i < mask.length then some i else none) =
    ((mask.zipIdx.filter Prod.fst).map Prod.snd)[k]?

/-- Delta-of-delta compressed series can skip a block from its sum and its first
positional moment; the starting velocity may be nonzero. -/
def integrate (p v : Int) (ds : List Int) :=
  ds.foldl (fun (pos,vel) d => (pos+vel+d,vel+d)) (p,v)

def SecondDifferences.Goal : Prop := ∀ (p v : Int) (ds : List Int),
  integrate p v ds =
    (p+(ds.length:Int)*v + (ds.zipIdx.map fun (d,i) => ((ds.length-i:Int)*d)).sum, v+ds.sum)

#guard (let actual := fun (code : UInt8) (patch : Option Int) => patch.getD (-2 + (code.toNat : Int))
  ([actual 2 (some 9), actual 255 (some 0), actual 3 none].filter (fun x => 0 ≤ x && x < 2)) == [0,1])
#guard dense [none,some (4:Int),none,some 4] == [4,4]
#guard spans [(0,9),(2,1),(1,-2)] == [(0,0,9),(0,2,1),(2,3,-2)]
#guard ((List.range 3).map (fun (i : Nat) => 1-(i:Int)*2)).sum == -3
#guard (impulses [(0,2,3),(2,4,-1),(3,1,8)]).getD 2 0 == -4
#guard ([[1,2],[],[2,2]] : List (List Int)).flatten.eraseDups.length == 2
#guard locate #[0,0,1,1,2] 1 6 0 4 == 3
#guard integrate 5 (-1) [2,-3,0] == (2,-2)

end TinylambOpt.ResearchGoals.EncodedBlocks
