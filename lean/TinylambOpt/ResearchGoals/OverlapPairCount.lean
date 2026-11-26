import Std

/-!
These claims count pairs whose intervals overlap. A SQL range join can use the endpoint
structure to compute counts without expanding the result pairs.

Modeled claims

- `Goal` — `∀ a b, valid a → valid b → (count a b).1 = reference a.data b.data ∧
  ((count a b).2 = true → ∃ x ∈ a.data, ∃ y ∈ b.data, hit x y = false)`
-/

namespace TinylambOpt.ResearchGoals.OverlapPairCount

-- Intervals must be nonempty for this specialized inner join.
structure Span where
  lo : Int
  hi : Int

def hit (a b : Span) := a.lo < b.hi && b.lo < a.hi
def reference (a b : List Span) :=
  (a.map fun x => (b.filter (hit x)).length).sum

structure Bounds where
  minLo : Int
  maxLo : Int
  minHi : Int
  maxHi : Int

def bounds : List Span → Option Bounds
  | [] => none
  | x :: xs => some (xs.foldl (fun s y =>
    ⟨min s.minLo y.lo,max s.maxLo y.lo,min s.minHi y.hi,max s.maxHi y.hi⟩)
    ⟨x.lo,x.lo,x.hi,x.hi⟩)

structure Page where
  header : Option Bounds
  size : Nat
  data : List Span

-- true indicates the residual nested-loop scan opened the page pair.
def count (a b : Page) : Nat × Bool :=
  match a.header, b.header with
  | some x, some y =>
    if x.minLo ≥ y.maxHi || y.minLo ≥ x.maxHi then (0,false)
    else if x.maxLo < y.minHi && y.maxLo < x.minHi then (a.size*b.size,false)
    else (reference a.data b.data,true)
  | _, _ => (0,false)

def valid (p : Page) := p.header = bounds p.data ∧ p.size = p.data.length ∧
  ∀ r ∈ p.data, r.lo < r.hi

def Goal : Prop := ∀ a b, valid a → valid b →
  (count a b).1 = reference a.data b.data ∧
  ((count a b).2 = true → ∃ x ∈ a.data, ∃ y ∈ b.data, hit x y = false)

private def page (xs : List Span) : Page := ⟨bounds xs,xs.length,xs⟩
example : count (page [⟨0,2⟩]) (page [⟨2,3⟩]) = (0,false) := by decide
example : count (page [⟨0,4⟩,⟨0,4⟩]) (page [⟨1,2⟩]) = (2,false) := by decide
example : count (page [⟨0,1⟩,⟨8,9⟩]) (page [⟨4,5⟩]) = (0,true) := by decide

end TinylambOpt.ResearchGoals.OverlapPairCount
