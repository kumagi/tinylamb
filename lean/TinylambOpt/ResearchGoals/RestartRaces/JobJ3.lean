import Std

/-!
These models specify the ordering of SQL transaction, recovery, and concurrent
maintenance events.
-/

namespace TinylambOpt.ResearchGoals.RestartRaces.JobJ3

abbrev Edge := Nat × Nat -- role-qualified cast occurrence (person, movie)

namespace ProbeSchedule
/-! Two exact pure filters, unbounded positive/negative lookup caches, fixed
snapshot. One probe order per morsel. The second lookup is short-circuited. -/
structure Cache where
  people : List Nat
  movies : List Nat
def touch (ids : List Nat) (k : Nat) := if ids.contains k then (ids,0) else (k::ids,1)
def work (person movie : Nat → Bool) (firstPerson : Bool)
    (s : Cache × Nat) (e : Edge) : Cache × Nat :=
  let (p,c) := if firstPerson || movie e.2 then touch s.1.people e.1 else (s.1.people,0)
  let (m,d) := if !firstPerson || person e.1 then touch s.1.movies e.2 else (s.1.movies,0)
  (⟨p,m⟩, s.2+c+d)
def cheapest (p m : Nat → Bool) (s : Cache) : List (List Edge) → Nat
  | [] => 0
  | b::bs =>
    let a := b.foldl (work p m true) (s,0)
    let z := b.foldl (work p m false) (s,0)
    min (a.2 + cheapest p m a.1 bs) (z.2 + cheapest p m z.1 bs)
def greedy (p m : Nat → Bool) (s : Cache) : List (List Edge) → Nat
  | [] => 0
  | b::bs =>
    let a := b.foldl (work p m true) (s,0)
    let z := b.foldl (work p m false) (s,0)
    let win := if a.2 ≤ z.2 then a else z
    win.2 + greedy p m win.1 bs

/-- Aggressive scheduling hypothesis: exact current-block miss counts suffice
to choose an optimal local order despite cache reuse in later morsels.
Refutation would justify pricing future reuse even with perfect selectivities. -/
def Goal : Prop := ∀ p m bs, greedy p m ⟨[],[]⟩ bs = cheapest p m ⟨[],[]⟩ bs
end ProbeSchedule

namespace PersonPages
/-! A title-side filter supplies exact match multiplicities first. Gender is
stored in independently readable pages, absent entries fail the filter. -/
def result (edges : List Edge) (female : Nat → Bool) (titles : Nat → Nat) :=
  (edges.map (fun (p,m) => if female p then titles m else 0)).sum
def demand (edges : List Edge) (titles : Nat → Nat) (page : Nat → Nat) :=
  (edges.filterMap (fun (p,m) => if titles m == 0 then none else some (page p))).eraseDups

/-- Derive the least sufficient set of person pages for COUNT(*) over all
possible gender assignments, including key holes and repeated cast credits.
A synopsis proving the contents of additional pages changes the model. -/
def Goal : Prop := ∀ edges titles page (loaded : List Nat),
  (∀ f ∈ demand edges titles page, f ∈ loaded) ↔
    ∀ (a b : Nat → Bool), (∀ k, page k ∈ loaded → a k = b k) →
      result edges a titles = result edges b titles

#guard demand [(2,1),(5000,0),(2,1)] id (· / 256) == [0]
end PersonPages

namespace RankOneTile
/-! A sparse bipartite cast tile with bag multiplicities. Store one vector on
each axis instead of listing every pair. Zero rows/columns are permitted. -/
def dot (n : Nat) (a : Nat → Nat) (x : Nat → Int) :=
  ((List.range n).map (fun i => (a i : Int) * x i)).sum
def full (n m : Nat) (w : Nat → Nat → Nat) (x y : Nat → Int) :=
  ((List.range n).map (fun i => x i * dot m (w i) y)).sum

/-- Is equality of every cross product both necessary and sufficient to
represent a tile by two *integer*, nonnegative vectors for every selection
and year-weighted sum? Allows proportional neighborhoods beyond exact twins. -/
def Goal : Prop := ∀ n m w,
  (∀ i ∈ List.range n, ∀ j ∈ List.range n, ∀ k ∈ List.range m, ∀ l ∈ List.range m,
    w i k * w j l = w i l * w j k) ↔
  ∃ a b : Nat → Nat, ∀ x y, full n m w x y = dot n a x * dot m b y

#guard full 2 2 (fun i j => (2*i+2)*(3*j+1)) (fun _ => 1) (fun j => if j == 0 then 1970 else 2000)
  == 6*(1970+4*2000)
end RankOneTile

namespace DeltaRefresh
/-! A cached J3 numerator/count over a fixed snapshot. A committed batch
changes cast edges, qualifying person multiplicities and title-side weights.
Zeros for absent keys; domain contains old and new keys. -/
abbrev Axis := Nat → Int
abbrev Matrix := Nat → Nat → Int
def cell (w : Matrix) (p t : Axis) (i j : Nat) := w i j * p i * t j
def total (n m : Nat) (v : Nat → Nat → Int) :=
  ((List.range n).flatMap (fun i => (List.range m).map (v i))).sum
def refresh (dw : Matrix) (dp dt p t : Axis) (w : Matrix) (i j : Nat) :=
  dw i j * p i * t j +
  (w i j + dw i j) * dp i * t j +
  (w i j + dw i j) * (p i + dp i) * dt j

/-- Refresh using three sparse change streams inside one publication fence.
Later streams read the advanced earlier axes, including mixed insert/delete
terms. Use separately for COUNT, non-NULL years and exact year sum. -/
def Goal : Prop := ∀ n m w p t dw dp dt,
  total n m (cell w p t) + total n m (refresh dw dp dt p t w) =
    total n m (cell (fun i j => w i j + dw i j) (fun i => p i + dp i) (fun j => t j + dt j))

#guard refresh (fun _ _ => 1) (fun _ => 1) (fun _ => 2000) (fun _ => 0) (fun _ => 0)
  (fun _ _ => 0) 0 0 == 2000
end DeltaRefresh

end TinylambOpt.ResearchGoals.RestartRaces.JobJ3
