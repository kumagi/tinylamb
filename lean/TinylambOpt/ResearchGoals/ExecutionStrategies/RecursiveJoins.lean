import TinylambOpt.ResearchGoals.RecursiveDelta

/-!
These contracts describe the state or ordering needed for alternative SQL execution
strategies.
-/

namespace TinylambOpt.ResearchGoals.RecursiveJoins
open RecursiveDelta (Edge Reach)

abbrev Graph := List Edge
abbrev path (g : Graph) (u v : Nat) := Reach g [u] v
def transpose (g : Graph) := g.map Prod.swap

/-- Incremental transitive closure after inserting one edge, even if it closes a cycle.
The two factors are from the old closure and include zero-length paths. -/
def InsertClosure.Goal : Prop := ∀ (g : Graph) u v s t,
  path ((u,v)::g) s t ↔ path g s t ∨ (path g s u ∧ path g v t)

/-- Contract a certified strongly connected partition before recursive UNION DISTINCT.
Projected self-edges and parallel edges can be removed from the quotient index. -/
def QuotientClosure.Goal : Prop := ∀ (g : Graph) (label : Nat → Nat) s t,
  (∀ x y, label x = label y → path g x y) →
  let edges := (g.map (fun (u,v) => (label u,label v))).eraseDups.filter (fun (u,v) => u != v)
  path g s t ↔ path edges (label s) (label t)

/-- Bidirectional search for bounded exact-length walks. Set membership is sufficient for EXISTS. -/
def endsAt (g : Graph) (s : Nat) : Nat → List Nat
  | 0 => [s]
  | d+1 => (endsAt g s d).flatMap fun u => (g.filter (fun e => e.1 == u)).map Prod.snd

def MeetFrontiers.Goal : Prop := ∀ g s t l r,
  t ∈ endsAt g s (l+r) ↔ ∃ v ∈ endsAt g s l, v ∈ endsAt (transpose g) t r

/-- Precomputed powers of a functional FK edge answer a k-hop self-join by the bits
of k. Keys without a successor remain absent at every positive distance. -/
def steps (f : Nat → Option Nat) (s : Nat) : Nat → Option Nat
  | 0 => some s
  | n+1 => (steps f s n).bind f

def liftJump (f : Nat → Option Nat) (k : Nat) : Nat → Nat → Option Nat
  | 0, s => some s
  | d+1, s =>
    let squared := fun v => (f v).bind f
    (if k%2 == 1 then f s else some s).bind (liftJump squared (k/2) d)

def BinaryLifting.Goal : Prop := ∀ f k s d, k < 2^d → liftJump f k d s = steps f s k

/-- Recursive UNION ALL path counts: propagate one frequency per vertex per layer.
Parallel edges contribute separately and cycles are safe at bounded depth. -/
abbrev Freq := Std.HashMap Nat Nat
def frequencies (g : Graph) (s : Nat) : Nat → Freq
  | 0 => ({} : Freq).insert s 1
  | d+1 =>
    let prev := frequencies g s d
    g.foldl (fun h (u,v) => h.insert v (h.getD v 0 + prev.getD u 0)) {}

def PathMultiplicity.Goal : Prop := ∀ g s d t,
  (frequencies g s d).getD t 0 = (endsAt g s d).count t

/-- Eliminate an internal degree-(1,1) connector from a recursive existence query.
Other edges exclude the connector and endpoints may coincide (cycle). -/
def SuppressConnector.Goal : Prop := ∀ (g : Graph) s t u v w,
  w ≠ s → w ≠ t → w ≠ u → w ≠ v → (∀ e ∈ g, e.1 ≠ w ∧ e.2 ≠ w) →
  (path ((u,w)::(w,v)::g) s t ↔ path ((u,v)::g) s t)

/-- Reverse reachability from a selective terminal is a magic-set filter. Edges
with an unreachable destination cannot contribute even to bounded UNION ALL bags. -/
def TargetCone.Goal : Prop := ∀ (g : Graph) s t d,
  let good := (RecursiveDelta.execute (transpose g) [t]).seen
  (endsAt (g.filter (fun e => good.contains e.2)) s d).count t = (endsAt g s d).count t

/-- Batch transitive reduction: each discarded edge must have a witness path in
the retained graph. Witnesses that only exist in another removed edge are insufficient. -/
def RedundantEdges.Goal : Prop := ∀ (kept removed : Graph) s t,
  (∀ e ∈ removed, path kept e.1 e.2) → (path (kept++removed) s t ↔ path kept s t)

/-- Bipartite four-cycle count. For each unordered left pair, choose two shared,
distinct right neighbors in one step, eliminating a second self-join. -/
def increasingPairs (vs : List Nat) := vs.flatMap fun u =>
  (vs.filter (u < ·)).map (u, ·)

def ButterflyCount.Goal : Prop := ∀ (g : Graph),
  let lefts := (g.map Prod.fst).eraseDups
  let rights := (g.map Prod.snd).eraseDups
  let covers := fun u v w => g.contains (u,w) && g.contains (v,w)
  ((increasingPairs lefts).map fun (u,v) =>
    let n := rights.countP (covers u v)
    n*(n-1)/2).sum =
  ((increasingPairs lefts).map fun (u,v) =>
    (increasingPairs rights).countP fun (w,z) => covers u v w && covers u v z).sum

/-- Count each undirected triangle in one canonical orientation; the three-role
self-join counts all six orientations, including when the input repeats edges. -/
def TriangleOrientation.Goal : Prop := ∀ (g : Graph),
  let vs := (g.map Prod.fst ++ g.map Prod.snd).eraseDups
  let adj := fun a b => a != b && (g.contains (a,b) || g.contains (b,a))
  let tri := fun a b c => adj a b && adj b c && adj c a
  (vs.map (fun a => (vs.map (fun b => vs.countP (tri a b))).sum)).sum =
    6 * (vs.map (fun a => ((vs.filter (a < ·)).map fun b =>
      (vs.filter (b < ·)).countP (tri a b)).sum)).sum

private def cycle : Graph := [(0,1),(1,2),(2,0),(0,1)]
#guard (RecursiveDelta.execute ((2,3)::cycle) [0]).seen.mergeSort == [0,1,2,3] &&
  !(RecursiveDelta.execute cycle [0]).seen.contains 3
#guard ((cycle.map (fun _ => (0,0))).eraseDups.filter (fun (u,v) => u != v)).isEmpty
#guard endsAt (transpose cycle) 2 1 == [1]
#guard liftJump (fun x => if x < 2 then some (x+1) else none) 3 2 0 == none
#guard (frequencies cycle 0 3).getD 0 0 == 2
#guard (RecursiveDelta.execute [(0,4),(4,2),(2,0)] [0]).seen.contains 2 &&
  (RecursiveDelta.execute [(0,2),(2,0)] [0]).seen.contains 2
#guard (RecursiveDelta.execute (transpose [(0,1),(0,9)]) [1]).seen.mergeSort == [0,1]
#guard (RecursiveDelta.execute [(0,1),(1,2)] [0]).seen.contains 2
#guard (let left := [1,2,3,3]; let right := [2,3,3,4]
  (increasingPairs (left.filter right.contains).eraseDups).length == 1)
#guard (([0,1,2].map fun a => ([0,1,2].map fun b =>
  [0,1,2].countP fun c => a != b && b != c && c != a).sum).sum) == 6

end TinylambOpt.ResearchGoals.RecursiveJoins
