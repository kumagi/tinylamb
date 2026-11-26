import Std

/-!
These models compare concrete implementation choices in planning, execution,
concurrency, or recovery against their stated observable contracts.
-/

namespace TinylambOpt.ResearchGoals.EngineDecisions.Optimizer

namespace SharedBuild
/-! A memo alternative has private execution work and named, reusable builds.
A sibling may have paid for any subset. Costs and required properties are fixed. -/
structure Alt where
  privateCost : Nat
  builds : List Nat

def extra (price : Nat → Nat) (available required : List Nat) :=
  ((required.eraseDups.filter (fun k => !available.contains k)).map price).sum
def cost (price : Nat → Nat) (available : List Nat) (p : Alt) :=
  p.privateCost + extra price available p.builds

/-- A context-independent pruning test: charge A for its A-only builds and
give B all of its reusable builds for free. Is this worst case also necessary? -/
def Goal : Prop := ∀ price a b,
  (∀ available, cost price available a ≤ cost price available b) ↔
  a.privateCost + extra price b.builds a.builds ≤ b.privateCost

#guard cost (fun _ => 10) [] ⟨0,[1]⟩ > cost (fun _ => 10) [] ⟨6,[]⟩
#guard cost (fun _ => 10) [1] ⟨0,[1]⟩ < cost (fun _ => 10) [1] ⟨6,[]⟩
end SharedBuild

namespace SearchFence
/-! An interrupted group search retains the costs of finished alternatives and
one lower bound per unopened branch. Branch identity/properties are unchanged. -/
def certificate (done bounds : List Nat) := (done ++ bounds).min?
def canImprove (incumbent : Nat) (costs : List Nat) := costs.any (· < incumbent)

/-- Reuse the negative result only up to its certificate. The equivalence
quantifies over every admissible completion of the *same* search frontier. -/
def Goal : Prop := ∀ done bounds cutoff,
  ((certificate done bounds).all (cutoff ≤ ·)) ↔
  ∀ (actual : List (List Nat)), actual.length = bounds.length →
    (∀ b ∈ bounds.zip actual, ∀ v ∈ b.2, b.1 ≤ v) →
    canImprove cutoff (done ++ actual.flatten) = false

#guard certificate [11] [4] == some 4
#guard canImprove 6 [11,5]
end SearchFence

namespace ProjectExcept
/-! Each pair holds the two multiplicities of one distinct full row. All
pairs in a fiber project to the same output row (including SQL NULL peers). -/
abbrev Fiber := List (Nat × Nat)
def before (f : Fiber) := (f.map fun (l,r) => l-r).sum
def afterProjection (f : Fiber) := (f.map Prod.fst).sum - (f.map Prod.snd).sum

/-- Exact local condition for pushing a non-injective projection below EXCEPT
ALL. Different rows in one fiber must not cancel opposite-sign debts. -/
def Goal : Prop := ∀ f, before f = afterProjection f ↔
  (∀ p ∈ f, p.1 ≤ p.2) ∨ (∀ p ∈ f, p.2 ≤ p.1)

#guard before [(1,0),(0,1)] == 1 && afterProjection [(1,0),(0,1)] == 0
end ProjectExcept

namespace JoinPrefix
/-! Left input is in final order; all matches of each left occurrence are
contiguous in deterministic suffix order. An independent lower bound on
post-residual fanout can be zero. Operators here are total. -/
def prefixFor (lower : Nat → Nat) : Nat → List Nat → List Nat
  | 0, _ => []
  | _+1, [] => []
  | k+1, l::ls => l :: prefixFor lower (k+1-lower l) ls

/-- Push a variable prefix demand through a one-to-many join for OFFSET/LIMIT,
even when particular left keys have zero matches. No upper fanout is needed. -/
def Goal : Prop := ∀ (matchesFor : Nat → List Nat) lower ls off n,
  (∀ l ∈ ls, lower l ≤ (matchesFor l).length) →
  ((((prefixFor lower (off+n) ls).flatMap matchesFor).drop off).take n) =
    ((ls.flatMap matchesFor).drop off).take n

#guard prefixFor (fun k => if k == 1 then 0 else 3) 2 [1,2,3] == [1,2]
end JoinPrefix

namespace TupleAnti
/-! WHERE (x,y) NOT IN (...). A component inequality decides FALSE for row
equality even if another component is unknown. Only TRUE survives WHERE. -/
abbrev Val := Option Nat
abbrev Key := Val × Val
def differs : Val → Val → Bool
  | some a, some b => a != b
  | _, _ => false
def whereRef (q : Key) (rs : List Key) :=
  rs.all fun r => differs q.1 r.1 || differs q.2 r.2
structure Index where
  nonempty : Bool
  bothNull : Bool
  leftOnly : List Nat
  rightOnly : List Nat
  complete : List (Nat × Nat)
  lefts : List Nat
  rights : List Nat
  missingLeft : Bool
  missingRight : Bool

def build (rs : List Key) : Index :=
  ⟨!rs.isEmpty, rs.contains (none,none),
   (rs.filter (·.2.isNone)).filterMap Prod.fst,
   (rs.filter (·.1.isNone)).filterMap Prod.snd,
   rs.filterMap (fun (a,b) => do return (← a, ← b)),
   rs.filterMap Prod.fst, rs.filterMap Prod.snd,
   rs.any (·.1.isNone), rs.any (·.2.isNone)⟩
def keep (h : Index) : Key → Bool
  | (none,none) => !h.nonempty
  | (some a,none) => !h.missingLeft && !h.lefts.contains a
  | (none,some b) => !h.missingRight && !h.rights.contains b
  | (some a,some b) =>
    !h.bothNull && !h.leftOnly.contains a && !h.rightOnly.contains b && !h.complete.contains (a,b)

/-- Lower the filter to presence probes partitioned by NULL pattern. The
lists specify lookup contents; an implementation can use exact hash sets. -/
def Goal : Prop := ∀ rs q, keep (build rs) q = whereRef q rs

#guard keep (build [(some 2,none)]) (some 1,some 3)
#guard !(keep (build [(some 1,none)]) (some 1,some 3))
end TupleAnti

namespace ProjectedOrder
/-! Each nonempty run has one original leading key, and ascending suffixes.
A monotone projection may merge several leading keys into the same bucket. -/
abbrev Run := Nat × List Nat
def rows (f : Nat → Nat) (runs : List Run) :=
  runs.flatMap fun (key,tailKeys) => tailKeys.map (f key,·)
def lexLE (a b : Nat × Nat) := a.1 < b.1 ∨ (a.1 = b.1 ∧ a.2 ≤ b.2)
def boundary (f : Nat → Nat) (l r : Run) :=
  f l.1 ≠ f r.1 ∨ l.2.getLast?.getD 0 ≤ r.2.head?.getD 0

/-- Elide a suffix re-sort after a monotone, non-injective projection exactly
when adjacent nonempty runs coalescing into one bucket have compatible endpoints. -/
def Goal : Prop := ∀ f runs, (∀ a b, a ≤ b → f a ≤ f b) →
  runs.Pairwise (fun a b => a.1 ≤ b.1) → (∀ r ∈ runs, r.2.Pairwise (· ≤ ·)) →
  let live := runs.filter (!·.2.isEmpty)
  ((rows f runs).Pairwise lexLE ↔ ∀ pair ∈ live.zip (live.drop 1), boundary f pair.1 pair.2)

#guard rows (· / 10) [(11,[9]),(12,[1])] == [(1,9),(1,1)]
end ProjectedOrder

namespace JoinRevalidation
/-! A prepared plan omitted an unprojected INNER JOIN after certifying exact
one-match coverage. Compare old and new snapshots by the affected *keys*.
Only pure ordinary equality on non-NULL join keys is modeled. -/
def oneMatch (fact dim : List Nat) := fact.all (fun k => dim.count k == 1)
def dirty (oldFact newFact oldDim newDim : List Nat) :=
  (newFact.filter fun k => !oldFact.contains k || oldDim.count k != newDim.count k).eraseDups

/-- Revalidate an old elimination certificate by checking new active fact keys
and changed dimension multiplicities. Checking only inserted fact rows misses a
deleted dimension row. A mutation journal could supply the dirty set. -/
def Goal : Prop := ∀ a b c d, oneMatch a c →
  oneMatch b d = (dirty a b c d).all (fun k => d.count k == 1)

#guard oneMatch [3,3] [3] && !(oneMatch [3,3] [])
end JoinRevalidation

namespace NullableGroupKey
/-! Group a pair (nullable determinant, dependent value) with SQL grouping
equality. For a non-NULL determinant store the dependent only in the group's
representative. NULL determinants still use the entire pair as a hash key. -/
abbrev Row := Option Nat × Option Nat
def compact (r : Row) : Row := if r.1.isSome then (r.1,none) else r
def groups (xs : List Row) (key : Row → Row) :=
  (xs.map key).eraseDups.filterMap fun k => do
    let representative ← xs.find? (fun r => key r == k)
    return (representative, xs.countP (fun r => key r == k))

/-- Exactly which observed dependency licenses a narrower GROUP BY hash key?
The output includes both original grouping values and their multiplicity. -/
def Goal : Prop := ∀ xs,
  (groups xs compact).Perm (groups xs id) ↔
    ∀ a ∈ xs, ∀ b ∈ xs, a.1.isSome → a.1 = b.1 → a.2 = b.2

#guard (groups [(none,some 1),(none,some 2),(some 3,none)] compact).length == 3
#guard (groups [(some 3,none),(some 3,some 2)] compact).length == 1
end NullableGroupKey

end TinylambOpt.ResearchGoals.EngineDecisions.Optimizer
