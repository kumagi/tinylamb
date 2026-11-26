import TinylambOpt.ResearchGoals.AnalyticsWorkloads.Common

/-!
These models describe SQL scans and aggregates over file metadata and selected column
data.
-/

namespace TinylambOpt.ResearchGoals.CloudBI.NestedBI
open AnalyticsWorkloads

/-- IDs name deterministic branch plans with identical bindings, predicates, types
and snapshot. Distinct resolved plans may still return identical bags. -/
inductive Branches where
  | leaf (id : Nat)
  | unionAll (left right : Branches)
  deriving Repr

def leaves : Branches → List Nat
  | .leaf k => [k]
  | .unionAll l r => leaves l ++ leaves r

def run (source : Nat → List (Option Int)) : Branches → List (Option Int)
  | .leaf k => source k
  | .unionAll l r => run source l ++ run source r

def scale (a : Totals) (n : Nat) : Totals := ⟨a.cnt*n, a.nn*n, a.sum*(n:Int)⟩

/-- Flatten UNION ALL under an exact aggregate. Evaluate each distinct branch as
one partial state and multiply it by its number of occurrences in the nested plan. -/
def BranchHistogram.Goal : Prop := ∀ plan source,
  let h := histogram (leaves plan)
  h.toList.foldl (fun a (k,n) => plus a (scale (summarize (source k)) n)) zero =
    summarize (run source plan)

/-- A grouped key summary drives both direct and dimension-weighted leaves; the
dimension may have duplicate matches or NULL measures. A missing dimension row
suppresses only joined leaves. The compared observation includes COUNT(*). -/
def JoinedLeafWeights.Goal : Prop := ∀ (facts : List (Nat × Option Int))
    (dim : List (Nat × Int)) (requests : List (Nat × Bool)),
  let ref := requests.flatMap fun (k,joined) =>
    (facts.filter (·.1 == k)).flatMap fun (_,v) =>
      if joined then (dim.filter (·.1 == k)).map (fun (_,w) => v.map (· * w)) else [v]
  let mult := histogram requests
  let folded := (facts.map Prod.fst).eraseDups.foldl (fun total k =>
    let a := summarize ((facts.filter (·.1 == k)).map Prod.snd)
    let weights := (dim.filter (·.1 == k)).map Prod.snd
    let joined : Totals := ⟨a.cnt*weights.length,a.nn*weights.length,a.sum*weights.sum⟩
    plus total (plus (scale a (mult.getD (k,false) 0)) (scale joined (mult.getD (k,true) 0)))) zero
  folded = summarize ref

/-- A nested scalar SUM emits one nullable row even for an empty leaf. Outer SUM
ignores NULL. Flattening preserves the final SUM, although intermediate COUNT(*) differs. -/
def finish (a : Totals) : Option Int := if a.nn == 0 then none else some a.sum
def FlattenSum.Goal : Prop := ∀ (groups : List (List (List (Option Int)))),
  finish (summarize (groups.map (fun group =>
    finish (summarize (group.map (fun leaf => finish (summarize leaf))))))) =
    finish (summarize groups.flatten.flatten)

/-- Fuse nested SELECT lists by composing source slots. Duplicate projections, column
reordering and null extensions are explicit. None denotes a literal SQL NULL. -/
abbrev Projection := List (Option Nat)
def project (p : Projection) (r : List (Option Int)) :=
  p.map (fun i => i.bind (fun k => (r[k]?).join))
def composeSlots (p q : Projection) : Projection :=
  q.map (fun i => i.bind (fun k => (p[k]?).join))

def ProjectionChain.Goal : Prop := ∀ (ps : List Projection) (row : List (Option Int)) p,
  project (ps.foldl composeSlots p) row =
    ps.foldl (fun r q => project q r) (project p row)

/-- Merge many disjoint/overlapping range branches into endpoint counts. At a
shared endpoint the upper bound is exclusive. NULL qualifies for no comparison. -/
abbrev Interval := Int × Int
def RangeMultiplicity.Goal : Prop := ∀ (cuts : List Interval) (rows : List (Option Int)),
  (∀ i ∈ cuts, i.1 ≤ i.2) →
  let opened := (cuts.map Prod.fst).mergeSort
  let closed := (cuts.map Prod.snd).mergeSort
  let n := fun x => (opened.takeWhile (· ≤ x)).length - (closed.takeWhile (· ≤ x)).length
  let state := (rows.filterMap id).foldl (fun a x => plus a (weighted (n x) (some x))) zero
  state = summarize (cuts.flatMap (fun (lo,hi) =>
    rows.filter (fun v => v.any (fun x => lo ≤ x && x < hi))))

/-- Memoize parameterized pure leaves across nested consumers and also memoize
empty results. The second observation counts actual cache misses in evaluation order. -/
def replay (source : Nat → List (Option Int)) (ids : List Nat) :=
  ids.foldl (fun (cache,acc,calls) k => match cache[k]? with
    | some a => (cache,plus acc a,calls)
    | none =>
      let a := summarize (source k)
      (cache.insert k a,plus acc a,calls+1))
    (({} : Std.HashMap Nat Totals),zero,0)
def MemoMisses.Goal : Prop := ∀ plan source,
  let (_,a,n) := replay source (leaves plan)
  a = summarize (run source plan) ∧ n = (leaves plan).eraseDups.length

/-- Choose which repeated subplans to spool. Independent leaf evaluations have
additive producer, write and replay costs; the first consumer may stream directly.
A cold one-shot leaf may still be kept inline when read costs are high. -/
def planCost (uses : List Nat) (build write read : Nat → Nat) (spool : Nat → Bool) :=
  uses.eraseDups.foldl (fun n k =>
    let u := uses.count k
    n + (if spool k then build k + write k + (u-1)*read k else u*build k)) 0
def SpoolBudget.Goal : Prop := ∀ (uses : List Nat) (build write read : Nat → Nat) (policy : Nat → Bool),
  let chosen := fun k => decide (write k + ((uses.count k)-1)*read k < ((uses.count k)-1)*build k)
  planCost uses build write read chosen ≤ planCost uses build write read policy

/-- One projection can remove duplicate GROUP BY slots across a nest. Original
tuple equality distinguishes NULL from every ordinary value. Reorder first occurrences
independently of the input layout, and permit repeating a slot. -/
def GroupSignature.Goal : Prop := ∀ (slots : Projection) (x y : List (Option Int)),
  project slots.eraseDups x = project slots.eraseDups y ↔ project slots x = project slots y

/-- Canonicalize conjunct sets for memo lookup once column bindings have been
resolved. Idempotence and reordering apply to pure total three-valued expressions
observed in WHERE. A duplicate UNION ALL consumer still contributes its full bag. -/
def CanonicalFilters.Goal : Prop := ∀ (branches : List (List Nat)) (rows : List Nat)
    (atom : Nat → Nat → Option Bool),
  let accepts := fun (ids : List Nat) r => ids.all (fun i => atom i r == some true)
  let key := fun (ids : List Nat) => ids.eraseDups.mergeSort
  let h := histogram (branches.map key)
  h.toList.foldl (fun n (ids,w) => n + (rows.countP (accepts ids))*w) 0 =
    (branches.flatMap (fun ids => rows.filter (accepts ids))).length

/-- Push a bag of pure projection consumers through a projection list. Memoizing
the distinct composed slot vectors preserves UNION ALL output multiplicities,
including consumers with distinct aliases that return the same value tuple. -/
def SharedProjection.Goal : Prop := ∀ (p : Projection) (qs : List Projection)
    (rows : List (List (Option Int))) value,
  let requests := histogram (qs.map (composeSlots p))
  requests.toList.foldl (fun n (q,w) => n + w * ((rows.map (project q)).count value)) 0 =
    (qs.flatMap (fun q => (rows.map (project p)).map (project q))).count value

#guard finish (summarize [none,none]) == none && finish (summarize [some 2,some (-2)]) == some 0
#guard composeSlots [some 3,none,some 1] [some 2,some 0,some 0,some 1] == [some 1,some 3,some 3,none]
#guard (replay (fun _ => []) [1,1,2,1]).2.2 == 2

end TinylambOpt.ResearchGoals.CloudBI.NestedBI
