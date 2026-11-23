import Std

namespace TinylambOpt.ResearchGoals.SearchBounds

/-! Conservative exact bounds for Cascades cardinality properties, grants, runtime
pruning and capped execution. Histograms/certificates refer to the queried snapshot. -/
def counts (xs : List Nat) := xs.eraseDups.map xs.count
def secondMoment (xs : List Nat) := ((counts xs).map (· ^ 2)).sum
def fanout (a b : List Nat) := (a.map b.count).sum

/-- The join size bound needs second moments only; no alignment of distinct-key lists. -/
def CollisionBound.Goal : Prop := ∀ a b,
  (fanout a b)^2 ≤ secondMoment a * secondMoment b

/-- With both complete frequency spectra available, align the largest with the largest.
This remains an upper bound when the two supports have different cardinalities. -/
def SortedDegreeBound.Goal : Prop := ∀ a b,
  fanout a b ≤ ((((counts a).mergeSort.reverse).zip ((counts b).mergeSort.reverse)).map
    (fun (x,y) => x*y)).sum

/-- Three-way cyclic set join; projection-size bound for a triangle (duplicates have
already been eliminated independently in each relation). -/
abbrev Edges := List (Nat × Nat)

def triangles (r s t : Edges) := r.flatMap fun (x,y) =>
  (s.filter fun (y',z) => y == y' && t.contains (z,x)).map fun (_,z) => (x,y,z)

def TriangleBound.Goal : Prop := ∀ (r s t : Edges), r.Nodup → s.Nodup → t.Nodup →
  (triangles r s t).length ^ 2 ≤ r.length * s.length * t.length

/-- Misra–Gries candidate extraction for exact HAVING. A complete recount only needs
the surviving candidates when the threshold is strictly above n/(slots+1). -/
def candidates (xs : List Nat) (slots : Nat) : Std.HashMap Nat Nat :=
  xs.foldl (fun h k =>
    if h.contains k then h.insert k (h.getD k 0 + 1)
    else if h.size < slots then h.insert k 1
    else h.fold (fun acc j n => if n == 1 then acc else acc.insert j (n-1)) {}) {}

def FrequentCandidates.Goal : Prop := ∀ xs slots k,
  (candidates xs slots).size ≤ slots ∧
  (xs.length < xs.count k * (slots+1) → (candidates xs slots).contains k = true)

/-- An equijoin count can finish after the prefix once LIMIT over matching pairs
saturates. Bounded arithmetic stores only the remaining quota. -/
def remaining (freq : Std.HashMap Nat Nat) : Nat → List Nat → Nat
  | 0, _ => 0
  | n+1, [] => n+1
  | n+1, x::xs => remaining freq (n+1 - freq.getD x 0) xs

def histogram (xs : List Nat) : Std.HashMap Nat Nat :=
  xs.foldl (fun h x => h.insert x (h.getD x 0 + 1)) {}

def CappedJoin.Goal : Prop := ∀ a b k, k - remaining (histogram b) k a = min k (fanout a b)

/-- Per-key upper bounds for a group sum with signed future values: positives can
improve it, negative terms cannot. Strict cutoff preserves threshold ties. -/
def optimistic (seen : Int) (rest : List (Int × Nat)) :=
  seen + (rest.map fun (upper,n) => max 0 upper * (n:Int)).sum

def GroupSumFence.Goal : Prop := ∀ (seen cutoff : Int) (blocks : List (List Int × Int × Nat)),
  (∀ b ∈ blocks, b.1.length ≤ b.2.2 ∧ ∀ x ∈ b.1, x ≤ b.2.1) →
  optimistic seen (blocks.map Prod.snd) < cutoff →
  seen + (blocks.map (fun b => b.1.sum)).sum < cutoff

/-- The union of shard-local top-k keys is incomplete in general. An unseen key
cannot outrank the global kth score if the *sum* of all shard omission bounds is lower. -/
def UnseenTopK.Goal : Prop := ∀ (shards : List (List Nat × List Nat × Nat)) (cutoff : Nat),
  (∀ s ∈ shards, ∀ k, k ∉ s.2.1 → s.1.count k ≤ s.2.2) →
  (shards.map fun s => s.2.2).sum < cutoff →
  ∀ k, cutoff ≤ (shards.map fun s => s.1.count k).sum →
    k ∈ shards.flatMap (fun s => s.2.1)

/-- A nested-loop probe of a pure conjunction can order its two predicates by
cost per expected rejection. The inequality uses integer counts and exact paired
outcomes, so does not require an independence estimate between their truth values. -/
def PredicateExchange.Goal : Prop := ∀ (rows : List (Bool × Bool)) (ca cb : Nat),
  let n := rows.length
  let a := rows.countP Prod.fst
  let b := rows.countP Prod.snd
  (ca*n + cb*a ≤ cb*n + ca*b) ↔ ca*(n-b) ≤ cb*(n-a)

#guard fanout [1,1,2] [1,1,1] ^ 2 ≤ secondMoment [1,1,2] * secondMoment [1,1,1]
#guard (((counts [3,3,1]).mergeSort.reverse.zip (counts [1,1,1]).mergeSort.reverse).map
  (fun (x,y) => x*y)).sum == 6
#guard (triangles [(0,1)] [(1,2)] [(2,0)]).length == 1
#guard (candidates [1,2,3,1,1] 2).contains 1
#guard remaining (histogram [1,1]) 3 [1,1,2] == 0
#guard optimistic (-2) [(-3,10),(4,2)] == 6
#guard ([[1,2,2],[1,3,3]].map (fun s => s.count 1)).sum == 2
#guard ([(true,false),(true,true),(false,false)].countP Prod.fst) == 2

end TinylambOpt.ResearchGoals.SearchBounds
