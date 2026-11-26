import TinylambOpt.ResearchGoals.QueryRewrites.Common

/-!
These goals model shipment filters and revenue or quantity aggregates.

They establish when SQL execution can reuse subtotals, narrow join inputs, or defer
reading payloads.
-/

namespace TinylambOpt.ResearchGoals.QueryRewrites.RevenueAndShipments

-- Benchmark matchup: Tpch.Q01DuckDB
/--
For date-qualified rows, computing the discounted price once preserves both sums that
use it. The scan can share the multiplication across aggregate updates.
-/
def SharedDiscountedPriceSums.Goal : Prop := ∀ (rows : List (Int × Int × Int)),
  let discounted := rows.map fun (p,d,t) => (p*(100-d),100+t)
  (total discounted Prod.fst, total discounted (fun (v,t) => v*t)) =
  (total rows (fun (p,d,_) => p*(100-d)), total rows (fun (p,d,t) => p*(100-d)*(100+t)))

-- Benchmark matchup: Tpch.Q03DuckDB
/--
The global top ten can be obtained from worker-local top tens once each order total is
complete. The final merge receives fewer rows.
-/
def MergeOfLocalRevenueTopK.Goal : Prop := ∀ (shards : List (List Nat)),
  best 10 (shards.flatMap (best 10)) = best 10 shards.flatten

-- Benchmark matchup: Tpch.Q07DuckDB
/--
An encoding for two distinct nations preserves the two opposite-direction pairs. The
join filter can compare compact codes.
-/
def TwoNationCodeDirectionalMatch.Goal : Prop := ∀ (rows : List (Nat × Nat)) a b, a ≠ b →
  rows.filter (fun (s,c) => [a,b].contains s && [a,b].contains c && s != c) =
  rows.filter (fun (s,c) => (s == a && c == b) || (s == b && c == a))

-- Benchmark matchup: Tpch.Q08DuckDB
/--
For each year, the sum of the Peru and non-Peru states is the total volume. The
aggregate can obtain the denominator without rereading volumes.
-/
def NationPartitionedYearVolume.Goal : Prop := ∀ (rows : List (Bool × Int)),
  total rows Prod.snd = total (rows.filter Prod.fst) Prod.snd + total (rows.filter (!·.1)) Prod.snd

-- Benchmark matchup: Tpch.Q15DuckDB
/--
Local maxima with all their ties suffice to identify all global maximum suppliers. The
executor can defer fetching supplier details until that selection.
-/
def GlobalMaximizersFromLocalTies.Goal : Prop := ∀ (shards : List (List (Nat × Int))),
  let maxima := fun (rs : List (Nat × Int)) => (rs.map Prod.snd).max?
  let candidates := shards.flatMap fun rs => rs.filter (fun r => some r.2 == maxima rs)
  candidates.filter (fun r => some r.2 == maxima candidates) =
    shards.flatten.filter (fun r => some r.2 == maxima shards.flatten)

-- Benchmark query: Tpch.Q01
/--
Using a block subtotal when its maximum date is at or before the cutoff gives the same
nullable sum as filtering every row.

A scan can merge stored subtotals for qualifying blocks and decode quantities only where
the date bound does not certify the whole block.
-/
def DateBoundBlockSubtotalReuse.Goal : Prop := ∀ (pages : List (List (Int × Option Int))) cutoff,
  let scan := fun (p : List (Int × Option Int)) => nsum ((p.filter (fun r => r.1 ≤ cutoff)).map Prod.snd)
  let state := fun (p : List (Int × Option Int)) =>
    if (p.map Prod.fst).max?.any (· ≤ cutoff) then nsum (p.map Prod.snd) else scan p
  nsum (pages.map state) = scan pages.flatten

-- Benchmark query: Tpch.Q03
/--
A pending revenue total cannot change the ten largest values when a valid upper bound is
strictly below at least ten known totals.

A top-ten aggregate can skip shipment rows for that order once the bound and the ten
qualifying totals are established.
-/
def NonnegativeRevenueUpperBoundPruning.Goal : Prop := ∀ (known : List Int) (pending : List Int) upper,
  pending.sum ≤ upper → 10 ≤ countWhere known (upper < ·) →
  (known.mergeSort.reverse.take 10) = ((pending.sum :: known).mergeSort.reverse.take 10)

-- Benchmark query: Tpch.Q06
/--
For exact integers, the sum of eligible discount-price products equals the sum of each
discount times its grouped price total.

The revenue aggregate can multiply once per discount after the ship-date, quantity, and
discount filters.
-/
def DiscountGroupedRevenueSum.Goal : Prop := ∀ (rows : List (Int × Int)) lo hi,
  let eligible := rows.filter (fun (d,_) => lo ≤ d && d ≤ hi)
  total eligible (fun (d,p) => d*p) =
    total (eligible.map Prod.fst).eraseDups (fun d => d*(forKey eligible d).sum)

-- Benchmark query: Tpch.Q07
/--
For a chosen supplier direction, counting matches using prefiltered order and shipment
keys gives the same count as expanding the join and then filtering nation pairs.

The join can restrict eligible order and supplier keys before assembling nation pairs.
-/
def DirectionalNationKeyPrefilter.Goal : Prop := ∀ (orders : List (Nat × Bool)) (lines : List (Nat × Bool)) (supplier : Bool),
  let matchingOrders := (orders.filter (fun r => r.2 != supplier)).map Prod.fst
  let matchingLines := (lines.filter (fun r => r.2 == supplier)).map Prod.fst
  (matchingLines.map matchingOrders.count).sum =
    (lines.flatMap (fun (k,s) => (forKey orders k).filter (fun c => s == supplier && s != c))).length

-- Benchmark query: Tpch.Q08
/--
For a fixed year, summing exact volume by supplier before applying the supplier nation
predicate preserves the selected total.

The executor can resolve the supplier's nation once per subtotal instead of probing for
every shipment.
-/
def SupplierYearVolumePreaggregation.Goal : Prop := ∀ (sales : List (Nat × Int)) (peru : Nat → Bool),
  total (sales.filter (fun r => peru r.1)) Prod.snd =
    total ((sales.map Prod.fst).eraseDups.filter peru) (fun supplier => (forKey sales supplier).sum)

-- Benchmark query: Tpch.Q10
/--
The same ordered top customers are selected when the ranking uses the narrow keys and
scores and their attributes are attached afterward.

The executor can defer reading wide customer attributes until the winning rows are
known, using the same ranking and tie order.
-/
def TopCustomersBeforeWideAttributeLookup.Goal : Prop := ∀ (ids : List Nat) (rank : Nat → Nat) (attrs : Nat → String),
  (((ids.map (fun i => (i,attrs i))).mergeSort (fun a b => decide (rank a.1 ≤ rank b.1))).take 20) =
    ((sortedBy ids rank).take 20).map (fun i => (i,attrs i))

-- Benchmark query: Tpch.Q19
/--
A shared shipment condition can be pulled out of a disjunction of item and quantity
predicates without changing which modeled rows pass.

The filter can evaluate the air-shipment condition once and use each item's quantity
range in the remaining branches.
-/
def CommonShipmentPredicateFactoring.Goal : Prop := ∀ (rows : List (Bool × Nat × Nat)),
  let bands := [(5,15),(15,25),(29,39)]
  let accepts := fun (r : Bool × Nat × Nat) => r.1 && (bands[r.2.1]?).any (fun (l,h) => l ≤ r.2.2 && r.2.2 ≤ h)
  rows.filter accepts = rows.filter (fun (air,tag,q) => air &&
    ((tag == 0 && 5 ≤ q && q ≤ 15) || (tag == 1 && 15 ≤ q && q ≤ 25) || (tag == 2 && 29 ≤ q && q ≤ 39)))

-- Benchmark query: Tpch.Q20
/--
Restricting an annual quantity aggregation to candidate keys preserves the nullable sum
for each candidate.

A semi-join or candidate-aware aggregate can avoid building state for irrelevant
partsupp keys while retaining NULL for inputs with no non-NULL quantity.
-/
def CandidateRestrictedNullableSupplierQuantity.Goal : Prop := ∀ (pairs : List (Nat × Nat)) (lines : List ((Nat × Nat) × Option Int)) k,
  k ∈ pairs → nsum (forKey (lines.filter (fun r => pairs.eraseDups.contains r.1)) k) = nsum (forKey lines k)

end TinylambOpt.ResearchGoals.QueryRewrites.RevenueAndShipments

-- C++ references to this model or its proof.
-- executor/detail/benchmark_rematch.hpp
