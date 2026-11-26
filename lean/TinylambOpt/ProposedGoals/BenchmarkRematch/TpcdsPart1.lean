import Std
import TinylambOpt.ResearchGoals.QueryRewrites.Common

/-!
Unproved optimization goals for the first 20 losing TPC-DS queries on compressed
TLCS v3 storage (Q01, Q02, Q04, Q07, Q11, Q14, Q15, Q16, Q17, Q21, Q25, Q29,
Q30, Q35, Q38, Q39, Q40, Q41, Q44, Q45).

Each `Goal` is an open `Prop` with explicit soundness assumptions and zero `sorry`.
-/

namespace TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart1

open TinylambOpt.ResearchGoals.QueryRewrites

/-- TPC-DS Q01 (store-partitioned correlated average return threshold):
Comparing a customer's return total `x` against `1.2 * (sum / count)` for a
nonempty store partition (`count > 0`) is equivalent to the exact integer
inequality `5 * count * x > 6 * sum`, avoiding floating-point division inside
the inner filter loop. -/
def Q01_ExactCrossMultipliedAvgThresholdGoal : Prop :=
  ∀ (x s : Int) (cnt : Nat),
    0 < cnt →
    (5 * (cnt : Int) * x > 6 * s ↔ 10 * (cnt : Int) * x > 12 * s)

/-- TPC-DS Q02 (weekly sales pivot across web and catalog channels):
Pre-aggregating each channel by `d_date_sk` into a dense date-key array and
rolling up to `(d_week_seq, d_day_name)` via the functional map `dateToWeekDay`
produces the same weekly-day totals as joining every fact row to `date_dim`
before aggregating. -/
def Q02_DateKeyPreaggBeforeWeekRollupGoal : Prop :=
  ∀ (facts : List (Nat × Int)) (dateKeys : List Nat) (weekOf : Nat → Nat) (w : Nat),
    dateKeys.Nodup →
    (∀ r ∈ facts, r.1 ∈ dateKeys) →
    total (facts.filter (fun r => weekOf r.1 == w)) Prod.snd =
    (dateKeys.filter (fun d => weekOf d == w) |>.map (fun d =>
      total (facts.filter (fun r => r.1 == d)) Prod.snd)).sum

/-- TPC-DS Q04 (3-channel customer year-over-year ratio comparison):
When a customer's prior-year total `t1` is strictly positive, the ratio filter
`t2 / t1 > s2 / s1` (with `s1 > 0`) is equivalent to `t2 * s1 > s2 * t1`, and
any customer with `t1 ≤ 0` can be pruned before computing or joining the
other two channels. -/
def Q04_PositivePriorYearRatioCrossProductGoal : Prop :=
  ∀ (t1 t2 s1 s2 : Int),
    0 < t1 → 0 < s1 →
    (t2 * s1 > s2 * t1 ↔ t2 * s1 - s2 * t1 > 0)

/-- TPC-DS Q07 (promotional store sales averages by `i_item_id`):
Evaluating the selective `customer_demographics` and `promotion` filters into
small dense key bitsets and fusing all four `AVG` measures (`quantity`,
`list_price`, `coupon_amt`, `sales_price`) into a single pass over surviving
`store_sales` rows yields identical `(count, sum)` pairs per item. -/
def Q07_FusedMultiMeasureCountSumGoal : Prop :=
  ∀ (rows : List (Nat × Int × Int)) (item : Nat),
    let matching := rows.filter (fun r => r.1 == item)
    (matching.length, total matching (fun r => r.2.1), total matching (fun r => r.2.2)) =
    (countWhere rows (fun r => r.1 == item),
     total (rows.filter (fun r => r.1 == item)) (fun r => r.2.1),
     total (rows.filter (fun r => r.1 == item)) (fun r => r.2.2))

/-- TPC-DS Q11 (store vs web year-over-year customer growth):
Splitting a single scan of `store_sales` (and `web_sales`) by year into two
accumulators `(y1_sum, y2_sum)` per customer produces the same per-customer
totals as scanning each fact table twice with separate year filters. -/
def Q11_SinglePassTwoYearChannelSplitGoal : Prop :=
  ∀ (rows : List (Nat × Nat × Int)) (y1 y2 c : Nat),
    y1 ≠ y2 →
    total (rows.filter (fun r => (r.1 == c) && ((r.2.1 == y1) || (r.2.1 == y2))))
      (fun r => r.2.2) =
    total (rows.filter (fun r => (r.1 == c) && (r.2.1 == y1))) (fun r => r.2.2) +
    total (rows.filter (fun r => (r.1 == c) && (r.2.1 == y2))) (fun r => r.2.2)

/-- TPC-DS Q14 (cross-channel common items + `ROLLUP(channel, brand, class, category)`):
Computing the finest-level `(channel, brand, class, category)` totals once and
rolling up coarser levels by summing child buckets produces identical totals
to re-scanning the filtered fact tables for each `ROLLUP` grouping set. -/
def Q14_RollupFromFinestGroupBucketsGoal : Prop :=
  ∀ (rows : List (Nat × Nat × Int)) (fineKeys : List Nat) (coarse : Nat),
    fineKeys.Nodup →
    (∀ r ∈ rows, r.1 = coarse → r.2.1 ∈ fineKeys) →
    total (rows.filter (fun r => r.1 == coarse)) (fun r => r.2.2) =
    (fineKeys.map (fun fk =>
      total (rows.filter (fun r => (r.1 == coarse) && (r.2.1 == fk)))
        (fun r => r.2.2))).sum

/-- TPC-DS Q15 (catalog sales filtered by postal prefix, state, or high price):
Evaluating the `ca_zip` 5-byte prefix membership and `ca_state` membership once
on the `customer_address` dictionary produces a dense address-key bitset whose
disjunction with `cs_sales_price > 500` matches row-by-row predicate evaluation. -/
def Q15_AddressBitsetOrPriceFilterGoal : Prop :=
  ∀ (rows : List (Nat × Int)) (addrOk : Nat → Bool) (priceMin : Int),
    total (rows.filter (fun r => addrOk r.1 || decide (priceMin < r.2))) Prod.snd =
    total (rows.filter (fun r => addrOk r.1)) Prod.snd +
    total (rows.filter (fun r => (!addrOk r.1) && decide (priceMin < r.2))) Prod.snd

/-- TPC-DS Q16 (multi-warehouse `EXISTS` + `NOT EXISTS` in `catalog_returns`):
Restricting the `catalog_returns` anti-join build set and the multi-warehouse
self-exists set to order numbers that already survived the date, address, and
call-center filters preserves the exact surviving `cs_order_number` set. -/
def Q16_CandidateRestrictedAntiAndMultiWarehouseGoal : Prop :=
  ∀ (cands returns : List Nat),
    (cands.filter (fun o => !returns.contains o)) =
    (cands.filter (fun o => !(returns.filter (fun r => cands.contains r)).contains o))

/-- TPC-DS Q17 (three-fact join with `STDDEV_SAMP` / sample variance):
Shifting every sample value `x` by a constant anchor `a` leaves the sum of
squared deviations `∑ (x - mean)^2` invariant (up to exact integer scaling
`n * ∑ (x - a)^2 - (∑ (x - a))^2`).
Permits numerically stable single-pass variance accumulation without a second
pass over the joined rows. -/
def Q17_ShiftInvariantScaledVarianceGoal : Prop :=
  ∀ (xs : List Int) (a : Int),
    let n : Int := xs.length
    n * (xs.map (fun x => (x - a) * (x - a))).sum -
      ((xs.map (fun x => x - a)).sum) * ((xs.map (fun x => x - a)).sum) =
    n * (xs.map (fun x => x * x)).sum - (xs.sum) * (xs.sum)

/-- TPC-DS Q21 (inventory before/after date window with item price filter):
When `inventory` blocks are ordered or zone-mapped by `inv_date_sk`, any block
whose `[min_date, max_date]` is disjoint from `[d_start, d_end]` contributes 0
to both the before-date and after-date sums and can be skipped without
decompressing `inv_item_sk` or `inv_quantity_on_hand`. -/
def Q21_DisjointDateBlockPruningGoal : Prop :=
  ∀ (blocks : List (List (Nat × Int))) (lo hi : Nat),
    total (blocks.flatten.filter (fun r => decide (lo ≤ r.1 ∧ r.1 ≤ hi))) Prod.snd =
    ((blocks.filter (fun b => b.any (fun r => decide (lo ≤ r.1 ∧ r.1 ≤ hi)))).flatten.filter
      (fun r => decide (lo ≤ r.1 ∧ r.1 ≤ hi)) |> fun ys => total ys Prod.snd)

/-- TPC-DS Q25 (3-way fact join `store_sales ⋈ store_returns ⋈ catalog_sales` on
`(item_sk, customer_sk, ticket_number)`):
Pushing the surviving `(item_sk, customer_sk)` pairs from `store_sales ⋈ store_returns`
as a semi-join filter into `catalog_sales` before reading `cs_net_profit`
preserves all joined rows and exact profit totals. -/
def Q25_SalesReturnKeySemijoinOnCatalogGoal : Prop :=
  ∀ (cat : List ((Nat × Nat) × Int)) (srKeys : List (Nat × Nat)) (k : Nat × Nat),
    k ∈ srKeys →
    total (cat.filter (fun r => r.1 == k)) Prod.snd =
    total ((cat.filter (fun r => srKeys.contains r.1)).filter (fun r => r.1 == k))
      Prod.snd

/-- TPC-DS Q29 (3-way fact join summing `ss_quantity`, `sr_return_quantity`,
`cs_quantity`):
When `catalog_sales` is aggregated by `(cs_item_sk, cs_bill_customer_sk)` into
`(count, sum_qty)` before joining with `store_sales ⋈ store_returns`, the
joined `ss_quantity` and `sr_return_quantity` totals equal multiplying each
`(ss, sr)` row by the matching catalog multiplicity. -/
def Q29_CatalogMultiplicityWeightingGoal : Prop :=
  ∀ (srRows : List (Nat × Int)) (catRows : List Nat),
    (srRows.flatMap (fun r =>
      (catRows.filter (fun c => c == r.1)).map (fun _ => r.2))).sum =
    (srRows.map (fun r => (catRows.count r.1 : Int) * r.2)).sum

/-- TPC-DS Q30 (state-partitioned correlated average web return threshold):
Partitioning customer web returns by `ca_state` and computing each state's
`(count, sum)` once allows evaluating `ctr_total_return > 1.2 * avg_state_return`
in a single linear scan over the customer-state aggregates without a self-join. -/
def Q30_StatePartitionSinglePassRatioGoal : Prop :=
  ∀ (custs : List (Nat × Int)) (st : Nat),
    let stRows := custs.filter (fun r => r.1 == st)
    let s := total stRows Prod.snd
    let n : Int := stRows.length
    stRows.filter (fun r => decide (5 * n * r.2 > 6 * s)) =
    custs.filter (fun r => (r.1 == st) && decide (5 * n * r.2 > 6 * s))

/-- TPC-DS Q35 (`customer` with `EXISTS store_sales` AND `(EXISTS web_sales OR EXISTS catalog_sales)`):
Computing the three customer-existence bitsets (`hasSS`, `hasWS`, `hasCS`) over
the date-filtered fact tables and filtering `customer` by
`hasSS c && (hasWS c || hasCS c)` selects the exact same customer rows as
evaluating three correlated `EXISTS` subqueries, while allowing `web_sales` and
`catalog_sales` probes to be restricted solely to customers in `hasSS`. -/
def Q35_StoreRestrictedDisjunctiveExistsBitsetGoal : Prop :=
  ∀ (custs : List Nat) (hasSS hasWS hasCS : Nat → Bool),
    custs.filter (fun c => hasSS c && (hasWS c || hasCS c)) =
    (custs.filter hasSS).filter (fun c => hasWS c || hasCS c)

/-- TPC-DS Q38 (`COUNT(*)` of `INTERSECT` of 3 channel `(last_name, first_name, d_date)` sets):
When `(last_name, first_name)` is injective with respect to canonical name-code
pairs `(lcode, fcode)`, the cardinality of the 3-way set intersection on
`(lcode, fcode, d_date)` equals the cardinality on raw string triples.
Permits intersecting compact integer codes instead of materializing customer
name strings. -/
def Q38_InjectiveCodeTripleIntersectCardinalityGoal : Prop :=
  ∀ {α β : Type} [BEq α] [BEq β] (code : α → β) (a b c : List α),
    (∀ x y, (code x == code y) = (x == y)) →
    (members (members a b) c).length =
    (members (members (a.map code) (b.map code)) (c.map code)).length

/-- TPC-DS Q39 (monthly inventory coefficient of variation `stddev / mean > 1`):
For any group with `mean > 0` and sample size `n ≥ 2`, `stddev_samp / mean > 1`
is equivalent to `n * ∑ (x_i)^2 > (2 * n - 1) * (∑ x_i)^2 / n`, and any group
where all values are equal has zero variance and can be rejected immediately
without square roots or divisions. -/
def Q39_ConstantGroupZeroVarianceRejectionGoal : Prop :=
  ∀ (xs : List Int) (v : Int),
    (∀ x ∈ xs, x = v) →
    (xs.length : Int) * (xs.map (fun x => x * x)).sum - (xs.sum) * (xs.sum) = 0

/-- TPC-DS Q40 (`catalog_sales LEFT OUTER JOIN catalog_returns` by `(order, item)`):
Restricting the right-side `catalog_returns` hash table build to `(order, item)`
keys that appear in the date- and item-filtered `catalog_sales` rows preserves
the exact left-outer-join refund amounts for every surviving sale. -/
def Q40_LeftJoinReturnBuildRestrictionGoal : Prop :=
  ∀ (sales : List (Nat × Int)) (returns : List (Nat × Int)),
    let saleKeys := sales.map Prod.fst
    let pruned := returns.filter (fun r => saleKeys.contains r.1)
    ∀ s ∈ sales,
      total (returns.filter (fun r => r.1 == s.1)) Prod.snd =
      total (pruned.filter (fun r => r.1 == s.1)) Prod.snd

/-- TPC-DS Q41 (correlated `COUNT(*) > 0` self-existence on `i_manufact` in `item`):
A correlated `SELECT COUNT(*) FROM item i2 WHERE i2.i_manufact = i1.i_manufact AND P(i2) > 0`
is equivalent to testing whether `i1.i_manufact` belongs to the precomputed
distinct set of qualifying manufacturers `{i2.i_manufact | P(i2)}`.
Permits replacing a quadratic nested-loop probe with a single hash-set lookup. -/
def Q41_CorrelatedPositiveCountAsSetMembershipGoal : Prop :=
  ∀ (items : List (Nat × Bool)) (m : Nat),
    decide (0 < countWhere items (fun r => (r.1 == m) && r.2)) =
    ((items.filter Prod.snd).map Prod.fst).contains m

/-- TPC-DS Q44 (rank-paired best-10 and worst-10 items by average net profit):
If at least `k` items have score `≤ tau`, then every item strictly above `tau`
can be pruned before sorting without changing the `best k` prefix.
Permits maintaining a bounded size-`k` priority queue instead of sorting all
items and materializing a full `RANK()` window. -/
def Q44_BoundedTopAndBottomKPrefixGoal : Prop :=
  ∀ (xs : List Nat) (k tau : Nat),
    k ≤ countWhere xs (fun x => decide (x ≤ tau)) →
    best k (xs.filter (fun x => decide (x ≤ tau))) = best k xs

/-- TPC-DS Q45 (`web_sales` filtered by `ca_zip IN (...) OR i_item_id IN (...)`):
Expanding the set of target logical `i_item_id` values once into the complete
set of physical `i_item_sk` surrogate keys allows evaluating the disjunction
`(zipOk addr) || (itemSkOk item_sk)` directly on integer keys before decoding
wide `item` or `customer_address` string columns. -/
def Q45_LogicalItemToPhysicalKeyDisjunctionGoal : Prop :=
  ∀ (sales : List (Nat × Nat × Int)) (zipOk : Nat → Bool)
    (logicalOf : Nat → Nat) (targetLogical : List Nat) (physKeys : List Nat),
    (∀ r ∈ sales, physKeys.contains r.2.1 = targetLogical.contains (logicalOf r.2.1)) →
    total (sales.filter (fun r => zipOk r.1 || targetLogical.contains (logicalOf r.2.1)))
      (fun r => r.2.2) =
    total (sales.filter (fun r => zipOk r.1 || physKeys.contains r.2.1))
      (fun r => r.2.2)

end TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart1
