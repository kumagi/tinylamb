import Std
import TinylambOpt.ResearchGoals.QueryRewrites.Common

/-!
Unproved optimization goals for the remaining 21 losing TPC-DS queries on
compressed TLCS v3 storage (Q49, Q53, Q57, Q58, Q59, Q62, Q63, Q66, Q71, Q72,
Q74, Q75, Q79, Q81, Q83, Q85, Q86, Q87, Q89, Q90, Q99).

Each `Goal` is an open `Prop` with explicit soundness assumptions and zero `sorry`.
-/

namespace TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2

open TinylambOpt.ResearchGoals.QueryRewrites

/-- TPC-DS Q49 (3-channel return ratio ranking with `LEFT OUTER JOIN` to returns
and `WHERE return_quantity > 0`):
When an outer query filters `r.return_quantity > 0`, any unmatched outer-join
row (`none`) fails the predicate, so the `LEFT OUTER JOIN` reduces to an
`INNER JOIN` on the qualifying returns. -/
def Q49_PositiveReturnFilterInnerJoinReductionGoal : Prop :=
  ∀ (rows : List (Option Int)),
    (rows.filter (fun r => match r with | some q => decide (0 < q) | none => false)).length =
    (rows.filterMap id |>.filter (fun q => decide (0 < q))).length

/-- TPC-DS Q53 (quarterly manufacturer sales with window `AVG(sum_sales) OVER (PARTITION BY i_manufact_id)`):
Computing the partition average over the `4` quarterly group totals per
manufacturer requires only the post-aggregation group table (at most 4 rows per
manufacturer) and is invariant to skipping any `store_sales` rows whose item is
outside the target `(category, class, brand)` bitset. -/
def Q53_ItemBitsetPrefilterBeforeQuarterWindowGoal : Prop :=
  ∀ (sales : List (Nat × Nat × Int)) (itemOk : Nat → Bool) (m q : Nat),
    itemOk m = true →
    total (sales.filter (fun r => itemOk r.1 && (r.1 == m) && (r.2.1 == q)))
      (fun r => r.2.2) =
    total ((sales.filter (fun r => itemOk r.1)).filter (fun r => (r.1 == m) && (r.2.1 == q)))
      (fun r => r.2.2)

/-- TPC-DS Q57 (monthly call-center sales joined to previous and next month via `rn - 1` / `rn + 1`):
Filtering current-month rows by the year-partition deviation predicate `keep`
before looking up their `(rn - 1, rn + 1)` neighbor rows produces the exact
same `(cur, prev, next)` triples as joining all months first and then filtering
on the current month. -/
def Q57_DeviationBandFilterBeforeNeighborJoinGoal : Prop :=
  ∀ (months : List (Nat × Int)) (keep : Nat × Int → Bool)
    (neighbors : Nat → List (Int × Int)),
    (months.flatMap (fun m => (neighbors m.1).map (fun p => (m, p)))).filter
      (fun t => keep t.1) =
    (months.filter keep).flatMap (fun m => (neighbors m.1).map (fun p => (m, p)))

/-- TPC-DS Q58 (3-channel weekly revenue within `[0.9, 1.1]` of each other):
If `0 < a, b, c` and pairwise ratios lie in `[9/10, 11/10]`, then the
cross-multiplied exact inequalities `9 * b ≤ 10 * a ∧ 10 * a ≤ 11 * b` hold
iff `min(a, b, c) * 11 ≥ max(a, b, c) * 10` is necessary for the tightest pair,
enabling early rejection of non-matching items across channels without floating
point division. -/
def Q58_PairwiseRatioBandCrossMultiplicationGoal : Prop :=
  ∀ (a b : Int),
    0 < a → 0 < b →
    (9 * b ≤ 10 * a ∧ 10 * a ≤ 11 * b) →
    9 * a ≤ 11 * b

/-- TPC-DS Q59 (year-1 vs year-2 weekly store sales ratio):
Joining year-1 weekly store totals to year-2 weekly store totals shifted by
`52` weeks (`d_week_seq2 = d_week_seq1 + 52`) on `(s_store_id, d_week_seq)`
can be evaluated by indexing the smaller year-2 weekly aggregate table in a
dense `(store, week - 52)` map rather than materializing two full-year CTEs. -/
def Q59_ShiftedWeekIndexLookupEquivGoal : Prop :=
  ∀ (y1 y2 : List (Nat × Int)) (shift : Nat),
    ((y1.flatMap (fun a =>
      (y2.filter (fun b => b.1 == a.1 + shift)).map (fun b => a.2 * b.2))).sum) =
    (y1.map (fun a => a.2 * total (y2.filter (fun b => b.1 == a.1 + shift)) Prod.snd)).sum

/-- TPC-DS Q62 (30/60/90/120-day shipping delay histogram by warehouse prefix,
ship mode, and web site):
Mapping each `w_warehouse_sk` to its 20-char prefix code once and classifying
`ws_ship_date_sk - ws_sold_date_sk` via `bucket [30, 60, 90, 120]` in a single
pass produces the exact same 5-bucket counts as evaluating 5 separate `CASE`
aggregates over joined wide strings. -/
def Q62_DelayBucketPartitionSumGoal : Prop :=
  ∀ (delays : List Int),
    let bs := [30, 60, 90, 120]
    (countWhere delays (fun d => bucket bs d == 0)) +
    (countWhere delays (fun d => bucket bs d == 1)) +
    (countWhere delays (fun d => bucket bs d == 2)) +
    (countWhere delays (fun d => bucket bs d == 3)) +
    (countWhere delays (fun d => bucket bs d == 4)) =
    delays.length

/-- TPC-DS Q63 (monthly manager sales with `i_category / i_class / i_brand` OR branches):
Compiling the two overlapping `(category, class, brand)` disjunction branches on
`item` into a single dense `i_item_sk` bitset before scanning `store_sales`
eliminates per-row string comparisons and deduplicates rows that match both
branches. -/
def Q63_DisjunctiveItemBitsetDeduplicationGoal : Prop :=
  ∀ (sales : List (Nat × Int)) (p1 p2 : Nat → Bool),
    total (sales.filter (fun r => p1 r.1 || p2 r.1)) Prod.snd =
    total (sales.filter (fun r => p1 r.1)) Prod.snd +
    total (sales.filter (fun r => (!p1 r.1) && p2 r.1)) Prod.snd

/-- TPC-DS Q66 (12-month pivoted web + catalog warehouse sales):
Pre-aggregating `web_sales` and `catalog_sales` by `(warehouse_sk, d_moy)` into
a dense `W × 12` table and joining each unique `warehouse_sk` once afterward
preserves all 12 monthly sums while avoiding copying 7 wide warehouse attributes
per fact row. -/
def Q66_WarehouseMonthPivotLateAttrLookupGoal : Prop :=
  ∀ (facts : List (Nat × Nat × Int)) (w m : Nat) (attr : Nat → Nat),
    (facts.filter (fun r => (r.1 == w) && (r.2.1 == m)) |>.map (fun r => (attr r.1, r.2.2))) =
    (facts.filter (fun r => (r.1 == w) && (r.2.1 == m)) |>.map (fun r => (attr w, r.2.2)))

/-- TPC-DS Q71 (`UNION ALL` of 3 fact tables joined to `item`, `date_dim`, `time_dim`):
Pushing the selective `item` (`i_manager_id = 1`), `date_dim` (`d_moy, d_year`),
and `time_dim` (`t_meal_time IN ('breakfast','dinner')`) semi-join bitsets into
each of the 3 `UNION ALL` branches independently and summing branch-level
`(brand_id, hour, minute)` totals equals unioning first and aggregating once. -/
def Q71_UnionAllBranchPreaggAdditiveGoal : Prop :=
  ∀ (b1 b2 b3 : List (Nat × Int)) (k : Nat),
    total ((b1 ++ b2 ++ b3).filter (fun r => r.1 == k)) Prod.snd =
    total (b1.filter (fun r => r.1 == k)) Prod.snd +
    total (b2.filter (fun r => r.1 == k)) Prod.snd +
    total (b3.filter (fun r => r.1 == k)) Prod.snd

/-- TPC-DS Q72 (`catalog_sales ⋈ inventory ⋈ ... LEFT OUTER JOIN promotion LEFT OUTER JOIN catalog_returns`):
Because `catalog_returns` is left-joined only to count rows (no return columns
are projected), each surviving `(cs_item_sk, cs_order_number)` row contributes
multiplicity `max(1, return_count(item, order))` to either the promo or
no-promo counter, avoiding materializing duplicate joined tuples. -/
def Q72_LeftJoinMultiplicityPromoSplitGoal : Prop :=
  ∀ (sales : List (Nat × Bool)) (retCount : Nat → Nat),
    let weight (k : Nat) : Nat := if retCount k == 0 then 1 else retCount k
    (sales.filter Prod.snd |>.map (fun r => weight r.1)).sum +
    (sales.filter (fun r => !r.2) |>.map (fun r => weight r.1)).sum =
    (sales.map (fun r => weight r.1)).sum

/-- TPC-DS Q74 (store + web customer year-over-year ratio with `ORDER BY ... LIMIT 100`):
Filtering customers with positive prior-year baselines (`store_y1 > 0` and
`web_y1 > 0`) and pruning any candidate whose sort ordinal exceeds a threshold
`tau` already met by `k` valid candidates preserves the exact `best k` output
while deferring wide customer string lookups to surviving candidates. -/
def Q74_PositiveBaselineFilterBeforeTopKGoal : Prop :=
  ∀ (cands : List (Nat × Int × Int)) (k tau : Nat),
    let valid := (cands.filter (fun r => decide (0 < r.2.1 ∧ 0 < r.2.2))).map Prod.fst
    k ≤ countWhere valid (fun x => decide (x ≤ tau)) →
    best k (valid.filter (fun x => decide (x ≤ tau))) = best k valid

/-- TPC-DS Q75 (`UNION` of 3 channels minus returns, then year-over-year sales ratio):
Deduplicating `(d_year, brand, class, category, manufact, item, order, net_cnt, net_amt)`
locally inside each channel before computing the 3-way `UNION DISTINCT` produces
the exact same deduplicated multiset as unioning raw rows and deduplicating once. -/
def Q75_ChannelLocalDedupBeforeUnionDistinctGoal : Prop :=
  ∀ {α : Type} [BEq α] (a b : List α),
    ((a.eraseDups ++ b.eraseDups).eraseDups).length =
    ((a ++ b).eraseDups).length

/-- TPC-DS Q79 (`store_sales` grouped by `(ss_ticket_number, ss_customer_sk, ss_addr_sk, s_city)`):
When `s_store_sk` functionally determines `s_city` and the outer query aggregates
by `(ss_ticket_number, ss_customer_sk, s_city)` without projecting `ss_addr_sk`,
eliding `ss_addr_sk` from the hash key is sound whenever `(ticket, customer)`
uniquely determines `ss_addr_sk`. -/
def Q79_FunctionalAddressKeyElisionGoal : Prop :=
  ∀ (rows : List (Nat × Nat × Int)) (addrOf : Nat → Nat) (t : Nat),
    (∀ r ∈ rows, r.2.1 = addrOf r.1) →
    total (rows.filter (fun r => (r.1 == t) && (r.2.1 == addrOf t))) (fun r => r.2.2) =
    total (rows.filter (fun r => r.1 == t)) (fun r => r.2.2)

/-- TPC-DS Q81 (state-partitioned correlated average catalog return threshold):
Computing each customer's `(customer_sk, state, sum_return)` once and deriving
each state's `(count, sum)` directly from that customer-level table (instead of
re-scanning `catalog_returns`) produces identical per-state averages. -/
def Q81_StateAvgFromCustomerTotalsGoal : Prop :=
  ∀ (custTotals : List (Nat × Int)) (st : Nat),
    let stRows := custTotals.filter (fun r => r.1 == st)
    (stRows.length, total stRows Prod.snd) =
    (countWhere custTotals (fun r => r.1 == st),
     total (custTotals.filter (fun r => r.1 == st)) Prod.snd)

/-- TPC-DS Q83 (3-channel return quantities for common items, `ORDER BY item_id LIMIT 100`):
Finding the `best k` common `i_item_id` keys across all three return channels
and then restricting each return table to those `k` keys preserves the exact
return quantity sum for every output item. -/
def Q83_CommonItemTopKBeforeQtyLookupGoal : Prop :=
  ∀ (sRows : List (Nat × Int)) (cItems wItems : List Nat) (k item : Nat),
    let top := best k (members (members (sRows.map Prod.fst) cItems) wItems)
    item ∈ top →
    total (sRows.filter (fun r => r.1 == item)) Prod.snd =
    total ((sRows.filter (fun r => top.contains r.1)).filter (fun r => r.1 == item))
      Prod.snd

/-- TPC-DS Q85 (`web_returns` with 3-branch `(cd_marital_status, cd_education_status, ws_sales_price)`
and 3-branch `(ca_state, ws_net_profit)` filters):
When `cd1.cd_marital_status = cd2.cd_marital_status` and both are non-NULL,
precomputing the small `cd_demo_sk → (marital, education)` lookup table allows
evaluating the 3-branch demographic predicate via a single bitset lookup on
`(cd1, cd2)` without joining `customer_demographics` twice per row. -/
def Q85_EqualNonNullDemoCodeFilterGoal : Prop :=
  ∀ (x y : Option Nat),
    hit x y = true → x = y ∧ x.isSome = true

/-- TPC-DS Q86 (`RANK() OVER (PARTITION BY lochierarchy, category ORDER BY total_sum DESC)` on `ROLLUP(i_category, i_class)`):
Because the `ROLLUP` produces at most a few hundred `(category, class)` groups,
computing each group's competition rank directly from the frequency table of
distinct group totals (`frequency scores`) matches counting strictly higher
rows in the ungrouped list. -/
def Q86_CompetitionRankFromHigherCountGoal : Prop :=
  ∀ (scores : List Int) (x : Int),
    countWhere scores (fun s => decide (x < s)) =
    ((frequency scores).filter (fun p => decide (x < p.1)) |>.map Prod.snd).sum

/-- TPC-DS Q87 (`COUNT(*)` of `store_sales EXCEPT catalog_sales EXCEPT web_sales` on `(last_name, first_name, d_date)`):
Only `(customer, date)` pairs present in `store_sales` can survive the two
`EXCEPT` operations, so `catalog_sales` and `web_sales` need only mark
membership bits for candidate pairs already seen in `store_sales`. -/
def Q87_CandidateOnlyDifferenceMarksGoal : Prop :=
  ∀ {α : Type} [BEq α] (s c w : List α),
    let sDedup := s.eraseDups
    (sDedup.filter (fun x => !c.contains x && !w.contains x)).length =
    (sDedup.filter (fun x =>
      !(c.filter (fun y => sDedup.contains y)).contains x &&
      !(w.filter (fun y => sDedup.contains y)).contains x)).length

/-- TPC-DS Q89 (monthly store-item sales with `|sum - avg_monthly| / avg_monthly > 0.1`):
Pre-aggregating `store_sales` by `(category, class, brand, store_name, company_name, d_moy)`
into a compact 12-month array per `(item_group, store)` allows evaluating the
exact deviation band `10 * |12 * m - year_sum| > year_sum` (when `year_sum > 0`
and all 12 months are present) in one pass over the group table. -/
def Q89_MonthlyDeviationBandExactCheckGoal : Prop :=
  ∀ (m yearSum : Int) (cnt : Nat),
    0 < cnt → 0 < yearSum →
    (10 * Int.natAbs ((cnt : Int) * m - yearSum) > Int.natAbs yearSum ↔
     10 * Int.natAbs ((cnt : Int) * m - yearSum) > yearSum.toNat)

/-- TPC-DS Q90 (ratio of morning `amc` to evening `pmc` web sales counts):
Evaluating both the morning (`t_hour ∈ [8, 9]`) and evening (`t_hour ∈ [19, 20]`)
counts in a single scan of `web_sales` filtered by `(household_demographics, web_page)`
bitsets produces the exact same `(amc, pmc)` pair as scanning `web_sales` twice. -/
def Q90_SinglePassAmPmTimeBandCountsGoal : Prop :=
  ∀ (rows : List (Nat × Bool)) (isAm isPm : Nat → Bool),
    (∀ h, !(isAm h && isPm h)) →
    countWhere rows (fun r => r.2 && (isAm r.1 || isPm r.1)) =
    countWhere rows (fun r => r.2 && isAm r.1) +
    countWhere rows (fun r => r.2 && isPm r.1)

/-- TPC-DS Q99 (30/60/90/120-day catalog shipping delay histogram by warehouse
prefix, ship mode, and call center):
When consecutive rows in a TLCS block share the same `(warehouse, ship_mode, call_center)`
key and delay bucket (run-length encoded), adding the run length `len` directly
to that bucket counter produces the exact same histogram as iterating `len` times. -/
def Q99_RunLengthDelayHistogramAccumGoal : Prop :=
  ∀ (runs : List (Nat × Nat)) (target : Nat),
    (runs.flatMap (fun r => List.replicate r.2 r.1) |> fun xs =>
      countWhere xs (fun b => b == target)) =
    (runs.filter (fun r => r.1 == target) |>.map Prod.snd).sum

end TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2
