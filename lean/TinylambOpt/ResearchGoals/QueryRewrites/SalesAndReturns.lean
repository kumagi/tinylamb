import TinylambOpt.ResearchGoals.QueryRewrites.Common

/-!
These goals model rewrites for sales and returns queries, including join multiplicity,
nullable sums, and ranking.

They provide conditions for reducing fact and dimension work while preserving the
modeled aggregates, filtered rows, or ordering.
-/

namespace TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

-- Benchmark matchup: Tpcds.Q01DuckDB
/--
Restricting customer return totals to eligible Tennessee stores preserves the inputs to
the modeled store average. The executor can filter store keys before accumulating the
correlation state.
-/
def EligibleStoreCustomerReturnTotals.Goal : Prop := ∀ (returns : List (Nat × Int)) (tn : Nat → Bool) store, tn store = true →
  ((returns.filter (fun r => tn r.1)).filter (fun r => r.1 == store)) =
    returns.filter (fun r => r.1 == store)

-- Benchmark matchup: Tpcds.Q02DuckDB
/--
Combining weekday subtotals from both sales channels preserves the nullable sum. The
executor can pass aggregate states through the union.
-/
def DayOfWeekNullablePartialSums.Goal : Prop := ∀ (a b : List (Option Int)), nsum [nsum a,nsum b] = nsum (a++b)

-- Benchmark matchup: Tpcds.Q02ClickHouse
/--
An indexed lookup of shifted week keys preserves the modeled self-join, including date
multiplicities. The executor can avoid comparing all week pairs.
-/
def ShiftedWeekArrayJoinMultiplicity.Goal : Prop := ∀ (a b : List Nat),
  (a.flatMap (fun w => b.filter (· == w+53))).length = (a.map (fun w => b.count (w+53))).sum

-- Benchmark matchup: Tpcds.Q04DuckDB
/--
Representing two years of three sales channels in six slots per customer preserves the
modeled year-total lookups. The executor can access the needed states together.
-/
def YearChannelSlotsReplaceSelfJoin.Goal : Prop := ∀ (xs : List ((Nat × Nat) × Int)), ∀ c y : Nat, c < 3 → y < 2 →
  xs.filter (fun r => r.1 == (c,y)) =
    xs.filter (fun r => r.1.2 < 2 && 2*r.1.1 + r.1.2 == 2*c+y)

-- Benchmark matchup: Tpcds.Q10DuckDB
/--
Combining the store mark with the union of the web and catalog marks preserves the
existence predicate. The executor can construct the qualifying customer set before
probing outer rows.
-/
def StoreAndRemoteCustomerMembership.Goal : Prop := ∀ (customers s w c : List Nat),
  customers.filter (fun k => s.contains k && (w.contains k || c.contains k)) =
    customers.filter ((members s (w++c)).contains ·)

-- Benchmark matchup: Tpcds.Q11DuckDB
/--
When customer attributes are determined by the customer ID, looking them up after the
year comparison preserves the modeled result. The executor can use narrower aggregate
keys.
-/
def LateFunctionalCustomerAttributes.Goal : Prop := ∀ (sales : List (Nat × Option Int)) (attr : Nat → String),
  let ids := (sales.map Prod.fst).eraseDups
  let wide := sales.map (fun (k,v) => ((k,attr k),v))
  let wideKeys := (wide.map Prod.fst).eraseDups
  ids.map (fun k => ((k,attr k), nsum ((sales.filter (fun r => r.1 == k)).map Prod.snd))) =
    wideKeys.map (fun key => (key, nsum ((wide.filter (fun r => r.1 == key)).map Prod.snd)))

-- Benchmark matchup: Tpcds.Q14DuckDB
/--
Intersecting the channel key sets once preserves the shared brand, class, and category
membership predicate. The executor can reuse that intersection for item filtering.
-/
def ThreeChannelProductKeyIntersection.Goal : Prop := ∀ (a b c : List (Nat × Nat × Nat)) k,
  (members (members a b) c).contains k = (a.contains k && b.contains k && c.contains k)

-- Benchmark matchup: Tpcds.Q15DuckDB
/--
An address that passes the ZIP or state test needs no sales-price comparison for the
modeled OR filter. The executor can evaluate the price condition only for the remaining
rows.
-/
def AddressMatchBeforePricePredicate.Goal : Prop := ∀ (rows : List (Bool × Int)),
  (rows.filter (fun (address,price) => address || price > 500)).Perm
    ((rows.filter Prod.fst) ++ rows.filter (fun (a,p) => !a && p > 500))

-- Benchmark matchup: Tpcds.Q16DuckDB
/--
For an order with non-NULL warehouse keys, the extremes detect whether a different
warehouse exists for the chosen key. The executor can reuse per-order extrema instead of
rescanning catalog sales.
-/
def OtherNonnullWarehouseFromExtrema.Goal : Prop := ∀ (warehouses : List (Option Nat)) w,
  warehouses.any (fun v => v.any (· != w)) =
    (((warehouses.filterMap id).min?).any (· < w) || ((warehouses.filterMap id).max?).any (w < ·))

-- Benchmark matchup: Tpcds.Q17DuckDB
/--
Weighting quantity moments by join multiplicity preserves the modeled moments of the
expanded data. The executor can update a STDDEV state without repeating contributions.
-/
def MultiplicityWeightedSecondMoments.Goal : Prop := ∀ (xs : List Int) n,
  total (xs.flatMap (List.replicate n)) (fun x => x*x) = (n:Int)*total xs (fun x => x*x)

-- Benchmark matchup: Tpcds.Q21DuckDB
/--
Separate streams on either side of the inventory date boundary preserve the modeled
before and after sums. The executor can avoid testing that boundary during each
aggregate update.
-/
def SplitInventorySumsAtDateBoundary.Goal : Prop := ∀ (rows : List (Int × Int)) cut,
  (total rows (fun (d,q) => if d < cut then q else 0),total rows (fun (d,q) => if d < cut then 0 else q)) =
    (total (rows.filter (fun r => r.1 < cut)) Prod.snd,total (rows.filter (fun r => cut ≤ r.1)) Prod.snd)

-- Benchmark matchup: Tpcds.Q25DuckDB
/--
Partial profit and loss totals scaled by matching join multiplicities preserve the
modeled three-way totals. The executor can aggregate on complete keys before expanding
their matches.
-/
def FanoutWeightedProfitAndLoss.Goal : Prop := ∀ (sales returns catalog : List Int),
  total (sales.flatMap (fun _ => returns.flatMap (fun v => catalog.map (fun _ => v)))) id =
    (sales.length:Int) * returns.sum * catalog.length

-- Benchmark matchup: Tpcds.Q29DuckDB
/--
Only catalog quantity rows for item and customer keys demanded by ticket matches can
contribute. The executor can restrict those payload reads to the demanded keys.
-/
def CustomerItemFilterBeforeQuantityRead.Goal : Prop := ∀ (catalog : List (Nat × Int)) (tickets : Nat → Nat),
  total (catalog.filter (fun r => tickets r.1 != 0)) (fun (k,q) => (tickets k:Int)*q) =
    total catalog (fun (k,q) => (tickets k:Int)*q)

-- Benchmark matchup: Tpcds.Q30DuckDB
/--
The candidate return states determine which state averages need to be built. Each state
uses all eligible values for its modeled average.
-/
def CandidateStatesPreserveAverageInputs.Goal : Prop := ∀ (states : List Nat) (returns : List (Nat × Int)) k, k ∈ states →
  ((returns.filter (fun r => states.eraseDups.contains r.1)).filter (fun r => r.1 == k)) =
    returns.filter (fun r => r.1 == k)

-- Benchmark matchup: Tpcds.Q35DuckDB
/--
A nonempty group's minimum and maximum dependent count are both the key's dependent
count when that value is constant within the group. The executor can omit these
aggregate states for such groups.
-/
def ConstantGroupKeyExtrema.Goal : Prop := ∀ (v : Option Int) n, 0 < n →
  let vals := (List.replicate n v).filterMap id
  (vals.min?,vals.max?) = (v,v)

-- Benchmark matchup: Tpcds.Q38DuckDB
/--
Intersecting the three sales channels in a shared name and date key space preserves the
qualifying keys. The executor can use a common membership representation.
-/
def OrderedChannelIntersectionDedup.Goal : Prop := ∀ (a b c : List (Option String × Option String × Nat)),
  (members (members a b) c).length = countWhere a.eraseDups (fun k => b.contains k && c.contains k)

-- Benchmark matchup: Tpcds.Q38ClickHouse
/--
Intersecting disjoint date partitions separately and adding their cardinalities
preserves the total intersection count. The executor can process smaller independent
hash tables.
-/
def DisjointDatePartitionIntersectionCount.Goal : Prop := ∀ (s c w : List (Nat × Nat)) n, 0 < n →
  let byDay := fun (rs : List (Nat × Nat)) (p : Nat) => rs.filter (fun k => k.2 % n == p)
  let intersectCount := fun a b d => (members (members a b) d).length
  ((List.range n).map (fun p => intersectCount (byDay s p) (byDay c p) (byDay w p))).sum =
    intersectCount s c w

-- Benchmark matchup: Tpcds.Q39DuckDB
/--
Only January and February inventory statistics can contribute to the modeled month
comparison. The executor can build states for those months alone.
-/
def RestrictMonthStatisticsBeforeSelfJoin.Goal : Prop := ∀ (xs : List (Nat × Int)) m, m = 1 ∨ m = 2 →
  ((xs.filter (fun r => r.1 == 1 || r.1 == 2)).filter (fun r => r.1 == m)) =
    xs.filter (fun r => r.1 == m)

-- Benchmark matchup: Tpcds.Q40DuckDB
/--
A sale with no returns contributes its NULL-extended row. Where there are matches,
return count and refund sum recover the modeled difference total without expanding the
return join.
-/
def ReturnCountAndRefundOuterJoinSubtotal.Goal : Prop := ∀ (price : Int) (refunds : List (Option Int)),
  total (if refunds.isEmpty then [none] else refunds) (fun r => price-r.getD 0) =
    (max 1 refunds.length:Int)*price - (refunds.filterMap id).sum

-- Benchmark matchup: Tpcds.Q49DuckDB
/--
The union of the two qualifying rank sets retains every requested row, including ties at
rank ten. The executor can defer loading details until the union has been selected.
-/
def UnionOfRanksWithTies.Goal : Prop := ∀ (rows : List (Nat × Nat)),
  let first := rows.map Prod.fst
  let second := rows.map Prod.snd
  let withinTen := fun (keys : List Nat) (x : Nat) => (keys.mergeSort[9]?).all (x ≤ ·)
  rows.filter (fun (x,y) => withinTen first x || withinTen second y) =
    rows.filter (fun (x,y) => countWhere first (· < x) < 10 || countWhere second (· < y) < 10)

-- Benchmark matchup: Tpcds.Q57DuckDB
/--
Adjacent rank lookups agree with the three-row window when ranks are unique and
contiguous in the ordered partition. The executor can use neighboring positions instead
of self-join probes.
-/
def AdjacentRankLookupByWindow.Goal : Prop := ∀ (xs : List Int) (i : Nat),
  let lookup := fun (k : Nat) => (xs.zipIdx.find? (fun r => r.2 == k)).map Prod.fst
  (lookup i, lookup (i+1), lookup (i+2)) = (xs[i]?, xs[i+1]?, xs[i+2]?)

-- Benchmark matchup: Tpcds.Q58DuckDB
/--
A shared set of qualifying dates represents the modeled week filter across channels. The
executor can resolve that set once and apply it to each fact stream.
-/
def SharedWeekDateFilterAcrossChannels.Goal : Prop := ∀ (days : List (Nat × Nat)) week date,
  ((days.filter (fun r => r.2 == week)).map Prod.fst).contains date =
    days.any (fun (d,w) => d == date && w == week)

-- Benchmark matchup: Tpcds.Q59DuckDB
/--
A store-week with no shifted partner cannot contribute to the comparison. The executor
can restrict the join input while retaining multiplicities for the remaining week keys.
-/
def MatchingPriorWeekPrefilter.Goal : Prop := ∀ (left right : List (Nat × Nat)),
  let hits := fun (r : Nat × Nat) => right.count (r.1,r.2+52)
  (left.map hits).sum = ((left.filter (fun k => hits k != 0)).map hits).sum

-- Benchmark matchup: Tpcds.Q62DuckDB
/--
A shipping delay maps to the same modeled indicator counts through a single bucket
index. The executor can replace several CASE evaluations with one classification.
-/
def DisjointShippingDelayBuckets.Goal : Prop := ∀ d : Int,
  let b := bucket [30,60,90,120] d
  (b == 0, b == 1, b == 2, b == 3, b == 4) =
    (decide (d ≤ 30), decide (30 < d ∧ d ≤ 60), decide (60 < d ∧ d ≤ 90),
     decide (90 < d ∧ d ≤ 120), decide (120 < d))

-- Benchmark matchup: Tpcds.Q63DuckDB
/--
The group sum and number of contributing months determine the average used in the
modeled deviation test. The executor can evaluate it from aggregate state instead of
rereading window rows.
-/
def MonthlyDeviationFromTotalAndCount.Goal : Prop := ∀ (blocks : List (List (Option Int))),
  ((blocks.flatten.filterMap id).sum,(blocks.flatten.filterMap id).length) =
    ((blocks.map (fun b => (b.filterMap id).sum)).sum,(blocks.map (fun b => (b.filterMap id).length)).sum)

-- Benchmark matchup: Tpcds.Q66DuckDB
/--
Combining monthly states in twelve slots preserves the modeled channel totals. The
executor can merge the fixed layout by month.
-/
def ChannelMonthlySlotSums.Goal : Prop := ∀ (pages : List (List (Nat × Int))) month,
  total pages.flatten (fun (m,v) => if m == month then v else 0) =
    total pages (fun p => total (p.filter (fun r => r.1 == month)) Prod.snd)

-- Benchmark matchup: Tpcds.Q72DuckDB
/--
In a sorted inventory quantity bucket, the boundary before a sale quantity gives the
number of smaller inventory rows. The executor can count those matches without comparing
each row.
-/
def SortedStockPrefixCounts.Goal : Prop := ∀ (stock : List Int) sold,
  (stock.mergeSort.takeWhile (· < sold)).length = countWhere stock (· < sold)

-- Benchmark matchup: Tpcds.Q74DuckDB
/--
Only customers with both channels and both years can enter the modeled ratio comparison.
The executor can check state presence before dividing.
-/
def CompleteYearChannelPairsBeforeRatios.Goal : Prop := ∀ (s1 s2 w1 w2 : List Nat) k,
  (members (members s1 s2) (members w1 w2)).contains k =
    (s1.contains k && s2.contains k && w1.contains k && w2.contains k)

-- Benchmark matchup: Tpcds.Q75DuckDB
/--
Accounting for return multiplicity within each channel preserves the modeled quantity
differences. The executor can combine results at the year and month aggregation instead
of sending expanded rows.
-/
def ReturnMultiplicityWeightedQuantityDelta.Goal : Prop := ∀ (q : Int) (rs : List Int),
  (if rs.isEmpty then q else total rs (fun v => q-v)) = (max 1 rs.length:Int)*q-rs.sum

-- Benchmark matchup: Tpcds.Q79DuckDB
/--
Deferring city-prefix extraction until the city and ticket groups are formed preserves
the modeled output for those groups. The executor can slice one city value per group.
-/
def CityPrefixAfterLocalGrouping.Goal : Prop := ∀ (cities : List String) name,
  ((frequency cities).filter (fun p => (p.1.take 30).toString == name) |>.map Prod.snd).sum =
    countWhere cities (fun s => (s.take 30).toString == name)

-- Benchmark matchup: Tpcds.Q81DuckDB
/--
The customer-ID cutoff and all its ties contain the winners under the full customer and
address order. The executor can defer fetching addresses until the candidate set is
known.
-/
def CustomerBoundaryTiesBeforeAddressSort.Goal : Prop := ∀ (ids : List Nat) (addressKey : Nat → Nat) (customerKey : Nat → Nat),
  let prefixKeys := (ids.map customerKey).mergeSort
  let candidates := ids.filter (fun i => (prefixKeys[99]?).all (customerKey i ≤ ·))
  let cmp := fun (a b : Nat) => decide (customerKey a < customerKey b ∨
    (customerKey a = customerKey b ∧ addressKey a ≤ addressKey b))
  (candidates.mergeSort cmp).take 100 = (ids.mergeSort cmp).take 100

-- Benchmark matchup: Tpcds.Q83DuckDB
/--
The union of weeks containing the selected dates preserves the return-date filter
without counting a shared week twice. The executor can reuse one eligibility set.
-/
def OverlappingWeekFilterUnion.Goal : Prop := ∀ (weeks : List Nat) (returns : List (Nat × Int)),
  returns.filter (fun r => weeks.eraseDups.contains r.1) = returns.filter (fun r => weeks.contains r.1)

-- Benchmark matchup: Tpcds.Q85DuckDB
/--
Independent counts of non-NULL inputs preserve the denominators of the three modeled
averages. The executor can merge partial states without rescanning joined rows.
-/
def IndependentNullableAverageStates.Goal : Prop := ∀ (rs : List (Option Int × Option Int × Option Int)) n,
  let cols : List ((Option Int × Option Int × Option Int) → Option Int) :=
    [Prod.fst, (fun r => r.2.1), (fun r => r.2.2)]
  cols.map (fun f => let vs := (rs.flatMap (List.replicate n)).filterMap f
                     (vs.sum, vs.length)) =
    cols.map (fun f => let vs := rs.filterMap f
                       ((n:Int)*vs.sum, n*vs.length))

-- Benchmark matchup: Tpcds.Q86DuckDB
/--
Grouping tags distinguish rollup-generated NULLs from data NULLs when assigning rank
partitions. The executor can route both kinds of rows to the appropriate sorts.
-/
def RollupTagSeparatesNullRankPartitions.Goal : Prop := ∀ (xs : List ((Nat × Option Nat) × Int)) tag parent sales,
  let partition := (xs.filter (fun r => r.1 == (tag,parent))).map Prod.snd
  1 + (partition.mergeSort.reverse.takeWhile (sales < ·)).length =
    1 + countWhere xs (fun r => r.1 == (tag,parent) && sales < r.2)

-- Benchmark matchup: Tpcds.Q87DuckDB
/--
The union of the two excluded name and date sets preserves the modeled set difference.
The executor can build one exclusion lookup for the store keys.
-/
def ExclusionByUnionOfChannels.Goal : Prop := ∀ (s c w : List (Option String × Option String × Nat)),
  (s.eraseDups.filter (!c.contains ·)).filter (!w.contains ·) = s.eraseDups.filter (!(c++w).contains ·)

-- Benchmark matchup: Tpcds.Q90DuckDB
/--
A single pass can maintain the modeled morning and afternoon counts separately. The
executor can share the fact scan.
-/
def SinglePassTimeBandCount.Goal : Prop := ∀ (hours : List Nat), let selected := hours.filter ([8,9,19,20].contains ·)
  (countWhere selected (· ≤ 9),countWhere selected (19 ≤ ·)) =
    (countWhere hours ([8,9].contains ·),countWhere hours ([19,20].contains ·))

-- Benchmark matchup: Tpcds.Q99DuckDB
/--
Adding local delay histograms by warehouse, shipping method, and call center preserves
their modeled bucket counts. The executor can merge partial states instead of sending
each row to the global aggregate.
-/
def MergeShippingDelayHistograms.Goal : Prop := ∀ (parts : List (List (Nat × Option Int))) groupKey,
  let classify := fun (p : List (Nat × Option Int)) =>
    ((p.filter (fun r => r.1 == groupKey)).filterMap Prod.snd).map (bucket [30,60,90,120])
  let bins := List.range 5
  bins.map (fun c => (parts.map (fun p => (classify p).count c)).sum) =
    bins.map (classify parts.flatten).count

-- Benchmark query: Tpcds.Q02
/--
A nullable sum for a selected week and weekday is unchanged when sales are first grouped
by date key and the qualifying date subtotals are added.

The aggregation can resolve the calendar attributes once per distinct date key and merge
NULL-aware states.
-/
def DateKeyPreaggregationBeforeWeekLookup.Goal : Prop := ∀ (sales : List (Nat × Option Int)) (weekDay : Nat → Nat × Nat) cell,
  nsum ((sales.filter (fun r => weekDay r.1 == cell)).map Prod.snd) =
    nsum ((((sales.map Prod.fst).eraseDups).filter (fun d => weekDay d == cell)).map (fun d => nsum (forKey sales d)))

-- Benchmark query: Tpcds.Q04
/--
Filtering sales using the prior-year activity predicate leaves the key's sales bucket
intact whenever its prior-year total is positive.

The executor can build second-year aggregate state only for customers eligible for the
ratio calculation.
-/
def PositivePriorYearDenominatorFilter.Goal : Prop := ∀ (prev : Nat → List Int) (sales : List (Nat × Option Int)) k,
  let active := fun i => decide (0 < (prev i).sum)
  active k = true → forKey (sales.filter (fun r => active r.1)) k = forKey sales k

-- Benchmark query: Tpcds.Q11
/--
With positive denominators and exact scaled division, comparing two ratios is equivalent
to comparing their cross-products.

The ratio filter can compare widened integer products directly when these arithmetic
conditions hold.
-/
def PositiveDenominatorRatioCrossProduct.Goal : Prop := ∀ (a b c d scale : Int), 0 < b → 0 < d → 0 < scale →
  b ∣ a*scale → d ∣ c*scale → (a*scale/b > c*scale/d ↔ a*d > c*b)

-- Benchmark query: Tpcds.Q14
/--
The nullable total of qualifying detail groups agrees with the sum over their combined
input values.

The rollup can consume the subtotal of each group that passes HAVING, with NULL
preserved when no qualifying value contributes.
-/
def HavingFilteredNullableRollup.Goal : Prop := ∀ (cells : List (List (Option Int))) threshold,
  let kept := cells.filter (fun g => (nsum g).any (threshold < ·))
  nsum (kept.map nsum) = nsum kept.flatten

-- Benchmark query: Tpcds.Q15
/--
Testing the postcode prefix on each address code's dictionary entry selects the same
encoded rows as applying the predicate to the decoded strings.

A dictionary scan can compute qualifying codes once and avoid repeating substring
extraction on the fact-side path.
-/
def PostalPrefixDictionaryFilter.Goal : Prop := ∀ (zips allowed : List String) (codes : List Nat),
  let good := (zips.zipIdx.filter (fun (z,_) => allowed.contains (z.take 5).toString)).map Prod.snd
  codes.filter good.contains = codes.filter (fun i => zips[i]?.any (fun z => allowed.contains (z.take 5).toString))

-- Benchmark query: Tpcds.Q16
/--
Restricting the return build to candidate order keys preserves anti-join results under
modeled SQL equality, where NULL does not match.

The executor can keep fewer return keys while still retaining candidate rows with no
equal non-NULL return key.
-/
def CandidateRestrictedNullAwareReturnAntijoin.Goal : Prop := ∀ (orders returns : List (Option Nat)),
  let anti := fun (ks : List (Option Nat)) => orders.filter (fun o => !(ks.any (hit o)))
  anti (returns.filter (fun r => orders.any (hit r))) = anti returns

-- Benchmark query: Tpcds.Q17
/--
Subtracting a common offset from the inputs preserves the exact integer variance
numerator.

The aggregate can accumulate differences from a block reference value, reducing the
magnitude of intermediate sums when the values are clustered.
-/
def ShiftedVarianceNumerator.Goal : Prop := ∀ (quantities : List Int) origin,
  let numerator := fun (xs : List Int) => (xs.length:Int)*total xs (fun x => x*x) - xs.sum*xs.sum
  numerator (quantities.map (· - origin)) = numerator quantities

-- Benchmark query: Tpcds.Q21
/--
A block whose item-key bounds exclude all eligible items cannot contribute a row to the
selected inventory result.

The scan can use stored key ranges to avoid decoding those inventory blocks.
-/
def EligibleItemRangeBlockPruning.Goal : Prop := ∀ (blocks : List (List Nat)) (cheap : List Nat),
  let possible := fun (p : List Nat) => cheap.any (fun k => p.min?.any (· ≤ k) && p.max?.any (k ≤ ·))
  ((blocks.filter possible).flatten.filter cheap.contains) = blocks.flatten.filter cheap.contains

-- Benchmark query: Tpcds.Q25
/--
Restricting returns to keys present in sales preserves the modeled three-way join count,
including sales and catalog multiplicities.

The executor can reduce the returns sent to the catalog join using a semi-join on
customer, item, and ticket.
-/
def SalesKeyFilterOnReturnJoin.Goal : Prop := ∀ (sales returns : List ((Nat × Nat) × Nat)) (catalog : Nat × Nat → Nat),
  let joinCount := fun (rs : List ((Nat × Nat) × Nat)) => (rs.map (fun k => sales.count k * catalog k.1)).sum
  joinCount (returns.filter sales.eraseDups.contains) = joinCount returns

-- Benchmark query: Tpcds.Q29
/--
Grouping repeated customer-item keys and weighting each nullable catalog subtotal by its
frequency preserves the total.

After ticket matching, the executor can reuse the catalog quantity aggregate for
repeated customer-item pairs.
-/
def CachedCatalogQuantityByCustomerItem.Goal : Prop := ∀ (matchedTickets : List (Nat × Nat)) (catalog : (Nat × Nat) → List (Option Int)),
  nsum (matchedTickets.flatMap catalog) =
    nsum ((frequency matchedTickets).map (fun (k,n) => (nsum (catalog k)).map (fun v => v*(n:Int))))

-- Benchmark query: Tpcds.Q30
/--
Under the stated positivity and exactness conditions, the comparison with the state
average can be expressed using cross-products.

The filter can use the state sum and non-NULL count directly instead of dividing to
obtain the threshold for each customer.
-/
def AverageRatioComparisonByCrossProduct.Goal : Prop := ∀ (stateTotals : List (Option Int)) (v : Int),
  let xs := stateTotals.filterMap id
  let n : Int := xs.length
  0 < n → 5*n ∣ 6*xs.sum → (v > 6*xs.sum/(5*n) ↔ 5*n*v > 6*xs.sum)

-- Benchmark query: Tpcds.Q35
/--
The modeled web and catalog existence tests agree for store customers when their mark
tables are restricted to store keys.

The executor can constrain both build sides with the store customer set before
constructing membership state.
-/
def StoreCustomerRestrictedExistenceMarks.Goal : Prop := ∀ (customers store web catalog : List Nat),
  let demand := members customers store
  let extra := (web++catalog).filter demand.contains
  customers.filter (fun k => store.contains k && extra.contains k) =
    customers.filter (fun k => store.contains k && (web.contains k || catalog.contains k))

-- Benchmark query: Tpcds.Q38
/--
Canonical codes for equal names preserve the cardinality of the modeled distinct
intersection on name and date.

The executor can compare compact name codes in the set operation when the code
assignment preserves SQL equality.
-/
def NameCodeIntersectionCardinality.Goal : Prop := ∀ (s c w : List (String × Nat)) (code : String → Nat),
  (∀ x y, code x = code y → x = y) →
  let encode := fun (p : String × Nat) => (code p.1,p.2)
  (members (members (s.map encode) (c.map encode)) (w.map encode)).length = (members (members s c) w).length

-- Benchmark query: Tpcds.Q39
/--
An integer group whose minimum and maximum agree has a zero variance numerator.

A variance-based HAVING test can avoid squared-value accumulation for a group certified
constant, subject to the SQL test's NULL and denominator conditions.
-/
def ConstantGroupVarianceRejection.Goal : Prop := ∀ (xs : List Int) v, xs.min? = some v → xs.max? = some v →
  (xs.length:Int)*total xs (fun x => x*x) - xs.sum*xs.sum = 0

-- Benchmark query: Tpcds.Q40
/--
Filtering the return build by keys present in sales preserves both matching rows and
NULL-extended sales rows in the modeled left join.

The executor can build a smaller return hash table while preserving duplicates and
unmatched rows.
-/
def OuterJoinBuildRestrictedToSalesKeys.Goal : Prop := ∀ (sold : List (Nat × Nat)) (returns : List ((Nat × Nat) × Int)),
  let outerRows := fun (rs : List ((Nat × Nat) × Int)) => sold.flatMap (fun k =>
    let vs := forKey rs k
    if vs.isEmpty then [none] else vs.map some)
  outerRows (returns.filter (fun r => sold.contains r.1)) = outerRows returns

-- Benchmark query: Tpcds.Q41
/--
A positive correlated count exists exactly for keys present among qualifying inner rows.

The executor can use a membership structure for eligible manufacturers instead of
counting inner matches per outer row.
-/
def CorrelatedPositiveCountAsMembership.Goal : Prop := ∀ (outerKeys : List (Option String)) (items : List (Option String × Bool)),
  let names := (items.filter Prod.snd).filterMap Prod.fst
  outerKeys.filter (fun k => k.any names.eraseDups.contains) =
    outerKeys.filter (fun k => 0 < countWhere items (fun r => hit k r.1 && r.2))

-- Benchmark query: Tpcds.Q45
/--
Selecting by logical item IDs obtained from the specified physical keys agrees with
expanding those IDs back to all matching physical keys.

The sales filter can use an integer semi-join and retain historical versions that share
the same logical item ID.
-/
def ExpandPhysicalKeysByLogicalItem.Goal : Prop := ∀ (versions : List (Nat × Option String)) (seed : List Nat)
    (facts : List (Nat × Bool)),
  (versions.map Prod.fst).Nodup →
  let names := (versions.filter (fun r => seed.contains r.1)).filterMap Prod.snd
  let expanded := (versions.filter (fun r => r.2.any names.contains)).map Prod.fst
  let joinItem := fun k => versions.filter (fun r => r.1 == k)
  (facts.flatMap (fun (k,z) => (joinItem k).filter (fun r => z || r.2.any names.contains))) =
    ((facts.filter (fun (k,z) => z || expanded.contains k)).flatMap (fun r => joinItem r.1))

-- Benchmark query: Tpcds.Q49
/--
The refund threshold rejects the NULL-extended rows produced by the modeled outer join.

The join can use the inner path when the refund predicate is a required condition and
rejects NULL under SQL semantics.
-/
def NullRejectingRefundOuterJoinReduction.Goal : Prop := ∀ (sold : List Nat) (refunds : Nat → List (Option Int)),
  let qualified := fun (x : Option Int) => x.any (10000 < ·)
  (sold.flatMap (fun k => if (refunds k).isEmpty then [none] else refunds k)).filter qualified =
    sold.flatMap (fun k => (refunds k).filter qualified)

-- Benchmark query: Tpcds.Q57
/--
Selecting the target-year window inputs separately preserves the average for that
partition.

The executor can aggregate the target year's rows and fetch neighboring months
separately for the subsequent lookup.
-/
def YearPartitionAverageBeforeNeighborLookup.Goal : Prop := ∀ (months : List ((Int × Nat) × Option Int)) (y : Int),
  let extend := months.filter (fun ((yr,m),_) => yr == y || (yr == y-1 && m == 12) || (yr == y+1 && m == 1))
  let yearValues := fun (rs : List ((Int × Nat) × Option Int)) =>
    (rs.filter (fun r => r.1.1 == y)).filterMap Prod.snd
  yearValues extend = yearValues months

-- Benchmark query: Tpcds.Q58
/--
Under the positive-value assumptions, bounding the minimum and maximum is sufficient to
bound all modeled pairwise ratios.

A filter over channel totals can test the extremes and avoid evaluating each pair
separately.
-/
def PairwiseRatioBoundsFromExtrema.Goal : Prop := ∀ (a b c : Nat),
  let xs := [a,b,c]
  (∀ x ∈ xs, ∀ y ∈ xs, 9*y ≤ 10*x ∧ 10*x ≤ 11*y) ↔
    (9*max a (max b c) ≤ 10*min a (min b c) ∧ 10*max a (max b c) ≤ 11*min a (min b c))

-- Benchmark query: Tpcds.Q59
/--
The number of matching date combinations for a week pair is the product of its two date
multiplicities.

The executor can carry a weight for the week pair and delay expanding identical output
rows.
-/
def WeekPairMultiplicityProduct.Goal : Prop := ∀ (weeks firstYear nextYear : List Nat) w,
  let joined := weeks.flatMap (fun k => (firstYear.filter (· == k)).flatMap
    (fun _ => (nextYear.filter (· == k+52)).map (fun _ => k)))
  joined.count w = weeks.count w * firstYear.count w * nextYear.count (w+52)

-- Benchmark query: Tpcds.Q62
/--
Merging the nullable subtotals of warehouses that share a name prefix preserves the sum
for that prefix.

The executor can derive the prefix on warehouse keys and combine aggregate states
instead of slicing each input row's name.
-/
def WarehousePrefixNullableSubtotalMerge.Goal : Prop := ∀ (rs : List (String × Nat)) name bin,
  let keys := (rs.map Prod.fst).eraseDups.filter (fun s => (s.take 20).toString == name)
  (keys.map (fun k => (forKey rs k).count bin)).sum = countWhere rs (fun r => (r.1.take 20).toString == name && r.2 == bin)

-- Benchmark query: Tpcds.Q63
/--
Testing membership in the union of eligible item sets agrees with the disjunctive
predicate, including items that satisfy more than one branch.

The executor can form a single eligibility set and accumulate each qualifying fact row
once.
-/
def OverlappingItemPredicateUnion.Goal : Prop := ∀ (facts a b : List Nat) (price : Nat → Int),
  total (facts.filter (a++b).eraseDups.contains) price =
    total (facts.filter (fun k => a.contains k || b.contains k)) price

-- Benchmark query: Tpcds.Q66
/--
When warehouse attributes are fixed by the warehouse key, attaching them after the
monthly sum preserves the grouped totals.

The hash aggregate can use a narrower key and look up the constant attributes on its
output.
-/
def FunctionalWarehouseAttributesAfterMonthlySum.Goal : Prop := ∀ (facts : List ((Nat × Nat) × Int)) (attrs : Nat → String) w m,
  let wide := facts.map (fun ((id,month),v) => ((id,attrs id,month),v))
  forKey wide (w,attrs w,m) = forKey facts (w,m)

-- Benchmark query: Tpcds.Q72
/--
The modeled outer-join counts can be recovered from return multiplicity and promotion
presence.

The executor can update the counts without materializing one joined row for each
duplicate return.
-/
def ReturnMultiplicityAndPromotionCounts.Goal : Prop := ∀ (promoCopies returnCopies : Nat),
  let promo := if promoCopies == 0 then [false] else List.replicate promoCopies true
  let expanded := promo.flatMap (List.replicate (max 1 returnCopies))
  (expanded.count false,expanded.count true,expanded.length) =
    (if promoCopies == 0 then max 1 returnCopies else 0, promoCopies*max 1 returnCopies,
     max 1 promoCopies * max 1 returnCopies)

-- Benchmark query: Tpcds.Q74
/--
The first qualifying rows in an already ordered stream determine the requested prefix of
the filtered result.

An ordered execution path can stop evaluating ratios once it has found the required
number of rows.
-/
def OrderedTopKAfterPriorYearRatioFilter.Goal : Prop := ∀ (ordered : List Nat) (pass : Nat → Bool) cut,
  100 ≤ countWhere (ordered.take cut) pass →
    ((ordered.take cut).filter pass).take 100 = (ordered.filter pass).take 100

-- Benchmark query: Tpcds.Q75
/--
Removing duplicate complete rows within each channel preserves the distinct union and
its downstream total.

The executor can reduce input to the global distinct step by deduplicating each channel
before the union.
-/
def ChannelLocalDedupBeforeUnionDistinct.Goal : Prop := ∀ (channels : List (List (Nat × Int × Int))) k,
  let qty := fun (rs : List (Nat × Int × Int)) => total (rs.eraseDups.filter (fun r => r.1 == k)) (fun r => r.2.1)
  qty (channels.flatMap List.eraseDups) = qty channels.flatten

-- Benchmark query: Tpcds.Q79
/--
If the remaining group keys determine the address, dropping it from the key does not
merge distinct original groups.

The executor can aggregate with a narrower key on groups where the dependency is
established.
-/
def AddressKeyElisionByFunctionalDependency.Goal : Prop := ∀ (rows : List (Nat × Option Int)) addr,
  (∀ r ∈ rows, r.1 = addr) →
  let wide := (rows.map Prod.fst).eraseDups.map (fun a => nsum (forKey rows a))
  wide = (if rows.isEmpty then [] else [nsum (rows.map Prod.snd)])

-- Benchmark query: Tpcds.Q80
/--
Combining nullable channel subtotals produces the same sum as aggregating all channel
rows, including empty and all-NULL inputs.

A rollup can update the grand total once per channel and preserve SQL SUM semantics.
-/
def NullableTotalFromChannelSubtotals.Goal : Prop := ∀ (channels : List (List (List (Option Int)))),
  nsum (channels.map (fun ids => nsum (ids.map nsum))) = nsum channels.flatten.flatten

-- Benchmark query: Tpcds.Q81
/--
The modeled state average can be computed from the total and count of customer-level
values for that state.

The executor can build state aggregate state once and look it up for each candidate
customer.
-/
def StateAverageFromCustomerTotals.Goal : Prop := ∀ (totals : List (Nat × Nat × Option Int)) (georgia : Nat → Bool),
  let vals := fun state => (totals.filter (fun r => r.1 == state)).filterMap (fun r => r.2.2)
  let states := ((totals.filter (fun r => georgia r.2.1)).map Prod.fst).eraseDups
  let cache := states.map (fun k => (k,((vals k).sum,(vals k).length)))
  ∀ r ∈ totals, georgia r.2.1 = true →
    forKey cache r.1 = [((vals r.1).sum,(vals r.1).length)]

-- Benchmark query: Tpcds.Q83
/--
The ordered prefix of item keys common to all three channels can be selected separately
from their quantities under the stated key assumptions.

The executor can locate the first 100 distinct matching keys and defer loading
quantities until afterward.
-/
def CommonItemTopKBeforeQuantityLookup.Goal : Prop := ∀ (sr cr wr : List Nat) stop (qty : Nat → Int × Int × Int),
  let common := members (members sr cr) wr
  let prefixOf := fun (keys : List Nat) => keys.filter (· ≤ stop)
  100 ≤ (prefixOf common).length →
  let winners := (members (members (prefixOf sr) (prefixOf cr)) (prefixOf wr)).mergeSort.take 100
  winners.map (fun k => (k,qty k)) =
    (common.mergeSort.map (fun k => (k,qty k))).take 100

-- Benchmark query: Tpcds.Q85
/--
Equality of canonical non-NULL demographic codes represents equality of the
corresponding attributes under the model's assumptions.

The join predicate can compare shared codes rather than comparing both attribute strings
on every pair.
-/
def EqualNonNullDemographicCodes.Goal : Prop := ∀ (a b : Option (String × String)) (code : String × String → Nat),
  (∀ x y, code x = code y → x = y) → hit (a.map code) (b.map code) = hit a b

-- Benchmark query: Tpcds.Q86
/--
Descending SQL RANK is one plus the number of rows with a strictly greater value, which
equals the sum of their frequencies.

A window operator can process distinct values with counts while preserving shared ranks
and gaps after ties.
-/
def RankFromDistinctValueFrequencies.Goal : Prop := ∀ (partition : List Int) value,
  1 + ((frequency partition).filter (fun r => value < r.1) |>.map Prod.snd).sum =
    1 + countWhere partition (value < ·)

-- Benchmark query: Tpcds.Q87
/--
Deleting catalog and web keys from the distinct store candidates gives the same
cardinality as the modeled set difference.

The executor can keep only the store candidate set and remove matches as it reads the
other channels.
-/
def CandidateOnlyDifferenceMarks.Goal : Prop := ∀ (store catalog web : List Nat),
  let eraseSeen := fun (keys : List Nat) (k : Nat) => keys.filter (· != k)
  ((catalog++web).foldl eraseSeen store.eraseDups).length =
    countWhere store.eraseDups (fun k => !(catalog.contains k || web.contains k))

-- Benchmark query: Tpcds.Q89
/--
If every modeled month lies within a certified interval inside the deviation band around
the positive mean, none passes the strict deviation filter.

The executor can reject a whole group using interval bounds before evaluating individual
month deviations.
-/
def MonthlyDeviationBandPruning.Goal : Prop := ∀ (months : List Int) (n sum lo hi : Int),
  0 < n → 0 < sum → (∀ x ∈ months, lo ≤ x ∧ x ≤ hi) →
  9*sum ≤ 10*n*lo → 10*n*hi ≤ 11*sum →
    months.filter (fun x => 10*(n*x-sum).natAbs > sum.toNat) = []

-- Benchmark query: Tpcds.Q90
/--
Counting qualifying joined rows by key frequency gives the same totals as processing
each fact row for the two time bands.

The executor can reuse dimension probes for repeated time, household, and page keys and
apply their multiplicities to the counts.
-/
def WeightedTimeBandJoinCounts.Goal : Prop := ∀ (keys : List (Nat × Nat × Nat)) (passes : Nat × Nat × Nat → Nat) (hour : Nat → Nat),
  let band := fun (low : Nat) (k : Nat × Nat × Nat) =>
    if low ≤ hour k.1 ∧ hour k.1 ≤ low+1 then passes k else 0
  let hist := frequency keys
  [8,19].map (fun low => (keys.map (band low)).sum) =
    [8,19].map (fun low => (hist.map (fun (k,n) => n*band low k)).sum)

-- Benchmark query: Tpcds.Q99
/--
Replacing runs of equal date pairs with their lengths preserves the counts in the
shipping-delay buckets.

The executor can compute each run's delay once and add the run length to its bucket.
-/
def RunLengthShippingDelayHistogram.Goal : Prop := ∀ (runs : List ((Int × Int) × Nat)) bin,
  let delay := fun (p : Int × Int) => bucket [30,60,90,120] (p.2-p.1)
  ((runs.filter (fun r => delay r.1 == bin)).map Prod.snd).sum =
    countWhere (runs.flatMap (fun (p,n) => List.replicate n p)) (fun p => delay p == bin)

end TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

-- C++ references to this model or its proof.
-- executor/detail/benchmark_rematch.hpp
