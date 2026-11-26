import TinylambOpt.ResearchGoals.QueryRewrites.Common

/-!
These goals model joins, filters, and aggregation patterns from analytical SQL.

They establish when SQL execution can retain multiplicities as weights, push filters
earlier, or build partial aggregate states before dimension lookup.
-/

namespace TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins

-- Benchmark matchup: Analytics.J3DuckDB
/--
Restricting the female-person filter to IDs demanded by qualifying cast rows preserves
the modeled matches. The executor can allocate the name filter for the demanded IDs.
-/
def CastRoleBoundedPersonFilter.Goal : Prop := ∀ (cast names : List Nat) lo hi,
  (∀ k ∈ cast, lo ≤ k ∧ k ≤ hi) →
  let bits := (List.range (hi+1-lo)).map (fun i => names.contains (lo+i))
  cast.filter (fun k => bits[k-lo]? == some true) = cast.filter names.contains

-- Benchmark matchup: Analytics.J3ClickHouse
/--
Weighting a movie year by the number of qualifying cast rows for that movie preserves
the sum. The executor can probe the title once per movie subtotal.
-/
def CastFrequencyWeightedYearTotals.Goal : Prop := ∀ (cast : List Nat) (years : Nat → List Int),
  let expanded := cast.flatMap years
  (expanded.length,expanded.sum) =
    (((frequency cast).map (fun (k,n) => n*(years k).length)).sum,
     total (frequency cast) (fun (k,n) => (n:Int)*(years k).sum))

-- Benchmark matchup: Analytics.C1DuckDB
/--
Weighting comments by the corresponding post's vote count preserves the join count and
nullable sum. The aggregate can avoid generating comment-vote pairs.
-/
def VoteWeightedCommentAggregates.Goal : Prop := ∀ (scores : List (Option Int)) votes,
  let expanded := scores.flatMap (List.replicate votes)
  (expanded.length,nsum expanded) = (scores.length*votes,
    if votes == 0 then none else (nsum scores).map (fun x => (votes:Int)*x))

-- Benchmark matchup: Analytics.C1ClickHouse
/--
Votes from a qualifying post with no matching comments cannot contribute. The executor
can check comment existence before reading the vote list.
-/
def SkipVoteReadsWithoutComments.Goal : Prop := ∀ (posts comments votes : List Nat),
  let joins := fun (ps : List Nat) => (ps.map fun k => comments.count k * votes.count k).sum
  joins (posts.filter comments.contains) = joins posts

-- Benchmark matchup: Analytics.C2DuckDB
/--
Multiplying a post's commenter frequencies by the multiplicity of users passing the
reputation condition preserves the join count. The executor can reuse the user-side
count across repeated commenter keys.
-/
def CommenterFrequencyWithUserMultiplicity.Goal : Prop := ∀ (commenters eligibleUsers : List Nat),
  ((frequency commenters).map (fun (k,n) => n * eligibleUsers.count k)).sum =
    (commenters.flatMap (fun k => eligibleUsers.filter (· == k))).length

-- Benchmark matchup: Analytics.H2DuckDB
/--
The bounded two-column group key has an injective mapping to an array index. The
executor can count groups in a dense array when the bounds are certified.
-/
def DenseCompositeCountIndex.Goal : Prop := ∀ (rows : List (Nat × Nat)) width a b,
  b < width → (∀ r ∈ rows, r.2 < width) →
  (rows.map (fun (x,y) => x*width+y)).count (a*width+b) = rows.count (a,b)

-- Benchmark matchup: Analytics.H4DuckDB
/--
A page excluded by the id3 lower bound contributes no AVG inputs. The scan can skip
decoding such pages and retain the original order of surviving non-NULL values.
-/
def ZoneMinimumPrunesNullableAverageInput.Goal : Prop := ∀ (blocks : List (List (Nat × Option Int))),
  let scan := fun (rs : List (Nat × Option Int)) => (rs.filter (fun r => r.1 < 4)).filterMap Prod.snd
  (blocks.filter (fun b => (b.map Prod.fst).min?.any (· < 4))).flatMap scan = scan blocks.flatten

-- Benchmark matchup: Analytics.H5DuckDB
/--
The two sums over matching pairs can be recovered by scaling each side's subtotal by the
other side's count for the same join keys. The executor can combine partial aggregates
without generating the pairs.
-/
def FactorizedNullableJoinSums.Goal : Prop := ∀ (x y : List (Option Int)),
  let pairs := x.flatMap fun a => y.map (fun b => (a,b))
  (nsum (pairs.map Prod.fst),nsum (pairs.map Prod.snd)) =
    (if y.isEmpty then none else (nsum x).map (fun v => v*(y.length:Int)),
     if x.isEmpty then none else (nsum y).map (fun v => v*(x.length:Int)))

-- Benchmark matchup: Analytics.L2DuckDB
/--
The forum-level member and person match count can weight the messages for that forum.
Grouped counts can avoid expanding each matching forum combination.
-/
def ForumMembershipWeightedMessageTotals.Goal : Prop := ∀ (members people : List Nat) (scores : List Int),
  let n := (members.map people.count).sum
  let expanded := members.flatMap fun k => (people.filter (· == k)).flatMap (fun _ => scores)
  (expanded.length,expanded.sum) = (n*scores.length,(n:Int)*scores.sum)

-- Benchmark matchup: Analytics.L4DuckDB
/--
Grouping eligible 2012 replies by forum before joining preserves the modeled forum
result. The executor can send one subtotal per forum to the join.
-/
def ForumReplyFrequencyJoinCount.Goal : Prop := ∀ (scores : List (Option Int)) (forumCopies : Nat),
  let joined := (List.replicate forumCopies scores).flatten
  (joined.length, (joined.filterMap id).sum, (joined.filterMap id).length) =
    (forumCopies*scores.length,(forumCopies:Int)*(scores.filterMap id).sum,forumCopies*(scores.filterMap id).length)

-- Benchmark matchup: Analytics.L5DuckDB
/--
Weighting a person's age and count state by outgoing edge multiplicity preserves the
modeled country aggregate. The executor can avoid expanding identical age contributions.
-/
def OutdegreeWeightedNullableAgeTotals.Goal : Prop := ∀ (people : List (Nat × Option Int)) (edges : List Nat),
  let ages := people.flatMap (fun (k,a) => List.replicate (edges.count k) a)
  (ages.length,(ages.filterMap id).sum,(ages.filterMap id).length) =
    ((people.map (fun r => edges.count r.1)).sum,
     total people (fun (k,a) => (edges.count k:Int)*a.getD 0),
     ((people.filter (fun r => r.2.isSome)).map (fun r => edges.count r.1)).sum)

-- Benchmark matchup: Analytics.L5ClickHouse
/--
Restricting the edge scan to source people who match the country and year predicates
preserves the qualifying edges. The executor can discard other edges before aggregation.
-/
def CountryYearSourceSemijoin.Goal : Prop := ∀ (edges peopleInCountry : List Nat),
  let admitted := edges.filter peopleInCountry.eraseDups.contains
  admitted.flatMap (fun k => peopleInCountry.filter (· == k)) =
    edges.flatMap (fun k => peopleInCountry.filter (· == k))

-- Benchmark matchup: Analytics.B3DuckDB
/--
The bounded region, category, and year tuple can be encoded without collisions. The
aggregate can use integer group keys under those bounds.
-/
def BoundedRegionCategoryYearKeyEncoding.Goal : Prop := ∀ (r c y r' c' y' categories : Nat),
  c < categories → c' < categories → y < 6 → y' < 6 →
  ((r*categories+c)*6+y = (r'*categories+c')*6+y' ↔ (r,c,y) = (r',c',y'))

-- Benchmark matchup: Analytics.B3ClickHouse
/--
Applying the customer filter first preserves the qualifying sales and their store and
product matches. The executor can skip those dimension probes for rejected sales.
-/
def CustomerFilterBeforeDimensionLookups.Goal : Prop := ∀ (sales : List Nat) (dimRows : Nat → List Int) (eligible : Nat → Nat),
  let project := fun k => (dimRows k).flatMap (List.replicate (eligible k))
  (sales.filter (fun k => eligible k > 0)).flatMap project = sales.flatMap project

-- Benchmark matchup: Analytics.B4DuckDB
/--
Counting US rows first and testing the substring only for other rows preserves the OR
count. The scan can avoid passing US rows to LIKE.
-/
def ShortCircuitCountryOrSubstringCount.Goal : Prop := ∀ (rs : List (Bool × Bool × Option Int)),
  let amounts := fun (xs : List (Bool × Bool × Option Int)) => xs.map (fun r => r.2.2)
  let us := rs.filter Prod.fst
  let textHits := rs.filter (fun r => !r.1 && r.2.1)
  let hits := rs.filter (fun r => r.1 || r.2.1)
  (hits.length,(nsum (amounts hits)).getD 0) =
    (us.length+textHits.length,(nsum (amounts us)).getD 0 + (nsum (amounts textHits)).getD 0)

-- Benchmark matchup: Analytics.B5DuckDB
/--
With unique product keys, checking the price predicate once per distinct product in a
sales block preserves the per-row selection. The scan can reuse the selected codes.
-/
def PerBlockDistinctPriceLookup.Goal : Prop := ∀ (sold : List Nat) (priced : Nat → Bool),
  let mask := sold.eraseDups.filter priced
  sold.filter mask.contains = sold.filter priced

-- Benchmark matchup: Analytics.B5ClickHouse
/--
Summing sales by product and scaling the nullable subtotal by qualifying price-match
multiplicity preserves the modeled annual SUM. The executor can pass partial states
through the price join.
-/
def HighPriceMultiplicityWeightedNullableSales.Goal : Prop := ∀ (sales : List (Nat × Option Int)) (items : List Nat),
  let keys := (sales.map Prod.fst).eraseDups
  let groupSum := fun k => nsum ((sales.filter (fun r => r.1 == k)).map Prod.snd)
  nsum (sales.flatMap (fun (k,v) => List.replicate (items.count k) v)) =
    nsum ((keys.filter items.contains).map (fun k => (groupSum k).map (fun v => v*items.count k)))

-- Benchmark query: Analytics.J1
/--
Aggregating company matches by movie and weighting the title's year by their
multiplicity preserves the modeled count and year sum.

The executor can look up each title once per movie key and account for all qualifying
company matches with weights.
-/
def CompanyWeightedMovieYearTotals.Goal : Prop := ∀ (movieYears : List (Nat × Int)) (companies : List Nat),
  let expanded := companies.flatMap (forKey movieYears)
  (expanded.length,expanded.sum) =
    ((movieYears.map (fun r => companies.count r.1)).sum,total movieYears (fun (k,y) => y*(companies.count k:Int)))

-- Benchmark query: Analytics.J2
/--
For each movie key, the modeled number of qualifying rating-genre pairs is the product
of the matching input multiplicities.

The executor can probe the genre index for eligible movies and update year groups
without enumerating every pair.
-/
def RatingAndGenreJoinMultiplicity.Goal : Prop := ∀ (titles drama ratings : List Nat),
  (titles.flatMap (fun k => (drama.filter (· == k)).flatMap (fun _ => ratings.filter (· == k)))).length =
    ((titles.filter ratings.contains).map (fun k => drama.count k * ratings.count k)).sum

-- Benchmark query: Analytics.J3
/--
For modeled years 1970 through 2010, codes 1 through 41 encode eligible years and zero
marks ineligible rows. The code sum and nonzero count recover the year total.

A join can read a compact year code per movie and update both aggregates without
fetching a separate year payload.
-/
def OptionalYearByteCodeTotals.Goal : Prop := ∀ (movieIDs : List Nat) (year : Nat → Option Nat),
  (∀ k y, year k = some y → 1970 ≤ y ∧ y ≤ 2010) →
  let encode := fun k => ((year k).map (· - 1969)).getD 0
  let codes := movieIDs.map encode
  let hits := codes.length - codes.count 0
  (∀ c ∈ codes, c ≤ 41) ∧ (hits,codes.sum+1969*hits) =
    ((movieIDs.filterMap year).length,(movieIDs.filterMap year).sum)

-- Benchmark query: Analytics.J5
/--
The modeled alias prefix test agrees with membership in the corresponding half-open
range under the same string ordering.

An ordered index can find matching aliases by boundary search when its collation
supports that equivalence.
-/
def AliasPrefixHalfOpenRange.Goal : Prop := ∀ (aliases : List (String × Nat)),
  aliases.filter (fun r => r.1.startsWith "aka-1") =
    aliases.filter (fun r => "aka-1" ≤ r.1 && r.1 < "aka-2")

-- Benchmark query: Analytics.C1
/--
A page can be skipped if its score or view-count bound excludes all eligible rows, with
qualifying post IDs left in their original order.

The scan can prune pages before comment and vote lookup without reordering surviving
IDs.
-/
def ScoreAndViewCountZonePruning.Goal : Prop := ∀ (pages : List (List (Int × Int × Nat))),
  let qualify := fun (p : Int × Int × Nat) => decide (5 < p.1 ∧ p.2.1 < 10000)
  let possible := fun (p : List (Int × Int × Nat)) =>
    (p.map Prod.fst).max?.any (5 < ·) && (p.map (fun r => r.2.1)).min?.any (· < 10000)
  (pages.filter possible).flatten.filter qualify = pages.flatten.filter qualify

-- Benchmark query: Analytics.C2
/--
Counting matching comments once per post key and weighting by qualifying post
multiplicity preserves the owner's join count.

The executor can accumulate per-owner counts without materializing each post-comment
pair.
-/
def OwnerCommentFrequencyJoinCount.Goal : Prop := ∀ (posts : List (Nat × Nat)) (passedComments : List Nat) owner,
  let owned := forKey posts owner
  (owned.flatMap (fun p => passedComments.filter (· == p))).length =
    ((frequency owned).map (fun (p,n) => n*passedComments.count p)).sum

-- Benchmark query: Analytics.H2
/--
Ordering groups by descending count buckets preserves count order and the modeled
relative order within ties.

The executor can enumerate completed groups by bucket when the count domain is suitable
for bucket storage.
-/
def StableDescendingCountBuckets.Goal : Prop := ∀ (groups : List (Nat × Nat)) maxCount,
  (∀ r ∈ groups, r.2 ≤ maxCount) →
  (List.range (maxCount+1)).reverse.flatMap (fun n => groups.filter (fun r => r.2 == n)) =
    groups.mergeSort (fun a b => decide (b.2 ≤ a.2))

-- Benchmark query: Analytics.H4
/--
Gathering nullable values from selected contiguous runs yields the same ordered values
as applying the row mask.

An AVG input path can traverse known passing runs and preserve the input order used by
floating-point accumulation.
-/
def OrderedNullableValuesFromSelectedRuns.Goal : Prop := ∀ (runs : List (Nat × List (Option Int))),
  (runs.filter (fun r => r.1 < 4)).flatMap (fun r => r.2.filterMap id) =
    (runs.flatMap (fun (id3,vs) => vs.map (fun v => (id3,v)))).filterMap
      (fun (k,v) => if k < 4 then v else none)

-- Benchmark query: Analytics.H5
/--
For a positive tile size, flattening the tiles reconstructs the exact Cartesian product
in its original order, with every tile within the size cap.

A join can schedule bounded batches of matching pairs even when one key produces a large
cross product.
-/
def BoundedCartesianProductTiles.Goal : Prop := ∀ (xs ys : List Int) cap, 0 < cap →
  let pairs := xs.flatMap (fun x => ys.map (fun y => (x,y)))
  let tiles := (List.range ((pairs.length+cap-1)/cap)).map (fun t =>
    (List.range (min cap (pairs.length-t*cap))).filterMap (fun j => do
      let p := t*cap+j
      pure (← xs[p/ys.length]?, ← ys[p%ys.length]?)))
  tiles.flatten = pairs ∧ (∀ t ∈ tiles, t.length ≤ cap)

-- Benchmark query: Analytics.L2
/--
Restricting forum members to message-bearing keys and using the message multiplicities
preserves the modeled join totals.

The executor can drop irrelevant memberships before probing the person side and carry
weights for repeated messages.
-/
def ForumMembershipSemijoinWeights.Goal : Prop := ∀ (members : List (Nat × Nat)) (forums : List Nat) (ageMatches : Nat → Nat),
  let weight := fun (r : Nat × Nat) => forums.count r.1 * ageMatches r.2
  ((members.filter (fun r => forums.eraseDups.contains r.1)).map weight).sum = (members.map weight).sum

-- Benchmark query: Analytics.L3
/--
Grouping qualifying replies by parent preserves their count and score sum when the
totals are attributed to the parent's year.

The executor can probe parent messages once per group rather than once per reply.
-/
def ParentReplyCountAndScoreTotals.Goal : Prop := ∀ (children : List (Option Nat × Int)) (parents : List Nat),
  let replies := fun p => (forKey children (some p)).filter (0 < ·)
  let joined := parents.flatMap replies
  (joined.length,joined.sum) =
    (((frequency parents).map (fun (p,n) => n*(replies p).length)).sum,
     total (frequency parents) (fun (p,n) => (n:Int)*(replies p).sum))

-- Benchmark query: Analytics.L4
/--
Gathering scores at the intersection of valid-parent and qualifying-year positions
preserves the filtered list in input order.

A columnar scan can combine validity and year masks before reading the selected
payloads.
-/
def NullableParentAndYearPositionGather.Goal : Prop := ∀ (rs : List (Nat × Option Nat × Option Int)),
  let indices := (rs.zipIdx.filter (fun (r,_) => r.1 == 2012 && r.2.1.isSome)).map Prod.snd
  valuesAt (rs.map (fun r => r.2.2)) indices =
    (rs.filter (fun (y,parent,_) => y == 2012 && parent != none)).map (fun r => r.2.2)

-- Benchmark query: Analytics.L5
/--
Grouping by place before applying the country lookup preserves the count of rows,
nullable age sum, and count of non-NULL ages.

The executor can resolve the country once per place and merge the three aggregate
states.
-/
def PlaceGroupedCountryAgeTotals.Goal : Prop := ∀ (edgeAges : List (Nat × Option Int)) (country : Nat → Nat) k,
  let places := ((edgeAges.map Prod.fst).eraseDups).filter (fun p => country p == k)
  let ages := (edgeAges.filter (fun r => country r.1 == k)).map Prod.snd
  (ages.length,nsum ages,(ages.filterMap id).length) =
    ((places.map (fun p => (forKey edgeAges p).length)).sum,nsum (places.map (fun p => nsum (forKey edgeAges p))),
     (places.map (fun p => ((forKey edgeAges p).filterMap id).length)).sum)

-- Benchmark query: Analytics.B3
/--
Summing the subtotals of physical keys with the selected dimension labels preserves the
nullable sum of the corresponding sales rows.

The executor can aggregate by product and store IDs before looking up category and
region for the rollup.
-/
def PhysicalKeySubtotalRollup.Goal : Prop := ∀ (sales : List ((Nat × Nat) × Option Int)) (labels : Nat × Nat → Nat × Nat) k,
  let rawKeys := (sales.map Prod.fst).eraseDups.filter (fun p => labels p == k)
  nsum (rawKeys.map (fun p => nsum (forKey sales p))) =
    nsum ((sales.filter (fun r => labels r.1 == k)).map Prod.snd)

-- Benchmark query: Analytics.B4
/--
Combining the row selections for the substring and flag branches preserves the modeled
OR filter and includes a row satisfying both branches once.

A scan can test the substring on dictionary entries, combine the resulting row IDs with
the country selection, and avoid repeated string checks.
-/
def DictionarySubstringOrFlagSelection.Goal : Prop := ∀ (rows : List (Bool × String)),
  let dict := (rows.map Prod.snd).eraseDups.filter (·.contains "data")
  (rows.zipIdx.filter (fun (r,_) => r.1 || dict.contains r.2)).map Prod.snd =
    (rows.zipIdx.filter (fun (r,_) => r.1 || r.2.contains "data")).map Prod.snd

-- Benchmark query: Analytics.B5
/--
Using the eligible suffix of a sorted price list preserves matching key counts,
including duplicate matches.

A price-index lookup can skip the ineligible prefix and pass the remaining key
multiplicities to the join.
-/
def SortedPriceSuffixKeyCounts.Goal : Prop := ∀ (items : List (Int × Nat)) k, (items.map Prod.fst).Pairwise (· ≤ ·) →
  ((items.dropWhile (fun r => r.1 ≤ 100)).map Prod.snd).count k =
    countWhere items (fun r => 100 < r.1 && r.2 == k)

end TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins

-- C++ references to this model or its proof.
-- executor/detail/benchmark_rematch.hpp
