import TinylambOpt.ResearchGoals.QueryRewrites.Common

/-!
These goals model dictionary-based predicates, grouped counts, and ordered limits over
event rows.

They establish when SQL execution can skip decoding, reuse predicate results, or bound
the candidates needed for ranking.
-/

namespace TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

-- Benchmark matchup: Click.Q12DuckDB
/--
Local UserID deduplication within each device group preserves the downstream distinct
count. The executor can insert fewer keys into global distinct state.
-/
def LocalDedupPreservesGlobalDistinct.Goal : Prop := ∀ (parts : List (List Nat)),
  (parts.flatMap List.eraseDups).eraseDups = parts.flatten.eraseDups

-- Benchmark matchup: Click.Q14ClickHouse
/--
Disjoint UserID partitions let local distinct counts be added for the same search
phrase. The executor can merge counts instead of transferring all IDs.
-/
def ModuloPartitionDistinctCount.Goal : Prop := ∀ (users : List Nat) n, 0 < n →
  ((List.range n).map (fun i => (users.filter (fun u => u%n == i)).eraseDups.length)).sum = users.eraseDups.length

-- Benchmark matchup: Click.Q18ClickHouse
/--
After fixing the first ten groups by encounter order, later rows can update only those
groups. The executor can cap the modeled group-count state.
-/
def FixedCandidateGroupCounts.Goal : Prop := ∀ (xs : List (Nat × Nat)),
  let keep := xs.eraseDups.take 10
  keep.map (fun k => (xs.filter keep.contains).count k) = keep.map xs.count

-- Benchmark matchup: Click.Q20DuckDB
/--
A block with UserID bounds that exclude the search target contributes no matches. The
scan can omit that block's decode.
-/
def BlockExtremaPreservePointCount.Goal : Prop := ∀ (blocks : List (List Int)) k,
  (blocks.filter (fun b => (b.min?).any (· ≤ k) && (b.max?).any (k ≤ ·))).flatten.count k = blocks.flatten.count k

-- Benchmark matchup: Click.Q20ClickHouse
/--
The target ID's encoded frequency determines the size of the matching constant output.
The executor can generate those rows without loading their original payloads.
-/
def PointFilterFromDictionaryFrequency.Goal : Prop := ∀ (xs : List Int) k,
  List.replicate (xs.count k) k = xs.filter (· == k)

-- Benchmark matchup: Click.Q21DuckDB
/--
Weighting each qualifying URL dictionary entry by its use count preserves the row-level
COUNT. The executor can run the substring predicate once per distinct URL.
-/
def SubstringCountFromDictionaryFrequency.Goal : Prop := ∀ (urls : List String),
  ((frequency urls).filter (fun r => r.1.contains "google") |>.map Prod.snd).sum =
    countWhere urls (·.contains "google")

-- Benchmark matchup: Click.Q21ClickHouse
/--
A URL satisfying the full pattern must contain the required substring. The executor can
use that containment check to omit work on certified nonmatching regions.
-/
def RequiredSubstringBlockPruning.Goal : Prop := ∀ (blocks : List (List String)),
  countWhere ((blocks.filter (fun b => b.any (·.contains "goo"))).flatten) (·.contains "google") =
    countWhere blocks.flatten (·.contains "google")

-- Benchmark matchup: Click.Q22DuckDB
/--
Combining local minima for the same search phrase preserves MIN(URL). The executor can
merge one candidate string per local group.
-/
def GlobalMinimumFromLocalMinima.Goal : Prop := ∀ (shards : List (List String)),
  (shards.filterMap List.min?).min? = shards.flatten.min?

-- Benchmark matchup: Click.Q22ClickHouse
/--
A URL's gram predicate answer can be reused across its occurrences while retaining the
counts by search phrase. The executor can avoid repeated predicate evaluations for that
URL.
-/
def PhraseSubstringCountFromPairFrequencies.Goal : Prop := ∀ (rows : List (String × String)) phrase, phrase ≠ "" →
  ((frequency rows).filter (fun ((u,p),_) => u.contains "google" && p == phrase) |>.map Prod.snd).sum =
    countWhere rows (fun (u,p) => p == phrase && u.contains "google")

-- Benchmark matchup: Click.Q23DuckDB
/--
Conjoining the title and nonempty-phrase tests with the negative URL predicate preserves
the modeled filter. The executor can evaluate the URL predicate only on rows surviving
the first tests.
-/
def SplitTitleAndUrlFilter.Goal : Prop := ∀ (xs : List (String × String × String)),
  ((xs.filter (fun (p,t,_) => p != "" && t.contains "Google")).filter (fun (_,_,u) => !u.contains ".google.")) =
  xs.filter (fun (p,t,u) => t.contains "Google" && !u.contains ".google." && p != "")

-- Benchmark matchup: Click.Q24DuckDB
/--
The top ten row IDs selected using the full ranking key determine the same output when
payload columns are gathered afterward. The executor can defer wide-row materialization.
-/
def TopRowsBeforePayloadGather.Goal : Prop := ∀ (ids : List Nat) (key : Nat → Nat) (payload : Nat → String),
  let cmp := fun (a b : Nat) => decide (key a ≤ key b)
  ((ids.mergeSort cmp).take 10).map payload =
    (((ids.map (fun i => (i,payload i))).mergeSort (fun a b => cmp a.1 b.1)).take 10).map Prod.snd

-- Benchmark matchup: Click.Q28ClickHouse
/--
For the modeled URL layout, endpoint differences and a nonempty count give the length
aggregate for an eligible CounterID run. The executor can use metadata instead of
decoding URL bodies.
-/
def NonZeroLengthSumAndCount.Goal : Prop := ∀ (lengths : List Nat),
  ((lengths.filter (· != 0)).sum, (lengths.filter (· != 0)).length) =
    (lengths.sum, lengths.length - lengths.count 0)

-- Benchmark matchup: Click.Q31ClickHouse
/--
Partitioning by SearchEngineID and then grouping by ClientIP preserves the compound
group keys. An inner hash table can use just the IP component.
-/
def PairCountAfterLeadingKeyPartition.Goal : Prop := ∀ (pairs : List (Nat × Nat)) e ip,
  ((pairs.filter (fun p => p.1 == e)).map Prod.snd).count ip = pairs.count (e,ip)

-- Benchmark matchup: Click.Q32DuckDB
/--
A partition with unique WatchIDs has singleton groups. The executor can construct each
aggregate directly on a partition whose uniqueness is known.
-/
def UniqueLeadingKeyImpliesSingletonGroups.Goal : Prop := ∀ (xs : List (Nat × Nat)), (xs.map Prod.fst).Nodup →
  ∀ k ∈ xs, xs.count k = 1

-- Benchmark matchup: Click.Q32ClickHouse
/--
Gathering WatchID and IP at positions passing the nonempty phrase predicate preserves
the selected key rows. The executor can defer reading these keys until after filtering.
-/
def SelectionBeforePairGather.Goal : Prop := ∀ (ids : List Nat) (keep : Nat → Bool) (pair : Nat → Nat × Nat),
  (ids.filter keep).map pair = (ids.map (fun i => (keep i,pair i))).filterMap (fun (b,k) => if b then some k else none)

-- Benchmark matchup: Click.Q37DuckDB
/--
A group's current URL count plus all unread rows bounds its final count. The executor
can discard groups that cannot reach the top-ten cutoff.
-/
def PageFrequencyUpperBoundExcludesTopK.Goal : Prop := ∀ (seen : List Nat) (remaining : List (List Nat)) (keys : List Nat) i threshold,
  keys.Nodup →
  let upper := seen.count i + (remaining.map (fun page => if page.contains i then page.length else 0)).sum
  10 ≤ countWhere keys (fun k => threshold ≤ seen.count k) → upper < threshold →
  let allRows := seen ++ remaining.flatten
  10 ≤ countWhere keys (fun k => allRows.count i < allRows.count k)

-- Benchmark matchup: Click.Q37ClickHouse
/--
Counting local URL codes for rows passing the flags and date filters preserves the
global counts once equivalent dictionary entries are merged. The executor can resolve
each qualifying code after local accumulation.
-/
def FilteredLocalDictionaryHistogramMerge.Goal : Prop := ∀ (pages : List (List (Bool × Nat) × (Nat → String))) key,
  let pageCount := fun (p : List (Bool × Nat) × (Nat → String)) =>
    let hist := frequency ((p.1.filter Prod.fst).map Prod.snd)
    ((hist.filter (fun r => p.2 r.1 == key)).map Prod.snd).sum
  (pages.map pageCount).sum = countWhere
    (pages.flatMap (fun (rs,dict) => (rs.filter Prod.fst).map (fun r => dict r.2))) (· == key)

-- Benchmark matchup: Click.Q38DuckDB
/--
Counts for identical titles in separate dictionaries must be added before ranking. The
executor can feed per-dictionary subtotals to the global aggregation.
-/
def MergeTitleFrequencyBeforeRanking.Goal : Prop := ∀ (pages : List (List String)) title,
  (pages.map (fun p => p.count title)).sum = pages.flatten.count title

-- Benchmark matchup: Click.Q38ClickHouse
/--
Intersecting the nonempty-title and flag masks and counting the result gives the number
of passing rows. The executor can combine both predicates in a bitmap scan.
-/
def NonemptyTitleAndFlagCount.Goal : Prop := ∀ (flags : List Bool) (titles : List String),
  countWhere (flags.zip titles) (fun (b,t) => b && t != "") =
    ((flags.zip titles).filterMap (fun (b,t) => if b then some t else none) |>.filter (· != "")).length

-- Benchmark matchup: Click.Q39DuckDB
/--
Keeping the first 1,010 groups in each partition suffices for an OFFSET 1000 LIMIT 10
merge under the same ranking order. The executor can bound the number of groups it
merges.
-/
def OffsetTopKFromPartitionPrefixes.Goal : Prop := ∀ (shards : List (List Nat)), ranges (shards.flatMap (best 1010)) 1000 10 =
  ranges shards.flatten 1000 10

-- Benchmark matchup: Click.Q40DuckDB
/--
Only the branch with both search and ad IDs equal to zero uses the original referrer.
The executor can defer retrieving Referer to that branch.
-/
def ConditionalReferrerGather.Goal : Prop := ∀ (rows : List (Nat × Nat × Nat)) (referrer : Nat → String),
  rows.map (fun (s,a,i) => if s == 0 && a == 0 then referrer i else "") =
    (rows.map (fun (s,a,i) => (s,a,referrer i))).map (fun (s,a,r) => if s == 0 && a == 0 then r else "")

-- Benchmark matchup: Click.Q42DuckDB
/--
If the number of distinct qualifying resolutions is no greater than the OFFSET, the
modeled result is empty. The executor can avoid sorting the groups in that case.
-/
def DistinctCardinalityMakesOffsetEmpty.Goal : Prop := ∀ (dims : List (Nat × Nat)) (rank : Nat × Nat → Nat),
  dims.eraseDups.length ≤ 10000 → ranges (dims.eraseDups.map rank) 10000 10 = []

-- Benchmark matchup: Click.Q43DuckDB
/--
Only minute keys that occur in the data affect the ordered result. The executor can
select the needed ten minute keys before counting their frequencies.
-/
def MinuteTopKBeforeFrequencyCount.Goal : Prop := ∀ (minutes : List Nat),
  let page := ranges minutes.eraseDups 1000 10
  page.map (fun m => (minutes.filter page.contains).count m) = page.map minutes.count

-- Benchmark query: Click.Q12
/--
Summing distinct counts for disjoint user ID partitions gives the distinct count for the
whole group.

A grouped DISTINCT can maintain per-partition bitmaps and add their population counts.
-/
def PartitionedUserBitmapDistinct.Goal : Prop := ∀ (users : List Nat) w, 0 < w →
  let pages := (users.map (· / w)).eraseDups
  (pages.map (fun p => ((List.range w).filter (fun bit => users.contains (p*w+bit))).length)).sum =
    users.eraseDups.length

-- Benchmark query: Click.Q14
/--
A candidate cannot attain a target distinct count when its current count plus all
remaining rows falls strictly below the target.

A top-k aggregate can retire a candidate's distinct state once the bound excludes it
from contention.
-/
def DistinctTopKPruningByRemainingRows.Goal : Prop := ∀ (seen rest : List Nat) (otherFinal : List Nat),
  10 ≤ countWhere otherFinal (seen.eraseDups.length + rest.length < ·) →
    10 ≤ countWhere otherFinal ((seen++rest).eraseDups.length < ·)

-- Benchmark query: Click.Q16
/--
If every omitted key has a count strictly below the candidate cutoff, no omitted key can
displace the top candidates.

A sampled candidate pass can use exact counts for its candidates and accept the result
only when the omitted-key bound certifies it.
-/
def CandidateTopKWithOmittedCountBound.Goal : Prop := ∀ (users candidates : List Nat) threshold,
  let keys := users.eraseDups
  let kept := keys.filter candidates.contains
  10 ≤ countWhere kept (fun k => threshold ≤ users.count k) →
  (∀ k ∈ keys, ¬ k ∈ candidates → users.count k < threshold) →
  let rank := fun (a b : Nat) => decide (users.count a > users.count b ∨
    (users.count a = users.count b ∧ a ≤ b))
  (kept.mergeSort rank).take 10 = (keys.mergeSort rank).take 10

-- Benchmark query: Click.Q18
/--
Summing the lengths of encoded runs for a key yields the same counts as processing their
expanded rows.

A grouped count can update the hash table once per consecutive user and phrase run.
-/
def RunLengthGroupCounts.Goal : Prop := ∀ (runs : List ((Nat × Nat) × Nat)) k,
  (forKey runs k).sum = (runs.flatMap (fun (key,n) => List.replicate n key)).count k

-- Benchmark query: Click.Q20
/--
On sorted keys, the distance between the equal range's endpoints is the count of the
requested key.

A scan can use two boundary searches on a certified sorted partition instead of testing
every ID.
-/
def SortedEqualRangeCount.Goal : Prop := ∀ (ids : List Int) target, ids.Pairwise (· ≤ ·) →
  (ids.takeWhile (· ≤ target)).length - (ids.takeWhile (· < target)).length = ids.count target

-- Benchmark query: Click.Q21
/--
The literal google occurs in two concatenated strings exactly when a chunk contains it
or it is found in the last five and first five characters around the boundary.

A substring filter can inspect chunks and a small boundary window without materializing
the full URL.
-/
def ChunkedSubstringBoundaryCarry.Goal : Prop := ∀ (a b : String),
  (a++b).contains "google" = (a.contains "google" || b.contains "google" ||
    ((a.takeEnd 5).toString ++ (b.take 5).toString).contains "google")

-- Benchmark query: Click.Q22
/--
Removing rows for keys below a supplied count cutoff leaves the minimum payload
unchanged for every key at or above the cutoff.

An aggregate can count phrases first and compute MIN(URL) only for keys that can still
qualify, including cutoff ties.
-/
def CountTopKBeforePayloadMinimum.Goal : Prop := ∀ (matched : List (Nat × String)) k cutoff,
  let phrases := matched.map Prod.fst
  cutoff ≤ phrases.count k →
  (forKey (matched.filter (fun r => cutoff ≤ phrases.count r.1)) k).min? = (forKey matched k).min?

-- Benchmark query: Click.Q23
/--
With stable dictionaries, evaluating a predicate on a pair of codes through a cached
lookup gives the same result as decoding and evaluating that pair.

The filter can reuse the predicate result across rows with the same title and URL codes.
-/
def DictionaryPairPredicateMemoization.Goal : Prop := ∀ (pairs : List (Nat × Nat)) (title url : Nat → String),
  let accepts := fun (k : Nat × Nat) => (title k.1).contains "Google" && !(url k.2).contains ".google."
  let allowed := pairs.eraseDups.filter accepts
  pairs.filter allowed.contains = pairs.filter accepts

-- Benchmark query: Click.Q24
/--
A page whose time lower bound is strictly beyond the current cutoff cannot contribute to
the modeled prefix.

A top-k scan can skip reading URL strings on those pages while preserving candidates
tied at the cutoff.
-/
def TimestampFenceBeforeSubstringRead.Goal : Prop := ∀ (previous page : List Nat) (passes : Nat → Bool) lower cutoff,
  10 ≤ countWhere previous (· ≤ cutoff) → cutoff < lower → (∀ k ∈ page, lower ≤ k) →
  best 10 (previous ++ page.filter passes) = best 10 previous

-- Benchmark query: Click.Q27
/--
The primary ordering determines a prefix through the last row tied at the cutoff, and
the secondary order resolves positions within that prefix.

A time-ordered scan can stop after the cutoff timestamp's entire tie group and sort the
necessary secondary keys.
-/
def TimestampTieBoundaryForSecondarySort.Goal : Prop := ∀ (rs : List (Nat × Nat)) t, (rs.map Prod.fst).Pairwise (· ≤ ·) →
  10 ≤ countWhere rs (fun r => r.1 ≤ t) →
  let cmp := fun (a b : Nat × Nat) => decide (a.1 < b.1 ∨ (a.1 = b.1 ∧ a.2 ≤ b.2))
  ((rs.takeWhile (fun r => r.1 ≤ t)).mergeSort cmp).take 10 = (rs.mergeSort cmp).take 10

-- Benchmark query: Click.Q28
/--
The number of nonempty URLs for a key cannot exceed the key's total row frequency.

An aggregate can reject keys that cannot exceed the HAVING threshold without examining
their URLs.
-/
def TotalFrequencyBoundsNonemptyHaving.Goal : Prop := ∀ (rows : List (Nat × Nat)) k,
  let lengths := forKey rows k
  100000 < (lengths.filter (· > 0)).length →
    100000 < (rows.map Prod.fst).count k

-- Benchmark query: Click.Q31
/--
The modeled upper and lower IP components reconstruct the original key, preserving group
identity.

A grouped aggregate can store lower-part arrays per upper prefix and reconstruct the
original key for its output.
-/
def SplitIpKeyReconstruction.Goal : Prop := ∀ (rows : List ((Nat × Nat) × Int)) engine ip,
  let split := rows.map (fun ((e,a),v) => ((e,a/256,a%256),v))
  forKey split (engine,ip/256,ip%256) = forKey rows (engine,ip)

-- Benchmark query: Click.Q32
/--
For a chosen representative, its frequency is the group size less the number of
exceptions. The frequency of any other value can be computed from the exceptions alone.

The executor can keep a compact common case and allocate auxiliary IP state only for
conflicting WatchIDs.
-/
def SingleValueGroupWithConflictFallback.Goal : Prop := ∀ (ips : List Nat) (first query : Nat),
  let exceptions := ips.filter (· != first)
  (if query == first then ips.length-exceptions.length else exceptions.count query) = ips.count query

-- Benchmark query: Click.Q36
/--
Grouping on a key together with deterministic functions of that key preserves the same
grouping as grouping on the key alone.

The executor can hash ClientIP alone and compute the derived IP columns when emitting
each group.
-/
def DerivedIpKeysAfterGrouping.Goal : Prop := ∀ (ips : List Int),
  let wide := fun (i : Int) => (i,i-1,i-2,i-3)
  frequency (ips.map wide) = (frequency ips).map (fun (i,n) => (wide i,n))

-- Benchmark query: Click.Q37
/--
A count over an inclusive date interval in a sorted posting list equals the distance
between the first date at or above the lower bound and the first date above the upper
bound.

The executor can count a CounterID's matching dates without scanning each posting in the
interval.
-/
def IntervalCountFromSortedPostings.Goal : Prop := ∀ (days : List Nat) lo hi, days.Pairwise (· ≤ ·) → lo ≤ hi →
  (days.takeWhile (· ≤ hi)).length - (days.takeWhile (· < lo)).length =
    countWhere days (fun d => lo ≤ d && d ≤ hi)

-- Benchmark query: Click.Q38
/--
Per-page count bounds can certify that an omitted title cannot reach the global
candidate cutoff.

An executor can reduce the number of titles entered into the global aggregation table
when the upper bound certifies the candidate set.
-/
def PageFrequencyBoundsForTitleCandidates.Goal : Prop := ∀ (pages : List (List String)) (candidates : List String) cap t,
  (∀ p ∈ pages, ∀ title ∈ p, ¬ title ∈ candidates → p.count title ≤ cap) →
  cap*pages.length < pages.flatten.count t → t ∈ candidates

-- Benchmark query: Click.Q39
/--
Counts of groups per frequency determine which frequency bucket contains an OFFSET in
count order.

The executor can skip complete buckets and apply the remaining offset using the required
order within one bucket.
-/
def OffsetSelectionByCountBuckets.Goal : Prop := ∀ (groups : List (Nat × Nat)) cutoff,
  let higher := countWhere groups (fun r => cutoff < r.2)
  let tied := groups.filter (fun r => r.2 == cutoff)
  let ranked := groups.mergeSort (fun a b => decide (b.2 ≤ a.2))
  higher ≤ 1000 → 1010 ≤ higher+tied.length →
    ((ranked.drop 1000).take 10) = ((tied.drop (1000-higher)).take 10)

-- Benchmark query: Click.Q40
/--
Groups from branches with disjoint complete keys cannot merge when the branch results
are combined.

The executor can aggregate the constant-referrer branch with a narrower state and
combine the completed rows afterward.
-/
def DisjointReferrerBranchKeys.Goal : Prop := ∀ (rows : List (Nat × Nat × String)) s a src,
  let key := fun (r : Nat × Nat × String) => (r.1,r.2.1,if r.1 == 0 && r.2.1 == 0 then r.2.2 else "")
  (rows.map key).count (s,a,src) =
    if s == 0 && a == 0 then rows.count (0,0,src)
    else if src == "" then (rows.map (fun r => (r.1,r.2.1))).count (s,a) else 0

-- Benchmark query: Click.Q42
/--
The number of distinct output pairs cannot exceed the count of qualifying input rows.

When this count is at most the OFFSET, the executor can return an empty page without
decoding the resolution pairs.
-/
def RowCountBoundsDistinctOffset.Goal : Prop := ∀ (hits : List Nat) (resolution : Nat → Nat × Nat),
  hits.length ≤ 10000 → ((hits.map resolution).eraseDups.drop 10000).isEmpty = true

-- Benchmark query: Click.Q43
/--
If both ends of a timestamp range truncate to the same minute, every timestamp within it
truncates to that minute.

The executor can compute the minute once for the certified block after applying the
filters.
-/
def MinuteTruncationFromBlockBounds.Goal : Prop := ∀ (seconds : List Int) lo hi,
  (∀ s ∈ seconds, lo ≤ s ∧ s ≤ hi) → lo/60 = hi/60 →
  seconds.map (· / 60) = List.replicate seconds.length (lo/60)

end TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

-- C++ references to this model or its proof.
-- executor/detail/benchmark_rematch.hpp
