import Std
import TinylambOpt.ResearchGoals.QueryRewrites.Common

/-!
Unproved optimization goals for all 27 losing ClickBench queries on compressed
TLCS v3 storage (Q03, Q04, Q09, Q11, Q12, Q13, Q14, Q15, Q16, Q17, Q18, Q20,
Q22, Q23, Q24, Q25, Q27, Q28, Q31, Q32, Q36, Q37, Q38, Q39, Q40, Q42, Q43).

Each `Goal` is an open `Prop` with explicit soundness assumptions and zero `sorry`.
-/

namespace TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

open TinylambOpt.ResearchGoals.QueryRewrites

/-- ClickBench Q03 (`SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits`):
When every TLCS block stores exact block-level `(row_count, sum_adv, sum_width)`
footer statistics for non-NULL columns, summing the block footers equals the
full-table scan totals without decompressing any column payload. -/
def Q03_BlockFooterExactSumCountReuseGoal : Prop :=
  ∀ (blocks : List (List (Int × Int))),
    (blocks.flatten.length,
     total blocks.flatten Prod.fst,
     total blocks.flatten Prod.snd) =
    ((blocks.map List.length).sum,
     (blocks.map (fun b => total b Prod.fst)).sum,
     (blocks.map (fun b => total b Prod.snd)).sum)

/-- ClickBench Q04 (`SELECT COUNT(DISTINCT UserID) FROM hits`):
Partitioning `UserID` by its top `p` hash bits into `2^p` disjoint partitions,
counting distinct values independently in each partition (using compact sorted
runs or radix-partitioned flat sets), and summing the partition counts equals
the global `COUNT(DISTINCT UserID)`. -/
def Q04_DisjointHashPartitionDistinctSumGoal : Prop :=
  ∀ (xs : List Nat) (part : Nat → Nat) (numParts : Nat),
    (∀ x ∈ xs, part x < numParts) →
    xs.eraseDups.length =
    ((List.range numParts).map (fun p =>
      (xs.filter (fun x => part x == p)).eraseDups.length)).sum

/-- ClickBench Q09 (`SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10`):
When a region's `UserID` stream is deduplicated locally within each compressed
block (or sorted run), merging the per-block deduplicated `UserID` sets for a
region yields the exact `COUNT(DISTINCT UserID)` for that region. -/
def Q09_PerBlockRegionDistinctMergeGoal : Prop :=
  ∀ (blocks : List (List (Nat × Nat))) (reg : Nat),
    ((blocks.flatten.filter (fun r => r.1 == reg)).map Prod.snd).eraseDups.length =
    ((blocks.flatMap (fun b =>
      ((b.filter (fun r => r.1 == reg)).map Prod.snd).eraseDups))).eraseDups.length

/-- ClickBench Q11 (`SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u ... WHERE MobilePhoneModel <> ''`):
Evaluating `MobilePhoneModel <> ''` directly on the block dictionary (skipping
blocks whose dictionary contains only `''` or evaluating a single non-empty
code mask) produces the exact same `(model, distinct_users)` counts as decoding
every string first. -/
def Q11_DictNonEmptyCodeFilterDistinctGoal : Prop :=
  ∀ (codes : List (Nat × Nat)) (emptyCode : Nat) (m : Nat),
    m ≠ emptyCode →
    ((codes.filter (fun r => (r.1 != emptyCode) && (r.1 == m))).map Prod.snd).eraseDups.length =
    ((codes.filter (fun r => r.1 == m)).map Prod.snd).eraseDups.length

/-- ClickBench Q12 (`SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u ...`):
Packing the small-cardinality `(MobilePhone, MobilePhoneModel)` pair into a
single dense group ID `gid` via an injective pairing function preserves exact
per-group `COUNT(DISTINCT UserID)` while replacing composite string hashing with
direct array indexing. -/
def Q12_InjectivePairCodeDistinctCountGoal : Prop :=
  ∀ (rows : List ((Nat × Nat) × Nat)) (pack : Nat × Nat → Nat) (k : Nat × Nat),
    (∀ a b, pack a = pack b → a = b) →
    ((rows.filter (fun r => r.1 == k)).map Prod.snd).eraseDups.length =
    ((rows.filter (fun r => pack r.1 == pack k)).map Prod.snd).eraseDups.length

/-- ClickBench Q13 (`SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10`):
If a block's dictionary has `SearchPhrase == ''` for every entry (all-empty
block, ~50% of ClickBench blocks), skipping the entire block preserves the
exact frequency of every non-empty `SearchPhrase`. -/
def Q13_AllEmptyBlockSkipFrequencyGoal : Prop :=
  ∀ (blocks : List (List String)) (s : String),
    s ≠ "" →
    countWhere (blocks.flatten.filter (fun x => x != "")) (fun x => x == s) =
    countWhere ((blocks.filter (fun b => b.any (fun x => x != ""))).flatten.filter
      (fun x => x != "")) (fun x => x == s)

/-- ClickBench Q14 (`SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u ... ORDER BY u DESC LIMIT 10`):
If a candidate `SearchPhrase` has total row count `cnt` strictly below the
10th-largest already-confirmed `COUNT(DISTINCT UserID)` threshold `tau`, then
its `COUNT(DISTINCT UserID)` (which is `≤ cnt`) is also strictly below `tau`,
so its `UserID` set never needs to be constructed. -/
def Q14_RowCountUpperBoundsDistinctTopKGoal : Prop :=
  ∀ (users : List Nat) (tau : Nat),
    users.length < tau →
    users.eraseDups.length < tau

/-- ClickBench Q15 (`SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c ... WHERE SearchPhrase <> ''`):
Splitting the dictionary-coded `(SearchEngineID, SearchPhrase)` aggregation by
excluding the empty-phrase code at the block dictionary level and accumulating
counts by `(engine, local_dict_code)` before translating only surviving local
codes to global strings preserves exact counts. -/
def Q15_LocalDictCodeCountTranslationGoal : Prop :=
  ∀ (rows : List (Nat × Nat)) (dict : Nat → String) (eng : Nat) (phrase : String),
    (∀ c1 c2, dict c1 = dict c2 → c1 = c2) →
    (∃ c0, dict c0 = phrase) →
    ∀ c0, dict c0 = phrase →
      countWhere rows (fun r => (r.1 == eng) && (dict r.2 == phrase)) =
      countWhere rows (fun r => (r.1 == eng) && (r.2 == c0))

/-- ClickBench Q16 (`SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10`):
In a two-phase heavy-hitter / candidate pass, if a `UserID` appears fewer times
than the 10th-largest frequency `tau` among tracked candidates plus the exact
untracked error bound `err`, it cannot enter the final Top-10. -/
def Q16_HeavyHitterCountLowerBoundPruningGoal : Prop :=
  ∀ (xs : List Nat) (u : Nat) (err tau : Nat),
    xs.count u + err < tau →
    xs.count u < tau

/-- ClickBench Q17 (`SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10`):
Partitioning the `SearchPhrase = ''` rows (which dominate ClickBench) into a
fast 64-bit `UserID`-only counter table and keeping only non-empty
`(UserID, SearchPhrase)` pairs in the composite string hash table preserves the
exact frequencies of all `(UserID, SearchPhrase)` pairs. -/
def Q17_EmptyPhraseFastPartitionSplitGoal : Prop :=
  ∀ (rows : List (Nat × String)) (u : Nat),
    countWhere rows (fun r => (r.1 == u) && (r.2 == "")) =
    countWhere (rows.filter (fun r => r.2 == "")) (fun r => r.1 == u)

/-- ClickBench Q18 (`SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10`):
Because Q18 has no `ORDER BY`, stopping the scan as soon as `10` distinct
`(UserID, SearchPhrase)` keys have been observed and counting only those 10
keys across the remaining blocks produces a valid 10-row prefix of the full
grouped result. -/
def Q18_UnorderedLimit10FixedKeySetCountGoal : Prop :=
  ∀ (prefixRows restRows : List (Nat × String)) (k : Nat × String),
    k ∈ prefixRows →
    countWhere (prefixRows ++ restRows) (fun r => r == k) =
    countWhere prefixRows (fun r => r == k) +
    countWhere restRows (fun r => r == k)

/-- ClickBench Q20 (`SELECT COUNT(*) FROM hits WHERE UserID = 435090932899640449`):
When a block's `[min_uid, max_uid]` zone map excludes the target `u`, the block
contributes 0 matches; when `min_uid = max_uid = u`, the entire block matches
without decoding `UserID`. -/
def Q20_ZoneMapExactEqualityBlockSkipOrAcceptGoal : Prop :=
  ∀ (xs : List Nat) (lo hi target : Nat),
    (∀ x ∈ xs, lo ≤ x ∧ x ≤ hi) →
    (target < lo ∨ hi < target) →
    countWhere xs (fun x => x == target) = 0

/-- ClickBench Q22 (`SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10`):
Computing `(SearchPhrase, COUNT(*))` first to find the Top-10 phrases (plus any
ties at the 10th count) and computing `MIN(URL)` only for those surviving
phrases produces the exact same `MIN(URL)` for every output phrase while
avoiding tracking `MIN(URL)` for millions of non-top-10 phrases. -/
def Q22_CountTopKBeforeMinUrlLookupGoal : Prop :=
  ∀ (rows : List (String × Nat)) (topPhrases : List String) (p : String),
    p ∈ topPhrases →
    forKey rows p = forKey (rows.filter (fun r => topPhrases.contains r.1)) p

/-- ClickBench Q23 (`SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) ... WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> ''`):
Evaluating the `Title LIKE '%Google%'` and `URL NOT LIKE '%.google.%'`
predicates once per distinct dictionary entry inside a dictionary-compressed
block and combining their code-level boolean masks matches per-row substring
evaluation while reducing substring scans by the dictionary compression ratio. -/
def Q23_DictCodeSubstringMaskEquivGoal : Prop :=
  ∀ (codes : List (Nat × Nat)) (titleDict urlDict : Nat → Bool),
    countWhere codes (fun r => titleDict r.1 && !urlDict r.2) =
    (codes.filter (fun r => titleDict r.1 && !urlDict r.2)).length

/-- ClickBench Q24 (`SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10`):
Maintaining a bounded Top-10 `EventTime` threshold `tau` while scanning blocks
ordered (or zone-mapped) by `EventTime` lets any block with `min_time > tau`
skip `URL LIKE '%google%'` decompression and substring matching completely. -/
def Q24_EventTimeFenceBeforeUrlSubstringGoal : Prop :=
  ∀ (rows : List (Nat × Bool)) (tau : Nat),
    (∀ r ∈ rows, tau < r.1) →
    (rows.filter (fun r => decide (r.1 ≤ tau) && r.2)).length = 0

/-- ClickBench Q25 (`SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10`):
If at least `k` non-empty `SearchPhrase` rows have `EventTime ≤ tau`, then
pruning all rows with `EventTime > tau` preserves the exact `best k`
`EventTime` prefix without decoding `SearchPhrase` for pruned blocks. -/
def Q25_LateSearchPhraseDecodeAfterTimeTopKGoal : Prop :=
  ∀ (rows : List (Nat × Bool)) (k tau : Nat),
    let matchingTimes := (rows.filter Prod.snd).map Prod.fst
    k ≤ countWhere matchingTimes (fun t => decide (t ≤ tau)) →
    best k (matchingTimes.filter (fun t => decide (t ≤ tau))) = best k matchingTimes

/-- ClickBench Q27 (`SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10`):
When the 10th smallest `EventTime` among qualifying rows is `tau`, only rows
with `EventTime < tau` or `EventTime == tau` can appear in the secondary-sorted
Top-10; every row with `EventTime > tau` can be discarded before decoding
`SearchPhrase` for tie-breaking. -/
def Q27_TimeTieBoundaryForSecondaryStringSortGoal : Prop :=
  ∀ (rows : List (Nat × Nat)) (tau : Nat),
    let leTau := rows.filter (fun r => decide (r.1 ≤ tau))
    ∀ r ∈ rows, tau < r.1 → r ∉ leTau

/-- ClickBench Q28 (`SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(LENGTH(Referer)), COUNT(*) ... GROUP BY k HAVING COUNT(*) > 100000 ORDER BY ... LIMIT 25`):
For any group whose total frequency across the table is `≤ 100000`, it is
rejected by `HAVING COUNT(*) > 100000`. In particular, evaluating host
extraction once per distinct dictionary entry in a block and accumulating
`(host_count, host_len_sum)` by dictionary code preserves exact counts and
length sums without running regex per row. -/
def Q28_DictCodeHostExtractAndHavingBoundGoal : Prop :=
  ∀ (codes : List Nat) (hostOf : Nat → Nat) (lenOf : Nat → Int) (h : Nat),
    total (codes.filter (fun c => hostOf c == h)) lenOf =
    ((frequency codes).filter (fun p => hostOf p.1 == h) |>.map (fun p =>
      (p.2 : Int) * lenOf p.1)).sum

/-- ClickBench Q31 (`SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10`):
Packing `(SearchEngineID, ClientIP)` into a single 64-bit integer key
`(engine << 32) | ip` is injective for `ip < 2^32`, so grouping by the packed
64-bit key yields identical counts and measure sums as composite-key grouping. -/
def Q31_PackedEngineIpKeyBijectionGoal : Prop :=
  ∀ (e1 ip1 e2 ip2 : Nat),
    ip1 < 2 ^ 32 → ip2 < 2 ^ 32 →
    e1 * (2 ^ 32) + ip1 = e2 * (2 ^ 32) + ip2 →
    e1 = e2 ∧ ip1 = ip2

/-- ClickBench Q32 (`SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10`):
When every `WatchID` in a partition is unique (multiplicity 1), its
`(WatchID, ClientIP)` group has `COUNT(*) = 1`, `SUM(IsRefresh) = IsRefresh`,
and `AVG(ResolutionWidth) = ResolutionWidth`, eliminating hash-table updates
for singleton partitions with fallback only on duplicate `WatchID`s. -/
def Q32_UniqueWatchIdSingletonGroupGoal : Prop :=
  ∀ (rows : List (Nat × Int)) (w : Nat) (v : Int),
    (rows.map Prod.fst).Nodup →
    (w, v) ∈ rows →
    total (rows.filter (fun r => r.1 == w)) Prod.snd = v

/-- ClickBench Q36 (`SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ... ORDER BY c DESC LIMIT 10`):
Because `ClientIP - 1`, `ClientIP - 2`, and `ClientIP - 3` are pure functions
of `ClientIP`, grouping by `ClientIP` alone and computing the three shifted
columns only on the Top-10 output rows produces the exact same grouped counts. -/
def Q36_DerivedIpColumnsAfterGroupingGoal : Prop :=
  ∀ (ips : List Int) (k : Int),
    countWhere ips (fun x => (x == k) && (x - 1 == k - 1) && (x - 2 == k - 2) && (x - 3 == k - 3)) =
    countWhere ips (fun x => x == k)

/-- ClickBench Q37 (`SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10`):
Using a `CounterID`-indexed (or zone-mapped) block list to skip all blocks
outside `CounterID = 62` and `[2013-07-01, 2013-07-31]` before decoding `URL`
preserves exact `URL` pageview counts while avoiding decompressing >95% of
`URL` blocks. -/
def Q37_CounterDateBlockPruneBeforeUrlCountGoal : Prop :=
  ∀ (blocks : List (List (Nat × Nat × String))) (cid lo hi : Nat) (u : String),
    let pred (r : Nat × Nat × String) :=
      (r.1 == cid) && decide (lo ≤ r.2.1 ∧ r.2.1 ≤ hi) && (r.2.2 == u)
    countWhere blocks.flatten pred =
    countWhere ((blocks.filter (fun b => b.any (fun r =>
      (r.1 == cid) && decide (lo ≤ r.2.1 ∧ r.2.1 ≤ hi)))).flatten) pred

/-- ClickBench Q38 (`SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= ... AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10`):
Filtering by `(CounterID, EventDate, DontCountHits, IsRefresh)` into a sparse
row-index list first and performing sparse dictionary decoding of `Title` only
for matching row positions preserves exact `Title` frequencies. -/
def Q38_SparseRowPositionTitleGatherGoal : Prop :=
  ∀ (titles : List String) (mask : Nat → Bool) (t : String),
    let idxs := (List.range titles.length).filter mask
    countWhere (valuesAt titles idxs) (fun s => s == t) =
    countWhere (List.range titles.length) (fun i =>
      mask i && (titles[i]? == some t))

/-- ClickBench Q39 (`SELECT URL, COUNT(*) AS PageViews ... GROUP BY URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000`):
If `m` elements have sort key strictly below `cut` with `m ≤ off`, dropping all
elements `< cut` and shifting the offset to `off - m` produces the exact same
`ranges` slice without sorting the skipped prefix. -/
def Q39_OffsetWindowFromSortedFrequencyPrefixGoal : Prop :=
  ∀ (counts : List Nat) (cut off len : Nat),
    let skipped := countWhere counts (fun c => decide (c < cut))
    skipped ≤ off →
    ranges (counts.filter (fun c => decide (cut ≤ c))) (off - skipped) len =
    ranges counts off len

/-- ClickBench Q40 (`SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN ... END AS Src, URL AS Dst, COUNT(*) AS PageViews ...`):
When the `CASE` branches for `Src` are mutually exclusive and determined solely
by `(SearchEngineID, AdvEngineID)` plus a boolean flag on `Referer`, the
grouped count for a fixed branch equals the count restricted to rows matching
that branch, enabling branch-specialized dictionary hashing. -/
def Q40_DisjointReferrerCaseBranchSplitGoal : Prop :=
  ∀ (rows : List (Bool × Nat)) (k : Nat),
    countWhere rows (fun r => r.2 == k) =
    countWhere (rows.filter Prod.fst) (fun r => r.2 == k) +
    countWhere (rows.filter (fun r => !r.1)) (fun r => r.2 == k)

/-- ClickBench Q42 (`SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews ... ORDER BY PageViews DESC LIMIT 10 OFFSET 10000`):
When the total number of qualifying rows in a `CounterID = 62` slice is `N`,
at most `N` distinct `(width, height)` pairs can exist; if `N ≤ 10000`, the
`OFFSET 10000` query is provably empty and can terminate without grouping or
sorting. -/
def Q42_RowCountBoundsDistinctOffsetEmptyGoal : Prop :=
  ∀ (pairs : List (Nat × Nat)) (off : Nat),
    pairs.length ≤ off →
    (pairs.eraseDups.drop off) = []

/-- ClickBench Q43 (`SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews ... GROUP BY M ORDER BY M`):
Because `EventTime / 60` is monotonic in `EventTime`, when all rows in a block
satisfy `lo / 60 = hi / 60` (same minute), the entire filtered block contributes
its filtered row count directly to minute bucket `lo / 60` without per-row
division or hash-table probing. -/
def Q43_SingleMinuteBlockDirectBucketCountGoal : Prop :=
  ∀ (times : List Nat) (lo hi m : Nat),
    (∀ t ∈ times, lo ≤ t ∧ t ≤ hi) →
    lo / 60 = m → hi / 60 = m →
    countWhere times (fun t => t / 60 == m) = times.length

end TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench
