import Std
import TinylambOpt.ResearchGoals.QueryRewrites.Common

/-!
Unproved optimization goals for the 12 losing Analytics-5 queries (JOB J1, J3;
STATS-CEB C2, C3; H2O H2, H4, H5; LDBC-SNB L2, L4; BigBench B3, B4, B5) and
the 2 losing Cloud/BI queries (Public BI MLB Q108; Iceberg SQL base).

Each `Goal` is an open `Prop` with explicit soundness assumptions and zero `sorry`.
-/

namespace TinylambOpt.ProposedGoals.BenchmarkRematch.AnalyticsAndCloudBI

open TinylambOpt.ResearchGoals.QueryRewrites

/-- JOB J1 (`title ⋈ kind_type ⋈ movie_companies ⋈ company_name ⋈ company_type`):
Pre-aggregating `movie_companies` (filtered by `company_name` and `company_type`
bitsets) into a dense `movie_id → multiplicity` array lets the final join with
filtered `title` compute `COUNT(*)` and `SUM(t.production_year)` by multiplying
each qualifying title's year by `mult(t.id)` without materializing joined rows. -/
def JOB_J1_CompanyMultiplicityWeightedTitleTotalsGoal : Prop :=
  ∀ (titles : List (Nat × Int)) (mcMovieIds : List Nat),
    (titles.flatMap (fun t =>
      (mcMovieIds.filter (fun m => m == t.1)).map (fun _ => t.2))).sum =
    (titles.map (fun t => (mcMovieIds.count t.1 : Int) * t.2)).sum

/-- JOB J3 (`cast_info ⋈ name ⋈ title` grouped by `n.gender`, `COUNT(*)`, `AVG(t.production_year)`):
Because `n.gender` has tiny domain (e.g. 3 codes including NULL), aggregating
`cast_info` by `movie_id` into per-gender counts first and multiplying each
title's non-NULL `production_year` by its per-gender count yields the exact
same `(count, sum_year)` per gender while scanning `title` only once. -/
def JOB_J3_GenderCodeCountWeightedYearAvgGoal : Prop :=
  ∀ (casts : List (Nat × Nat)) (yearOf : Nat → Option Int) (g : Nat),
    let matching := casts.filter (fun c => c.2 == g)
    (matching.filterMap (fun c => yearOf c.1)).sum =
    ((matching.map Prod.fst).eraseDups.filterMap (fun mid =>
      (yearOf mid).map (fun y => ((matching.map Prod.fst).count mid : Int) * y))).sum

/-- STATS-CEB C2 (`users u ⋈ posts p ⋈ comments c ⋈ users u2`, `GROUP BY u.id`):
When `u2` is a unique-key primary-key join that only filters `u2.reputation > ...`,
replacing the `u2` join with a bitset filter on `c.user_id`, counting comments
per `post_id` in a dense array, and summing `comment_count(p.id)` per
`p.owner_user_id` yields the exact `COUNT(*)` per `u.id`. -/
def STATS_C2_PostCommentFreqOwnerSumGoal : Prop :=
  ∀ (posts : List (Nat × Nat)) (comments : List Nat) (owner : Nat),
    let ownerPosts := posts.filter (fun p => p.1 == owner)
    (ownerPosts.flatMap (fun p =>
      comments.filter (fun cid => cid == p.2))).length =
    (ownerPosts.map (fun p => comments.count p.2)).sum

/-- STATS-CEB C3 (`posts p ⋈ postlinks l ⋈ posts r`, `GROUP BY l.link_type`, `COUNT(*)`, `AVG(p.score)`):
Filtering `r` into a dense valid-`related_post_id` bitset first and probing
`p.score` via a dense `post_id → score` array (instead of two general hash
joins) preserves exact per-`link_type` counts and score sums. -/
def STATS_C3_RelatedPostBitsetAndDenseScoreLookupGoal : Prop :=
  ∀ (links : List (Nat × Nat × Nat)) (validRelated : Nat → Bool)
    (scoreOf : Nat → Int) (lt : Nat),
    total (links.filter (fun l => (l.1 == lt) && validRelated l.2.2))
      (fun l => scoreOf l.2.1) =
    total ((links.filter (fun l => validRelated l.2.2)).filter (fun l => l.1 == lt))
      (fun l => scoreOf l.2.1)

/-- H2O H2 (`SELECT id1, id2, COUNT(*) AS n FROM x GROUP BY id1, id2 ORDER BY n DESC`):
When `id1` and `id2` are dictionary-coded with bounded domains `D1` and `D2`,
accumulating counts into a dense `D1 × D2` flat array (indexed by
`c1 * D2 + c2`) and sorting only the nonzero `(c1, c2, n)` slots produces the
exact same counts as hash-table grouping on strings. -/
def H2O_H2_BoundedDictGridDirectCountGoal : Prop :=
  ∀ (rows : List (Nat × Nat)) (d2 c1 c2 : Nat),
    (∀ r ∈ rows, r.2 < d2) →
    c2 < d2 →
    countWhere rows (fun r => (r.1 == c1) && (r.2 == c2)) =
    countWhere rows (fun r => r.1 * d2 + r.2 == c1 * d2 + c2)

/-- H2O H4 (`SELECT id1, id2, AVG(v1) AS a FROM x WHERE id3 < 4 GROUP BY id1, id2`):
Evaluating `id3 < 4` directly on bit-packed `id3` lanes to build a sparse row
selection vector and accumulating `(count, sum_v1)` into a dense `(id1, id2)`
array only for selected rows preserves exact group averages while avoiding
decoding `id1`, `id2`, and `v1` for rejected rows. -/
def H2O_H4_SparseSelectedRunsGroupAvgGoal : Prop :=
  ∀ (rows : List (Nat × Nat × Int)) (bound k : Nat),
    let sel := rows.filter (fun r => decide (r.1 < bound))
    (countWhere sel (fun r => r.2.1 == k),
     total (sel.filter (fun r => r.2.1 == k)) (fun r => r.2.2)) =
    (countWhere rows (fun r => decide (r.1 < bound) && (r.2.1 == k)),
     total (rows.filter (fun r => decide (r.1 < bound) && (r.2.1 == k)))
       (fun r => r.2.2))

/-- H2O H5 (`SELECT x.id1, SUM(x.v1) AS sx, SUM(y.v2) AS sy FROM x JOIN y ON x.id4 = y.id4 GROUP BY x.id1 ORDER BY sx DESC`):
Pre-aggregating `y` by `y.id4` into `(cnt_y, sum_y2)` and `x` by
`(x.id1, x.id4)` into `(cnt_x, sum_x1)` allows computing
`sx = ∑_{id4} sum_x1 * cnt_y` and `sy = ∑_{id4} cnt_x * sum_y2` without ever
materializing the many-to-many join product on `id4`. -/
def H2O_H5_FactorizedManyToManyJoinSumsGoal : Prop :=
  ∀ (xs : List (Nat × Int)) (ys : List (Nat × Int)) (k : Nat),
    let xk := xs.filter (fun r => r.1 == k)
    let yk := ys.filter (fun r => r.1 == k)
    (xk.flatMap (fun a => yk.map (fun b => a.2))).sum =
      total xk Prod.snd * (yk.length : Int) ∧
    (xk.flatMap (fun a => yk.map (fun b => b.2))).sum =
      (xk.length : Int) * total yk Prod.snd

/-- LDBC-SNB L2 (`forum ⋈ forum_member ⋈ person ⋈ message`, `GROUP BY f.id, m.creation_yr`):
Because `forum_member ⋈ person` only multiplies each `(f.id, m.creation_yr)`
message row by the count `memCount(f.id)` of qualifying members in forum `f`,
pre-aggregating `message` by `(forum_id, creation_yr)` into `(msg_cnt, msg_sum)`
and multiplying both by `memCount(f.id)` avoids the `forum_member × message`
Cartesian blowup. -/
def LDBC_L2_ForumMemberWeightFactoringGoal : Prop :=
  ∀ (msgs : List (Nat × Int)) (members : List Nat) (fid : Nat),
    let fMsgs := msgs.filter (fun m => m.1 == fid)
    let fMems := members.filter (fun m => m == fid)
    (fMsgs.flatMap (fun m => fMems.map (fun _ => m.2))).sum =
    (fMems.length : Int) * total fMsgs Prod.snd

/-- LDBC-SNB L4 (`message m JOIN forum f ON m.forum = f.id WHERE m.creation_yr = 2012 AND m.parent_id IS NOT NULL GROUP BY f.id`):
Filtering `message` by `creation_yr = 2012` (using block zone maps) and the
`parent_id` validity bitmap before gathering `m.forum` and `m.score` into a
dense `forum_id`-indexed `(count, sum)` array preserves exact group averages
while skipping `parent_id` value decompression completely. -/
def LDBC_L4_NullBitmapAndYearZoneFilterGoal : Prop :=
  ∀ (msgs : List (Nat × Option Nat × Nat × Int)) (yr fid : Nat),
    let pred (r : Nat × Option Nat × Nat × Int) :=
      (r.1 == yr) && r.2.1.isSome && (r.2.2.1 == fid)
    total (msgs.filter pred) (fun r => r.2.2.2) =
    total ((msgs.filter (fun r => (r.1 == yr) && r.2.1.isSome)).filter
      (fun r => r.2.2.1 == fid)) (fun r => r.2.2.2)

/-- BigBench B3 (`store_sales ⋈ store ⋈ item` grouped by `(s.region, i.category, ss.yr)`):
Aggregating `store_sales` first into a compact `(store_id, item_id, yr)` table
(or dense `(region_of store_id, cat_of item_id, yr)` array via direct lookup
tables) produces identical `(region, category, yr)` revenue sums without
copying `region` and `category` strings per fact row. -/
def BigBench_B3_DirectDimLookupGridSumGoal : Prop :=
  ∀ (sales : List (Nat × Nat × Int)) (regOf catOf : Nat → Nat) (rg ct : Nat),
    total (sales.filter (fun r => (regOf r.1 == rg) && (catOf r.2.1 == ct)))
      (fun r => r.2.2) =
    total (sales.filter (fun r => (regOf r.1, catOf r.2.1) == (rg, ct)))
      (fun r => r.2.2)

/-- BigBench B4 (`SELECT COUNT(*), COALESCE(SUM(ad_revenue), 0) FROM uservisits WHERE search_word LIKE '%data%' OR country_code = 'US'`):
Checking `country_code = 'US'` first (via dictionary code equality) and
evaluating `search_word LIKE '%data%'` only on rows where `country_code ≠ 'US'`
(and once per distinct dictionary entry in each block) produces the exact same
count and revenue sum while short-circuiting string matching on US rows. -/
def BigBench_B4_CountryShortCircuitBeforeSubstringGoal : Prop :=
  ∀ (rows : List (Bool × Bool × Int)),
    total (rows.filter (fun r => r.1 || r.2.1)) (fun r => r.2.2) =
    total (rows.filter (fun r => r.2.1)) (fun r => r.2.2) +
    total (rows.filter (fun r => (!r.2.1) && r.1)) (fun r => r.2.2)

/-- BigBench B5 (`SELECT yr, SUM(revenue) FROM (store_sales ⋈ item WHERE i.price > 100) GROUP BY yr`):
Precomputing the dense bitset `expensive(item_id) := (price(item_id) > 100)`
from `item` (using a binary search on sorted `price` or single pass over `item`)
and summing `ss.revenue` directly into a dense `yr`-indexed array for rows with
`expensive(ss.item_id)` eliminates the hash join and subquery materialization. -/
def BigBench_B5_ExpensiveItemBitsetDirectYearSumGoal : Prop :=
  ∀ (sales : List (Nat × Nat × Int)) (expensive : Nat → Bool) (yr : Nat),
    total (sales.filter (fun r => expensive r.1 && (r.2.1 == yr))) (fun r => r.2.2) =
    total ((sales.filter (fun r => expensive r.1)).filter (fun r => r.2.1 == yr))
      (fun r => r.2.2)

/-- Public BI / MLB Q108 (paired `MIN`/`MAX` and `COUNT` extrema over a wide table):
When every TLCS block stores exact block-level `(min, max, non_null_count)`
footer statistics for a column, reducing the global `(MIN, MAX, COUNT)` directly
over the block footers equals scanning and decompressing all rows. -/
def PublicBI_MLBQ108_BlockFooterExtremaReductionGoal : Prop :=
  ∀ (blocks : List (List Int)) (init : Int),
    blocks.flatten.foldl max init =
    blocks.foldl (fun acc b => b.foldl max acc) init

/-- Iceberg SQL / base (`SELECT COUNT(*), SUM(val)` with positional delete files):
For a contiguous data file of `N` rows with a sorted, duplicate-free list of
deleted row positions `D ⊆ [0, N)`, the surviving row count is `N - |D|` and
the surviving sum equals `total_file_sum - sum_at_deleted_positions`, permitting
metadata-level evaluation when `|D| ≪ N` without scanning undeleted rows. -/
def Iceberg_Base_ComplementDeleteSumGoal : Prop :=
  ∀ (vals : List Int) (del : List Nat),
    del.Nodup →
    (∀ i ∈ del, i < vals.length) →
    total (valuesAt vals ((List.range vals.length).filter (fun i => !del.contains i))) id +
    total (valuesAt vals del) id =
    total vals id

end TinylambOpt.ProposedGoals.BenchmarkRematch.AnalyticsAndCloudBI
