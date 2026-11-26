import TinylambOpt.ResearchGoals.AnalyticsWorkloads.Common

/-!
These claims cover grouping by surrogate keys before rollup, distinct set
counts, disjoint histogram buckets, and separate counts for NULL-extended
join rows. They support SQL plans that aggregate early or use narrower keys
while preserving counts, non-NULL counts, and exact sums.
-/


namespace TinylambOpt.ResearchGoals.QueryRewrites.SurrogateKeyRollupsAndSetCounts

open TinylambOpt.ResearchGoals.AnalyticsWorkloads (Totals zero weighted plus summarize histogram)

/-! ## Wave 9: TPC-DS ROLLUP, Set Operations, Disjoint CASE Buckets, LEFT JOIN Null Flags

Targets:
- TPC-DS Q22 (941.7ms vs DuckDB 144.4ms): `SurrogateKeyPreAggRollup`
- TPC-DS Q38 (259.9ms vs 72.4ms) / Q87 (218.5ms vs 85.7ms): `SurrogateIntersectSetCount`
- TPC-DS Q99 (67.2ms vs 15.9ms): `DisjointRangeBucketHistogram`
- TPC-DS Q72 (293.4ms vs 87.4ms): `LeftJoinNullFlagSplitCount`
-/

/-- TPC-DS Q22. Pre-aggregate fact rows by surrogate key `item_sk` before joining
the unique `item` dimension and expanding `ROLLUP(a, b, c, d)` prefixes.
Every ROLLUP level (0..4) preserves exact `(cnt, nn, sum)` for `AVG`. -/
structure ItemDim where
  sk : Nat
  a : Option String
  b : Option String
  c : Option String
  d : Option String
  deriving BEq, DecidableEq

abbrev RollupKey := Option String × Option String × Option String × Option String

def rollupPrefixes (it : ItemDim) : List RollupKey :=
  [ (it.a, it.b, it.c, it.d),
    (it.a, it.b, it.c, none),
    (it.a, it.b, none, none),
    (it.a, none, none, none),
    (none, none, none, none) ]

def preAggRollup (facts : List (Nat × Option Int)) (dims : List ItemDim) (key : RollupKey) : Totals :=
  let bySk : Std.HashMap Nat Totals :=
    facts.foldl (fun m (sk, v) => m.insert sk (plus (m.getD sk zero) (weighted 1 v))) {}
  dims.foldl (fun acc it =>
    let st := bySk.getD it.sk zero
    if st.cnt == 0 then acc else
      (rollupPrefixes it).foldl (fun a rk => if rk == key then plus a st else a) acc) zero

def refRollup (facts : List (Nat × Option Int)) (dims : List ItemDim) (key : RollupKey) : Totals :=
  summarize (facts.flatMap fun (sk, v) =>
    (dims.filter (fun it => it.sk == sk)).flatMap fun it =>
      (rollupPrefixes it).filterMap fun rk => if rk == key then some v else none)

def SurrogateKeyPreAggRollup.Goal : Prop :=
  ∀ (facts : List (Nat × Option Int)) (dims : List ItemDim) (key : RollupKey),
    (dims.map ItemDim.sk).Nodup →
    preAggRollup facts dims key = refRollup facts dims key

/-- TPC-DS Q38 / Q87. Deduplicate each sales channel on surrogate `(cust_sk, date_sk)`
before resolving `(name_id, date_val)` for 3-way `INTERSECT DISTINCT`.
Even when distinct `cust_sk` share the same `name_id`, the deduplicated surrogate
projection preserves the exact distinct triple set and its cardinality. -/
def surrogateIntersect (c1 c2 c3 : List (Nat × Nat))
    (nameOf : Nat → Nat) (dateOf : Nat → Nat) : List (Nat × Nat) :=
  let proj := fun (ch : List (Nat × Nat)) =>
    (ch.eraseDups.map fun (c, d) => (nameOf c, dateOf d)).eraseDups
  let s1 := proj c1
  let s2 := proj c2
  let s3 := proj c3
  s1.filter fun k => s2.contains k && s3.contains k

def refIntersect (c1 c2 c3 : List (Nat × Nat))
    (nameOf : Nat → Nat) (dateOf : Nat → Nat) : List (Nat × Nat) :=
  let proj := fun (ch : List (Nat × Nat)) =>
    (ch.map fun (c, d) => (nameOf c, dateOf d)).eraseDups
  let s1 := proj c1
  let s2 := proj c2
  let s3 := proj c3
  s1.filter fun k => s2.contains k && s3.contains k

def SurrogateIntersectSetCount.Goal : Prop :=
  ∀ (c1 c2 c3 : List (Nat × Nat)) (nameOf dateOf : Nat → Nat),
    (surrogateIntersect c1 c2 c3 nameOf dateOf).length =
      (refIntersect c1 c2 c3 nameOf dateOf).length

/-- TPC-DS Q99. Five mutually disjoint `CASE` range sums over `ship - sold`
collapse into a single 5-bin bucket dispatch per row, skipping NULL diffs. -/
def diffBucket (ship sold : Option Int) : Option (Fin 5) :=
  match ship, sold with
  | some sh, some so =>
    let d := sh - so
    if d ≤ 30 then some ⟨0, by decide⟩
    else if d ≤ 60 then some ⟨1, by decide⟩
    else if d ≤ 90 then some ⟨2, by decide⟩
    else if d ≤ 120 then some ⟨3, by decide⟩
    else some ⟨4, by decide⟩
  | _, _ => none

def bucketHist (rows : List (Option Int × Option Int)) (bin : Fin 5) : Nat :=
  rows.countP fun (sh, so) => diffBucket sh so == some bin

def refCaseSum (rows : List (Option Int × Option Int)) (bin : Fin 5) : Nat :=
  rows.countP fun (sh, so) =>
    match sh, so with
    | some s, some o =>
      let d := s - o
      match bin.val with
      | 0 => d ≤ 30
      | 1 => 30 < d && d ≤ 60
      | 2 => 60 < d && d ≤ 90
      | 3 => 90 < d && d ≤ 120
      | _ => 120 < d
    | _, _ => false

def DisjointRangeBucketHistogram.Goal : Prop :=
  ∀ (rows : List (Option Int × Option Int)) (bin : Fin 5),
    bucketHist rows bin = refCaseSum rows bin

/-- TPC-DS Q72. A LEFT JOIN whose nullable side is only inspected via
`p_promo_sk IS NULL` / `IS NOT NULL` collapses into a non-NULL multiplicity map:
`no_promo`, `promo`, and `total_cnt` are exact without widening or payload copies. -/
structure PromoCounts where
  noPromo : Nat
  promo : Nat
  total : Nat
  deriving BEq, DecidableEq

def collapsedLeftPromo (facts promos : List (Option Nat)) : PromoCounts :=
  let mult := histogram (promos.filterMap id)
  facts.foldl (fun acc fk =>
    let m := fk.elim 0 (mult.getD · 0)
    if m == 0 then ⟨acc.noPromo + 1, acc.promo, acc.total + 1⟩
    else ⟨acc.noPromo, acc.promo + m, acc.total + m⟩) ⟨0, 0, 0⟩

def refLeftPromo (facts promos : List (Option Nat)) : PromoCounts :=
  let pairs : List (Option Nat) := facts.flatMap fun fk =>
    match fk with
    | none => [none]
    | some k =>
      let hits := promos.filter (· == some k)
      if hits.isEmpty then [none] else hits
  ⟨pairs.countP Option.isNone, pairs.countP Option.isSome, pairs.length⟩

def LeftJoinNullFlagSplitCount.Goal : Prop :=
  ∀ (facts promos : List (Option Nat)),
    collapsedLeftPromo facts promos = refLeftPromo facts promos

#guard preAggRollup [(1, some 10), (1, none), (2, some 20)]
  [⟨1, some "A", some "B", some "C", some "D"⟩, ⟨2, some "A", some "X", some "Y", some "Z"⟩]
  (some "A", none, none, none) ==
  refRollup [(1, some 10), (1, none), (2, some 20)]
    [⟨1, some "A", some "B", some "C", some "D"⟩, ⟨2, some "A", some "X", some "Y", some "Z"⟩]
    (some "A", none, none, none)
#guard (surrogateIntersect [(1, 10), (2, 10)] [(2, 10)] [(1, 10)] (fun _ => 7) id).length == 1
#guard bucketHist [(some 50, some 10), (none, some 10), (some 140, some 10)] ⟨1, by decide⟩ == 1
#guard collapsedLeftPromo [none, some 1, some 2] [none, some 1, some 1] ==
  refLeftPromo [none, some 1, some 2] [none, some 1, some 1]

end TinylambOpt.ResearchGoals.QueryRewrites.SurrogateKeyRollupsAndSetCounts
