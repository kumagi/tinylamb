import Std

/-!
Parallel probe over fanout-weighted probe chunks `pChunks` and build shards `bShards`:
tracks global `anyMatched` across shards before emitting a single unmatched `(l, none)`
sentinel.

Buggy per-shard outer join that emits `(l, none)` independently on any shard that fails
to match `l`, duplicating NULL extensions when match occurs only in other shards.

Modeled claims

- `Goal` — Fanout-weighted bucket-shard parallel join split for skewed `LEFT` and
  `INNER` joins (STATS-CEB C4 / DictionaryJoinsAndLateLabels B2 / H2O H5 / JOB J4):
  splitting skewed build buckets across `bShards` and probe batches by fanout cost
  preserves the exact multiset of `(LRow, Option RRow)` pairs iff unmatched NULL
  emission waits on global `anyMatched` across all shards.
-/

namespace TinylambOpt.ResearchGoals.FanoutWeightedBucketWorkSplit

structure LRow where
  id : Nat
  k : Option Int
  deriving DecidableEq

structure RRow where
  id : Nat
  k : Option Int
  deriving DecidableEq

def refLeftJoin (ls : List LRow) (rs : List RRow) : List (LRow × Option RRow) :=
  ls.flatMap fun l =>
    match l.k with
    | none => [(l, none)]
    | some lk =>
        let hits := rs.filter (fun r => r.k == some lk)
        if hits.isEmpty then [(l, none)]
        else hits.map (fun r => (l, some r))

def probeOneRowOverShards (l : LRow) (bShards : List (List RRow)) :
    List (LRow × Option RRow) :=
  match l.k with
  | none => [(l, none)]
  | some lk =>
      let shardResults : List (List (LRow × Option RRow) × Bool) :=
        bShards.map fun shard =>
          let hits := shard.filter (fun r => r.k == some lk)
          if hits.isEmpty then ([], false)
          else (hits.map (fun r => (l, some r)), true)
      let anyMatched := shardResults.any Prod.snd
      let pairs := (shardResults.map Prod.fst).flatten
      if anyMatched then pairs else [(l, none)]

/-- Parallel probe over fanout-weighted probe chunks `pChunks` and build shards
    `bShards`: tracks global `anyMatched` across shards before emitting a single
    unmatched `(l, none)` sentinel. -/
def optParallelLeftJoin (pChunks : List (List LRow)) (bShards : List (List RRow)) :
    List (LRow × Option RRow) :=
  (pChunks.map fun chunk =>
    (chunk.map fun l => probeOneRowOverShards l bShards).flatten).flatten

/-- Buggy per-shard outer join that emits `(l, none)` independently on any shard
    that fails to match `l`, duplicating NULL extensions when match occurs only in
    other shards. -/
def buggyPerShardLeftJoin (ls : List LRow) (bShards : List (List RRow)) :
    List (LRow × Option RRow) :=
  (ls.map fun l =>
    (bShards.map fun shard => refLeftJoin [l] shard).flatten).flatten

/-- Fanout-weighted bucket-shard parallel join split for skewed `LEFT` and `INNER`
    joins (STATS-CEB C4 / DictionaryJoinsAndLateLabels B2 / H2O H5 / JOB J4): splitting skewed build
    buckets across `bShards` and probe batches by fanout cost preserves the exact
    multiset of `(LRow, Option RRow)` pairs iff unmatched NULL emission waits on
    global `anyMatched` across all shards. -/
def Goal : Prop :=
  ∀ (ls : List LRow) (pChunks : List (List LRow)) (bShards : List (List RRow)),
    pChunks.flatten = ls →
    ∀ (x : LRow × Option RRow),
      (optParallelLeftJoin pChunks bShards).count x =
        (refLeftJoin ls bShards.flatten).count x

example :
    let ls : List LRow :=
      [ { id := 1, k := some 7 }
      , { id := 2, k := some 9 }
      , { id := 3, k := none }
      ]
    let pChunks : List (List LRow) := [[ls.getD 0 { id := 0, k := none }], [ls.getD 1 { id := 0, k := none }, ls.getD 2 { id := 0, k := none }]]
    let bShards : List (List RRow) :=
      [ [{ id := 100, k := some 7 }]
      , [{ id := 101, k := some 7 }, { id := 102, k := some 8 }]
      ]
    optParallelLeftJoin pChunks bShards = refLeftJoin ls bShards.flatten ∧
    buggyPerShardLeftJoin ls bShards ≠ refLeftJoin ls bShards.flatten := by
  decide

example :
    let ls : List LRow := [{ id := 1, k := some 7 }]
    let bShards : List (List RRow) := [[], [{ id := 10, k := some 7 }], []]
    (optParallelLeftJoin [ls] bShards).count ({ id := 1, k := some 7 }, none) = 0 ∧
    (buggyPerShardLeftJoin ls bShards).count ({ id := 1, k := some 7 }, none) = 2 := by
  decide

end TinylambOpt.ResearchGoals.FanoutWeightedBucketWorkSplit
