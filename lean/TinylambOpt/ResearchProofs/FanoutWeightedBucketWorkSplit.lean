import TinylambOpt.ResearchGoals.FanoutWeightedBucketWorkSplit

/-!
Proved

- `Goal` — Fanout-weighted bucket-shard parallel join split for skewed `LEFT` and
  `INNER` joins (STATS-CEB C4 / DictionaryJoinsAndLateLabels B2 / H2O H5 / JOB J4):
  splitting skewed build buckets across `bShards` and probe batches by fanout cost
  preserves the exact multiset of `(LRow, Option RRow)` pairs iff unmatched NULL
  emission waits on global `anyMatched` across all shards.

The reference model is defined in
`TinylambOpt.ResearchGoals.FanoutWeightedBucketWorkSplit`.
-/

namespace TinylambOpt.ResearchProofs.FanoutWeightedBucketWorkSplit

open TinylambOpt.ResearchGoals.FanoutWeightedBucketWorkSplit

/-- The result of probing one shard for a non-NULL key `lk`: the matching pairs
    and the flag telling whether this shard matched at all. -/
private def shardResult (l : LRow) (lk : Int) (shard : List RRow) :
    List (LRow × Option RRow) × Bool :=
  let hits := shard.filter (fun r => r.k == some lk)
  if hits.isEmpty then ([], false) else (hits.map (fun r => (l, some r)), true)

private theorem fst_shardResult (l : LRow) (lk : Int) (shard : List RRow) :
    (shardResult l lk shard).1 =
      (shard.filter (fun r => r.k == some lk)).map (fun r => (l, some r)) := by
  unfold shardResult
  cases h : shard.filter (fun r => r.k == some lk) with
  | nil => simp
  | cons r rs => simp

private theorem snd_shardResult (l : LRow) (lk : Int) (shard : List RRow) :
    (shardResult l lk shard).2 = !(shard.filter (fun r => r.k == some lk)).isEmpty := by
  unfold shardResult
  cases h : shard.filter (fun r => r.k == some lk) with
  | nil => simp
  | cons r rs => simp

/-- Concatenating all per-shard pair lists is the same as mapping over the
    hits of the flattened build relation. -/
private theorem pairs_flatten (l : LRow) (lk : Int) (bShards : List (List RRow)) :
    ((bShards.map (shardResult l lk)).map Prod.fst).flatten =
      (bShards.flatten.filter (fun r => r.k == some lk)).map (fun r => (l, some r)) := by
  induction bShards with
  | nil => simp
  | cons s ss ih =>
      simp only [List.map_cons, List.flatten_cons]
      rw [fst_shardResult, ih]
      simp [List.filter_append, List.map_append]

/-- The OR of the per-shard match flags says exactly whether the flattened
    build relation has any hit. -/
private theorem anyMatched_eq (l : LRow) (lk : Int) (bShards : List (List RRow)) :
    (bShards.map (shardResult l lk)).any Prod.snd =
      !(bShards.flatten.filter (fun r => r.k == some lk)).isEmpty := by
  induction bShards with
  | nil => simp
  | cons s ss ih =>
      cases hs : s.filter (fun r => r.k == some lk) with
      | nil =>
          simp [shardResult, List.filter_append, hs, ih]
      | cons r rs =>
          simp [shardResult, List.filter_append, hs, ih]

/-- One probe row is equivalent to the reference left join against the
    flattened build relation. -/
private theorem probe_one_eq_ref_one (l : LRow) (bShards : List (List RRow)) :
    probeOneRowOverShards l bShards = refLeftJoin [l] bShards.flatten := by
  rcases l with ⟨id, k⟩
  cases k with
  | none =>
      simp [probeOneRowOverShards, refLeftJoin]
  | some lk =>
      unfold probeOneRowOverShards refLeftJoin
      dsimp
      simp only [List.flatMap, List.map_cons, List.map_nil, List.flatten_cons, List.flatten_nil,
        List.append_nil]
      change
        (let shardResults := bShards.map (shardResult { id := id, k := some lk } lk)
         let anyMatched := shardResults.any Prod.snd
         let pairs := (shardResults.map Prod.fst).flatten
         if anyMatched then pairs else [({ id := id, k := some lk }, none)]) =
          (let hits := bShards.flatten.filter (fun r : RRow => r.k == some lk)
           if hits.isEmpty then [({ id := id, k := some lk }, none)]
           else hits.map (fun r => ({ id := id, k := some lk }, some r)))
      simp only
      rw [pairs_flatten, anyMatched_eq]
      cases h : (bShards.flatten.filter (fun r : RRow => r.k == some lk)).isEmpty <;>
        simp

private theorem chunk_eq_ref (chunk : List LRow) (bShards : List (List RRow)) :
    (chunk.map fun l => probeOneRowOverShards l bShards).flatten =
      refLeftJoin chunk bShards.flatten := by
  induction chunk with
  | nil => simp [refLeftJoin]
  | cons l ls ih =>
      simp [refLeftJoin, probe_one_eq_ref_one, List.flatMap]

private theorem ref_left_join_append (ls₁ ls₂ : List LRow) (rs : List RRow) :
    refLeftJoin (ls₁ ++ ls₂) rs = refLeftJoin ls₁ rs ++ refLeftJoin ls₂ rs := by
  simp [refLeftJoin, List.flatMap_append]

private theorem opt_eq_ref (pChunks : List (List LRow)) (bShards : List (List RRow)) :
    optParallelLeftJoin pChunks bShards = refLeftJoin pChunks.flatten bShards.flatten := by
  induction pChunks with
  | nil => simp [optParallelLeftJoin, refLeftJoin]
  | cons chunk chunks ih =>
      change (chunk.map fun l => probeOneRowOverShards l bShards).flatten ++
               optParallelLeftJoin chunks bShards =
             refLeftJoin (chunk ++ chunks.flatten) bShards.flatten
      rw [chunk_eq_ref, ref_left_join_append, ih]

theorem sound : TinylambOpt.ResearchGoals.FanoutWeightedBucketWorkSplit.Goal := by
  intro ls pChunks bShards hp x
  rw [opt_eq_ref pChunks bShards, hp]

end TinylambOpt.ResearchProofs.FanoutWeightedBucketWorkSplit

-- C++ references to this model or its proof.
-- executor/detail/fused_grouped_join.cpp
