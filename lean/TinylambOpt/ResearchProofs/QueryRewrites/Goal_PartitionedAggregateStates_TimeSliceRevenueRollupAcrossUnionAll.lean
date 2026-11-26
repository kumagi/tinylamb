import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

/-!
# Goal_PartitionedAggregateStates_TimeSliceRevenueRollupAcrossUnionAll

Registers the proof of the TimeSliceRevenueRollupAcrossUnionAll goal in PartitionedAggregateStates: the modeled rewrite
preserves the exact result, so SQL execution may apply it.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_TimeSliceRevenueRollupAcrossUnionAll
open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.TimeSliceRevenueRollupAcrossUnionAll
open TinylambOpt.ResearchGoals.QueryRewrites (nsum)

private theorem nsum_append (xs ys : List (Option Int)) : nsum (xs ++ ys) =
    match nsum xs, nsum ys with
    | none, y => y
    | x, none => x
    | some a, some b => some (a + b) := by
  simp only [nsum]
  have hfm : (xs ++ ys).filterMap id = xs.filterMap id ++ ys.filterMap id := by
    simp [List.filterMap_append]
  rw [hfm]
  cases hx : (xs.filterMap id) with
  | nil =>
      cases hy : (ys.filterMap id) with
      | nil => simp [hx, hy]
      | cons y ys' => simp [hx, hy]
  | cons x xs' =>
      cases hy : (ys.filterMap id) with
      | nil => simp [hx, hy]
      | cons y ys' => simp [hx, hy, List.sum_append]; omega

private theorem nsum_cons (x : Option Int) (xs : List (Option Int)) : nsum (x :: xs) =
    match nsum [x], nsum xs with
    | none, y => y
    | y, none => y
    | some a, some b => some (a + b) := by
  simpa using (nsum_append [x] xs)

theorem sound : Goal := by
  intro channels qualifies
  dsimp [Goal]
  let project := fun (xs : List (Nat × Option Int)) => (xs.filter (fun r => qualifies r.1)).map Prod.snd
  change nsum (channels.map (fun c => nsum (project c))) = nsum (project channels.flatten)
  induction channels with
  | nil => simp [nsum, project]
  | cons c cs ih =>
      have hproj : project (c ++ cs.flatten) = project c ++ project cs.flatten := by
        simp [project, List.filter_append, List.map_append]
      simp only [List.map_cons, List.flatten_cons]
      rw [hproj, nsum_append]
      rw [nsum_cons]
      rw [ih]
      cases h1 : nsum (project c) <;> cases h2 : nsum (project cs.flatten) <;> simp [nsum, h1, h2]
end TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_TimeSliceRevenueRollupAcrossUnionAll
