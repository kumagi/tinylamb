import TinylambOpt.ResearchGoals.November26.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false


namespace TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q80
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.November26.Tpcds

theorem nsum_fm (xs : List (Option Int)) (zs : List Int) (h : xs.filterMap id = zs) :
    nsum xs = if zs.isEmpty then none else some zs.sum := by
  unfold nsum
  rw [h]

/-- Nullable sum of an append is the nullable addition of the two nullable sums. -/
theorem nsum_append (xs ys : List (Option Int)) :
    nsum (xs ++ ys) = match nsum xs, nsum ys with
      | none, none => none
      | some a, none => some a
      | none, some b => some b
      | some a, some b => some (a + b) := by
  cases hx : xs.filterMap id with
  | nil =>
    cases hy : ys.filterMap id with
    | nil =>
      rw [nsum_fm xs [] hx, nsum_fm ys [] hy]
      rw [nsum_fm (xs ++ ys) [] (by simp [List.filterMap_append, hx, hy])]
      rfl
    | cons y ys' =>
      rw [nsum_fm xs [] hx, nsum_fm ys (y :: ys') hy]
      rw [nsum_fm (xs ++ ys) (y :: ys') (by simp [List.filterMap_append, hx, hy])]
      rfl
  | cons x xs' =>
    cases hy : ys.filterMap id with
    | nil =>
      rw [nsum_fm xs (x :: xs') hx, nsum_fm ys [] hy]
      rw [nsum_fm (xs ++ ys) (x :: xs') (by simp [List.filterMap_append, hx, hy])]
      rfl
    | cons y ys' =>
      rw [nsum_fm xs (x :: xs') hx, nsum_fm ys (y :: ys') hy]
      rw [nsum_fm (xs ++ ys) (x :: xs' ++ y :: ys') (by simp [List.filterMap_append, hx, hy])]
      simp
      omega

theorem nsum_singleton (x : Option Int) : nsum [x] = x := by
  cases x <;> simp [nsum]

theorem nsum_map_nsum : ∀ xs : List (List (Option Int)),
    nsum (xs.map nsum) = nsum xs.flatten := by
  intro xs
  induction xs with
  | nil => rfl
  | cons ids rest ih =>
    rw [List.map_cons, List.flatten_cons]
    change nsum ([nsum ids] ++ rest.map nsum) = nsum (ids ++ rest.flatten)
    rw [nsum_append, nsum_append, nsum_singleton, ih]

theorem nsum_map_flatten : ∀ cs : List (List (List (Option Int))),
    nsum (cs.map (fun ids => nsum ids.flatten)) = nsum (cs.flatten.map nsum) := by
  intro cs
  induction cs with
  | nil => rfl
  | cons ids rest ih =>
    rw [List.map_cons, List.flatten_cons, List.map_append]
    change nsum ([nsum ids.flatten] ++ rest.map (fun g => nsum g.flatten)) =
      nsum (ids.map nsum ++ rest.flatten.map nsum)
    rw [nsum_append, nsum_append, nsum_singleton, nsum_map_nsum ids, ih]

theorem sound : TinylambOpt.ResearchGoals.November26.Tpcds.Q80.Goal := by
  intro channels
  have hper : channels.map (fun ids => nsum (ids.map nsum)) =
      channels.map (fun ids => nsum ids.flatten) := by
    apply List.map_congr_left
    intro ids hids
    exact nsum_map_nsum ids
  rw [hper, nsum_map_flatten, nsum_map_nsum]
end TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q80
