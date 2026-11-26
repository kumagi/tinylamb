import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_IndexWeighted_PrefixCheckpoint

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.IndexWeighted

-- Truth check on small inputs: all sampled instances satisfy the goal.
private def sampleLists : List (List Nat) :=
  [] :: [0] :: [1] :: [2] :: [0, 0] :: [1, 2] :: [2, 0, 1] :: []

private def checkOne (init : List Nat) (ps : List (List Nat)) : Bool :=
  (ps.map agg.reference).scanl agg.join (agg.reference init) ==
    (List.range (ps.length + 1)).map (fun i => agg.reference (init ++ (ps.take i).flatten))

private def exhaustive : Bool :=
  (sampleLists.all fun init =>
    sampleLists.all fun p =>
      sampleLists.all fun q =>
        checkOne init [p] && checkOne init [p, q])

private theorem sampled_true : exhaustive = true := by native_decide

private def mom (xs : List Nat) : Nat :=
  (xs.zipIdx.map (fun p => p.2 * p.1)).sum

private theorem mom_add (xs : List Nat) (n : Nat) :
    ((xs.zipIdx n).map (fun p => p.2 * p.1)).sum = mom xs + n * xs.sum := by
  induction xs generalizing n with
  | nil => simp [mom]
  | cons x xs ih =>
      simp only [mom, List.zipIdx_cons, List.map_cons, List.sum_cons]
      rw [ih (n + 1), ih 1]
      simp [Nat.mul_add, Nat.add_mul]
      omega

private theorem stitchAgg : stitch agg := by
  intro a b
  change agg.join (agg.reference a) (agg.reference b) = agg.reference (a ++ b)
  simp only [agg, List.length_append, List.sum_append, List.zipIdx_append, List.map_append,
    List.sum_append, Nat.zero_add]
  apply Prod.ext
  · rfl
  apply Prod.ext
  · rfl
  · rw [show ((b.zipIdx a.length).map (fun p => p.2 * p.1)).sum = mom b + a.length * b.sum from
      mom_add b a.length]
    simp [mom]
    omega

theorem sound_PrefixCheckpoint : IndexWeighted.PrefixCheckpoint.Goal := by
  intro init ps
  induction ps generalizing init with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.scanl_cons]
      rw [stitchAgg init p]
      rw [ih (init ++ p)]
      simp [List.range_succ_eq_map, Function.comp_def, List.append_assoc]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_IndexWeighted_PrefixCheckpoint
