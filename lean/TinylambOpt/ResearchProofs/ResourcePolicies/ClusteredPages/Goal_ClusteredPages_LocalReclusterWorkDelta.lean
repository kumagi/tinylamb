import TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ClusteredPages_LocalReclusterWorkDelta
open TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages LocalReclusterWorkDelta

private theorem readBytes_append (xs ys : List Page) (lo hi : Nat) :
    readBytes (xs ++ ys) lo hi = readBytes xs lo hi + readBytes ys lo hi := by
  simp [readBytes, List.filter_append, List.map_append]

private theorem score_append (xs ys : List Page) (workload : List (Nat × Nat)) :
    score (xs ++ ys) workload = score xs workload + score ys workload := by
  unfold score
  induction workload with
  | nil => simp
  | cons q qs ih =>
    simp only [List.map, List.sum_cons, readBytes_append]
    -- goal: readBytes xs q.fst q.snd + readBytes ys q.fst q.snd + sum(map ... (xs++ys) ...) = ...
    -- ih: sum(map ... (xs++ys) ...) = sum(map ... xs ...) + sum(map ... ys ...)
    -- but ih is in terms of score
    unfold score at ih
    simp only [List.map, readBytes_append] at ih
    omega
 
theorem sound : Goal := by
  intro a b kept workload
  rw [score_append, score_append]
  ac_rfl
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ClusteredPages_LocalReclusterWorkDelta
