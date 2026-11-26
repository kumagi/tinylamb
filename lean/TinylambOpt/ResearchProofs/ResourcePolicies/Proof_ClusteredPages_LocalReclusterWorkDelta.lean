import TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages

/-!
Proof contribution for ResourcePolicies_ClusteredPages_LocalReclusterWorkDelta. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
namespace TinylambOpt.ResearchProofs.ResourcePolicies.ClusteredPages.Goal_LocalReclusterWorkDelta

open TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages


private theorem readBytes_append (ps qs : List Page) (lo hi : Nat) :
    readBytes (ps ++ qs) lo hi = readBytes ps lo hi + readBytes qs lo hi := by
  unfold readBytes
  rw [List.filter_append, List.map_append, List.sum_append]

private theorem score_append (ps qs : List Page) (workload : List (Nat × Nat)) :
    score (ps ++ qs) workload = score ps workload + score qs workload := by
  unfold score
  induction workload with
  | nil => simp
  | cons q qs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [readBytes_append, ih]
      ac_rfl

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages.LocalReclusterWorkDelta.Goal := by
  intro a b kept workload
  rw [score_append, score_append]
  ac_rfl

end TinylambOpt.ResearchProofs.ResourcePolicies.ClusteredPages.Goal_LocalReclusterWorkDelta
