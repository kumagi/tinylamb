import TinylambOpt.ResearchGoals.Atlas.PlanSearch

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PredicateOrder_DeferredPass

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.PredicateOrder

private def survivors (ts : List (Nat × (Nat → Bool))) (rows : List Nat) : List Nat :=
  rows.filter (fun r => ts.all (fun t => t.2 r))

private theorem filter_true_rows (rows : List Nat) : rows.filter (fun _ => true) = rows := by
  induction rows with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem survivors_cons (c p ts rows) :
    survivors ((c, p) :: ts) rows = survivors ts (rows.filter p) := by
  unfold survivors
  simp only [List.all_cons]
  ext r
  simp [Bool.and_comm]

private theorem actual_append (ts us : List (Nat × (Nat → Bool))) (rows : List Nat) :
    actual (ts ++ us) rows = actual ts rows + actual us (survivors ts rows) := by
  induction ts generalizing rows with
  | nil =>
      simp [actual, survivors, filter_true_rows]
  | cons t ts ih =>
      rcases t with ⟨c, p⟩
      simp only [List.cons_append, actual]
      rw [ih]
      rw [survivors_cons]
      omega

theorem sound_PredicateOrder_DeferredPass : PredicateOrder.DeferredPass.Goal := by
  intro c tests rows
  have hleft : actual (tests ++ [(c, fun _ => true)]) rows =
      actual tests rows + c * (survivors tests rows).length := by
    rw [actual_append]
    simp [actual, survivors]
  have hright : actual ((c, fun _ => true) :: tests) rows =
      c * rows.length + actual tests rows := by
    simp [actual, filter_true_rows]
  rw [hleft, hright]
  have hlen : (survivors tests rows).length ≤ rows.length := by
    unfold survivors
    exact List.length_filter_le _ _
  have hmul : c * (survivors tests rows).length ≤ c * rows.length :=
    Nat.mul_le_mul_left c hlen
  omega

end TinylambOpt.ResearchProofs.Atlas.Goal_PredicateOrder_DeferredPass
