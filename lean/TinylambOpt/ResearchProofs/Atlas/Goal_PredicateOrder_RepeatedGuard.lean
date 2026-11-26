import TinylambOpt.ResearchGoals.Atlas.PlanSearch

/-!
The SQL optimizer can prune or reorganize its plan search only under the relevant
ordering and cost assumptions.

Proved

- `PredicateOrder.RepeatedGuard` — `∀ c p middle tail rows, actual
  ((c,p)::middle++(c,p)::tail) rows = actual ((c,p)::middle++tail) rows + c *
  (rows.filter (fun r => p r && middle.all (fun t => t.2 r))).length`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PredicateOrder_RepeatedGuard

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

private theorem survivors_filter_idem (ts : List (Nat × (Nat → Bool)))
    (p : Nat → Bool) (rows : List Nat) :
    (survivors ts (rows.filter p)).filter p = survivors ts (rows.filter p) := by
  induction ts generalizing rows with
  | nil => simp [survivors]
  | cons t ts ih =>
      rcases t with ⟨c, q⟩
      rw [survivors_cons]
      have hcomm : (rows.filter p).filter q = (rows.filter q).filter p := by
        ext r
        simp [Bool.and_comm]
      rw [hcomm]
      exact ih (rows.filter q)

private theorem filter_survivors_eq (p : Nat → Bool)
    (ts : List (Nat × (Nat → Bool))) (rows : List Nat) :
    rows.filter (fun r => p r && ts.all (fun t => t.2 r)) = survivors ts (rows.filter p) := by
  induction ts generalizing rows with
  | nil => simp [survivors]
  | cons t ts ih =>
      rcases t with ⟨c, q⟩
      rw [survivors_cons]
      have hcomm : (rows.filter p).filter q = (rows.filter q).filter p := by
        ext r
        simp [Bool.and_comm]
      rw [hcomm]
      rw [← ih (rows.filter q)]
      ext r
      simp [Bool.and_comm, Bool.and_left_comm]

theorem sound_PredicateOrder_RepeatedGuard : PredicateOrder.RepeatedGuard.Goal := by
  intro c p middle tail rows
  let rows₁ := rows.filter p
  let mid := survivors middle rows₁
  have hleft₁ : actual ((c, p) :: middle ++ (c, p) :: tail) rows =
      c * rows.length + actual (middle ++ (c, p) :: tail) rows₁ := by
    simp [actual, rows₁]
  have hleft₂ : actual (middle ++ (c, p) :: tail) rows₁ =
      actual middle rows₁ + actual ((c, p) :: tail) mid := by
    change actual (middle ++ ((c, p) :: tail)) rows₁ =
      actual middle rows₁ + actual ((c, p) :: tail) (survivors middle rows₁)
    exact actual_append middle ((c, p) :: tail) rows₁
  have hleft₃ : actual ((c, p) :: tail) mid =
      c * mid.length + actual tail (mid.filter p) := by
    simp [actual]
  have hleft₄ : mid.filter p = mid := by
    change (survivors middle rows₁).filter p = survivors middle rows₁
    exact survivors_filter_idem middle p rows
  have hright₁ : actual ((c, p) :: middle ++ tail) rows =
      c * rows.length + actual (middle ++ tail) rows₁ := by
    simp [actual, rows₁]
  have hright₂ : actual (middle ++ tail) rows₁ =
      actual middle rows₁ + actual tail mid := by
    change actual (middle ++ tail) rows₁ =
      actual middle rows₁ + actual tail (survivors middle rows₁)
    exact actual_append middle tail rows₁
  have hright₃ : rows.filter (fun r => p r && middle.all (fun t => t.2 r)) = mid := by
    change rows.filter (fun r => p r && middle.all (fun t => t.2 r)) =
      survivors middle rows₁
    exact filter_survivors_eq p middle rows
  rw [hleft₁, hleft₂, hleft₃, hleft₄, hright₁, hright₂, hright₃]
  omega

end TinylambOpt.ResearchProofs.Atlas.Goal_PredicateOrder_RepeatedGuard
