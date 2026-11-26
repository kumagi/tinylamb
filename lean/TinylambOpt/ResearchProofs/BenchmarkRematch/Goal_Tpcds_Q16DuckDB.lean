import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q16DuckDB

open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

private theorem lhsAny (w : Nat) :
    ∀ (ws : List (Option Nat)),
      (ws.any (fun v => v.any (· != w))) = ((ws.filterMap id).any (· != w)) := by
  intro ws
  induction ws with
  | nil => rfl
  | cons v l ih =>
    rw [List.any_cons, List.filterMap_cons, ih]
    cases v <;> rfl

private theorem foldlMinKey (w : Nat) :
    ∀ (l : List Nat) (z : Nat),
      decide (l.foldl min z < w) = (decide (z < w) || l.any (fun x => decide (x < w))) := by
  intro l
  induction l with
  | nil => intro z; rw [List.any_nil, Bool.or_false]; rfl
  | cons d t ih =>
    intro z
    rw [List.foldl_cons, ih, List.any_cons]
    have keyMin : decide (min z d < w) = (decide (z < w) || decide (d < w)) := by
      apply Bool.eq_iff_iff.mpr
      simp only [Bool.or_eq_true, decide_eq_true_eq]
      omega
    rw [keyMin, Bool.or_assoc]

private theorem foldlMaxKey (w : Nat) :
    ∀ (l : List Nat) (z : Nat),
      decide (w < l.foldl max z) = (decide (w < z) || l.any (fun x => decide (w < x))) := by
  intro l
  induction l with
  | nil => intro z; rw [List.any_nil, Bool.or_false]; rfl
  | cons d t ih =>
    intro z
    rw [List.foldl_cons, ih, List.any_cons]
    have keyMax : decide (w < max z d) = (decide (w < z) || decide (w < d)) := by
      apply Bool.eq_iff_iff.mpr
      simp only [Bool.or_eq_true, decide_eq_true_eq]
      omega
    rw [keyMax, Bool.or_assoc]

private theorem minAny (w : Nat) :
    ∀ (ys : List Nat),
      ((ys.min?).any (fun v => decide (v < w))) = ys.any (fun x => decide (x < w)) := by
  intro ys
  induction ys with
  | nil => rfl
  | cons y l ih =>
    rw [List.min?_cons', Option.any_some, foldlMinKey, List.any_cons]

private theorem maxAny (w : Nat) :
    ∀ (ys : List Nat),
      ((ys.max?).any (fun v => decide (w < v))) = ys.any (fun x => decide (w < x)) := by
  intro ys
  induction ys with
  | nil => rfl
  | cons y l ih =>
    rw [List.max?_cons', Option.any_some, foldlMaxKey, List.any_cons]

private theorem splitBridge (w : Nat) :
    ∀ (ys : List Nat),
      ys.any (fun x => x != w) =
        (ys.any (fun x => decide (x < w)) || ys.any (fun x => decide (w < x))) := by
  intro ys
  induction ys with
  | nil => simp only [List.any_nil, Bool.false_or]
  | cons y l ih =>
    rw [List.any_cons, List.any_cons, List.any_cons, ih]
    have hatom : (y != w) = (decide (y < w) || decide (w < y)) := by
      rw [show (y != w) = decide (y ≠ w) from rfl]
      apply Bool.eq_iff_iff.mpr
      simp only [Bool.or_eq_true, decide_eq_true_eq]
      omega
    rw [hatom]
    simp only [Bool.or_assoc, Bool.or_left_comm, Bool.or_comm]

theorem sound : Q16DuckDB.Goal := by
  intro warehouses w
  rw [lhsAny, splitBridge, minAny, maxAny]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q16DuckDB
