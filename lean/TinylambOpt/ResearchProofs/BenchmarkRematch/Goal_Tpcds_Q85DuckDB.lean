import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q85DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

private theorem rep_none {α β : Type} (a : α) (f : α → Option β)
    (hfa : f a = none) : ∀ n, (List.replicate n a).filterMap f = [] := by
  intro n
  induction n with
  | zero => rfl
  | succ n ih => rw [List.replicate_succ, List.filterMap_cons, hfa, ih]

private theorem rep_some {α β : Type} (a : α) (f : α → Option β) {b : β}
    (hfa : f a = some b) : ∀ n, (List.replicate n a).filterMap f = List.replicate n b := by
  intro n
  induction n with
  | zero => rfl
  | succ n ih => rw [List.replicate_succ, List.filterMap_cons, hfa, ih, List.replicate_succ]

/-- Replicating every row `n` times then extracting a column equals extracting
the column first and replicating each value `n` times. -/
private theorem key {α β : Type} (n : Nat) (f : α → Option β) :
    ∀ (rs : List α),
      (rs.flatMap (fun x => List.replicate n x)).filterMap f =
        (rs.filterMap f).flatMap (fun v => List.replicate n v) := by
  intro rs
  induction rs with
  | nil => rfl
  | cons a as ih =>
    rw [List.flatMap_cons, List.filterMap_append, ih, List.filterMap_cons]
    cases hfa : f a with
    | none => rw [rep_none a f hfa, List.nil_append]
    | some b => rw [List.flatMap_cons, rep_some a f hfa]

private theorem rep_sum (n : Nat) (y : Int) : (List.replicate n y).sum = (n : Int) * y := by
  induction n with
  | zero => simp
  | succ n ih =>
    rw [List.replicate_succ, List.sum_cons, ih]
    simp only [Int.natCast_add, Int.natCast_one, Int.add_mul, Int.one_mul]
    omega

private theorem sum_flat_rep (n : Nat) : ∀ (ys : List Int),
    (ys.flatMap (fun v => List.replicate n v)).sum = (n : Int) * ys.sum := by
  intro ys
  induction ys with
  | nil => rfl
  | cons y ys ih =>
    rw [List.flatMap_cons, List.sum_append, ih, List.sum_cons, rep_sum n y, Int.mul_add]

private theorem len_flat_rep {α : Type} (n : Nat) : ∀ (ys : List α),
    (ys.flatMap (fun v => List.replicate n v)).length = n * ys.length := by
  intro ys
  induction ys with
  | nil => rfl
  | cons y ys ih =>
    rw [List.flatMap_cons, List.length_append, ih, List.length_cons, List.length_replicate]
    rw [Nat.mul_add, Nat.mul_one]
    omega

theorem sound : Q85DuckDB.Goal := by
  intro rs n
  refine List.map_congr_left ?_
  intro f _
  show ((List.filterMap f (rs.flatMap (fun x => List.replicate n x))).sum,
      (List.filterMap f (rs.flatMap (fun x => List.replicate n x))).length) =
    ((n : Int) * (rs.filterMap f).sum, n * (rs.filterMap f).length)
  rw [key n f rs, sum_flat_rep n, len_flat_rep n]

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q85DuckDB.sound
