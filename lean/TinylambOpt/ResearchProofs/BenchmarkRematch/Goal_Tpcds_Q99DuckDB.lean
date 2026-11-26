import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q99DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

private def clsf (g : Nat) (p : List (Nat × Option Int)) : List Nat :=
  ((p.filter (fun r => r.1 == g)).filterMap Prod.snd).map (bucket [30, 60, 90, 120])

private theorem clsf_append (g : Nat) (a b : List (Nat × Option Int)) :
    clsf g (a ++ b) = clsf g a ++ clsf g b := by
  simp only [clsf, List.filter_append, List.filterMap_append, List.map_append]

/-- Classifying the concatenation of all parts equals appending the per-part
classifications. -/
private theorem dist (g : Nat) :
    ∀ (xs : List (List (Nat × Option Int))), clsf g (xs.flatten) = (xs.map (clsf g)).flatten := by
  intro xs
  induction xs with
  | nil => rfl
  | cons p ps ih =>
    rw [List.flatten_cons, clsf_append, ih, List.map_cons, List.flatten_cons]

/-- Counting a value in the flattening is the sum of the per-part counts. -/
private theorem lemB {α : Type} [BEq α] :
    ∀ (xs : List (List α)) (a : α),
      (xs.flatten).count a = (xs.map (fun x => x.count a)).sum := by
  intro xs a
  induction xs with
  | nil => rfl
  | cons x xs ih =>
    rw [List.flatten_cons, List.map_cons, List.sum_cons, List.count_append, ih]

theorem sound : Q99DuckDB.Goal := by
  intro parts groupKey
  show List.map
      (fun c => (List.map (fun p => (clsf groupKey p).count c) parts).sum) (List.range 5) =
    List.map (fun c => (clsf groupKey (List.flatten parts)).count c) (List.range 5)
  refine congrArg (fun phi : Nat → Nat => List.map phi (List.range 5)) ?_
  funext c
  rw [dist, lemB, List.map_map]
  rfl

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q99DuckDB.sound
