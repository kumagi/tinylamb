import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q90DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

/-- Inside the already-selected hours, `≤ 9` carves out exactly the morning
pair `{8,9}` and `19 ≤ ·` exactly the afternoon pair `{19,20}`. -/
theorem sound : Q90DuckDB.Goal := by
  intro hours
  classical
  have key1 : ∀ x : Nat, ((x ≤ 9) && [8, 9, 19, 20].contains x) = [8, 9].contains x := by
    intro x
    rw [Bool.eq_iff_iff]
    simp only [List.contains_eq_mem, List.mem_cons, List.not_mem_nil,
      decide_eq_true_eq, Bool.and_eq_true]
    simp only [or_false]
    omega
  have key2 : ∀ x : Nat, ((19 ≤ x) && [8, 9, 19, 20].contains x) = [19, 20].contains x := by
    intro x
    rw [Bool.eq_iff_iff]
    simp only [List.contains_eq_mem, List.mem_cons, List.not_mem_nil,
      decide_eq_true_eq, Bool.and_eq_true]
    simp only [or_false]
    omega
  have h1 : (hours.filter (fun x => [8, 9, 19, 20].contains x)).filter (fun x => x ≤ 9) =
      hours.filter (fun x => [8, 9].contains x) := by
    rw [List.filter_filter, List.filter_congr (fun x _ => key1 x)]
  have h2 : (hours.filter (fun x => [8, 9, 19, 20].contains x)).filter (fun x => 19 ≤ x) =
      hours.filter (fun x => [19, 20].contains x) := by
    rw [List.filter_filter, List.filter_congr (fun x _ => key2 x)]
  simp only [countWhere, h1, h2]

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q90DuckDB.sound
