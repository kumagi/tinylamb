import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q58DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

private theorem beq_true_of_eq {a b : Nat} (h : a = b) : (a == b) = true := by
  subst h
  exact Nat.beq_refl a

/-- Resolving the day list once and probing it is the same as scanning the
pair list for the matching (date, week) row. -/
theorem sound : Q58DuckDB.Goal := by
  intro days week date
  classical
  have key : (date ∈ List.map Prod.fst (List.filter (fun r => r.snd == week) days)) ↔
      ∃ a, (a ∈ days ∧ (a.snd == week) = true) ∧ a.fst = date := by
    rw [List.mem_map]
    simp only [List.mem_filter]
  rw [Bool.eq_iff_iff, List.contains_eq_mem, List.any_eq_true]
  simp only [key, decide_eq_true_eq, Bool.and_eq_true]
  constructor
  · rintro ⟨a, ⟨ha, hw⟩, hd⟩
    exact ⟨a, ha, beq_true_of_eq hd, hw⟩
  · rintro ⟨a, ha, hd, hw⟩
    exact ⟨a, ⟨ha, hw⟩, Nat.eq_of_beq_eq_true hd⟩

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q58DuckDB.sound
