import TinylambOpt.ResearchGoals.BenchmarkRematch.Cloud

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Cloud_Public108DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Cloud

theorem sound : Public108DuckDB.Goal := by
  intro rows league pos yr
  dsimp only
  have hmem : ∀ i, (members (members league pos) yr).contains i = true ↔ i ∈ league ∧ i ∈ pos ∧ i ∈ yr := by
    intro i
    simp [members, List.contains_iff_mem, List.mem_filter, List.mem_eraseDups, and_assoc]
  have hf : rows.filter (members (members league pos) yr).contains
      = rows.filter (fun i => league.contains i && pos.contains i && yr.contains i) := by
    apply List.filter_congr
    intro x _
    rw [Bool.eq_iff_iff, hmem]
    simp [List.contains_iff_mem, and_assoc]
  refine ⟨hf, ?_⟩
  rw [hf, List.isEmpty_iff, List.filter_eq_nil_iff]
  constructor
  · intro h ⟨i, hi, h1, h2, h3⟩
    have := h i hi
    simp [List.contains_iff_mem, h1, h2, h3] at this
  · intro h i hi
    simp only [List.contains_iff_mem, Bool.and_eq_true, decide_eq_true_eq, not_and]
    intro h1 h2
    exact absurd ⟨i, hi, h1.1, h1.2, h2⟩ h

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Cloud_Public108DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Cloud_Public108DuckDB.sound
