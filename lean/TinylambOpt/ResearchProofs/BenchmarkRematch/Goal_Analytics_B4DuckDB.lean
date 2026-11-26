import TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
import TinylambOpt.ResearchProofs.BenchmarkRematch.Group
import TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_B4DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
open TinylambOpt.ResearchProofs.BenchmarkRematch.Group
open TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum

theorem nsum_getD (xs : List (Option Int)) : (nsum xs).getD 0 = (xs.filterMap id).sum := by
  rw [nsum_eq]
  by_cases h : (xs.filterMap id).isEmpty = true
  · have : xs.filterMap id = [] := List.isEmpty_iff.mp h
    simp [h, this]
  · simp [h]

theorem sound : B4DuckDB.Goal := by
  intro rs
  dsimp only
  simp only [nsum_getD]
  induction rs with
  | nil => simp
  | cons r rs ih =>
    obtain ⟨u, t, v⟩ := r
    obtain ⟨ih1, ih2⟩ := Prod.mk.inj ih
    simp at ih1 ih2
    cases u <;> cases t <;> cases v <;> simp [List.filter_cons, ih1, ih2] <;> omega

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_B4DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_B4DuckDB.sound
