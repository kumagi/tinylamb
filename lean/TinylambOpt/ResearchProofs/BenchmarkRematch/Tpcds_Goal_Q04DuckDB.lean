import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds.Goal_Q04DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds.Q04DuckDB.Goal := by
  intro xs c y hc hy
  apply List.filter_congr
  intro r hr
  rcases r with ⟨⟨a, b⟩, v⟩
  by_cases h : (a, b) == (c, y)
  · have heq : (a, b) = (c, y) := beq_iff_eq.mp h
    simp at heq
    rcases heq with ⟨rfl, rfl⟩
    simp [hy]
  · have hneq : (a, b) ≠ (c, y) := by intro heq; exact h (by simp [heq])
    have hrhs : ¬ (b < 2 ∧ 2 * a + b == 2 * c + y) := by
      intro hp
      have hb : b < 2 := by simpa using hp.1
      have heq : 2 * a + b = 2 * c + y := by simpa using hp.2
      have : a = c ∧ b = y := by omega
      exact hneq (Prod.ext this.1 this.2)
    simp [h]
    intro hb heq
    exact hrhs ⟨hb, by simpa using heq⟩
