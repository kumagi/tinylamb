import TinylambOpt.ResearchGoals.BenchmarkRematch.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Click_Q32DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click

private theorem count_le_count_map_fst (xs : List (Nat × Nat)) (k : Nat × Nat) :
    xs.count k ≤ (xs.map Prod.fst).count k.1 := by
  have h : ∀ x ∈ xs, (x == k) = true → (x.fst == k.1) = true := by
    intro x _ hx
    have hx' : x = k := by simpa [beq_iff_eq] using hx
    subst hx'
    simp
  rw [List.count_eq_countP, List.count_eq_countP, List.countP_map]
  exact List.countP_mono_left h

theorem sound : Q32DuckDB.Goal := by
  intro xs hnd k hk
  have hmem : k.1 ∈ xs.map Prod.fst := by
    rw [List.mem_map]
    exact ⟨k, hk, rfl⟩
  have hc : (xs.map Prod.fst).count k.1 = 1 := hnd.count_of_mem hmem
  have hle : xs.count k ≤ (xs.map Prod.fst).count k.1 := count_le_count_map_fst xs k
  have h1 : 1 ≤ xs.count k := by
    rw [List.one_le_count_iff]
    exact hk
  omega

end TinylambOpt.ResearchProofs.BenchmarkRematch.Click_Q32DuckDB
