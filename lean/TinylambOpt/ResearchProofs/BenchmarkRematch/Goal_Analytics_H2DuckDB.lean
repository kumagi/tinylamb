import TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
import TinylambOpt.ResearchProofs.BenchmarkRematch.Group
import TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_H2DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
open TinylambOpt.ResearchProofs.BenchmarkRematch.Group
open TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum

theorem sound : H2DuckDB.Goal := by
  intro rows w a b hb hr
  induction rows with
  | nil => simp
  | cons r rows ih =>
    obtain ⟨x, y⟩ := r
    have hy : y < w := hr (x, y) (by simp)
    have ih' := ih (fun r hr' => hr r (List.mem_cons_of_mem _ hr'))
    simp only [List.map_cons, List.count_cons, ih']
    by_cases h : x * w + y = a * w + b
    · obtain ⟨h1, h2⟩ := inj_pair w x y a b hy hb h
      subst h1; subst h2
      simp
    · have h2 : ¬ (x = a ∧ y = b) := fun ⟨e1, e2⟩ => h (by subst e1; subst e2; rfl)
      simp [h, h2]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_H2DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_H2DuckDB.sound
