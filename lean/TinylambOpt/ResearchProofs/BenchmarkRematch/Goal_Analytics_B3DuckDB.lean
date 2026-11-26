import TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
import TinylambOpt.ResearchProofs.BenchmarkRematch.Group
import TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_B3DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
open TinylambOpt.ResearchProofs.BenchmarkRematch.Group
open TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum

theorem sound : B3DuckDB.Goal := by
  intro r c y r' c' y' cats hc hc' hy hy'
  constructor
  · intro h
    obtain ⟨h1, h2⟩ := inj_pair 6 (r*cats+c) y (r'*cats+c') y' hy hy' h
    obtain ⟨h3, h4⟩ := inj_pair cats r c r' c' hc hc' h1
    subst h3; subst h4; subst h2; rfl
  · intro h
    simp only [Prod.mk.injEq] at h
    obtain ⟨e1, e2, e3⟩ := h
    subst e1; subst e2; subst e3; rfl

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_B3DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_B3DuckDB.sound
