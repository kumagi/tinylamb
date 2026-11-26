import TinylambOpt.ResearchGoals.RestartRaces.JobJ3

/-!
These models specify the ordering of SQL transaction, recovery, and concurrent
maintenance events.

Refuted by counterexample

- `ProbeSchedule` — Aggressive scheduling hypothesis: exact current-block miss counts
  suffice to choose an optimal local order despite cache reuse in later morsels.
  Refutation would justify pricing future reuse even with perfect selectivities.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.RestartRaces.JobJ3_ProbeSchedule
open TinylambOpt.ResearchGoals.RestartRaces.JobJ3.ProbeSchedule

def counterP (k : Nat) : Bool := k == 1
def counterM (k : Nat) : Bool := k == 1
def counterBlocks : List (List (Nat × Nat)) :=
  [[(2, 0)], [(0, 0), (2, 0), (1, 1)]]

theorem refute : ¬ Goal := by
  intro h
  have hEq := h counterP counterM counterBlocks
  have hne : greedy counterP counterM ⟨[], []⟩ counterBlocks ≠
      cheapest counterP counterM ⟨[], []⟩ counterBlocks := by
    native_decide
  exact hne hEq

-- C++ references to this model or its proof.
-- executor/detail/analytics_factorized_join.cpp
