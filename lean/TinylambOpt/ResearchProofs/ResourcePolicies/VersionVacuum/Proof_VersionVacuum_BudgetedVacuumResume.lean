import TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_VersionVacuum_BudgetedVacuumResume
open TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum
theorem sound : BudgetedVacuumResume.Goal := by
  intro n stride start a b hn
  simp only [visits, List.range_add, List.map_append, List.map_map, Function.comp]
  congr 1
  apply List.map_congr_left
  intro i hi
  have hn0 : n ≠ 0 := Nat.pos_iff_ne_zero.mp hn
  have hmul : (a + i) * stride = a * stride + i * stride := by rw [Nat.add_mul]
  symm
  calc
    (start + (a + i) * stride) % n
        = (start + (a * stride + i * stride)) % n := by rw [hmul]
    _ = ((start + a * stride) + i * stride) % n := by congr 1; omega
    _ = (((start + a * stride) % n) + i * stride) % n := by
        rw [Nat.add_mod (start + a * stride) (i * stride) n, Nat.add_mod ((start + a * stride) % n) (i * stride) n, Nat.mod_mod]
end Nov26.Proof_VersionVacuum_BudgetedVacuumResume
