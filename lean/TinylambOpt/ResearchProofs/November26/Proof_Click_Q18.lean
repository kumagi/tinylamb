import TinylambOpt.ResearchGoals.November26.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.November26.Click.Goal_Q18
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.November26.Click

theorem forKey_cons (key : Nat × Nat) (n : Nat) (runs : List ((Nat × Nat) × Nat)) (k : Nat × Nat) :
    forKey ((key, n) :: runs) k = if (key == k) = true then n :: forKey runs k else forKey runs k := by
  unfold forKey
  by_cases h : (key == k) = true
  · simp [h]
  · simp [h]

theorem forKey_replicate_count : ∀ (runs : List ((Nat × Nat) × Nat)) k,
    (forKey runs k).sum = (runs.flatMap (fun (key, n) => List.replicate n key)).count k
  | [], k => by simp [forKey]
  | (key, n) :: runs, k => by
      have ih := forKey_replicate_count runs k
      rw [forKey_cons, List.flatMap_cons, List.count_append, List.count_replicate]
      by_cases h : (key == k) = true
      · have hk : (k == key) = true := by
          rw [beq_iff_eq] at h ⊢
          exact h.symm
        simp [h, hk, ih]
      · have hne : key ≠ k := by
          rw [beq_iff_eq] at h
          exact h
        have hk : (k == key) = false := by
          simp [beq_iff_eq]
          intro hkeq
          exact hne hkeq.symm
        simp [h, hk, ih]

theorem sound : TinylambOpt.ResearchGoals.November26.Click.Q18.Goal := by
  intro runs k
  exact forKey_replicate_count runs k
end TinylambOpt.ResearchProofs.November26.Click.Goal_Q18
