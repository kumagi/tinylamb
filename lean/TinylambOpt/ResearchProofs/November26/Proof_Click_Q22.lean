import TinylambOpt.ResearchGoals.November26.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.November26.Click.Goal_Q22
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.November26.Click

theorem forKey_filter_keep (matched : List (Nat × String)) (p : Nat × String → Bool) k
    (h : ∀ r, (r.1 == k) = true → p r = true) :
    forKey (matched.filter p) k = forKey matched k := by
  unfold forKey
  rw [List.filter_filter]
  have hf : List.filter (fun a : Nat × String => a.1 == k && p a) matched =
      List.filter (fun r : Nat × String => r.1 == k) matched := by
    apply List.filter_congr
    intro r hr
    by_cases hrk : (r.1 == k) = true
    · have hpr : p r = true := h r hrk
      simp [hrk, hpr]
    · simp [hrk]
  rw [hf]

theorem sound : TinylambOpt.ResearchGoals.November26.Click.Q22.Goal := by
  intro matched k cutoff phrases h
  let p : Nat × String → Bool := fun r => decide (cutoff ≤ phrases.count r.1)
  have hkeep : ∀ r : Nat × String, (r.1 == k) = true → p r = true := by
    intro r hr
    have heq : r.1 = k := by simpa using hr
    subst heq
    simpa [p] using h
  have heq := forKey_filter_keep matched p k hkeep
  simpa [p] using congrArg List.min? heq
end TinylambOpt.ResearchProofs.November26.Click.Goal_Q22
