import TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

/-!
# CountTopKBeforePayloadMinimum

Removing rows for keys below a supplied count cutoff leaves the minimum payload
unchanged for every key at or above the cutoff.

An aggregate can count phrases first and compute MIN(URL) only for keys that can still
qualify, including cutoff ties.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.CountTopKBeforePayloadMinimum
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

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

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking.CountTopKBeforePayloadMinimum.Goal := by
  intro matched k cutoff phrases h
  let p : Nat × String → Bool := fun r => decide (cutoff ≤ phrases.count r.1)
  have hkeep : ∀ r : Nat × String, (r.1 == k) = true → p r = true := by
    intro r hr
    have heq : r.1 = k := by simpa using hr
    subst heq
    simpa [p] using h
  have heq := forKey_filter_keep matched p k hkeep
  simpa [p] using congrArg List.min? heq
end TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.CountTopKBeforePayloadMinimum
