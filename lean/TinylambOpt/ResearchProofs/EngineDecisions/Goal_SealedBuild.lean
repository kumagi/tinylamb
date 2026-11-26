import TinylambOpt.ResearchGoals.EngineDecisions.Executor

/-!
These models compare concrete implementation choices in planning, execution,
concurrency, or recovery against their stated observable contracts.

Proved

- `SealedBuild` — Return a final decision before the global build barrier exactly when
  all legal future build tails agree. This licenses both positive early emission and
  partition-local negative/anti emission.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.EngineDecisions.Ex_SealedBuild

open TinylambOpt.ResearchGoals.EngineDecisions.Executor.SealedBuild

theorem sound : Goal := by
  intro part sealed built q b
  cases q with
  | none =>
      cases b with
      | false =>
          simp [decideNow, existsMatch]
      | true =>
          constructor
          · intro h
            simp [decideNow] at h
          · intro h
            have ht0 := h [] (by simp)
            simp [existsMatch] at ht0
  | some k =>
      by_cases hb : built.contains k
      · have hkmem : k ∈ built := by simpa using hb
        cases b with
        | false =>
            constructor
            · intro h
              simp [decideNow, hkmem] at h
            · intro h
              have ht0 := h [] (by simp)
              simp [existsMatch, hkmem] at ht0
        | true =>
            constructor
            · intro h tail htail
              simp [existsMatch, hkmem]
            · intro h
              simp [decideNow, hkmem]
      · by_cases hs : sealed.contains (part k)
        · have hkmem : k ∉ built := by simpa using hb
          have hsmem : part k ∈ sealed := by simpa using hs
          cases b with
          | false =>
              constructor
              · intro h tail htail
                have hkt : k ∉ tail := by
                  intro hkin
                  exact (htail k hkin) hsmem
                simp [existsMatch, hkmem, hkt]
              · intro h
                simp [decideNow, hkmem, hsmem]
          | true =>
              constructor
              · intro h
                simp [decideNow, hkmem, hsmem] at h
              · intro h
                have ht0 := h [] (by simp)
                simp [existsMatch, hkmem] at ht0
        · have hkmem : k ∉ built := by simpa using hb
          have hsmem : part k ∉ sealed := by simpa using hs
          constructor
          · intro h
            simp [decideNow, hkmem, hsmem] at h
          · intro h
            have ht0 := h [] (by simp)
            have ht1 := h [k] (by simpa using hsmem)
            have hbfalse : b = false := by simpa [existsMatch, hkmem] using ht0.symm
            have hbtrue : b = true := by simpa [existsMatch] using ht1.symm
            simp [hbfalse] at hbtrue

end TinylambOpt.ResearchProofs.EngineDecisions.Ex_SealedBuild
