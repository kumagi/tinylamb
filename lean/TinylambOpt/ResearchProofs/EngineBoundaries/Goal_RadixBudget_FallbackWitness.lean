import TinylambOpt.ResearchGoals.EngineBoundaries.RadixBudget

/-!
Partitioning for a spillable SQL operator must satisfy the modeled size or resource
bound for its partitions.

Proved

- `FallbackWitness` — Fallback has a byte-sized witness; a large row count alone cannot
  trigger it.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_RadixBudget_FallbackWitness

open TinylambOpt.ResearchGoals.EngineBoundaries.RadixBudget

theorem sound : FallbackWitness.Goal := by
  intro b d rs l hmem hfb
  induction d generalizing rs with
  | zero =>
      simp [leaves] at hmem
      subst l
      simpa [leaves] using hfb
  | succ d ih =>
      by_cases hsmall : size rs ≤ b
      · simp [leaves, hsmall] at hmem
        subst l
        simp at hfb
      · simp [leaves, hsmall] at hmem
        rcases hmem with ⟨_, hmem⟩ | ⟨_, hmem⟩
        · by_cases hstag : b < size (bucket d false rs) ∧
              stagnant (size (bucket d false rs)) (size rs) = true
          · simp [hstag] at hmem
            subst l
            simpa using hstag.1
          · simp [hstag] at hmem
            exact ih (bucket d false rs) hmem
        · by_cases hstag : b < size (bucket d true rs) ∧
              stagnant (size (bucket d true rs)) (size rs) = true
          · simp [hstag] at hmem
            subst l
            simpa using hstag.1
          · simp [hstag] at hmem
            exact ih (bucket d true rs) hmem

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_RadixBudget_FallbackWitness
