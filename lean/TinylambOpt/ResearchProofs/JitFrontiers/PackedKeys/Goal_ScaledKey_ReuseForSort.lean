import TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Common_PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.BackFields_PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Specific_PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Refutations_PackedKeys

/-!
SQL grouping or ordering can use a packed key when the representation preserves the
relevant comparisons and validity bits.

Proved

- `ScaledKey.ReuseForSort` — Strong proposal: the unsigned packed comparison also
  implements SQL lexicographic order.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Goal_ScaledKey_ReuseForSort

open TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys
open TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys.ScaledKey
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Common
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.BackFields
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Specific

theorem hord : ∀ k1 k2 : Key, ∀ A B C D : Nat, ScaledKey.codec.fields k1 = some (A, B) →
    ScaledKey.codec.fields k2 = some (C, D) →
    A < 2 ^ 32 → B < 2 ^ 32 → C < 2 ^ 32 → D < 2 ^ 32 →
    (decide (A < C ∨ (A = C ∧ B < D)) = lex k1 k2) := by
  intro k1 k2 A B C D h1 h2 _ _ _ _
  rcases k1 with ⟨k1a, k1b⟩
  rcases k2 with ⟨k2a, k2b⟩
  cases k1a with
  | none => simp [ScaledKey.codec, pick] at h1
  | some x1 =>
    cases k1b with
    | none => simp [ScaledKey.codec, pick] at h1
    | some y1 =>
      cases k2a with
      | none => simp [ScaledKey.codec, pick] at h2
      | some x2 =>
        cases k2b with
        | none => simp [ScaledKey.codec, pick] at h2
        | some y2 =>
          simp [ScaledKey.codec, pick] at h1 h2
          by_cases he1 : x1 % 100 = 0
          · by_cases he2 : x2 % 100 = 0
            · cases hxu : unsigned (x1 / 100) with
              | none => simp [hxu] at h1
              | some A' =>
                cases hyu : unsigned y1 with
                | none => simp [hyu] at h1
                | some B' =>
                  cases hxu2 : unsigned (x2 / 100) with
                  | none => simp [hxu2] at h2
                  | some C' =>
                    cases hyu2 : unsigned y2 with
                    | none => simp [hyu2] at h2
                    | some D' =>
                      simp [he1, he2, hxu, hyu, hxu2, hyu2] at h1 h2
                      rcases h1 with ⟨rfl, rfl⟩
                      rcases h2 with ⟨rfl, rfl⟩
                      have vA := unsigned_val hxu
                      have vB := unsigned_val hyu
                      have vC := unsigned_val hxu2
                      have vD := unsigned_val hyu2
                      show decide (A' < C' ∨ (A' = C' ∧ B' < D')) =
                        lex (pair x1 y1) (pair x2 y2)
                      rw [lex_pair]
                      apply decide_eq_decide.mpr
                      constructor
                      · intro h
                        cases h with
                        | inl h => exact Or.inl (by omega)
                        | inr h => exact Or.inr ⟨by omega, by omega⟩
                      · intro h
                        cases h with
                        | inl h => exact Or.inl (by omega)
                        | inr h => exact Or.inr ⟨by omega, by omega⟩
            · simp [he2] at h2
          · simp [he1] at h1

theorem sound : ScaledKey.ReuseForSort.Goal :=
  reuseForSort_of ScaledKey.codec (by decide) hord

end TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Goal_ScaledKey_ReuseForSort
