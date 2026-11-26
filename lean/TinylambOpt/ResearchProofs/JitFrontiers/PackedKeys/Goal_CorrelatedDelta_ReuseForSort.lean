import TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Common_PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.BackFields_PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Specific_PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Refutations_PackedKeys

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Goal_CorrelatedDelta_ReuseForSort

open TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys
open TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys.CorrelatedDelta
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Common
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.BackFields
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Specific

theorem hord : ∀ k1 k2 : Key, ∀ A B C D : Nat, CorrelatedDelta.codec.fields k1 = some (A, B) →
    CorrelatedDelta.codec.fields k2 = some (C, D) →
    A < 2 ^ 48 → B < 2 ^ 16 → C < 2 ^ 48 → D < 2 ^ 16 →
    (decide (A < C ∨ (A = C ∧ B < D)) = lex k1 k2) := by
  intro k1 k2 A B C D h1 h2 _ _ _ _
  rcases k1 with ⟨k1a, k1b⟩
  rcases k2 with ⟨k2a, k2b⟩
  cases k1a with
  | none => simp [CorrelatedDelta.codec, pick] at h1
  | some x1 =>
    cases k1b with
    | none => simp [CorrelatedDelta.codec, pick] at h1
    | some y1 =>
      cases k2a with
      | none => simp [CorrelatedDelta.codec, pick] at h2
      | some x2 =>
        cases k2b with
        | none => simp [CorrelatedDelta.codec, pick] at h2
        | some y2 =>
          simp [CorrelatedDelta.codec, pick] at h1 h2
          cases hxu : unsigned x1 with
          | none => simp [hxu] at h1
          | some A' =>
            cases hbd : biased 16 (y1 - x1) with
            | none => simp [hbd] at h1
            | some B' =>
              cases hxu2 : unsigned x2 with
              | none => simp [hxu2] at h2
              | some C' =>
                cases hbd2 : biased 16 (y2 - x2) with
                | none => simp [hbd2] at h2
                | some D' =>
                  simp [hxu, hbd, hxu2, hbd2] at h1 h2
                  rcases h1 with ⟨rfl, rfl⟩
                  rcases h2 with ⟨rfl, rfl⟩
                  have vA := unsigned_val hxu
                  have vB := biased_val hbd
                  have vC := unsigned_val hxu2
                  have vD := biased_val hbd2
                  show decide (A' < C' ∨ (A' = C' ∧ B' < D')) = lex (pair x1 y1) (pair x2 y2)
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

theorem sound : CorrelatedDelta.ReuseForSort.Goal :=
  reuseForSort_of CorrelatedDelta.codec (by decide) hord

end TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Goal_CorrelatedDelta_ReuseForSort
