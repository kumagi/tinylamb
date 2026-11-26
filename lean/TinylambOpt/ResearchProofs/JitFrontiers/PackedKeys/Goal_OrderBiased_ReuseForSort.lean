import TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Common_PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.BackFields_PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Specific_PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Refutations_PackedKeys

/-!
SQL grouping or ordering can use a packed key when the representation preserves the
relevant comparisons and validity bits.

Proved

- `OrderBiased.ReuseForSort` — Strong proposal: the unsigned packed comparison also
  implements SQL lexicographic order.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Goal_OrderBiased_ReuseForSort

open TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys
open TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys.OrderBiased
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Common
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.BackFields
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Specific

theorem hord : ∀ k1 k2 : Key, ∀ A B C D : Nat, OrderBiased.codec.fields k1 = some (A, B) →
    OrderBiased.codec.fields k2 = some (C, D) →
    A < 2 ^ 32 → B < 2 ^ 32 → C < 2 ^ 32 → D < 2 ^ 32 →
    (decide (A < C ∨ (A = C ∧ B < D)) = lex k1 k2) := by
  intro k1 k2 A B C D h1 h2 _ _ _ _
  rcases k1 with ⟨k1a, k1b⟩
  rcases k2 with ⟨k2a, k2b⟩
  cases k1a with
  | none => simp [OrderBiased.codec, pick] at h1
  | some x1 =>
    cases k1b with
    | none => simp [OrderBiased.codec, pick] at h1
    | some y1 =>
      cases k2a with
      | none => simp [OrderBiased.codec, pick] at h2
      | some x2 =>
        cases k2b with
        | none => simp [OrderBiased.codec, pick] at h2
        | some y2 =>
          simp [OrderBiased.codec, pick] at h1 h2
          cases hbx : biased 32 x1 with
          | none => simp [hbx] at h1
          | some A' =>
            cases hby : biased 32 y1 with
            | none => simp [hby] at h1
            | some B' =>
              cases hcx : biased 32 x2 with
              | none => simp [hcx] at h2
              | some C' =>
                cases hcy : biased 32 y2 with
                | none => simp [hcy] at h2
                | some D' =>
                  simp [hbx, hby, hcx, hcy] at h1 h2
                  rcases h1 with ⟨rfl, rfl⟩
                  rcases h2 with ⟨rfl, rfl⟩
                  have vA := biased_val hbx
                  have vB := biased_val hby
                  have vC := biased_val hcx
                  have vD := biased_val hcy
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

theorem sound : OrderBiased.ReuseForSort.Goal :=
  reuseForSort_of OrderBiased.codec (by decide) hord

end TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Goal_OrderBiased_ReuseForSort
