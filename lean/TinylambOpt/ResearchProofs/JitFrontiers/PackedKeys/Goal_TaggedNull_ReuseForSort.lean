import TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Common_PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.BackFields_PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Specific_PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Refutations_PackedKeys

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Goal_TaggedNull_ReuseForSort

open TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys
open TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys.TaggedNull
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Common
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.BackFields
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Specific

/-- A secondary tag word is `0` for SQL `NULL` and `n + 1` for the value `↑n`. -/
def TagShape (p : Option Int) (b : Nat) : Prop :=
  (p = none ∧ b = 0) ∨ ∃ n : Nat, p = some (n : Int) ∧ b = n + 1

theorem tag_lt {b d : Nat} {p q : Option Int} (hb : TagShape p b) (hd : TagShape q d) :
    (b < d) = (compare p q = Ordering.lt) := by
  rcases hb with ⟨rfl, rfl⟩ | ⟨m, rfl, rfl⟩
  · rcases hd with ⟨rfl, hz⟩ | ⟨n, rfl, rfl⟩
    · rw [hz, compare_none_none]
      simp
    · rw [compare_none_some]
      simp
  · rcases hd with ⟨rfl, hz⟩ | ⟨n, rfl, rfl⟩
    · rw [hz, compare_some_none]
      simp
    · show (m + 1 < n + 1) = (compare (m : Int) (n : Int) = Ordering.lt)
      rw [Int.compare_eq_lt, Nat.succ_lt_succ_iff, Int.ofNat_lt]

theorem hord : ∀ k1 k2 : Key, ∀ A B C D : Nat, TaggedNull.codec.fields k1 = some (A, B) →
    TaggedNull.codec.fields k2 = some (C, D) →
    A < 2 ^ 32 → B < 2 ^ 32 → C < 2 ^ 32 → D < 2 ^ 32 →
    (decide (A < C ∨ (A = C ∧ B < D)) = lex k1 k2) := by
  intro k1 k2 A B C D h1 h2 _ _ _ _
  rcases k1 with ⟨k1a, p1⟩
  rcases k2 with ⟨k2a, p2⟩
  cases k1a with
  | none => simp [TaggedNull.codec] at h1
  | some x1 =>
    cases k2a with
    | none => simp [TaggedNull.codec] at h2
    | some x2 =>
      simp only [TaggedNull.codec] at h1 h2
      cases p1 with
      | none =>
        cases p2 with
        | none =>
          cases hxu : unsigned x1 with
          | none => simp [hxu] at h1
          | some A' =>
            cases hxu2 : unsigned x2 with
            | none => simp [hxu2] at h2
            | some C' =>
              simp [hxu, hxu2] at h1 h2
              rcases h1 with ⟨rfl, rfl⟩
              rcases h2 with ⟨rfl, rfl⟩
              have vA := unsigned_val hxu
              have vC := unsigned_val hxu2
              have s1 : TagShape none (0 : Nat) := Or.inl ⟨rfl, rfl⟩
              have s2 : TagShape none (0 : Nat) := Or.inl ⟨rfl, rfl⟩
              rw [lex_fst]
              apply decide_eq_decide.mpr
              constructor
              · intro h
                cases h with
                | inl h => exact Or.inl (by omega)
                | inr h =>
                  rw [tag_lt s1 s2] at h
                  exact Or.inr ⟨by omega, h.2⟩
              · intro h
                cases h with
                | inl h => exact Or.inl (by omega)
                | inr h =>
                  rw [tag_lt s1 s2]
                  exact Or.inr ⟨by omega, h.2⟩
        | some y2 =>
          cases hxu : unsigned x1 with
          | none => simp [hxu] at h1
          | some A' =>
            cases hxu2 : unsigned x2 with
            | none => simp [hxu2] at h2
            | some C' =>
              cases hy2 : unsigned y2 with
              | none => simp [hy2] at h2
              | some N2 =>
                simp [hxu, hxu2, hy2] at h1 h2
                rcases h1 with ⟨rfl, rfl⟩
                rcases h2 with ⟨rfl, rfl⟩
                have vA := unsigned_val hxu
                have vC := unsigned_val hxu2
                have vy2 := unsigned_val hy2
                have s1 : TagShape none (0 : Nat) := Or.inl ⟨rfl, rfl⟩
                have s2 : TagShape (some y2) (N2 + 1) := Or.inr ⟨N2, congrArg some vy2.symm, rfl⟩
                rw [lex_fst]
                apply decide_eq_decide.mpr
                constructor
                · intro h
                  cases h with
                  | inl h => exact Or.inl (by omega)
                  | inr h =>
                    rw [tag_lt s1 s2] at h
                    exact Or.inr ⟨by omega, h.2⟩
                · intro h
                  cases h with
                  | inl h => exact Or.inl (by omega)
                  | inr h =>
                    rw [tag_lt s1 s2]
                    exact Or.inr ⟨by omega, h.2⟩
      | some y1 =>
        cases p2 with
        | none =>
          cases hxu : unsigned x1 with
          | none => simp [hxu] at h1
          | some A' =>
            cases hy1 : unsigned y1 with
            | none => simp [hy1] at h1
            | some N1 =>
              cases hxu2 : unsigned x2 with
              | none => simp [hxu2] at h2
              | some C' =>
                simp [hxu, hy1, hxu2] at h1 h2
                rcases h1 with ⟨rfl, rfl⟩
                rcases h2 with ⟨rfl, rfl⟩
                have vA := unsigned_val hxu
                have vC := unsigned_val hxu2
                have vy1 := unsigned_val hy1
                have s1 : TagShape (some y1) (N1 + 1) := Or.inr ⟨N1, congrArg some vy1.symm, rfl⟩
                have s2 : TagShape none (0 : Nat) := Or.inl ⟨rfl, rfl⟩
                rw [lex_fst]
                apply decide_eq_decide.mpr
                constructor
                · intro h
                  cases h with
                  | inl h => exact Or.inl (by omega)
                  | inr h =>
                    rw [tag_lt s1 s2] at h
                    exact Or.inr ⟨by omega, h.2⟩
                · intro h
                  cases h with
                  | inl h => exact Or.inl (by omega)
                  | inr h =>
                    rw [tag_lt s1 s2]
                    exact Or.inr ⟨by omega, h.2⟩
        | some y2 =>
          cases hxu : unsigned x1 with
          | none => simp [hxu] at h1
          | some A' =>
            cases hy1 : unsigned y1 with
            | none => simp [hy1] at h1
            | some N1 =>
              cases hxu2 : unsigned x2 with
              | none => simp [hxu2] at h2
              | some C' =>
                cases hy2 : unsigned y2 with
                | none => simp [hy2] at h2
                | some N2 =>
                  simp [hxu, hy1, hxu2, hy2] at h1 h2
                  rcases h1 with ⟨rfl, rfl⟩
                  rcases h2 with ⟨rfl, rfl⟩
                  have vA := unsigned_val hxu
                  have vC := unsigned_val hxu2
                  have vy1 := unsigned_val hy1
                  have vy2 := unsigned_val hy2
                  have s1 : TagShape (some y1) (N1 + 1) := Or.inr ⟨N1, congrArg some vy1.symm, rfl⟩
                  have s2 : TagShape (some y2) (N2 + 1) := Or.inr ⟨N2, congrArg some vy2.symm, rfl⟩
                  rw [lex_fst]
                  apply decide_eq_decide.mpr
                  constructor
                  · intro h
                    cases h with
                    | inl h => exact Or.inl (by omega)
                    | inr h =>
                      rw [tag_lt s1 s2] at h
                      exact Or.inr ⟨by omega, h.2⟩
                  · intro h
                    cases h with
                    | inl h => exact Or.inl (by omega)
                    | inr h =>
                      rw [tag_lt s1 s2]
                      exact Or.inr ⟨by omega, h.2⟩

theorem sound : TaggedNull.ReuseForSort.Goal :=
  reuseForSort_of TaggedNull.codec (by decide) hord

end TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Goal_TaggedNull_ReuseForSort
