import TinylambOpt.ResearchGoals.DatabaseCorrectness.VarcharKeys
import TinylambOpt.ResearchProofs.DatabaseCorrectness.VarcharOrderLemmas

/-! A following field cannot reverse an unequal leading field.

Since encoded fields are self-delimiting and order-reflecting, the first
differing field decides a compound-key comparison regardless of the trailing
bytes. SQL execution may compare physical compound keys field by field without
letting a later component shadow an earlier inequality. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.VarcharKeys_CompoundSuffixCannotReverseKey

open TinylambOpt.ResearchGoals.DatabaseCorrectness.VarcharKeys
open TinylambOpt.ResearchProofs.DatabaseCorrectness.VarcharOrderLemmas

private theorem encode_append_eq_iff (x y : Bytes) (a b : List UInt8) :
    encode x ++ a = encode y ++ b ↔ x = y ∧ a = b := by
  constructor
  · intro h
    have h1 : encode x ++ a ≤ encode y ++ b := by rw [h]; exact List.le_refl _
    have h2 : encode y ++ b ≤ encode x ++ a := by rw [← h]; exact List.le_refl _
    rw [encode_append_le_iff] at h1 h2
    rcases h1 with hxy | ⟨hxy, hab_le⟩
    · rcases h2 with hyx | ⟨hyx, _⟩
      · exact False.elim ((List.lt_irrefl x) (List.lt_trans hxy hyx))
      · subst y
        exact False.elim ((List.lt_irrefl x) hxy)
    · rcases h2 with hyx | ⟨hyx, hba_le⟩
      · subst y
        exact False.elim ((List.lt_irrefl x) hyx)
      · subst y
        exact ⟨rfl, List.le_antisymm hab_le hba_le⟩
  · intro h
    rcases h with ⟨rfl, rfl⟩
    rfl

private theorem encode_append_lt_iff (x y : Bytes) (a b : List UInt8) :
    encode x ++ a < encode y ++ b ↔ x < y ∨ (x = y ∧ a < b) := by
  rw [list_lt_iff_le_ne, encode_append_le_iff]
  constructor
  · intro h
    rcases h.1 with hlt | ⟨rfl, hle⟩
    · exact Or.inl hlt
    · right
      constructor
      · rfl
      · rw [list_lt_iff_le_ne]
        refine ⟨hle, ?_⟩
        intro heq
        apply h.2
        rw [heq]
  · intro h
    rcases h with hlt | ⟨rfl, hlt⟩
    · exact ⟨Or.inl hlt, by
        intro heq
        have heq' := (encode_append_eq_iff x y a b).mp heq
        rcases heq' with ⟨rfl, rfl⟩
        exact False.elim ((List.lt_irrefl x) hlt)⟩
    · exact ⟨Or.inr ⟨rfl, List.le_of_lt hlt⟩, by
        intro heq
        have heq' := (encode_append_eq_iff x x a b).mp heq
        rcases heq' with ⟨_, rfl⟩
        exact False.elim ((List.lt_irrefl a) hlt)⟩

theorem sound : CompoundSuffixCannotReverseKey.Goal := by
  intro a b x y hab
  have h := encode_append_lt_iff a b x y
  rw [h]
  simp [hab]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.VarcharKeys_CompoundSuffixCannotReverseKey
