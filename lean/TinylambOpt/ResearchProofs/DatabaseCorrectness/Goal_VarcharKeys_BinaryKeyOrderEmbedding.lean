import TinylambOpt.ResearchGoals.DatabaseCorrectness.VarcharKeys
import TinylambOpt.ResearchProofs.DatabaseCorrectness.VarcharOrderLemmas

/-! Unsigned byte order survives VARCHAR encoding.

The block-wise order lemma says that appending any suffixes to two encoded
fields compares exactly as the raw fields (with equality only when both
fields and both suffixes agree). With empty suffixes this is precisely the
index order of the source bytes, so SQL range bounds can be applied directly
to the binary memcomparable key. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.VarcharKeys_BinaryKeyOrderEmbedding

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

theorem sound : BinaryKeyOrderEmbedding.Goal := by
  intro a b
  have h := encode_append_lt_iff a b ([] : List UInt8) []
  simp at h
  exact h

end TinylambOpt.ResearchProofs.DatabaseCorrectness.VarcharKeys_BinaryKeyOrderEmbedding
