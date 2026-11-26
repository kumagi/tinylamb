import TinylambOpt.ResearchGoals.DatabaseCorrectness.VarcharKeys

/-! The empty-string terminator is valid only in the first block.

A continuation marker followed by the canonical empty-string terminator is
rejected because the zero-length flag is illegal after the first block. SQL
execution cannot mistake a closed continuation chain for an empty leading
field. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.VarcharKeys_EmptyTerminatorOnlyAtStart

open TinylambOpt.ResearchGoals.DatabaseCorrectness.VarcharKeys

theorem sound : EmptyTerminatorOnlyAtStart.Goal := by
  intro block rest h
  rcases List.exists_cons_of_length_eq_add_one (by omega : block.length = 7 + 1) with ⟨a, t, rfl⟩
  simp at h
  rcases List.exists_cons_of_length_eq_add_one (by omega : t.length = 6 + 1) with ⟨b, t, rfl⟩
  simp at h
  rcases List.exists_cons_of_length_eq_add_one (by omega : t.length = 5 + 1) with ⟨c, t, rfl⟩
  simp at h
  rcases List.exists_cons_of_length_eq_add_one (by omega : t.length = 4 + 1) with ⟨d, t, rfl⟩
  simp at h
  rcases List.exists_cons_of_length_eq_add_one (by omega : t.length = 3 + 1) with ⟨e, t, rfl⟩
  simp at h
  rcases List.exists_cons_of_length_eq_add_one (by omega : t.length = 2 + 1) with ⟨f, t, rfl⟩
  simp at h
  rcases List.exists_cons_of_length_eq_add_one (by omega : t.length = 1 + 1) with ⟨g, t, rfl⟩
  simp at h
  rcases List.exists_cons_of_length_eq_add_one (by omega : t.length = 0 + 1) with ⟨h8, t, rfl⟩
  simp at h
  have ht : t = [] := by omega
  subst t
  simp [decode, groups, body]
end TinylambOpt.ResearchProofs.DatabaseCorrectness.VarcharKeys_EmptyTerminatorOnlyAtStart
