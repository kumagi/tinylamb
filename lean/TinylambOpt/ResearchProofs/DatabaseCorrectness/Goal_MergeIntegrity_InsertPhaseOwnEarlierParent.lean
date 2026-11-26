import TinylambOpt.ResearchGoals.DatabaseCorrectness.MergeIntegrity

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_MergeIntegrity_InsertPhaseOwnEarlierParent

open TinylambOpt.ResearchGoals.DatabaseCorrectness.MergeIntegrity


theorem acceptInserts_mono (l1 l2 : List Nat) (hsub : ∀ x, x ∈ l1 → x ∈ l2) :
    ∀ fresh, acceptInserts l1 fresh = true → acceptInserts l2 fresh = true := by
  intro fresh
  induction fresh generalizing l1 l2 with
  | nil => simp [acceptInserts]
  | cons r rs ih =>
      simp [acceptInserts] at ⊢
      intro h htail
      constructor
      · cases hrp : r.parent with
        | none => simp [hrp]
        | some p =>
            simp [hrp] at h ⊢
            exact hsub p h
      · apply ih (r.key :: l1) (r.key :: l2)
        · intro x hx
          simp at hx ⊢
          rcases hx with rfl | hx
          · exact Or.inl rfl
          · exact Or.inr (hsub x hx)
        · exact htail

theorem sound : InsertPhaseOwnEarlierParent.Goal := by
  intro base fresh hbase hacc
  induction fresh generalizing base with
  | nil => simpa using hbase
  | cons r rs ih =>
      simp [acceptInserts] at hacc
      rw [show base ++ r :: rs = (base ++ [r]) ++ rs by simp]
      apply ih (base ++ [r])
      · intro x hx p hp
        have hx' : x ∈ base ∨ x = r := by simpa using hx
        rcases hx' with hxb | hxr
        · have hb := hbase x hxb p hp
          simpa [List.map_append] using (Or.inl hb : p ∈ base.map Row.key ∨ p ∈ [r.key])
        · subst x
          cases hrp : r.parent with
          | none => simp [hrp] at hp
          | some p0 =>
              simp [hrp] at hp
              subst p0
              have hpbase : p ∈ base.map Row.key := by
                simpa [hrp] using hacc.1
              simpa [List.map_append] using (Or.inl hpbase : p ∈ base.map Row.key ∨ p ∈ [r.key])
      · apply acceptInserts_mono (r.key :: base.map Row.key) ((base ++ [r]).map Row.key)
        · intro x hx
          simp [List.map_append] at hx ⊢
          rcases hx with rfl | hx
          · exact Or.inr rfl
          · exact Or.inl hx
        · exact hacc.2

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_MergeIntegrity_InsertPhaseOwnEarlierParent
