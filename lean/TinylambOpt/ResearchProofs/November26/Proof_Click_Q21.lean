import TinylambOpt.ResearchGoals.November26.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.November26.Click.Goal_Q21
open TinylambOpt.ResearchGoals.November26.Click

theorem suffix_prefix_append_infix {α : Type} {x y A B : List α}
    (hx : x <:+ A) (hy : y <+: B) : x ++ y <:+: A ++ B := by
  rcases hx with ⟨s, hs⟩
  rcases hy with ⟨t, ht⟩
  refine ⟨s, t, ?_⟩
  rw [← hs, ← ht]
  simp [List.append_assoc]

theorem sound : TinylambOpt.ResearchGoals.November26.Click.Q21.Goal := by
  intro a b
  rw [Bool.eq_iff_iff]
  constructor
  · intro h
    simp only [Bool.or_eq_true]
    have hG : "google".toList <:+: (a ++ b).toList := String.contains_string_iff.mp h
    rw [String.toList_append] at hG
    rw [List.infix_append_iff] at hG
    rcases hG with hga | hgb | hcross
    · exact Or.inl (Or.inl (String.contains_string_iff.mpr hga))
    · exact Or.inl (Or.inr (String.contains_string_iff.mpr hgb))
    · rcases hcross with ⟨l₁, l₂, hGl, hl₁, hl₂⟩
      have hlenG : "google".toList.length = 6 := by decide
      have hlen : l₁.length + l₂.length = 6 := by
        have := congrArg List.length hGl
        simp [List.length_append] at this
        exact this.symm
      by_cases hl₁nil : l₁ = []
      · subst l₁
        rw [List.nil_append] at hGl
        have hgb' : "google".toList <:+: b.toList := by
          rw [hGl]
          exact hl₂.isInfix
        exact Or.inl (Or.inr (String.contains_string_iff.mpr hgb'))
      · by_cases hl₂nil : l₂ = []
        · subst l₂
          rw [List.append_nil] at hGl
          have hga' : "google".toList <:+: a.toList := by
            rw [hGl]
            exact hl₁.isInfix
          exact Or.inl (Or.inl (String.contains_string_iff.mpr hga'))
        · have hl₁pos : 0 < l₁.length := List.ne_nil_iff_length_pos.mp hl₁nil
          have hl₂pos : 0 < l₂.length := List.ne_nil_iff_length_pos.mp hl₂nil
          have hl₁le5 : l₁.length ≤ 5 := by omega
          have hl₂le5 : l₂.length ≤ 5 := by omega
          have hl₁suf : l₁ <:+ a.toList.drop (a.toList.length - 5) := by
            rw [List.suffix_iff_eq_drop] at hl₁
            rw [hl₁]
            apply List.drop_suffix_drop_left
            omega
          have hl₂pre : l₂ <+: b.toList.take 5 :=
            List.prefix_take_iff.mpr ⟨hl₂, hl₂le5⟩
          have hcrossinf : "google".toList <:+:
              a.toList.drop (a.toList.length - 5) ++ b.toList.take 5 := by
            rw [hGl]
            exact List.infix_append_iff.mpr
              (Or.inr (Or.inr ⟨l₁, l₂, rfl, hl₁suf, hl₂pre⟩))
          have hsuf : ((a.takeEnd 5).toString ++ (b.take 5).toString).contains "google" = true := by
            rw [String.contains_string_iff]
            rw [String.toList_append]
            simpa using hcrossinf
          exact Or.inr hsuf
  · intro h
    simp only [Bool.or_eq_true] at h
    apply String.contains_string_iff.mpr
    rw [String.toList_append]
    rcases h with (ha | hb) | hs
    · exact List.infix_append_of_infix_left (String.contains_string_iff.mp ha)
    · exact List.infix_append_of_infix_right (String.contains_string_iff.mp hb)
    · have hs' : "google".toList <:+:
          a.toList.drop (a.toList.length - 5) ++ b.toList.take 5 := by
        rw [String.contains_string_iff] at hs
        rw [String.toList_append] at hs
        simpa using hs
      exact hs'.trans (suffix_prefix_append_infix (List.drop_suffix _ _) (List.take_prefix _ _))
end TinylambOpt.ResearchProofs.November26.Click.Goal_Q21
