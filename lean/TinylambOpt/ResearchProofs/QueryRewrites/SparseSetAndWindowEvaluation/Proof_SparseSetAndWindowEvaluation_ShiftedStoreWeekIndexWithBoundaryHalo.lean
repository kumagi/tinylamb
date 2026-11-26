import TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

/-!
# ShiftedStoreWeekIndexWithBoundaryHalo

For a left-side week partition bounded by `[lo, hi]`, only right-side sales
weeks in the shifted halo `[lo+52, hi+52]` can match the `week_seq + 52`
join. Restricting the right index to that halo preserves all matches,
including boundary weeks.
-/

namespace Nov26.Proof_SparseSetAndWindowEvaluation_ShiftedStoreWeekIndexWithBoundaryHalo

open TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation.ShiftedStoreWeekIndexWithBoundaryHalo

private theorem countP_filter_eq {α : Type} (l : List α) (p q : α → Bool)
    (h : ∀ x ∈ l, (p x && q x) = q x) : (l.filter p).countP q = l.countP q := by
  induction l with
  | nil => simp
  | cons a as ih =>
      have ha := h a (by simp)
      have ih' := ih (fun x hx => h x (by simp [hx]))
      by_cases hp : p a = true
      · simp [List.filter_cons, hp, List.countP_cons, ha, ih']
      · have hp' : p a = false := by cases hpa : p a <;> simp_all
        have hqa : q a = false := by
          have := ha
          simp [hp'] at this
          exact this
        simp [List.filter_cons, hp', List.countP_cons, hqa, ih']

theorem sound : Goal := by
  intro left right lo hi hleft
  apply List.map_congr_left
  intro k hk
  have hkbound := hleft k hk
  have hsub : ∀ w ∈ right,
      ((decide (lo + 52 ≤ w) && decide (w ≤ hi + 52)) && (w == k + 52)) = (w == k + 52) := by
    intro w hw
    by_cases heq : w = k + 52
    · subst heq
      have hk1 : lo + 52 ≤ k + 52 := by omega
      have hk2 : k + 52 ≤ hi + 52 := by omega
      simp [hk1, hk2]
    · have hbeq : (w == k + 52) = false := by
        simp [beq_eq_false_iff_ne, heq]
      simp [hbeq]
  simp only [List.count_eq_countP]
  exact countP_filter_eq right _ _ hsub

#print axioms sound

end Nov26.Proof_SparseSetAndWindowEvaluation_ShiftedStoreWeekIndexWithBoundaryHalo
