import TinylambOpt.ResearchGoals.November26.Analytics

set_option maxHeartbeats 0
set_option maxRecDepth 10000


namespace TinylambOpt.ResearchProofs.November26.Analytics.Goal_J5
open TinylambOpt.ResearchGoals.November26

theorem char_eq_one_of_bounds (c : Char) (h1 : ¬ c < '1') (h2 : c < '2') : c = '1' := by
  simp [Char.lt_def, UInt32.lt_iff_toNat_lt] at h1 h2
  apply Char.ext
  rw [← UInt32.toNat_inj]
  simp
  omega

theorem pref1 : ∀ L : List Char, (['1'] <+: L) ↔ (¬ L < ['1'] ∧ L < ['2']) := by
  intro L
  cases L with
  | nil => simp
  | cons c cs =>
      simp [List.cons_lt_cons_iff, eq_comm]
      constructor
      · intro h
        subst c
        simp
      · intro h
        exact char_eq_one_of_bounds c (Char.not_lt.mpr h.1) h.2

theorem cons_prefix_interval {c : Char} {P Q : List Char}
    (ih : ∀ T : List Char, (P <+: T) ↔ (¬ T < P ∧ T < Q)) :
    ∀ L : List Char, (c :: P <+: L) ↔ (¬ L < c :: P ∧ L < c :: Q) := by
  intro L
  cases L with
  | nil => simp
  | cons d ds =>
      by_cases hdc : d = c
      · subst d
        simpa using ih ds
      · have hcd : c ≠ d := fun h => hdc h.symm
        by_cases hlt : d < c
        · have hLP : d :: ds < c :: P := List.Lex.rel hlt
          simp [hdc, hcd, hLP]
        · have hnotLQ : ¬ d :: ds < c :: Q := by
            intro h
            rcases (List.cons_lt_cons_iff.mp h) with h' | ⟨h', _⟩
            · exact hlt h'
            · exact hdc h'
          simp [hdc, hcd, hnotLQ]

theorem pref2 : ∀ L : List Char, (['-', '1'] <+: L) ↔ (¬ L < ['-', '1'] ∧ L < ['-', '2']) :=
  cons_prefix_interval pref1

theorem pref3 : ∀ L : List Char, (['a', '-', '1'] <+: L) ↔ (¬ L < ['a', '-', '1'] ∧ L < ['a', '-', '2']) :=
  cons_prefix_interval pref2

theorem pref4 : ∀ L : List Char, (['k', 'a', '-', '1'] <+: L) ↔ (¬ L < ['k', 'a', '-', '1'] ∧ L < ['k', 'a', '-', '2']) :=
  cons_prefix_interval pref3

theorem pref5 : ∀ L : List Char, (['a', 'k', 'a', '-', '1'] <+: L) ↔ (¬ L < ['a', 'k', 'a', '-', '1'] ∧ L < ['a', 'k', 'a', '-', '2']) :=
  cons_prefix_interval pref4

theorem string_lt_iff_list_lt {s t : String} : s < t ↔ s.toList < t.toList := by
  exact String.toByteArray_lt_toByteArray_iff.symm.trans
    String.toList_lt_toList_iff_toByteArray_lt_toByteArray.symm

theorem interval_iff_prefix (s : String) :
    ("aka-1" ≤ s ∧ s < "aka-2") ↔ s.startsWith "aka-1" = true := by
  change (¬ s < "aka-1") ∧ s < "aka-2" ↔ s.startsWith "aka-1" = true
  rw [string_lt_iff_list_lt, string_lt_iff_list_lt]
  simpa [String.startsWith_string_iff] using (pref5 s.toList).symm

theorem sound : TinylambOpt.ResearchGoals.November26.Analytics.J5.Goal := by
  intro aliases
  apply List.filter_congr
  intro r hr
  rw [Bool.eq_iff_iff]
  simpa [Bool.and_eq_true_iff] using (interval_iff_prefix r.1).symm
end TinylambOpt.ResearchProofs.November26.Analytics.Goal_J5
