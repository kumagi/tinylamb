import TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SharedVocabulary.Goal_LongestPrefixPredecessor

open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary


/-! Scratch proof for the LongestPrefixPredecessor goal. Front-coding a symbol
against a predecessor with a longer common prefix never costs more bytes, even
across varint width cliffs, preserving the SQL-visible value after expansion. -/

open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary
open TinylambOpt.ResearchGoals.Compression.Wire

theorem log2_mono {a b : Nat} (h : a ≤ b) : a.log2 ≤ b.log2 := by
  rcases Nat.eq_zero_or_pos a with ha | ha
  · rw [ha]
    simp
  · have hb : b ≠ 0 := by omega
    have h2 := (Nat.le_log2 (by omega : a ≠ 0)).mp (Nat.le_refl (a.log2))
    exact (Nat.le_log2 hb).mpr (Nat.le_trans h2 h)

theorem width_mono {a b : Nat} (h : a ≤ b) : width a ≤ width b := by
  rcases Nat.eq_zero_or_pos a with ha | ha
  · subst a
    cases b with
    | zero => simp [width]
    | succ b => simp [width]
  · have hb : b ≠ 0 := by omega
    have ha' : ¬(a == 0) := by simpa using (by omega : a ≠ 0)
    have hb' : ¬(b == 0) := by simpa using (by omega : b ≠ 0)
    simp [width, ha', hb']
    exact log2_mono h

theorem width_succ (n : Nat) : width (n + 1) ≤ width n + 1 := by
  cases n with
  | zero => simp [width]
  | succ m =>
      have h0 : ¬(m + 1 + 1 == 0) := by simp
      have h1 : ¬(m + 1 == 0) := by simp
      have h2 : m + 1 < 2 ^ ((m + 1).log2 + 1) :=
        (Nat.log2_lt (show m + 1 ≠ 0 by simp) (k := (m + 1).log2 + 1)).mp (by omega)
      have h3 : m + 1 + 1 ≤ 2 ^ ((m + 1).log2 + 1) := by omega
      have h4 : m + 1 + 1 < 2 ^ ((m + 1).log2 + 2) := by
        rw [show (m + 1).log2 + 2 = (m + 1).log2 + 1 + 1 by omega, Nat.pow_succ]
        omega
      have h5 := (Nat.log2_lt (show m + 1 + 1 ≠ 0 by simp) (k := (m + 1).log2 + 2)).mpr h4
      simp only [width, ite_eq_right h0, ite_eq_right h1]
      omega

theorem ubytes_mono {a b : Nat} (h : a ≤ b) : ubytes a ≤ ubytes b := by
  show max 1 ((width a + 6) / 7) ≤ max 1 ((width b + 6) / 7)
  have h1 : 1 ≤ max 1 ((width b + 6) / 7) := Nat.le_max_left _ _
  have h2 : (width a + 6) / 7 ≤ (width b + 6) / 7 :=
    Nat.div_le_div_right (Nat.add_le_add_right (width_mono h) 6)
  have h3 : (width b + 6) / 7 ≤ max 1 ((width b + 6) / 7) := Nat.le_max_right _ _
  exact Nat.max_le.mpr ⟨h1, Nat.le_trans h2 h3⟩

theorem ubytes_succ (n : Nat) : ubytes (n + 1) ≤ ubytes n + 1 := by
  show max 1 ((width (n + 1) + 6) / 7) ≤ max 1 ((width n + 6) / 7) + 1
  have hw : width (n + 1) ≤ width n + 1 := width_succ n
  have hd : (width (n + 1) + 6) / 7 ≤ (width n + 6) / 7 + 1 := by
    have hx : width (n + 1) + 6 ≤ 7 * ((width n + 6) / 7 + 1) := by
      have hmod := Nat.div_add_mod (width n + 6) 7
      omega
    exact Nat.div_le_of_le_mul hx
  have hm : (width n + 6) / 7 ≤ max 1 ((width n + 6) / 7) := Nat.le_max_right _ _
  exact Nat.max_le.mpr ⟨by omega, by omega⟩

theorem common_le_right (prev s : Symbol) : common prev s ≤ s.length := by
  induction prev generalizing s with
  | nil => simp [common]
  | cons p ps ih =>
      cases s with
      | nil => simp [common]
      | cons x xs =>
          by_cases h : p == x
          · simp [common, h]
            have := ih xs
            omega
          · simp [common, h]

theorem cost_eq (prev s : Symbol) :
    cost prev s =
      ubytes (common prev s) + ubytes (s.length - common prev s) +
        (s.length - common prev s) := by
  simp [cost, front, List.length_drop]

theorem cost_index_mono (s : Symbol) (k : Nat) : ∀ l, k ≤ l → l ≤ s.length →
    ubytes l + ubytes (s.length - l) + (s.length - l) ≤
      ubytes k + ubytes (s.length - k) + (s.length - k) := by
  intro l hkl hls
  induction l generalizing k with
  | zero =>
      have : k = 0 := by omega
      subst k
      exact Nat.le_refl _
  | succ l ih =>
      by_cases hk : k = l + 1
      · subst k
        exact Nat.le_refl _
      · have hkl' : k ≤ l := by omega
        have hls' : l ≤ s.length := by omega
        have hlt : l < s.length := by omega
        have hstep :
            ubytes (l + 1) + ubytes (s.length - (l + 1)) + (s.length - (l + 1)) ≤
              ubytes l + ubytes (s.length - l) + (s.length - l) := by
          have hsub : s.length - (l + 1) = (s.length - l) - 1 := by omega
          rw [hsub]
          have hu1 := ubytes_succ l
          have hu2 := ubytes_mono (show (s.length - l) - 1 ≤ s.length - l by omega)
          omega
        exact Nat.le_trans hstep (ih k hkl' hls')

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary.LongestPrefixPredecessor.Goal := by
  intro a b s h
  rw [cost_eq a s, cost_eq b s]
  exact cost_index_mono s (common a s) (common b s) h (common_le_right b s)
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SharedVocabulary.Goal_LongestPrefixPredecessor
