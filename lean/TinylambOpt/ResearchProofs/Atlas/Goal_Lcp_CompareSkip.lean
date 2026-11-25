import TinylambOpt.ResearchGoals.Atlas.TextExecution

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Lcp_CompareSkip

open TinylambOpt.ResearchGoals.Atlas.TextExecution
open TinylambOpt.ResearchGoals.Atlas.TextExecution.Lcp

/-- If the first symbols differ, lexicographic comparison is decided by them,
    so the tails are irrelevant. -/
private theorem compare_cons_of_ne (x z : Nat) (xs zs : List Nat) (h : x ≠ z) :
    compare (x :: xs) (z :: zs) = compare x z := by
  change List.compareLex compare (x :: xs) (z :: zs) = compare x z
  rw [List.compareLex_cons_cons]
  cases hc : compare x z with
  | eq =>
      have hxz : x = z := by
        by_cases hlt : x < z
        · have hh := Nat.compare_eq_lt.mpr hlt
          rw [hc] at hh
          cases hh
        · by_cases hgt : z < x
          · have hh := Nat.compare_eq_gt.mpr hgt
            rw [hc] at hh
            cases hh
          · omega
      exact False.elim (h hxz)
  | lt => simp
  | gt => simp

/-- A binary-search key that stops before the first differing symbol of two
    words sees the same comparison result for both words. -/
theorem sound_Lcp_CompareSkip : CompareSkip.Goal := by
  intro a b needle hab
  induction a generalizing b needle with
  | nil => simp [common] at hab
  | cons x xs ih =>
      cases b with
      | nil => simp [common] at hab
      | cons y ys =>
          cases needle with
          | nil => simp
          | cons z zs =>
              by_cases hxz : x = z
              · subst z
                by_cases hxy : x = y
                · subst y
                  have hprem : common xs ys > common xs zs := by simpa [common] using hab
                  have hrec := ih ys zs hprem
                  simpa using hrec
                · have hxne : x ≠ y := hxy
                  simp [common, hxne] at hab
              · have hxzne : x ≠ z := hxz
                have hcneedle : common (x :: xs) (z :: zs) = 0 := by simp [common, hxzne]
                rw [hcneedle] at hab
                have hxybool : (x == y) = true := by
                  by_cases hbeq : (x == y) = true
                  · exact hbeq
                  · have hc : common (x :: xs) (y :: ys) = 0 := by simp [common, hbeq]
                    rw [hc] at hab
                    simp at hab
                have hxy : x = y := beq_iff_eq.mp hxybool
                subst y
                rw [compare_cons_of_ne x z xs zs hxzne]
                rw [compare_cons_of_ne x z ys zs hxzne]

end TinylambOpt.ResearchProofs.Atlas.Goal_Lcp_CompareSkip
