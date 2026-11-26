import TinylambOpt.ResearchGoals.Atlas.TextExecution

/-!
SQL string predicates can use encoded or summarized text when the modeled match,
boundaries, and collation are preserved.

Proved

- `EditFilter.ClassLength` — `∀ k s t (keep : Nat → Bool), Edits k s t → ((s.countP
  keep:Int)-t.countP keep).natAbs ≤ k`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_EditFilter_ClassLength

open TinylambOpt.ResearchGoals.Atlas.TextExecution
open TinylambOpt.ResearchGoals.Atlas.TextExecution.EditFilter

/-- Inserting one symbol changes the number of kept symbols by 0 or 1. -/
theorem countP_insert (keep : Nat → Bool) (l r : Word) (x : Nat) :
    (l ++ x :: r).countP keep = (l ++ r).countP keep + (if keep x then 1 else 0) := by
  simp [List.countP_append, List.countP_cons]
  omega

theorem sound_EditFilter_ClassLength : ClassLength.Goal := by
  intro k s t keep hed
  induction hed with
  | refl s =>
      simp
  | drop l r x h ih =>
      rename_i k a
      have hstep : (((l ++ x :: r).countP keep : Int) - ((l ++ r).countP keep : Int)).natAbs ≤ 1 := by
        rw [countP_insert]
        by_cases hx : keep x = true <;> simp [hx] <;> omega
      have hdecomp : ((a.countP keep : Int) - ((l ++ r).countP keep : Int)) =
          (((a.countP keep : Int) - ((l ++ x :: r).countP keep : Int)) +
           (((l ++ x :: r).countP keep : Int) - ((l ++ r).countP keep : Int))) := by
        omega
      rw [hdecomp]
      exact Nat.le_trans (Int.natAbs_add_le _ _) (Nat.add_le_add ih hstep)
  | add l r x h ih =>
      rename_i k a
      have hstep : (((l ++ r).countP keep : Int) - ((l ++ x :: r).countP keep : Int)).natAbs ≤ 1 := by
        rw [countP_insert]
        by_cases hx : keep x = true <;> simp [hx] <;> omega
      have hdecomp : ((a.countP keep : Int) - ((l ++ x :: r).countP keep : Int)) =
          (((a.countP keep : Int) - ((l ++ r).countP keep : Int)) +
           (((l ++ r).countP keep : Int) - ((l ++ x :: r).countP keep : Int))) := by
        omega
      rw [hdecomp]
      exact Nat.le_trans (Int.natAbs_add_le _ _) (Nat.add_le_add ih hstep)
  | subst l r x y h ih =>
      rename_i k a
      have hstep : (((l ++ x :: r).countP keep : Int) - ((l ++ y :: r).countP keep : Int)).natAbs ≤ 1 := by
        rw [countP_insert, countP_insert]
        by_cases hx : keep x = true <;> by_cases hy : keep y = true <;> simp [hx, hy] <;> omega
      have hdecomp : ((a.countP keep : Int) - ((l ++ y :: r).countP keep : Int)) =
          (((a.countP keep : Int) - ((l ++ x :: r).countP keep : Int)) +
           (((l ++ x :: r).countP keep : Int) - ((l ++ y :: r).countP keep : Int))) := by
        omega
      rw [hdecomp]
      exact Nat.le_trans (Int.natAbs_add_le _ _) (Nat.add_le_add ih hstep)

end TinylambOpt.ResearchProofs.Atlas.Goal_EditFilter_ClassLength
