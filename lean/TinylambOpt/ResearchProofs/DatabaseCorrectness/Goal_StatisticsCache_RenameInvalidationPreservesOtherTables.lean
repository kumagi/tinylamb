import TinylambOpt.ResearchGoals.DatabaseCorrectness.StatisticsCache

/-! Renaming invalidates the two names without disturbing other tables.

A key whose lower-cased spelling differs from both the old and the new name
survives both filter passes at exactly the same position. SQL execution may
drop the stale bindings of a renamed table while keeping the first visible
binding of every unrelated table in the context cache. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.StatisticsCache_RenameInvalidationPreservesOtherTables

open TinylambOpt.ResearchGoals.DatabaseCorrectness.StatisticsCache

theorem sound : RenameInvalidationPreservesOtherTables.Goal := by
  intro xs old fresh k h1 h2
  induction xs with
  | nil => simp [purge]
  | cons x xs ih =>
    by_cases hxk : x.1 = k
    · subst k
      have ho : (x.1.toLower != old.toLower) = true := by simp [h1]
      have hf : (x.1.toLower != fresh.toLower) = true := by simp [h2]
      change List.lookup x.1 (List.filter (fun e : String × Nat => e.1.toLower != fresh.toLower)
          (List.filter (fun e : String × Nat => e.1.toLower != old.toLower) (x :: xs))) = List.lookup x.1 (x :: xs)
      rw [List.filter_cons, ite_eq_left ho]
      rw [List.filter_cons, ite_eq_left hf]
      rw [List.lookup_cons, List.lookup_cons]
      simp only [beq_self_eq_true]
    · by_cases ho : (x.1.toLower != old.toLower) = true
      · by_cases hf : (x.1.toLower != fresh.toLower) = true
        · change List.lookup k (List.filter (fun e : String × Nat => e.1.toLower != fresh.toLower)
            (List.filter (fun e : String × Nat => e.1.toLower != old.toLower) (x :: xs))) = List.lookup k (x :: xs)
          rw [List.filter_cons, ite_eq_left ho]
          rw [List.filter_cons, ite_eq_left hf]
          rw [List.lookup_cons, List.lookup_cons]
          have hxk' : k ≠ x.1 := by simpa [eq_comm] using hxk
          have hbeq : (k == x.1) = false := by simp [hxk']
          rw [hbeq]
          exact ih
        · change List.lookup k (List.filter (fun e : String × Nat => e.1.toLower != fresh.toLower)
            (List.filter (fun e : String × Nat => e.1.toLower != old.toLower) (x :: xs))) = List.lookup k (x :: xs)
          rw [List.filter_cons, ite_eq_left ho]
          rw [List.filter_cons, ite_eq_right hf]
          rw [List.lookup_cons]
          have hxk' : k ≠ x.1 := by simpa [eq_comm] using hxk
          have hbeq : (k == x.1) = false := by simp [hxk']
          rw [hbeq]
          exact ih
      · change List.lookup k (List.filter (fun e : String × Nat => e.1.toLower != fresh.toLower)
          (List.filter (fun e : String × Nat => e.1.toLower != old.toLower) (x :: xs))) = List.lookup k (x :: xs)
        rw [List.filter_cons, ite_eq_right ho]
        rw [List.lookup_cons]
        have hxk' : k ≠ x.1 := by simpa [eq_comm] using hxk
        have hbeq : (k == x.1) = false := by simp [hxk']
        rw [hbeq]
        exact ih

end TinylambOpt.ResearchProofs.DatabaseCorrectness.StatisticsCache_RenameInvalidationPreservesOtherTables
