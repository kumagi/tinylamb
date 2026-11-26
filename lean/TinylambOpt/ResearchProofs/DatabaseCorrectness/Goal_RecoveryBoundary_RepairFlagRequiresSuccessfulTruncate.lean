import TinylambOpt.ResearchGoals.DatabaseCorrectness.RecoveryBoundary

/-!
The repaired completion flag is observable only when the log actually extends
past the validated boundary, a writer exists, truncation succeeds, and either
force mode or a benign tail permits repair. This characterizes exactly when
the recovery model may request a replacement checkpoint.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.RecoveryBoundary.Goal_RepairFlagRequiresSuccessfulTruncate

open TinylambOpt.ResearchGoals.DatabaseCorrectness.RecoveryBoundary

theorem sound : RepairFlagRequiresSuccessfulTruncate.Goal := by
  intro v n t forced writer ok
  by_cases hvn : v < n
  · have hnv : ¬ n ≤ v := Nat.not_le_of_gt hvn
    by_cases hf : forced <;> by_cases hw : writer <;>
      by_cases hb : benign t <;> by_cases hok : ok <;>
      simp [finish, hvn, hnv, hf, hw, hb, hok]
  · have hnvle : n ≤ v := by omega
    simp [finish, hvn, hnvle]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.RecoveryBoundary.Goal_RepairFlagRequiresSuccessfulTruncate
