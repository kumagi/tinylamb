import TinylambOpt.ResearchGoals.JitKernels.DispatchTables

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! A CASE arm whose half-open range is contained in an earlier arm's range is
dead: whenever it would match, the earlier arm already matched. Deleting it
preserves the dispatch result even though its result code differs, because
intervening arms keep their original priority. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_ShadowedArmDeletion

open TinylambOpt.ResearchGoals.JitKernels.DispatchTables

theorem sound : TinylambOpt.ResearchGoals.JitKernels.DispatchTables.ShadowedArmDeletion.Goal := by
  intro a b middle tail x hablo habhi
  have hbsub : holds b x = true → holds a x = true := by
    intro hb
    have hb' : b.lo ≤ x ∧ x < b.hi := by simpa [holds] using hb
    simp [holds]
    omega
  induction middle with
  | nil =>
      by_cases ha : holds a x
      · simp [pick, ha]
      · have hb : holds b x = false := by
          cases hbv : holds b x <;> simp_all
        simp [pick, ha, hb]
  | cons m ms ih =>
      by_cases ha : holds a x
      · simp [pick, ha]
      · by_cases hm : holds m x
        · simp [pick, ha, hm]
        · simpa [pick, ha, hm] using ih

end TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_ShadowedArmDeletion
