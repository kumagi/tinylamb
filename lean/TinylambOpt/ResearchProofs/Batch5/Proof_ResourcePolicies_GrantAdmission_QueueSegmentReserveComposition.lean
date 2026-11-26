import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.GrantAdmission.Goal_QueueSegmentReserveComposition

open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission


set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! Reserve composition for a queued suffix: a prefix raises the minimum free
memory of the whole queue exactly by releasing its own holdings before the
suffix starts. The proof uses distributivity of truncated subtraction over
`max`. -/

namespace Nov26.GrantAdmissionQueueSegment

open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

theorem max_sub_right (a b c : Nat) : max a b - c = max (a - c) (b - c) := by
  by_cases h : a ≤ b
  · rw [Nat.max_eq_right h, Nat.max_eq_right (Nat.sub_le_sub_right h c)]
  · have hba : b ≤ a := Nat.le_of_not_ge h
    rw [Nat.max_eq_left hba, Nat.max_eq_left (Nat.sub_le_sub_right hba c)]

end Nov26.GrantAdmissionQueueSegment

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.QueueSegmentReserveComposition.Goal := by
  intro front back
  induction front with
  | nil =>
      simp [TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.headroom]
  | cons j front ih =>
      change TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.headroom
          (j :: (front ++ back)) = _
      simp only [TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.headroom,
        List.map_cons, List.sum_cons]
      rw [ih]
      rw [Nov26.GrantAdmissionQueueSegment.max_sub_right]
      rw [← Nat.max_assoc]
      congr 1
      omega
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.GrantAdmission.Goal_QueueSegmentReserveComposition
