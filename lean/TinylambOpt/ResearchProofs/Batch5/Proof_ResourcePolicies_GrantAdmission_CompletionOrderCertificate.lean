import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.GrantAdmission.Goal_CompletionOrderCertificate

open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission


set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! A serial completion order never stalls exactly when the scalar headroom
certificate fits the initial free memory. Each job returns its holdings after
completion, so the tail only sees `free + held`; `Nat.sub_le_iff_le_add`
converts that to the headroom subtraction in the certificate. -/

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.CompletionOrderCertificate.Goal := by
  intro jobs free
  induction jobs generalizing free with
  | nil => simp [TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.headroom,
      TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.stalled]
  | cons j js ih =>
      simp only [TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.headroom,
        TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.stalled]
      by_cases hs : j.scratch ≤ free
      · simp [hs, ih]
        constructor
        · intro hle
          rw [Nat.max_le]
          exact ⟨hs, (Nat.sub_le_iff_le_add).2 hle⟩
        · intro hmax
          exact (Nat.sub_le_iff_le_add).1 (Nat.max_le.mp hmax).2
      · simp [hs]
        have hfirst : j.scratch ≤ max j.scratch
            (TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.headroom js - j.held) :=
          Nat.le_max_left _ _
        omega
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.GrantAdmission.Goal_CompletionOrderCertificate
