import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.GrantAdmission.Goal_EmergencyFloorAcrossClients

open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission


set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! Independently charged emergency buffers are bounded by the connection count
times the per-client floor. Summing the per-buffer bounds first and then using
monotonicity of multiplication gives the simultaneous peak bound. -/

namespace Nov26.GrantAdmissionEmergencyFloor

theorem sum_le_length_mul (buffers : List Nat) (floor : Nat)
    (hb : ∀ b ∈ buffers, b ≤ floor) : buffers.sum ≤ buffers.length * floor := by
  induction buffers with
  | nil => simp
  | cons b bs ih =>
      simp only [List.sum_cons, List.length_cons]
      have hbnd : b ≤ floor := hb b (by simp)
      have hrest : ∀ x ∈ bs, x ≤ floor := by
        intro x hx
        exact hb x (by simp [hx])
      have ih' := ih hrest
      simp [Nat.succ_mul]
      omega

end Nov26.GrantAdmissionEmergencyFloor

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.EmergencyFloorAcrossClients.Goal := by
  intro buffers floor maxClients hlen hb
  exact Nat.le_trans (Nov26.GrantAdmissionEmergencyFloor.sum_le_length_mul buffers floor hb)
    (Nat.mul_le_mul_right floor hlen)
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.GrantAdmission.Goal_EmergencyFloorAcrossClients
