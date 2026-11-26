import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.GrantAdmission.Goal_CanceledGrantByteConservation

open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission


set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

/-! Closing a generation twice returns its bytes exactly once. The first close
removes every matching ticket; the second close finds none of them, so its
refund is zero. Byte sums split additively across a predicate and its negation. -/

namespace Nov26.GrantAdmissionConservation

open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

theorem sum_filter_add_sum_filter_not (ts : List Ticket) (p : Ticket → Bool) :
    liveBytes (ts.filter p) + liveBytes (ts.filter (fun t => !p t)) = liveBytes ts := by
  induction ts with
  | nil => simp [liveBytes]
  | cons t ts ih =>
      by_cases hp : p t = true
      · have hpn : !p t = false := by simp [hp]
        simp [liveBytes, hp, hpn] at ih ⊢
        omega
      · have hpf : p t = false := by cases hpt : p t <;> simp_all
        have hpn : !p t = true := by simp [hpf]
        simp [liveBytes, hp, hpn] at ih ⊢
        omega

theorem revoke_idem (owner gen : Nat) (ts : List Ticket) :
    revoke owner gen (revoke owner gen ts) = revoke owner gen ts := by
  simp [revoke, List.filter_filter]

end Nov26.GrantAdmissionConservation

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.CanceledGrantByteConservation.Goal := by
  intro ts owner gen
  dsimp
  let p : Ticket → Bool := fun t => t.owner == owner && t.generation == gen
  have hrev : revoke owner gen ts = ts.filter (fun t => !p t) := by
    rfl
  have hsum := Nov26.GrantAdmissionConservation.sum_filter_add_sum_filter_not ts p
  rw [Nov26.GrantAdmissionConservation.revoke_idem]
  rw [hrev]
  change (liveBytes ts - liveBytes (ts.filter (fun t => !p t))) +
      (liveBytes (ts.filter (fun t => !p t)) - liveBytes (ts.filter (fun t => !p t))) =
    liveBytes (ts.filter p)
  omega
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.GrantAdmission.Goal_CanceledGrantByteConservation
