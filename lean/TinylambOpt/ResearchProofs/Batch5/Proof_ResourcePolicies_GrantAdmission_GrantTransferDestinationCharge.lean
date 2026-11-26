import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.GrantAdmission.Goal_GrantTransferDestinationCharge

open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission


set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! Moving a scratch grant to another pool preserves total process occupancy and
moves its bytes exactly into the destination pool's charge. The source and
destination pools must differ, otherwise a ticket would be charged twice. -/

namespace Nov26.GrantAdmissionTransfer

open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

theorem map_pool_preserves_liveBytes (ts : List Ticket) (owner fromPool toPool : Nat) :
    liveBytes (ts.map (fun t =>
      if t.owner == owner && t.pool == fromPool then {t with pool := toPool} else t)) = liveBytes ts := by
  induction ts with
  | nil => simp [liveBytes]
  | cons t ts ih =>
      have ih' : (List.map (Ticket.bytes ∘ fun t =>
          if t.owner == owner && t.pool == fromPool then {t with pool := toPool} else t) ts).sum =
          (List.map Ticket.bytes ts).sum := by
        simpa [liveBytes] using ih
      by_cases hp : t.owner = owner ∧ t.pool = fromPool
      · simpa [liveBytes, hp] using congrArg (fun x => t.bytes + x) ih'
      · simpa [liveBytes, hp] using congrArg (fun x => t.bytes + x) ih'

theorem transfer_dest_bytes (ts : List Ticket) (owner fromPool toPool : Nat)
    (hne : fromPool ≠ toPool) :
    liveBytes ((ts.map (fun t =>
      if t.owner == owner && t.pool == fromPool then {t with pool := toPool} else t)).filter
        (fun t => t.pool == toPool)) =
      liveBytes (ts.filter (fun t => t.pool == toPool)) +
        liveBytes (ts.filter (fun t => t.owner == owner && t.pool == fromPool)) := by
  induction ts with
  | nil => simp [liveBytes]
  | cons t ts ih =>
      have ih' : (List.map Ticket.bytes (List.filter (fun t => t.pool == toPool)
          (List.map (fun t => if t.owner = owner ∧ t.pool = fromPool then
            {t with pool := toPool} else t) ts))).sum =
          (List.map Ticket.bytes (ts.filter (fun t => t.pool == toPool))).sum +
          (List.map Ticket.bytes (ts.filter (fun t => t.owner == owner && t.pool == fromPool))).sum := by
        simpa [liveBytes] using ih
      simp only [List.map_cons, List.filter_cons]
      by_cases hp : t.owner = owner ∧ t.pool = fromPool
      · simp [liveBytes, hp, hne]
        rw [ih']
        omega
      · by_cases hq : (t.pool == toPool) = true
        · simp [liveBytes, hp, hq]
          rw [ih']
          omega
        · simp [liveBytes, hp, hq]
          rw [ih']

end Nov26.GrantAdmissionTransfer

open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.GrantTransferDestinationCharge.Goal := by
  intro ts fromPool toPool owner cap hne
  dsimp
  constructor
  · exact Nov26.GrantAdmissionTransfer.map_pool_preserves_liveBytes ts owner fromPool toPool
  · rw [Nov26.GrantAdmissionTransfer.transfer_dest_bytes ts owner fromPool toPool hne]
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.GrantAdmission.Goal_GrantTransferDestinationCharge
