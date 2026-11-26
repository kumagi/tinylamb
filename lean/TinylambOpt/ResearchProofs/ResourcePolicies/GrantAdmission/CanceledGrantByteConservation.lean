import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

namespace TinylambOpt.ResearchProofs.ResourcePolicies.GrantAdmission

open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

private theorem sum_map_filter_add_sum_map_filter_not (f : Ticket → Nat) (p : Ticket → Bool) (ts : List Ticket) :
    ((ts.filter p).map f).sum + ((ts.filter (fun t => !p t)).map f).sum = (ts.map f).sum := by
  induction ts with
  | nil => simp
  | cons t ts ih =>
      by_cases hp : p t = true
      · simp [hp, ih]; omega
      · have hp' : p t = false := by cases h : p t <;> simp_all
        simp [hp', ih]; omega

theorem canceledGrantByteConservation : CanceledGrantByteConservation.Goal := by
  intro ts owner gen
  unfold liveBytes revoke
  dsimp
  let p : Ticket → Bool := fun t => !(t.owner == owner && t.generation == gen)
  have hdouble : (ts.filter p).filter p = ts.filter p := by
    rw [List.filter_filter]
    congr 1; funext t; cases h : p t <;> simp [h]
  have hpart := sum_map_filter_add_sum_map_filter_not Ticket.bytes p ts
  have hnotp : (ts.filter (fun t => t.owner == owner && t.generation == gen)) = ts.filter (fun t => !p t) := by
    congr 1; funext t; simp [p]
  -- rewrite goal using these facts
  change (List.map Ticket.bytes ts).sum - (List.map Ticket.bytes (List.filter p ts)).sum +
      ((List.map Ticket.bytes (List.filter p ts)).sum -
        (List.map Ticket.bytes ((List.filter p ts).filter p)).sum) =
    (List.map Ticket.bytes (List.filter (fun t => t.owner == owner && t.generation == gen) ts)).sum
  rw [hdouble, hnotp]
  omega
