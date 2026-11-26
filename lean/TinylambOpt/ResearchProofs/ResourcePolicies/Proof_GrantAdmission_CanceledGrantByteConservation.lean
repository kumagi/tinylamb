import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-! Closing an owner's grant generation twice releases exactly the bytes that
generation owned: the first revoke strips the matching tickets, the second is a
no-op on the already-filtered list, so the two reported releases add up to the
generation's original byte total even when the grants are split across pools. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.GrantAdmission_CanceledGrantByteConservation
open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

/-- Splitting a byte sum along a Boolean predicate: the predicate-true part
plus the predicate-false part is exactly the whole sum. -/
private theorem sum_split {α : Type} (bytes : α → Nat) (P : α → Bool) :
    ∀ l : List α, ((l.filter P).map bytes).sum + ((l.filter (fun x => !P x)).map bytes).sum
      = (l.map bytes).sum := by
  intro l
  induction l with
  | nil => rfl
  | cons a l ih =>
    cases h : P a with
    | true =>
      rw [List.filter_cons_of_pos h,
        List.filter_cons_of_neg (fun hh => absurd h (by simpa using hh))]
      simp only [List.map_cons, List.sum_cons]
      omega
    | false =>
      rw [List.filter_cons_of_neg (fun hh => absurd hh (by simpa using h)),
        List.filter_cons_of_pos (by simpa using h)]
      simp only [List.map_cons, List.sum_cons]
      omega

/-- Closing the same generation twice: the second revoke sees no matching
tickets left, so it releases nothing additional and the two reported releases
sum to exactly the generation's original bytes. -/
theorem sound : CanceledGrantByteConservation.Goal := by
  intro ts owner gen
  dsimp only [liveBytes, revoke]
  have hidem : (ts.filter (fun t => !(t.owner == owner && t.generation == gen))).filter
      (fun t => !(t.owner == owner && t.generation == gen))
      = ts.filter (fun t => !(t.owner == owner && t.generation == gen)) := by
    rw [List.filter_filter]
    refine congrArg (fun p : Ticket → Bool => ts.filter p) ?_
    funext t
    cases h : (!(t.owner == owner && t.generation == gen)) <;> simp [h]
  rw [hidem]
  have hsplit := sum_split Ticket.bytes (fun t => t.owner == owner && t.generation == gen) ts
  omega

end TinylambOpt.ResearchProofs.ResourcePolicies.GrantAdmission_CanceledGrantByteConservation
