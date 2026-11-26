import TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.WaitQueue.Goal_ArrivalIndependentCohortWait

open TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue


/-! Appending future arrivals after an already queued ticket cannot change the
prefix of tickets ahead of that ticket, so its turn count is fixed. -/

namespace Scratch.ArrivalIndependentCohortWait

open TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue

private theorem takeWhile_append_stop : ∀ (q arr : List Ticket) (p : Ticket → Bool),
    (∃ x ∈ q, p x = false) → (q ++ arr).takeWhile p = q.takeWhile p := by
  intro q
  induction q with
  | nil => intro arr p h; simp at h
  | cons x xs ih =>
      intro arr p h
      by_cases hx : p x = false
      · simp [List.takeWhile, hx]
      · have hxtrue : p x = true := by cases h : p x <;> simp_all
        have hex : ∃ y ∈ xs, p y = false := by
          rcases h with ⟨y, hy, hp⟩
          cases hy with
          | head => simp [hxtrue] at hp
          | tail _ hys => exact ⟨y, hys, hp⟩
        simp [List.takeWhile, hxtrue, ih arr p hex]

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue.ArrivalIndependentCohortWait.Goal := by
  intro q arrivals t ht
  unfold waitTurns
  have hex : ∃ x ∈ q, (x != t) = false := ⟨t, ht, by simp⟩
  rw [takeWhile_append_stop q arrivals (fun x => x != t) hex]

end Scratch.ArrivalIndependentCohortWait
end TinylambOpt.ResearchProofs.ResourcePolicies.WaitQueue.Goal_ArrivalIndependentCohortWait
