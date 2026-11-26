import TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue
/-!
Registers the checked result for `WaitQueue.ArrivalIndependentCohortWait.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WaitQueue_ArrivalIndependentCohortWait
open TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue
open TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue.ArrivalIndependentCohortWait

private theorem mem_takeWhile_prop {α : Type} {p : α → Bool} {l : List α} {x : α}
    (hx : x ∈ List.takeWhile p l) : p x = true := by
  induction l with
  | nil => simp at hx
  | cons y ys ih =>
      simp only [List.takeWhile] at hx
      split at hx
      · rename_i hp
        simp at hx
        rcases hx with rfl | hx
        · exact hp
        · exact ih hx
      · simp at hx

theorem sound : Goal := by
  intro q arrivals t ht
  unfold waitTurns
  have htake : (q ++ arrivals).takeWhile (fun x => x != t) = q.takeWhile (fun x => x != t) := by
    rw [List.takeWhile_append]
    have hne : (q.takeWhile (fun x => x != t)).length ≠ q.length := by
      intro heq
      have hpfx : q.takeWhile (fun x => x != t) <+: q := List.takeWhile_prefix _
      have hall : q.takeWhile (fun x => x != t) = q := List.IsPrefix.eq_of_length hpfx heq
      have hall' : ∀ x ∈ q, (fun x => x != t) x = true := by
        intro x hx
        have hmem : x ∈ q.takeWhile (fun x => x != t) := by rw [hall]; exact hx
        exact mem_takeWhile_prop (p := fun x => x != t) (l := q) hmem
      have := hall' t ht
      simp at this
    simp [hne]
  rw [htake]
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WaitQueue_ArrivalIndependentCohortWait
