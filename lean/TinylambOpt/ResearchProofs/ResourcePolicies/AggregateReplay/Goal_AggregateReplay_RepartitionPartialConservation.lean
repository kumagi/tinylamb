import TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Splitting one partial record into two records of the same group key conserves the group
state: `combine` is associative and commutative per component, and the group filter keeps
or drops both pieces together (their keys are equal), so the merge order does not matter.
Repartitioning a spilled partial across records is therefore invisible to replay. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_AggregateReplay_RepartitionPartialConservation
open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

theorem sound : RepartitionPartialConservation.Goal := by
  intro a b tail k hk
  unfold replay
  by_cases h : (a.key == k) = true
  · have hb : (b.key == k) = true := by rw [← hk]; exact h
    obtain ⟨sa, ca, has⟩ : ∃ sa ca, a.partialState = (sa, ca) := ⟨_, _, rfl⟩
    obtain ⟨sb, cb, hbs⟩ : ∃ sb cb, b.partialState = (sb, cb) := ⟨_, _, rfl⟩
    have hstep : combine (combine (0, 0) a.partialState) b.partialState =
        combine (0, 0) (combine a.partialState b.partialState) := by
      rw [has, hbs]
      show (0 + sa + sb, 0 + ca + cb) = (0 + (sa + sb), 0 + (ca + cb))
      rw [Prod.mk.injEq]; exact ⟨by omega, by omega⟩
    have hl : (a :: b :: tail).filter (fun r => r.key == k) = a :: b ::
        tail.filter (fun r => r.key == k) := by
      rw [List.filter_cons, List.filter_cons, if_pos h, if_pos hb]
    have hr : List.filter (fun r => r.key == k)
        ({ a with partialState := combine a.partialState b.partialState } :: tail) =
        { a with partialState := combine a.partialState b.partialState } ::
        tail.filter (fun r => r.key == k) := by
      rw [List.filter_cons, if_pos (by simpa using h)]
    rw [hl, hr, List.map_cons, List.map_cons, List.foldl_cons, List.foldl_cons]
    exact congrArg (fun s => List.foldl combine s
      ((tail.filter (fun r => r.key == k)).map Record.partialState)) hstep
  · have hnf : (a.key == k) = false := by
      cases h2 : a.key == k with
      | true => exact absurd h2 (by simpa using h)
      | false => exact rfl
    have hbnf : (b.key == k) = false := by
      cases h2 : b.key == k with
      | true =>
          have : (a.key == k) = true := by rw [hk]; exact h2
          exact absurd this (by simpa using h)
      | false => exact rfl
    have hl : (a :: b :: tail).filter (fun r => r.key == k) =
        tail.filter (fun r => r.key == k) := by
      rw [List.filter_cons, List.filter_cons, if_neg (by simpa using hnf),
        if_neg (by simpa using hbnf)]
    have hr : List.filter (fun r => r.key == k)
        ({ a with partialState := combine a.partialState b.partialState } :: tail) =
        tail.filter (fun r => r.key == k) := by
      rw [List.filter_cons, if_neg (by simpa using hnf)]
    rw [hl, hr]

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_AggregateReplay_RepartitionPartialConservation
