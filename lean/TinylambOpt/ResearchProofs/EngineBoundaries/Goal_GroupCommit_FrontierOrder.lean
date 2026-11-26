import TinylambOpt.ResearchGoals.EngineBoundaries.GroupCommit

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_GroupCommit_FrontierOrder

open TinylambOpt.ResearchGoals.EngineBoundaries.GroupCommit

theorem sound : FrontierOrder.Goal := by
  intro s es h
  induction es generalizing s with
  | nil => exact h
  | cons e es ih =>
      apply ih
      rcases s with ⟨a,w,d,f⟩
      simp [ordered] at h ⊢
      rcases h with ⟨h1,h2,hf⟩
      cases e with
      | append n =>
          simp [step]
          exact ⟨h1, ⟨by omega, hf⟩⟩
      | write n =>
          simp [step]
          refine ⟨by omega, by omega, ?_⟩
          intro x hx
          exact Nat.le_trans (hf x hx) (by omega)
      | submit =>
          simp [step]
          cases f with
          | none => exact ⟨h1, h2, by simp⟩
          | some x => exact ⟨h1, h2, by simp_all⟩
      | finish =>
          cases f with
          | none =>
              simp [step]
              exact ⟨h1, h2⟩
          | some x =>
              have hxw : x ≤ w := by simpa using hf x rfl
              simp [step]
              omega
      | fail =>
          simp [step]
          exact ⟨h1,h2⟩

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_GroupCommit_FrontierOrder
