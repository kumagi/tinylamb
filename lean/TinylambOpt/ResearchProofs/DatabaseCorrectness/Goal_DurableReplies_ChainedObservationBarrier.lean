import TinylambOpt.ResearchGoals.DatabaseCorrectness.DurableReplies

/-! Flattening inherited transitive ends cannot delay a reply when they are all
    covered by an already observed producer end. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.DurableReplies.Goal_ChainedObservationBarrier

open TinylambOpt.ResearchGoals.DatabaseCorrectness.DurableReplies

private theorem maxD_le_iff (l : List Nat) (d : Nat) :
    l.max?.getD 0 ≤ d ↔ ∀ x ∈ l, x ≤ d := by
  cases hm : l.max? with
  | none =>
      have hl : l = [] := List.max?_eq_none_iff.mp hm
      simp [hl]
  | some a =>
      simp only [Option.getD_some]
      exact List.max?_le_iff hm

private theorem le_maxD_of_mem {l : List Nat} {x : Nat} (h : x ∈ l) : x ≤ l.max?.getD 0 := by
  cases hm : l.max? with
  | none =>
      have hl : l = [] := List.max?_eq_none_iff.mp hm
      subst l
      simp at h
  | some a =>
      simp only [Option.getD_some]
      exact (List.max?_eq_some_iff.mp hm).2 x h

theorem sound : ChainedObservationBarrier.Goal := by
  intro t producer inherited hmem hle
  apply Nat.le_antisymm
  · dsimp [target]
    apply Nat.max_le.mpr
    constructor
    · exact Nat.le_max_left _ _
    · rw [maxD_le_iff]
      intro x hx
      simp only [List.mem_append] at hx
      rcases hx with hx | hx
      · exact Nat.le_trans (Nat.le_trans (hle x hx) (le_maxD_of_mem hmem)) (Nat.le_max_right _ _)
      · exact Nat.le_trans (le_maxD_of_mem hx) (Nat.le_max_right _ _)
  · dsimp [target]
    apply Nat.max_le.mpr
    constructor
    · exact Nat.le_max_left _ _
    · rw [maxD_le_iff]
      intro x hx
      exact Nat.le_trans (le_maxD_of_mem (by simp [hx])) (Nat.le_max_right _ _)

end TinylambOpt.ResearchProofs.DatabaseCorrectness.DurableReplies.Goal_ChainedObservationBarrier
