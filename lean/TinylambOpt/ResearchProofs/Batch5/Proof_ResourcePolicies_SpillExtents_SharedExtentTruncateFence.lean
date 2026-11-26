import TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SpillExtents.Goal_SharedExtentTruncateFence

open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents


/-! Scratch proof: truncation at the highest still-referenced block is safe for
    shared immutable extents. Roof membership is characterized by a bound on
    every referenced id, so flattening the directory loses no fence. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

private theorem max_add_one_le_iff (x m limit : Nat) :
    max x m + 1 ≤ limit ↔ x < limit ∧ m + 1 ≤ limit := by
  omega

private theorem roof_le_iff (ids : List Nat) (limit : Nat) :
    roof ids ≤ limit ↔ ∀ id ∈ ids, id < limit := by
  induction ids with
  | nil => simp [roof]
  | cons x xs ih =>
      cases xs with
      | nil =>
          simp [roof]
          omega
      | cons y ys =>
          have hroof_cons : roof (y :: ys) = ((y :: ys).max?.getD 0) + 1 := by
            simp [roof, List.max?_cons]
          have ih' : ((y :: ys).max?.getD 0) + 1 ≤ limit ↔
              ∀ id ∈ y :: ys, id < limit := by
            simpa [hroof_cons] using ih
          have hroof_all : roof (x :: y :: ys) = max x ((y :: ys).max?.getD 0) + 1 := by
            simp [roof, List.max?_cons]
          rw [hroof_all, max_add_one_le_iff, ih']
          simp

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents.SharedExtentTruncateFence.Goal := by
  intro segments limit
  rw [roof_le_iff]
  simp only [List.mem_flatten]
  constructor
  · intro h seg hseg id hid
    exact h id ⟨seg, hseg, hid⟩
  · intro h id hid
    rcases hid with ⟨seg, hseg, hidseg⟩
    exact h seg hseg id hidseg
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SpillExtents.Goal_SharedExtentTruncateFence
