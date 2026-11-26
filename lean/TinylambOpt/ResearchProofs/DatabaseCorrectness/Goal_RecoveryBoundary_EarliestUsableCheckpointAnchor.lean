import TinylambOpt.ResearchGoals.DatabaseCorrectness.RecoveryBoundary

/-!
Combining two checkpoint anchors needs the earlier of the two replay points.
A positive request always selects `min req snapshot`, so a start is available
exactly when both callers' first position is at or before that minimum; SQL
recovery may then open WAL from that position without losing either commit.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.RecoveryBoundary.Goal_EarliestUsableCheckpointAnchor

open TinylambOpt.ResearchGoals.DatabaseCorrectness.RecoveryBoundary

theorem sound : EarliestUsableCheckpointAnchor.Goal := by
  intro first req snapshot hreq
  have hnz : req ≠ 0 := Nat.ne_of_gt hreq
  simp [startAt, anchor, hnz]
  by_cases h : min req snapshot < first
  · have hnot : ¬ (first ≤ req ∧ first ≤ snapshot) := by
      intro hboth
      have hle : first ≤ min req snapshot := Nat.le_min.mpr hboth
      omega
    simp [h, hnot]
  · have hle : first ≤ min req snapshot := Nat.le_of_not_gt h
    have h1 : first ≤ req := Nat.le_trans hle (Nat.min_le_left _ _)
    have h2 : first ≤ snapshot := Nat.le_trans hle (Nat.min_le_right _ _)
    have hmax : max first (min req snapshot) = min req snapshot := Nat.max_eq_right hle
    simp [h, h1, h2, hmax]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.RecoveryBoundary.Goal_EarliestUsableCheckpointAnchor
