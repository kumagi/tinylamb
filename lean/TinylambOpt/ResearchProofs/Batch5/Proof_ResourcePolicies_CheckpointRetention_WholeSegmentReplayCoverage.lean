import TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CheckpointRetention.Goal_WholeSegmentReplayCoverage

open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention


/-! Whole-segment recycling cannot drop a segment that intersects a record whose
start is at or above the recycle floor. Each intersecting segment ends after
the record start, hence after the floor, so the retention filter keeps it. -/

open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention.WholeSegmentReplayCoverage.Goal := by
  intro segs cp pins r hfloor
  change List.filter (intersects r)
      (List.filter (fun s : Span => decide (floorAt cp pins < s.hi)) segs) =
    List.filter (intersects r) segs
  rw [List.filter_filter]
  apply List.filter_congr
  intro s hs
  by_cases hsi : intersects r s = true
  · have hi : r.lo < s.hi ∧ s.lo < r.hi := by
      simpa [intersects] using hsi
    have hkeep : decide (floorAt cp pins < s.hi) = true := by
      simp [Nat.lt_of_le_of_lt hfloor hi.1]
    simp [hsi, hkeep]
  · simp [hsi]
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CheckpointRetention.Goal_WholeSegmentReplayCoverage
