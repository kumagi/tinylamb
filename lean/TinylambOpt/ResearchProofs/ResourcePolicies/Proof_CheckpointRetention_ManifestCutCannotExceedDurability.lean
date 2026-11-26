import TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention

namespace TinylambOpt.ResearchProofs.ResourcePolicies.CheckpointRetention.Goal_ManifestCutCannotExceedDurability

open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention


/-! `choose` returns a generation only when `ready` holds: its cut is within
the durable WAL and every snapshot file it names is present on disk. -/

open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention

theorem sound :
    TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention.ManifestCutCannotExceedDurability.Goal := by
  intro gs wal disk g hchoose
  induction gs with
  | nil => simp [choose] at hchoose
  | cons g0 gs ih =>
      by_cases hg0 : ready wal disk g0 = true
      · have hg : g0 = g := by
          simpa [choose, hg0] using hchoose
        subst g
        simpa [ready] using hg0
      · have hrest : choose gs wal disk = some g := by
          simpa [choose, hg0] using hchoose
        exact ih hrest
end TinylambOpt.ResearchProofs.ResourcePolicies.CheckpointRetention.Goal_ManifestCutCannotExceedDurability
