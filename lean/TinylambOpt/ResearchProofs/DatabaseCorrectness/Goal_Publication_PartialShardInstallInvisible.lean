import TinylambOpt.ResearchGoals.DatabaseCorrectness.Publication

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_Publication_PartialShardInstallInvisible

open TinylambOpt.ResearchGoals.DatabaseCorrectness.Publication


theorem sound : PartialShardInstallInvisible.Goal := by
  intro c t old batch snap row hc ht hsnap hb
  have hfence : c.stable < t := (hc.2.2 t ht).1
  have hts : snap < t := by omega
  have hempty : batch.filter (fun w : ChangeRow => w.row == row && decide (w.ticket ≤ snap)) = [] := by
    induction batch with
    | nil => simp
    | cons w ws ih =>
        have hw : w.ticket = t := hb w (by simp)
        have hnot : ¬ w.ticket ≤ snap := by omega
        simp [hnot, ih (by intro x hx; exact hb x (by simp [hx]))]
  simp [readAt, hempty]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_Publication_PartialShardInstallInvisible
