import TinylambOpt.ResearchGoals.DatabaseCorrectness.Publication

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_Publication_LateInstallCannotResurrectDeletedRow

open TinylambOpt.ResearchGoals.DatabaseCorrectness.Publication


theorem sound : LateInstallCannotResurrectDeletedRow.Goal := by
  intro r v a b snap hab hbs
  have ha : a ≤ snap := by omega
  have hf : [⟨a,r,some v⟩,⟨b,r,none⟩].filter
      (fun w : ChangeRow => w.row == r && decide (w.ticket ≤ snap)) =
      [⟨a,r,some v⟩,⟨b,r,none⟩] := by simp [ha, hbs]
  have hba : ¬ b ≤ a := by omega
  simp [readAt, hf, hba]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_Publication_LateInstallCannotResurrectDeletedRow
