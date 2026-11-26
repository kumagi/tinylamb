import TinylambOpt.ResearchGoals.DatabaseCorrectness.CatalogDrop

/-! Once the closing gate is set, late readers cannot join the quiescence wait
    set: releasing an old lease keeps the catalog in the closing mode. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.CatalogDrop.Goal_NoLeaseAdmissionAfterReferenceCheck

open TinylambOpt.ResearchGoals.DatabaseCorrectness.CatalogDrop

theorem sound : NoLeaseAdmissionAfterReferenceCheck.Goal := by
  intro c q owner old hbegin
  rcases c with ⟨mode, gen, leases, children⟩
  cases mode <;> simp [beginDrop] at hbegin
  rcases hbegin with ⟨_, rfl⟩
  simp [release, acquire]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.CatalogDrop.Goal_NoLeaseAdmissionAfterReferenceCheck
