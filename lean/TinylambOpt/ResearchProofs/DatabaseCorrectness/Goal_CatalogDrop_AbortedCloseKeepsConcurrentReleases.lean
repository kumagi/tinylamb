import TinylambOpt.ResearchGoals.DatabaseCorrectness.CatalogDrop

/-! Aborting a close restores the open catalog gate without undoing releases
    that were made while the gate was closing. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.CatalogDrop.Goal_AbortedCloseKeepsConcurrentReleases

open TinylambOpt.ResearchGoals.DatabaseCorrectness.CatalogDrop

theorem sound : AbortedCloseKeepsConcurrentReleases.Goal := by
  intro c q owner hbegin
  rcases c with ⟨mode, gen, leases, children⟩
  cases mode <;> simp [beginDrop] at hbegin
  rcases hbegin with ⟨_, rfl⟩
  simp [abortDrop, release]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.CatalogDrop.Goal_AbortedCloseKeepsConcurrentReleases
