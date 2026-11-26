import TinylambOpt.ResearchGoals.DatabaseCorrectness.CatalogDrop

/-! Handles carry the table incarnation: a stale lease release from the dropped
    table cannot decrement a lease of the recreated table of the same name. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.CatalogDrop.Goal_RecreatedNameRejectsOldLease

open TinylambOpt.ResearchGoals.DatabaseCorrectness.CatalogDrop

theorem sound : RecreatedNameRejectsOldLease.Goal := by
  intro old fresh held owner hrecreate hacquire
  rcases old with ⟨mode, gen, leases, children⟩
  cases mode <;> simp [recreate] at hrecreate
  subst fresh
  simp [acquire] at hacquire
  subst held
  simp [release]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.CatalogDrop.Goal_RecreatedNameRejectsOldLease
