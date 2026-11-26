import TinylambOpt.ResearchGoals.DatabaseCorrectness.CatalogDrop

/-! Serialization of child-reference creation and parent-close: the catalog gate
    admits only one of the two orders, so a child cannot be added after the
    parent starts closing and a drop cannot start while a child exists. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.CatalogDrop.Goal_ForeignReferenceDropInterlock

open TinylambOpt.ResearchGoals.DatabaseCorrectness.CatalogDrop

theorem sound : ForeignReferenceDropInterlock.Goal := by
  intro c child
  rcases c with ⟨mode, gen, leases, children⟩
  cases mode <;> cases children <;> simp [beginDrop, reference]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.CatalogDrop.Goal_ForeignReferenceDropInterlock
