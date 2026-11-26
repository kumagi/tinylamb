import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

/-!
A fixed-width SQL expression fast path depends on the integer range and representation
facts used to justify its machine operations.

Proved

- `DisjointOr.CarryFree` — `∀ x y v, ir op (x,y)=some v → v=x+y`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_DisjointOr_CarryFree

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises


theorem sound : DisjointOr.CarryFree.Goal := by
  intro x y v h
  unfold ir at h
  dsimp [DisjointOr.op] at h
  simp at h
  obtain ⟨h1, h2⟩ := h
  rw [← h2]
  exact (BitVec.add_eq_or_of_and_eq_zero x y h1).symm

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_DisjointOr_CarryFree
