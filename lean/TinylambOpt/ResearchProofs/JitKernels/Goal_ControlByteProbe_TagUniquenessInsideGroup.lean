import TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

/-! A unique fingerprint match does not imply a key match: the coherent tag is
attached to a different key. Skipping the full-key comparison would return a
false hit. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.ControlByteProbe.Goal_TagUniquenessInsideGroup

open TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

theorem refute : ¬ TagUniquenessInsideGroup.Goal := by
  intro h
  have hc : coherent (fun _ => 1) [.live 1 5 7] := by
    intro t k v hm
    simp at hm
    rcases hm with ⟨rfl, rfl, rfl⟩
    rfl
  have hw := h [.live 1 5 7] 9 (fun _ => 1) hc
  simp [untilEmpty, tagHit, probe, value] at hw

end TinylambOpt.ResearchProofs.JitKernels.ControlByteProbe.Goal_TagUniquenessInsideGroup
