import TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

/-! Fingerprint hits after the first vacant lane are never valid candidates, so
loading a whole control group can exceed the scalar candidate traffic; the
first vacant lane is the fence for the scalar probe. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.ControlByteProbe.Goal_ProbeGroupSpeculativeReadBudget

open TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

theorem refute : ¬ ProbeGroupSpeculativeReadBudget.Goal := by
  intro h
  have hw := h [.live 1 9 7, .empty, .live 1 9 8] (fun _ => 1) 9
  have hc : coherent (fun _ => 1) [.live 1 9 7, .empty, .live 1 9 8] := by
    intro t k v hm
    simp at hm
    rcases hm with (⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩)
    all_goals rfl
  simp [untilEmpty, tagHit] at hw
  exact hw hc

end TinylambOpt.ResearchProofs.JitKernels.ControlByteProbe.Goal_ProbeGroupSpeculativeReadBudget
