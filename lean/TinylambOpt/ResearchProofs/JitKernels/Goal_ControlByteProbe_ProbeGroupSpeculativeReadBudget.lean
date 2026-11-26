import TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

/-!
A matching fingerprint beyond the first vacant control byte is not a probe
candidate, so loading every speculative tag in the group can exceed the traffic
of the scalar probe prefix.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ControlByteProbe_ProbeGroupSpeculativeReadBudget

open TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe.ProbeGroupSpeculativeReadBudget.Goal := by
  intro h
  let slots : List Slot := [.empty, .live 3 9 7]
  have hf : coherent (fun _ => 3) slots := by
    intro t k v hm
    simp [slots] at hm
    rcases hm with rfl | rfl <;> rfl
  have h' := h slots (fun _ => 3) 9 hf
  have hne : ¬ ((slots.filter (tagHit 3)).length ≤ ((untilEmpty slots).filter (tagHit 3)).length) := by
    native_decide
  exact hne h'

end TinylambOpt.ResearchProofs.JitKernels.Goal_ControlByteProbe_ProbeGroupSpeculativeReadBudget
