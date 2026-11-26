import TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_BytecodeStackMaps_StackMapLiveRegisterClosure

theorem sound : TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps.StackMapLiveRegisterClosure.Goal := by
  intro a b stack hlive
  have hsingle : ∀ r : TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps.Recipe,
      (∀ reg ∈ TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps.uses r, a reg = b reg) →
      TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps.eval a r =
        TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps.eval b r := by
    intro r
    induction r with
    | reg i =>
        intro hu
        exact hu i (by simp [TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps.uses])
    | lit v =>
        intro hu
        simp [TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps.eval]
    | plus x y ihx ihy =>
        intro hu
        simp only [TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps.eval,
          TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps.uses, List.mem_append] at hu ⊢
        rw [ihx (by intro reg hreg; exact hu reg (Or.inl hreg))]
        rw [ihy (by intro reg hreg; exact hu reg (Or.inr hreg))]
    | neg x ih =>
        intro hu
        simp only [TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps.eval,
          TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps.uses] at hu ⊢
        rw [ih hu]
  induction stack with
  | nil => simp
  | cons r rs ih =>
      have hr := hsingle r (by
        intro reg hreg
        exact hlive reg (by simp [hreg]))
      have hrs : rs.map (TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps.eval a) =
          rs.map (TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps.eval b) := ih (by
        intro reg hreg
        exact hlive reg (by simp [hreg]))
      simp [hr, hrs]

end TinylambOpt.ResearchProofs.JitKernels.Goal_BytecodeStackMaps_StackMapLiveRegisterClosure
