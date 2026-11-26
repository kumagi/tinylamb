import TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# StackMapLiveRegisterClosure

A side exit may evacuate (spill) only the registers that occur in some recipe
reachable from its logical stack slots: if two register files agree on exactly
those registers (`stack.flatMap uses`), the reconstructed stacks agree. -/

namespace TinylambOpt.ResearchProofs.JitKernels.BytecodeStackMaps_StackMapLiveRegisterClosure
open TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

private theorem eval_congr (a b : Regs) :
    ∀ (r : Recipe), (∀ i ∈ uses r, a i = b i) → eval a r = eval b r := by
  intro r
  induction r with
  | reg i => exact fun h => h i (by simp [uses])
  | lit v => exact fun _ => rfl
  | plus x y ihx ihy =>
    intro h
    have hx : eval a x = eval b x :=
      ihx fun i hi => h i (List.mem_append.mpr (Or.inl hi))
    have hy : eval a y = eval b y :=
      ihy fun i hi => h i (List.mem_append.mpr (Or.inr hi))
    simp only [eval, hx, hy]
  | neg x ih =>
    intro h
    simp only [eval, ih h]

theorem sound : StackMapLiveRegisterClosure.Goal := by
  intro a b stack h
  induction stack with
  | nil => rfl
  | cons r rs ih =>
    simp only [List.map_cons]
    have hr : eval a r = eval b r :=
      eval_congr a b r fun i hi =>
        h i (List.mem_flatMap.mpr ⟨r, List.mem_cons_self, hi⟩)
    have hrs : ∀ i ∈ rs.flatMap uses, a i = b i := by
      intro i hi
      rcases List.mem_flatMap.mp hi with ⟨j, hjr, hij⟩
      exact h i (List.mem_flatMap.mpr ⟨j, List.mem_cons.mpr (Or.inr hjr), hij⟩)
    rw [hr, ih hrs]
