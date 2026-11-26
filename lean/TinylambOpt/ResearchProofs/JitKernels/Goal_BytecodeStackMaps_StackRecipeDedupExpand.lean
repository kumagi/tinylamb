import TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

/-
Proves `StackRecipeDedupExpand`: shared duplicate recipes can be expanded back
to the original positional stack via `idxOf`. A SQL exit may compact its
rematerialization map without changing observed slot values.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.BytecodeStackMaps.Goal_StackRecipeDedupExpand

open TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

theorem eval_subst (regs : Regs) (f : Nat → Recipe) (r : Recipe) :
    eval regs (subst f r) = eval (fun i => eval regs (f i)) r := by
  induction r with
  | reg i => rfl
  | lit v => rfl
  | plus a b ihA ihB => simp [subst, eval, ihA, ihB]
  | neg a ihA => simp [subst, eval, ihA]

theorem eval_eq_of_uses (a b : Regs) (r : Recipe) (h : ∀ x ∈ uses r, a x = b x) :
    eval a r = eval b r := by
  induction r with
  | reg i => exact h i (by simp [uses])
  | lit v => rfl
  | plus p q ihp ihq =>
      have hp : ∀ i ∈ uses p, a i = b i := by intro i hi; exact h i (by simp [uses]; exact Or.inl hi)
      have hq : ∀ i ∈ uses q, a i = b i := by intro i hi; exact h i (by simp [uses]; exact Or.inr hi)
      simp [eval]
      rw [ihp hp, ihq hq]
  | neg p ihp =>
      have hp : ∀ i ∈ uses p, a i = b i := by intro i hi; exact h i (by simpa [uses] using hi)
      simp [eval]
      rw [ihp hp]

theorem idxOf_get? (r : Recipe) : ∀ l : List Recipe, r ∈ l → l[l.idxOf r]? = some r
| [], h => by simp at h
| a :: l, h => by
    by_cases har : a == r
    · have haeq : a = r := eq_of_beq har
      subst a
      simp
    · have hrl : r ∈ l := by
        simp at h
        rcases h with hra | hrl
        · subst a
          simp at har
        · exact hrl
      rw [List.idxOf_cons, ite_eq_right har]
      rw [List.getElem?_cons_succ]
      exact idxOf_get? r l hrl

theorem compact_get (regs : Regs) (r : Recipe) (l : List Recipe) (hr : r ∈ l) :
    ((l.map (eval regs))[l.idxOf r]? = some (eval regs r)) := by
  rw [List.getElem?_map, idxOf_get? r l hr]
  rfl

theorem checked_value : ∀ (w : Nat) (regs : Regs) (r : Recipe) (v : Int),
    checked w regs r = some v → v = eval regs r := by
  intro w regs r
  induction r with
  | reg i => intro v h; simpa [checked, eval] using h.symm
  | lit v' => intro v h; simpa [checked, eval] using h.symm
  | plus a b iha ihb =>
      intro v h
      cases hca : checked w regs a with
      | none => simp [checked, hca] at h
      | some x =>
          cases hcb : checked w regs b with
          | none => simp [checked, hca, hcb] at h
          | some y =>
              simp [checked, hca, hcb] at h
              rcases h with ⟨hw, rfl⟩
              simp [eval, iha x hca, ihb y hcb]
  | neg a iha =>
      intro v h
      cases hca : checked w regs a with
      | none => simp [checked, hca] at h
      | some x =>
          simp [checked, hca] at h
          rcases h with ⟨hw, rfl⟩
          simp [eval, iha x hca]

theorem filterMap_optionMap (regs : Regs) (stack : List Recipe) (wanted : List Nat) :
    wanted.filterMap (fun i => Option.map (eval regs) stack[i]?) =
    (wanted.filterMap (fun i => stack[i]?)).map (eval regs) := by
  induction wanted with
  | nil => simp
  | cons i is ih =>
      cases hi : stack[i]? <;> simp [hi, ih]


theorem proof : StackRecipeDedupExpand.Goal := by
  intro regs stack
  rw [List.map_map]
  apply List.map_congr_left
  intro r hr
  have hrmem : r ∈ stack.eraseDups := List.mem_eraseDups.mpr hr
  exact compact_get regs r stack.eraseDups hrmem

end TinylambOpt.ResearchProofs.JitKernels.BytecodeStackMaps.Goal_StackRecipeDedupExpand
