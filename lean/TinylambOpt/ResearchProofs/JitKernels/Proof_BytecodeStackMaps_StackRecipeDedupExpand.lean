import TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# StackRecipeDedupExpand

When several stack slots hold the same recipe, the side exit can evaluate each
distinct recipe once (into `compact = stack.eraseDups`) and expand positionally
via `idxOf`.  Since `eraseDups` keeps the same element set and `idxOf` returns
the first matching position, the compact entry at `compact.idxOf r` is
`some (eval regs r)` for every slot, so the expanded evaluation coincides with
mapping `some ∘ eval regs` over the original positional stack. -/

namespace TinylambOpt.ResearchProofs.JitKernels.BytecodeStackMaps_StackRecipeDedupExpand
open TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

private theorem get?_idxOf {α : Type} [BEq α] [LawfulBEq α] (a : α) :
    ∀ (l : List α), a ∈ l → l[l.idxOf a]? = some a := by
  intro l
  induction l with
  | nil => intro h; exact absurd h List.not_mem_nil
  | cons b bs ih =>
    intro h
    rw [List.idxOf_cons]
    by_cases hb : (b == a) = true
    · rw [if_pos hb, List.getElem?_cons_zero]
      exact (LawfulBEq.eq_of_beq hb).symm ▸ rfl
    · rw [if_neg hb, List.getElem?_cons_succ]
      rw [List.mem_cons] at h
      cases h with
      | inl he => exact False.elim (hb (by rw [he]; exact beq_self_eq_true b))
      | inr h' => exact ih h'

private theorem map_congr {α β : Type} {f g : α → β} :
    ∀ (l : List α), (∀ x ∈ l, f x = g x) → l.map f = l.map g
  | [], _ => rfl
  | a :: as, h => by
      rw [List.map_cons, List.map_cons, h a List.mem_cons_self,
          map_congr as (fun x hx => h x (List.mem_cons.mpr (Or.inr hx)))]

theorem sound : StackRecipeDedupExpand.Goal := by
  intro regs stack
  show stack.map (fun r => (stack.eraseDups.map (eval regs))[stack.eraseDups.idxOf r]?) =
      (stack.map (eval regs)).map some
  have key : ∀ r ∈ stack,
      (stack.eraseDups.map (eval regs))[stack.eraseDups.idxOf r]? = some (eval regs r) := by
    intro r hr
    have hcm : r ∈ stack.eraseDups := List.mem_eraseDups.mpr hr
    calc (stack.eraseDups.map (eval regs))[stack.eraseDups.idxOf r]?
        = Option.map (eval regs) stack.eraseDups[stack.eraseDups.idxOf r]? := List.getElem?_map
      _ = Option.map (eval regs) (some r) := by rw [get?_idxOf r stack.eraseDups hcm]
      _ = some (eval regs r) := rfl
  exact (map_congr (f := fun r => (stack.eraseDups.map (eval regs))[stack.eraseDups.idxOf r]?)
      (g := fun r => some (eval regs r)) stack key).trans (List.map_map (g := some) (f := eval regs)).symm
