import Std
import TinylambOpt.ProposedGoals.GeneratedExprTierSplit

/-!
Proved

- `Goal` — Staging an unsupported expression into a bytecode pipeline plus host calls
  preserves values, NULL and the first error.

The reference model is defined in `TinylambOpt.ProposedGoals.GeneratedExprTierSplit`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_GeneratedExprTierSplit

open TinylambOpt.ProposedGoals.GeneratedExprTierSplit

/-- The AST `add` node is exactly `addVals` of its two operand values: the
    short-circuit error checks agree because `addVals` propagates the first
    (i.e. the left operand's) error. -/
private theorem eval_add (a b : Expr) (row : List (Option Int)) :
    eval (.add a b) row = addVals (eval a row) (eval b row) := by
  cases ha : eval a row <;> cases hb : eval b row <;>
    simp [eval, ha, hb, addVals]

/-- Generalized continuation invariant: compiling `e` and then running the
    continuation `ops` leaves the value of `e` on top of the incoming stack. -/
private theorem runOps_compile_cont (e : Expr) (ops : List Op)
    (row : List (Option Int)) (st : List Val) :
    runOps (compile e ++ ops) row st = runOps ops row (eval e row :: st) := by
  induction e generalizing ops st with
  | col i =>
      simp [compile, eval, runOps]
  | konst v =>
      simp [compile, eval, runOps, toVal]
  | add a b iha ihb =>
      simp only [compile, List.append_assoc]
      rw [ihb (ops := compile a ++ ([Op.add] ++ ops)) (st := st)]
      rw [iha (ops := [Op.add] ++ ops) (st := eval b row :: st)]
      rw [eval_add]
      cases eval a row <;> cases eval b row <;> simp [runOps, addVals]
  | host tag a ih =>
      simp only [compile]
      rw [List.append_assoc]
      rw [ih (ops := [Op.host tag] ++ ops) (st := st)]
      cases h : eval a row <;> simp [runOps, eval, hostFn, h]

theorem sound_GeneratedExprTierSplit :
    TinylambOpt.ProposedGoals.GeneratedExprTierSplit.Goal := by
  intro e row
  simpa [runOps] using (runOps_compile_cont e [] row [])

end TinylambOpt.ResearchProofs.Atlas.Goal_GeneratedExprTierSplit
