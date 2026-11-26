import TinylambOpt.ResearchGoals.EngineDecisions.Optimizer

/-!
These models compare concrete implementation choices in planning, execution,
concurrency, or recovery against their stated observable contracts.

Proved

- `TupleAnti` — Lower the filter to presence probes partitioned by NULL pattern. The
  lists specify lookup contents; an implementation can use exact hash sets.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.EngineDecisions.Opt_TupleAnti

open TinylambOpt.ResearchGoals.EngineDecisions.Optimizer.TupleAnti

theorem beq_not_eq (a b : Nat) : (!(a == b)) = (a != b) := by
  rfl

theorem not_decide_eq (a b : Nat) : (!(decide (a = b))) = (a != b) := by
  by_cases h : a == b
  · have hab : a = b := beq_iff_eq.mp h
    simp [hab]
  · have hab : a ≠ b := beq_eq_false_iff_ne.mp (by simpa using h)
    simp [hab]

theorem pair_ne (a b c d : Nat) : (!((a,b) == (c,d))) = (a != c || b != d) := by
  cases h : (a,b) == (c,d) <;> cases hac : a == c <;> cases hbd : b == d <;>
    simp_all [beq_iff_eq, beq_eq_false_iff_ne]

theorem sound : Goal := by
  intro rs q
  rcases q with ⟨q1,q2⟩
  cases q1 <;> cases q2 <;> induction rs with
  | nil => simp [keep, whereRef, build, differs]
  | cons r rs ih =>
    rcases r with ⟨r1,r2⟩
    cases r1 <;> cases r2 <;> simp_all [keep, whereRef, build, differs, beq_not_eq, not_decide_eq, pair_ne, Bool.and_assoc, Bool.and_left_comm, Bool.and_comm]

end TinylambOpt.ResearchProofs.EngineDecisions.Opt_TupleAnti

-- C++ references to this model or its proof.
-- executor/detail/engine_decisions.hpp
