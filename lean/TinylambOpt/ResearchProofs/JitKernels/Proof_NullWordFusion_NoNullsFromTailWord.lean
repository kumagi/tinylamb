import TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

/-!
**Proved: no nulls from the tail word.** For a packet of at most 64 rows, a
masked validity word equal to the logical window is equivalent to every logical
input lane being non-null. SQL execution may therefore use one word comparison
as the non-null guard on the last input word while ignoring unconstrained
padding bits.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.NullWordFusion_NoNullsFromTailWord
open TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

/-- The logical window has a set bit exactly below its bounded row count. -/
private theorem window_bit (n i : Nat) (hi : i < 64) (hn : n ≤ 64) :
    (window n).getLsbD i = decide (i < n) := by
  unfold window
  rw [BitVec.getLsbD_ofNat, Nat.testBit_two_pow_sub_one, Nat.min_eq_right hn]
  simp [hi]

theorem sound : NoNullsFromTailWord.Goal := by
  intro n m hn
  constructor
  · intro h i hi
    have hb : i < 64 := by omega
    have hbit := congrArg (fun w : W => w.getLsbD i) h
    rw [valid, BitVec.getLsbD_and, BitVec.getLsbD_not, window_bit n i hb hn] at hbit
    cases hmx : m.getLsbD i <;> simp_all
  · intro hm
    apply BitVec.eq_of_getLsbD_eq_iff.mpr
    intro i hi
    rw [valid, BitVec.getLsbD_and, BitVec.getLsbD_not, window_bit n i hi hn]
    by_cases hin : i < n
    · rw [decide_eq_true hin, hm i hin]
      simp_all
    · rw [decide_eq_false hin]
      simp_all

end TinylambOpt.ResearchProofs.JitKernels.NullWordFusion_NoNullsFromTailWord
