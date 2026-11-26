import TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_NullWordFusion_NoNullsFromTailWord

open TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

theorem window_bit (n i : Nat) (hi : i < 64) (hn : n ≤ 64) :
    (window n).getLsbD i = decide (i < n) := by
  unfold window
  rw [BitVec.getLsbD_ofNat, Nat.testBit_two_pow_sub_one, Nat.min_eq_right hn]
  simp [hi]

theorem sound : NoNullsFromTailWord.Goal := by
  intro n m hn
  constructor
  · intro heq i hi
    have hb := congrArg (fun (x : W) => x.getLsbD i) heq
    simp [valid, BitVec.getLsbD_and, BitVec.getLsbD_not,
      window_bit n i (by omega) hn, hi] at hb
    exact hb.2
  · intro hall
    apply BitVec.eq_of_getLsbD_eq
    intro i hi
    simp only [valid, BitVec.getLsbD_and, BitVec.getLsbD_not]
    rw [show decide (i < 64) = true from decide_eq_true hi]
    rw [window_bit n i hi hn]
    by_cases hin : i < n
    · have hm := hall i hin
      simp [hin, hm]
    · simp [hin]

end TinylambOpt.ResearchProofs.JitKernels.Goal_NullWordFusion_NoNullsFromTailWord
