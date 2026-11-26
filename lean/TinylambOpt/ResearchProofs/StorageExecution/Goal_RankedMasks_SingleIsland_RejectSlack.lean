import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_SingleIsland_RejectSlack

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.SingleIsland

-- Truth check on small inputs, including ranks past the code space.
private def check : Bool :=
  (List.range 8).all fun n =>
  (List.range 6).all fun q =>
  (List.range 40).all fun r =>
    decide (((unrank model n q r).isNone) = decide (count model n q ≤ r))

#eval check

-- `count` splits into the false-branch (`lowCount`) and the true-branch.
private theorem count_succ (m : Machine) (n q : Nat) :
    count m (n + 1) q =
      lowCount m n q + ((m.step q true).map (count m n)).getD 0 := by
  rfl

private theorem bind_cons_isNone (o : Option (List Bool)) (b : Bool) :
    (o.bind fun bs => some (b :: bs)).isNone = o.isNone := by
  cases o <;> simp

-- In fact every machine rejects exactly the ranks outside its code space:
-- unrank follows the same branch partition as count, so an failed step only
-- happens on an empty branch, which is precisely when the rank is too large.
private theorem rejectSlack_all (m : Machine) : rejectSlack m := by
  intro n
  induction n with
  | zero =>
      intro q r
      by_cases hacc : m.accept q
      · by_cases hr : r = 0
        · subst r
          simp [unrank, count, hacc]
        · have hrb : (r == 0) = false := by simp [hr]
          by_cases hle : 1 ≤ r
          · simp [unrank, count, hacc, hrb, hle]
          · omega
      · simp [unrank, count, hacc]
  | succ n ih =>
      intro q r
      by_cases hb : lowCount m n q ≤ r
      · rcases hst : m.step q true with _ | qt
        · simp [unrank, count_succ, hb, hst]
        · have hunrank :
              (unrank m (n + 1) q r).isNone =
                (unrank m n qt (r - lowCount m n q)).isNone := by
            simp [unrank, hb, hst, bind_cons_isNone]
          rw [hunrank, ih qt (r - lowCount m n q)]
          rw [count_succ, hst]
          by_cases hle : count m n qt ≤ r - lowCount m n q
          · have htot : lowCount m n q + count m n qt ≤ r := by omega
            simp [hle, htot]
          · have htot : ¬ lowCount m n q + count m n qt ≤ r := by omega
            simp [hle, htot]
      · rcases hst : m.step q false with _ | qf
        · have hlow : lowCount m n q = 0 := by simp [lowCount, hst]
          have hfalse : False := by omega
          exact False.elim hfalse
        · have hunrank :
              (unrank m (n + 1) q r).isNone = (unrank m n qf r).isNone := by
            simp [unrank, hb, hst, bind_cons_isNone]
          rw [hunrank, ih qf r]
          rw [count_succ]
          have hlow : lowCount m n q = count m n qf := by simp [lowCount, hst]
          have hleft : ¬ count m n qf ≤ r := by omega
          have hright :
              ¬ lowCount m n q + ((m.step q true).map (count m n)).getD 0 ≤ r := by
            omega
          simp [hleft, hright]

theorem sound_RejectSlack : SingleIsland.RejectSlack.Goal := by
  exact rejectSlack_all model

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_SingleIsland_RejectSlack
