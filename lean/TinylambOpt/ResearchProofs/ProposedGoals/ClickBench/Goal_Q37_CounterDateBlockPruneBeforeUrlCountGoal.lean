import TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q37_CounterDateBlockPruneBeforeUrlCountGoal

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

private theorem countWhere_flatten {α : Type} (blocks : List (List α)) (p : α → Bool) :
    countWhere blocks.flatten p = (blocks.map (fun b => countWhere b p)).sum := by
  induction blocks with
  | nil => simp [countWhere]
  | cons b bs ih =>
      simp only [List.flatten_cons, countWhere, List.filter_append, List.length_append,
        List.map_cons, List.sum_cons]
      have ih' : (bs.flatten.filter p).length = (bs.map (fun x => (x.filter p).length)).sum := by
        simpa [countWhere] using ih
      rw [ih']

theorem sound : Q37_CounterDateBlockPruneBeforeUrlCountGoal := by
  intro blocks cid lo hi u
  dsimp only
  let base : Nat × Nat × String → Bool := fun r => (r.1 == cid) && decide (lo ≤ r.2.1 ∧ r.2.1 ≤ hi)
  let pred : Nat × Nat × String → Bool := fun r => base r && (r.2.2 == u)
  let keep : List (Nat × Nat × String) → Bool := fun b => b.any base
  calc
    countWhere blocks.flatten pred = (blocks.map (fun b => countWhere b pred)).sum :=
      countWhere_flatten blocks pred
    _ = ((blocks.filter keep).map (fun b => countWhere b pred)).sum := by
          induction blocks with
          | nil => simp
          | cons b bs ih =>
              by_cases hb : keep b = true
              · simp [hb, ih]
              · have hzero : countWhere b pred = 0 := by
                  have hnil : b.filter pred = [] := by
                    apply List.filter_eq_nil_iff.mpr
                    intro r hr hpred
                    have hab : ((r.1 == cid) && decide (lo ≤ r.2.1 ∧ r.2.1 ≤ hi)) = true :=
                      (Bool.and_eq_true_iff.mp hpred).1
                    have ha : (r.1 == cid) = true := (Bool.and_eq_true_iff.mp hab).1
                    have hbdec : decide (lo ≤ r.2.1 ∧ r.2.1 ≤ hi) = true :=
                      (Bool.and_eq_true_iff.mp hab).2
                    have hbase : base r = true := by
                      dsimp [base]
                      exact Bool.and_eq_true_iff.mpr ⟨ha, hbdec⟩
                    have h' : keep b = true := List.any_eq_true.mpr ⟨r, hr, hbase⟩
                    simp [hb] at h'
                  simp [countWhere, hnil]
                simp [hb, ih, hzero]
    _ = countWhere ((blocks.filter keep).flatten) pred :=
      (countWhere_flatten (blocks.filter keep) pred).symm

end TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q37_CounterDateBlockPruneBeforeUrlCountGoal
