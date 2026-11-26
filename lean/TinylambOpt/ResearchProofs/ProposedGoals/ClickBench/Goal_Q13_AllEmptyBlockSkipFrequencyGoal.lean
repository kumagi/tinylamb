import TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q13_AllEmptyBlockSkipFrequencyGoal

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

private theorem countWhere_filter_and {α : Type} (xs : List α) (p q : α → Bool) :
    countWhere (xs.filter p) q = countWhere xs (fun x => p x && q x) := by
  simp [countWhere, List.filter_filter, Bool.and_comm]

theorem sound : Q13_AllEmptyBlockSkipFrequencyGoal := by
  intro blocks s hs
  let q : String → Bool := fun x => (x != "") && (x == s)
  let keep : List String → Bool := fun b => b.any (fun x => x != "")
  calc
    countWhere (blocks.flatten.filter (fun x => x != "")) (fun x => x == s)
        = (blocks.map (fun b => countWhere b q)).sum := by
            rw [countWhere_filter_and]
            exact countWhere_flatten blocks q
    _ = ((blocks.filter keep).map (fun b => countWhere b q)).sum := by
          induction blocks with
          | nil => simp
          | cons b bs ih =>
              by_cases hb : keep b = true
              · simp [hb, ih]
              · have hzero : countWhere b q = 0 := by
                  have hnil : b.filter q = [] := by
                    apply List.filter_eq_nil_iff.mpr
                    intro x hx hqx
                    have hxne : (x != "") = true := (Bool.and_eq_true_iff.mp hqx).1
                    have hb' : keep b = true := List.any_eq_true.mpr ⟨x, hx, hxne⟩
                    simp [hb] at hb'
                  simp [countWhere, hnil]
                simp [hb, ih, hzero]
    _ = countWhere ((blocks.filter keep).flatten.filter (fun x => x != "")) (fun x => x == s) := by
          rw [countWhere_filter_and]
          exact (countWhere_flatten (blocks.filter keep) q).symm

end TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q13_AllEmptyBlockSkipFrequencyGoal
