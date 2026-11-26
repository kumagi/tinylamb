import TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q40_DisjointReferrerCaseBranchSplitGoal

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

private theorem filter_partition {α : Type} (rows : List α) (b p : α → Bool) :
    (rows.filter p).length =
      ((rows.filter b).filter p).length + ((rows.filter (fun x => !b x)).filter p).length := by
  induction rows with
  | nil => simp
  | cons x xs ih =>
      by_cases hb : b x = true
      · by_cases hp : p x = true <;>
          simp [hb, hp, ih, Nat.add_assoc, Nat.add_comm]
      · have hbf : b x = false := by simpa using hb
        by_cases hp : p x = true <;>
          simp [hbf, hp, ih, Nat.add_comm, Nat.add_left_comm]

theorem sound : Q40_DisjointReferrerCaseBranchSplitGoal := by
  intro rows k
  simp only [countWhere]
  exact filter_partition rows (fun r : Bool × Nat => r.1) (fun r => r.2 == k)

end TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q40_DisjointReferrerCaseBranchSplitGoal
