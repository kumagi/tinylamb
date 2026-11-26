import TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2

/-!
# Run-length accumulation of a delay histogram (TPC-DS Q99)

Expanding each run into `len` copies of its bucket and counting the target bucket is
exactly the sum of the lengths of runs carrying that bucket. The executor may therefore
add the run length directly to one counter instead of iterating `len` times.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q99_RunLengthDelayHistogramAccumGoal

open TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2
open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : Q99_RunLengthDelayHistogramAccumGoal := by
  intro runs target
  induction runs with
  | nil => simp [countWhere]
  | cons r runs ih =>
    change countWhere (List.replicate r.2 r.1 ++
      runs.flatMap (fun r => List.replicate r.2 r.1)) (fun b => b == target) =
      ((r :: runs).filter (fun r => r.1 == target) |>.map Prod.snd).sum
    rw [countWhere, List.filter_append, List.length_append]
    change (List.filter (fun b => b == target) (List.replicate r.2 r.1)).length +
      countWhere (runs.flatMap (fun r => List.replicate r.2 r.1)) (fun b => b == target) = _
    have htail := ih
    change countWhere (runs.flatMap (fun r => List.replicate r.2 r.1)) (fun b => b == target) =
        (runs.filter (fun r => r.1 == target) |>.map Prod.snd).sum at htail
    rw [htail]
    by_cases hr : (r.1 == target) = true
    · simp [List.filter_cons, hr, List.filter_replicate, Nat.add_comm]
    · have hr' : (r.1 == target) = false := by simpa using hr
      simp [List.filter_cons, hr', List.filter_replicate]

end TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q99_RunLengthDelayHistogramAccumGoal
