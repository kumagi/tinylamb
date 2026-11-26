import TinylambOpt.ResearchGoals.JitKernels.RadixCursors
set_option maxHeartbeats 0
set_option maxRecDepth 10000
open TinylambOpt.ResearchGoals.JitKernels.RadixCursors

-- sanity checks
example : List.countP (fun i => i == 1) [0,1,2,1] = 2 := by decide

-- count k (range N)
example (k N : Nat) : List.count k (List.range N) = if k < N then 1 else 0 := by simp
