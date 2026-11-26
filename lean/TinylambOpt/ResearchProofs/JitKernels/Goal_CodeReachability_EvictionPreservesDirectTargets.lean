import TinylambOpt.ResearchGoals.JitKernels.CodeReachability

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_CodeReachability_EvictionPreservesDirectTargets

open TinylambOpt.ResearchGoals.JitKernels

theorem sound : TinylambOpt.ResearchGoals.JitKernels.CodeReachability.EvictionPreservesDirectTargets.Goal := by
  intro heap roots
  dsimp only
  intro k hk
  let live := CodeReachability.mark heap roots heap.length
  have hk' : k ∈ live := by simpa [live] using hk
  have hfind : ∀ (xs : List CodeReachability.Obj) (k : CodeReachability.Key), k ∈ live →
      (xs.filter (fun o => live.contains o.key)).find? (fun o => o.key == k) =
        xs.find? (fun o => o.key == k) := by
    intro xs k hk
    induction xs generalizing k with
    | nil => simp
    | cons o os ih =>
        by_cases hok : o.key == k
        · have hokeq : o.key = k := beq_iff_eq.mp hok
          have holive : live.contains o.key = true := by simpa [hokeq] using hk
          simp [hok, hokeq, hk]
        · have hok' : o.key ≠ k := by
            intro h
            exact hok (by simp [h])
          by_cases holive : live.contains o.key
          · simp [hok', holive]
            simpa [beq_iff_eq] using ih k hk
          · simp [hok', holive]
            simpa [beq_iff_eq] using ih k hk
  have hf := hfind heap k hk'
  simpa [CodeReachability.edges, CodeReachability.retain, live, beq_iff_eq] using
    congrArg (fun o => (o.map CodeReachability.Obj.calls).getD []) hf

end TinylambOpt.ResearchProofs.JitKernels.Goal_CodeReachability_EvictionPreservesDirectTargets
