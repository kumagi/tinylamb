import TinylambOpt.ResearchGoals.JitKernels.DispatchTables

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_SplitRangeAtVectorBoundary

theorem sound : TinylambOpt.ResearchGoals.JitKernels.DispatchTables.SplitRangeAtVectorBoundary.Goal := by
  intro before a after mid x hlo hmid
  unfold TinylambOpt.ResearchGoals.JitKernels.DispatchTables.pick
  induction before with
  | nil =>
      by_cases ha : TinylambOpt.ResearchGoals.JitKernels.DispatchTables.holds a x = true
      · have haInt : a.lo ≤ x ∧ x < a.hi := by
          simpa [TinylambOpt.ResearchGoals.JitKernels.DispatchTables.holds] using ha
        by_cases hxmid : x < mid
        · have h1 : TinylambOpt.ResearchGoals.JitKernels.DispatchTables.holds { a with hi := mid } x = true := by
            simp [TinylambOpt.ResearchGoals.JitKernels.DispatchTables.holds] <;> omega
          have h2 : TinylambOpt.ResearchGoals.JitKernels.DispatchTables.holds { a with lo := mid } x = false := by
            simp [TinylambOpt.ResearchGoals.JitKernels.DispatchTables.holds] <;> omega
          simp [ha, h1, h2]
        · have hxge : mid ≤ x := by omega
          have h1 : TinylambOpt.ResearchGoals.JitKernels.DispatchTables.holds { a with hi := mid } x = false := by
            simp [TinylambOpt.ResearchGoals.JitKernels.DispatchTables.holds] <;> omega
          have h2 : TinylambOpt.ResearchGoals.JitKernels.DispatchTables.holds { a with lo := mid } x = true := by
            simp [TinylambOpt.ResearchGoals.JitKernels.DispatchTables.holds] <;> omega
          simp [ha, h1, h2]
      · have haInt : ¬ (a.lo ≤ x ∧ x < a.hi) := by
          simpa [TinylambOpt.ResearchGoals.JitKernels.DispatchTables.holds] using ha
        have h1 : TinylambOpt.ResearchGoals.JitKernels.DispatchTables.holds { a with hi := mid } x = false := by
          simp [TinylambOpt.ResearchGoals.JitKernels.DispatchTables.holds] <;> omega
        have h2 : TinylambOpt.ResearchGoals.JitKernels.DispatchTables.holds { a with lo := mid } x = false := by
          simp [TinylambOpt.ResearchGoals.JitKernels.DispatchTables.holds] <;> omega
        simp [ha, h1, h2]
  | cons b bs ih =>
      cases hb : (bs.find? (fun arm => TinylambOpt.ResearchGoals.JitKernels.DispatchTables.holds arm x)) with
      | none =>
          by_cases hbh : TinylambOpt.ResearchGoals.JitKernels.DispatchTables.holds b x = true
          · simp [hb, hbh]
          · simpa [hb, hbh] using ih
      | some arm =>
          by_cases hbh : TinylambOpt.ResearchGoals.JitKernels.DispatchTables.holds b x = true
          · simp [hbh]
          · simpa [hb, hbh] using ih

end TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_SplitRangeAtVectorBoundary
