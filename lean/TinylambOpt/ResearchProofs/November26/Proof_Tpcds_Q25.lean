import TinylambOpt.ResearchGoals.November26.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000


namespace TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q25
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.November26.Tpcds

theorem sound : TinylambOpt.ResearchGoals.November26.Tpcds.Q25.Goal := by
  intro sales returns catalog
  dsimp only
  induction returns with
  | nil => rfl
  | cons r returns ih =>
      by_cases hmem : sales.eraseDups.contains r = true
      · have hrmem : r ∈ sales := by
          have herase : r ∈ sales.eraseDups := by simpa using hmem
          exact List.mem_eraseDups.mp herase
        rw [List.filter_cons]
        simp [hrmem, ih]
      · have hnotmem : ¬ r ∈ sales := by
          intro hr
          have herase : r ∈ sales.eraseDups := List.mem_eraseDups.mpr hr
          have hc : sales.eraseDups.contains r = true := by simpa using herase
          exact hmem hc
        have hcnt : sales.count r = 0 := List.count_eq_zero.mpr hnotmem
        rw [List.filter_cons]
        simp [hnotmem, hcnt, ih]
end TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q25
