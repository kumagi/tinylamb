import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# StoreCustomerRestrictedExistenceMarks

The modeled web and catalog existence tests agree for store customers when their mark
tables are restricted to store keys.

The executor can constrain both build sides with the store customer set before
constructing membership state.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000


namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.StoreCustomerRestrictedExistenceMarks
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.StoreCustomerRestrictedExistenceMarks.Goal := by
  intro customers store web catalog
  dsimp only
  apply List.filter_congr
  intro k hk
  by_cases hs : store.contains k = true
  · have hk' : k ∈ customers.eraseDups := List.mem_eraseDups.mpr hk
    have hdmem : k ∈ customers.eraseDups.filter store.contains := by
      rw [List.mem_filter]
      exact ⟨hk', hs⟩
    have hdemand : (members customers store).contains k = true := by
      simpa [members] using hdmem
    have hkextra : ((web ++ catalog).filter (members customers store).contains).contains k = true ↔
        (web.contains k || catalog.contains k) = true := by
      constructor
      · intro h
        have hm : k ∈ (web ++ catalog).filter (members customers store).contains := by simpa using h
        rw [List.mem_filter] at hm
        have hwc : k ∈ web ++ catalog := hm.1
        simpa [List.mem_append] using hwc
      · intro h
        have hwc : k ∈ web ++ catalog := by simpa [List.mem_append] using h
        have hm : k ∈ (web ++ catalog).filter (members customers store).contains := by
          rw [List.mem_filter]
          exact ⟨hwc, hdemand⟩
        simpa using hm
    rw [hs]
    simp only [Bool.true_and]
    exact Bool.eq_iff_iff.mpr hkextra
  · have hsmem : ¬ k ∈ store := by simpa using hs
    simp [hsmem]
end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.StoreCustomerRestrictedExistenceMarks
