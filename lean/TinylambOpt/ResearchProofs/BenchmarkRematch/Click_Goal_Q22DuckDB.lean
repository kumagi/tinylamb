import TinylambOpt.ResearchGoals.BenchmarkRematch.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Click.Click_Goal_Q22DuckDB
open Std
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click

theorem min?_filterMap_min?_eq (shards : List (List String)) :
    (shards.filterMap List.min?).min? = shards.flatten.min? := by
  by_cases hempty : shards.flatten = []
  · have hfmempty : shards.filterMap List.min? = [] := by
      induction shards with
      | nil => simp
      | cons b bs ih =>
          have happ : b ++ bs.flatten = [] := by simpa only [List.flatten_cons] using hempty
          have hbnil : b = [] := (List.eq_nil_of_append_eq_nil happ).1
          have hbsnil : bs.flatten = [] := (List.eq_nil_of_append_eq_nil happ).2
          have ih' := ih hbsnil
          rw [hbnil]
          simpa using ih'
    simp [hempty, hfmempty]
  · cases hmin : shards.flatten.min? with
    | none =>
        have : shards.flatten = [] := List.min?_eq_none_iff.mp hmin
        exact False.elim (hempty this)
    | some m =>
        have hmprops := List.min?_eq_some_iff.mp hmin
        have hm_mem_flat : m ∈ shards.flatten := hmprops.1
        have hm_min_flat : ∀ x ∈ shards.flatten, m ≤ x := hmprops.2
        have hm_mem_fm : m ∈ shards.filterMap List.min? := by
          rcases List.mem_flatten.mp hm_mem_flat with ⟨b, hbshards, hmemb⟩
          have hbmin_eq : b.min? = some m := by
            cases hbmin : b.min? with
            | none =>
                have hs := List.isSome_min?_of_mem hmemb
                simp [hbmin] at hs
            | some q =>
                have hqmem : q ∈ b := List.min?_mem hbmin
                have hqflat : q ∈ shards.flatten := List.mem_flatten.mpr ⟨b, hbshards, hqmem⟩
                have hqm : q ≤ m := (List.min?_eq_some_iff.mp hbmin).2 m hmemb
                have hmq : m ≤ q := hm_min_flat q hqflat
                have hqeq : q = m := le_antisymm hqm hmq
                simpa [hqeq] using hbmin
          exact List.mem_filterMap.mpr ⟨b, hbshards, hbmin_eq⟩
        have hlower : ∀ y ∈ shards.filterMap List.min?, m ≤ y := by
          intro y hy
          rcases List.mem_filterMap.mp hy with ⟨b, hbshards, hbmin⟩
          have hymem : y ∈ b := List.min?_mem hbmin
          have hyflat : y ∈ shards.flatten := List.mem_flatten.mpr ⟨b, hbshards, hymem⟩
          exact hm_min_flat y hyflat
        exact List.min?_eq_some_iff.mpr ⟨hm_mem_fm, hlower⟩

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Click.Q22DuckDB.Goal := by
  intro shards
  exact min?_filterMap_min?_eq shards
end TinylambOpt.ResearchProofs.BenchmarkRematch.Click.Click_Goal_Q22DuckDB
