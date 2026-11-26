import TinylambOpt.ResearchGoals.RestartRaces.Catalog
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.RestartRaces.Catalog_CascadeFence
open TinylambOpt.ResearchGoals.RestartRaces.Catalog.CascadeFence

theorem sound : Goal := by
  intro old live root
  constructor
  · intro h
    constructor
    · intro a b ha hab
      exact (h b).2 (Reach.follow ((h a).1 ha) hab)
    · intro a ha
      have hra : Reach live root a := (h a).1 ha
      clear ha
      induction hra with
      | here => exact Reach.here
      | follow hprev hedge ih =>
          exact Reach.follow ih ⟨(h _).2 hprev, (h _).2 (Reach.follow hprev hedge), hedge⟩
  · rintro ⟨hclosed, hinside⟩ k
    constructor
    · intro hk
      have hpath := hinside k hk
      clear hk
      induction hpath with
      | here => exact Reach.here
      | follow hprev hedge ih => exact Reach.follow ih hedge.2.2
    · intro hk
      have hk' := hk
      clear hk
      induction hk' with
      | here => exact Reach.here
      | follow hprev hedge ih => exact hclosed _ _ ih hedge
