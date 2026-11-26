import TinylambOpt.ResearchGoals.EngineBoundaries.SpillLeases

/-!
A spillable operator's resource accounting must protect partitions still referenced by
execution.

Proved

- `LastRelease` — The last release after a drop is the precise point where the physical
  space is available.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_SpillLeases_LastRelease

open TinylambOpt.ResearchGoals.EngineBoundaries.SpillLeases

theorem sound : LastRelease.Goal := by
  intro s l hnd hread
  rcases s with ⟨e, live, pay, pins⟩
  rcases l with ⟨le, lt⟩
  simp [readLease] at hread
  rcases hread with ⟨hle, hmem⟩
  subst le
  simp at hnd
  simp [release, dropBlock, reclaimable]
  cases pins with
  | nil => simp at hmem
  | cons x xs =>
      cases xs with
      | nil =>
          simp at hmem
          subst lt
          simp
      | cons y ys =>
          have hpair := List.pairwise_cons.mp hnd
          have hxy : x ≠ y := hpair.1 y (by simp)
          have hmem' : lt = x ∨ lt = y ∨ lt ∈ ys := by simpa using hmem
          rcases hmem' with hltx | hlty | hltys
          · subst lt
            simp [hxy.symm]
          · subst lt
            simp [hxy]
          · have hxne : x ≠ lt := by
              intro hh
              exact (hpair.1 lt (by simp [hltys])) hh
            simp [hxne]

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_SpillLeases_LastRelease
