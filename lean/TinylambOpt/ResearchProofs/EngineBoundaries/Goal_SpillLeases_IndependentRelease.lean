import TinylambOpt.ResearchGoals.EngineBoundaries.SpillLeases

/-!
A spillable operator's resource accounting must protect partitions still referenced by
execution.

Proved

- `IndependentRelease` — Releasing one independent read leaves every other ticket
  readable.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_SpillLeases_IndependentRelease

open TinylambOpt.ResearchGoals.EngineBoundaries.SpillLeases

theorem sound : IndependentRelease.Goal := by
  intro s a b hne
  have hbne : b.ticket ≠ a.ticket := fun h => hne h.symm
  by_cases he : s.epoch = a.epoch
  · have hbea : (s.epoch == a.epoch) = true := beq_iff_eq.mpr he
    by_cases hab : a.epoch = b.epoch
    · have hbeab : (a.epoch == b.epoch) = true := beq_iff_eq.mpr hab
      have hmemiff : b.ticket ∈ s.pins.erase a.ticket ↔ b.ticket ∈ s.pins :=
        List.mem_erase_of_ne hbne
      by_cases hbmem : b.ticket ∈ s.pins
      · simp [release, readLease, hbea, hbeab, hmemiff.mpr hbmem, hbmem]
      · simp [release, readLease, hbea, hbeab, hmemiff, hbmem]
    · have hsbne : s.epoch ≠ b.epoch := by omega
      simp [release, readLease, hbea, hsbne]
  · have hbea : (s.epoch == a.epoch) = false := beq_eq_false_iff_ne.mpr he
    simp [release, readLease, hbea]

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_SpillLeases_IndependentRelease
