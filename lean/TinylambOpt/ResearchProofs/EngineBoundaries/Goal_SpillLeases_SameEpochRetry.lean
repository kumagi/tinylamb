import TinylambOpt.ResearchGoals.EngineBoundaries.SpillLeases

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_SpillLeases_SameEpochRetry

open TinylambOpt.ResearchGoals.EngineBoundaries.SpillLeases

theorem refute : ¬ SameEpochRetry.Goal := by
  intro h
  have h1 : acquire (⟨4,true,42,[]⟩ : Block) 7 =
      some ((⟨4,7⟩ : Lease), (⟨4,true,42,[7]⟩ : Block)) := by rfl
  have h2 : acquire (release (⟨4,true,42,[7]⟩ : Block) (⟨4,7⟩ : Lease)) 7 =
      some ((⟨4,7⟩ : Lease), (⟨4,true,42,[7]⟩ : Block)) := by rfl
  have hh := h (⟨4,true,42,[]⟩ : Block) 7 (⟨4,7⟩ : Lease)
    (⟨4,true,42,[7]⟩ : Block) (⟨4,true,42,[7]⟩ : Block) (⟨4,7⟩ : Lease) h1 h2
  have hnone : readLease (release (⟨4,true,42,[7]⟩ : Block) (⟨4,7⟩ : Lease)) (⟨4,7⟩ : Lease) = none := by rfl
  rw [hnone] at hh
  contradiction

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_SpillLeases_SameEpochRetry
