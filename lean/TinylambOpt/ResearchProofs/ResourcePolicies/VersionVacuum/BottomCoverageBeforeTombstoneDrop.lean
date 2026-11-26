import TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum

namespace TinylambOpt.ResearchProofs.ResourcePolicies.VersionVacuum.BottomCoverageBeforeTombstoneDrop

open TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum

theorem sound : Not BottomCoverageBeforeTombstoneDrop.Goal := by
  intro h
  have hord : ordered ([⟨5, none⟩] ++ [⟨2, some 9⟩]) := by simp [ordered]
  have hw : (winner [⟨5, none⟩] 8).isSome = true := by decide
  have := h [⟨5, none⟩] [⟨2, some 9⟩] 8 hord hw
  simp [readAt, winner] at this

end TinylambOpt.ResearchProofs.ResourcePolicies.VersionVacuum.BottomCoverageBeforeTombstoneDrop
