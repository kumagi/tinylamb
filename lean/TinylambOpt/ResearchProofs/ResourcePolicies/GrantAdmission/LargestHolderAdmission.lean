import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

namespace TinylambOpt.ResearchProofs.ResourcePolicies.GrantAdmission

open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

theorem largestHolderAdmission_refute : ¬ LargestHolderAdmission.Goal := by
  intro h
  have := h [⟨7,1⟩, ⟨10,7⟩] [⟨7,1⟩, ⟨10,7⟩] (by simp [List.Perm])
  simp [headroom, List.mergeSort, List.merge] at this
