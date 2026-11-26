import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

namespace TinylambOpt.ResearchProofs.ResourcePolicies.GrantAdmission

open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

theorem generationScopedLateRelease : GenerationScopedLateRelease.Goal := by
  intro ts owner old fresh hne
  unfold revoke
  rw [List.filter_filter]
  congr 1
  funext t
  cases ho : (t.owner == owner) <;> cases hg : (t.generation == old) <;> cases hf : (t.generation == fresh) <;> simp_all
