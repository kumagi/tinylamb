import TinylambOpt.ResearchGoals.DatabaseCorrectness.NullableMembership

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_NullableMembership_NaNOnlyBuildIsNotEmpty

open TinylambOpt.ResearchGoals.DatabaseCorrectness.NullableMembership

theorem sound : NaNOnlyBuildIsNotEmpty.Goal := by
  intro xs hne hall
  have hnan : ∀ x ∈ xs, key x = none := by
    intro x hx
    simp [key, hall x hx]
  have hkeys : ((xs.map some).filterMap (fun v => v.bind key)) = [] := by
    rw [List.filterMap_eq_nil_iff]
    intro v hv
    rcases List.mem_map.mp hv with ⟨x, hx, rfl⟩
    simp [hnan x hx]
  have hcnt : xs.length ≠ 0 := by
    cases xs with
    | nil => simp at hne
    | cons a tl => simp
  simp [build, hkeys, probe, hcnt]
  have h1 : Float.isNaN (1 : Float) = false := by native_decide
  simp [key, h1]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_NullableMembership_NaNOnlyBuildIsNotEmpty
