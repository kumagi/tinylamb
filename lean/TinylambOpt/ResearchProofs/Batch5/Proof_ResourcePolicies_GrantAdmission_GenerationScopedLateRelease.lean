import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.GrantAdmission.Goal_GenerationScopedLateRelease

open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission


set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! A late release scoped to an old generation only removes tickets carrying
that generation, so tickets of a replacement generation survive unchanged. -/

namespace Nov26.GrantAdmissionLateRelease

open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

/-- Filtering by `p` after discarding all `q` tickets is a no-op when the two
predicates are mutually exclusive. -/
theorem filter_after_revoke {p q : Ticket → Bool} (ts : List Ticket)
    (h : ∀ t, p t = true → q t = false) :
    ts.filter (fun t => p t && !q t) = ts.filter p := by
  induction ts with
  | nil => simp
  | cons t ts ih =>
      by_cases hp : p t = true
      · have hq : q t = false := h t hp
        simp [hp, hq, ih]
      · have hpfalse : p t = false := by cases hpt : p t <;> simp_all
        simp [hp, hpfalse, ih]

end Nov26.GrantAdmissionLateRelease

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.GenerationScopedLateRelease.Goal := by
  intro ts owner old fresh hne
  have hdisj : ∀ t : TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.Ticket,
      (t.owner == owner && t.generation == fresh) = true →
      (t.owner == owner && t.generation == old) = false := by
    intro t hp
    simp at hp ⊢
    omega
  simpa [TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.revoke] using
    (Nov26.GrantAdmissionLateRelease.filter_after_revoke ts hdisj)
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.GrantAdmission.Goal_GenerationScopedLateRelease
