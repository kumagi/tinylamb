import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_GrantAdmission_DisjointVictimAccounting
open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission DisjointVictimAccounting

private theorem nodup_eraseDups_id {α : Type} [DecidableEq α] (l : List α) (h : l.Nodup) : l.eraseDups = l := by
  induction l with
  | nil => simp [List.eraseDups]
  | cons a as ih =>
    rw [List.nodup_cons] at h
    rw [List.eraseDups_cons]
    have hfa : List.filter (fun b => !b == a) as = as := by
      apply List.filter_eq_self.mpr
      intro b hb
      simp only [Bool.not_eq_true', bne_iff_ne, ne_eq, beq_eq_false_iff_ne, decide_eq_true_eq]
      intro hba
      exact h.1 (hba ▸ hb)
    rw [hfa, ih h.2]

theorem sound : Goal := by
  intro a b bytes ha hb hdisj
  have hnd : (a ++ b).Nodup := by
    rw [List.nodup_append]
    refine ⟨ha, hb, ?_⟩
    intro x hx y hy heq
    subst heq
    exact hdisj x hx hy
  rw [nodup_eraseDups_id _ hnd]
  simp [List.map_append, List.sum_append]
end Nov26.Proof_GrantAdmission_DisjointVictimAccounting
