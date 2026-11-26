import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.GrantAdmission.Goal_DisjointVictimAccounting

open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission


set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! Two disjoint, duplicate-free victim lists report additive reclaimed bytes:
deduplicating their concatenation changes nothing when the stages name distinct
objects. -/

namespace Nov26.GrantAdmissionVictims

theorem eraseDups_eq_self_of_nodup (l : List Nat) (h : l.Nodup) : l.eraseDups = l := by
  induction l with
  | nil => simp
  | cons a as ih =>
      have hd := List.nodup_cons.mp h
      have htail : as.Nodup := hd.2
      have hfilter : as.filter (fun b => !b == a) = as := by
        rw [List.filter_eq_self]
        intro x hx
        have hxa : x ≠ a := by
          intro heq
          exact hd.1 (heq ▸ hx)
        simp [hxa]
      rw [List.eraseDups_cons, hfilter, ih htail]

end Nov26.GrantAdmissionVictims

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.DisjointVictimAccounting.Goal := by
  intro a b bytes ha hb hdis
  have hnodup : (a ++ b).Nodup := by
    rw [List.nodup_append]
    refine ⟨ha, hb, ?_⟩
    intro x hx y hy hxy
    subst hxy
    exact hdis x hx hy
  rw [Nov26.GrantAdmissionVictims.eraseDups_eq_self_of_nodup (a ++ b) hnodup]
  simp [List.sum_append]
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.GrantAdmission.Goal_DisjointVictimAccounting
