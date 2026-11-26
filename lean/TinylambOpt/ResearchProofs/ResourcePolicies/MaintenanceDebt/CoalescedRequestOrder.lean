import TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_MaintenanceDebt_CoalescedRequestOrder
open TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt CoalescedRequestOrder

private theorem nodup_eraseDups (l : List Nat) : l.eraseDups.Nodup := by
  induction hn : l.length using Nat.strongRecOn generalizing l with
  | _ n ih =>
    match l with
    | [] => simp [List.eraseDups]
    | a :: as =>
      rw [List.eraseDups_cons, List.nodup_cons, List.mem_eraseDups, List.mem_filter]
      refine ⟨?_, ?_⟩
      · intro ⟨_, h⟩; simp at h
      · apply ih (List.filter (fun b => !b == a) as).length
        · have hle := List.length_filter_le (fun b => !b == a) as
          simp [List.length_cons] at hn
          omega
        · rfl

private theorem nodup_eraseDups_id (l : List Nat) (h : l.Nodup) : l.eraseDups = l := by
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

private theorem eraseDups_idem (l : List Nat) : l.eraseDups.eraseDups = l.eraseDups :=
  nodup_eraseDups_id _ (nodup_eraseDups l)

private theorem removeAll_eraseDups (r l : List Nat) :
    r.removeAll l.eraseDups = r.removeAll l := by
  induction r with
  | nil => simp [List.removeAll]
  | cons x xs ih =>
    simp [List.removeAll, ih]

private theorem eraseDups_append_eraseDups (l r : List Nat) :
    (l.eraseDups ++ r).eraseDups = (l ++ r).eraseDups := by
  rw [List.eraseDups_append, eraseDups_idem, removeAll_eraseDups]
  rw [List.eraseDups_append]

theorem sound : Goal := by
  intro old a b
  simp only [queue]
  rw [← List.append_assoc]
  exact eraseDups_append_eraseDups (old ++ a) b
end Nov26.Proof_MaintenanceDebt_CoalescedRequestOrder
