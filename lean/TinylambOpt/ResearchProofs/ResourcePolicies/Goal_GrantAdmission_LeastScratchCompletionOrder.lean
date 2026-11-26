import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.LeastScratchCompletionOrder.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_GrantAdmission_LeastScratchCompletionOrder

open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

set_option maxHeartbeats 0
set_option maxRecDepth 10000
open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission
attribute [local instance] boolRelToRel

namespace Nov26.LeastScratch

attribute [local instance] boolRelToRel

def leScratch (a b : Job) : Bool := decide (a.scratch ≤ b.scratch)

theorem leScratch_trans : ∀ a b c : Job, leScratch a b → leScratch b c → leScratch a c := by
  intro a b c
  simp [leScratch]
  omega

theorem leScratch_total : ∀ a b : Job, leScratch a b || leScratch b a := by
  intro a b
  simp [leScratch]
  omega

theorem max_sub_right (a b c : Nat) : max a b - c = max (a - c) (b - c) := by
  by_cases h : a ≤ b
  · rw [Nat.max_eq_right h, Nat.max_eq_right (Nat.sub_le_sub_right h c)]
  · have hba : b ≤ a := Nat.le_of_not_ge h
    rw [Nat.max_eq_left hba, Nat.max_eq_left (Nat.sub_le_sub_right hba c)]

theorem headroom_append (front back : List Job) :
    headroom (front ++ back) = max (headroom front) (headroom back - (front.map Job.held).sum) := by
  induction front with
  | nil => simp [headroom]
  | cons j front ih =>
      change headroom (j :: (front ++ back)) = _
      simp only [headroom, List.map_cons, List.sum_cons]
      rw [ih]
      rw [max_sub_right]
      rw [← Nat.max_assoc]
      congr 1
      omega

theorem headroom_le_of_all_scratch_le (l : List Job) (k : Nat)
    (h : ∀ j ∈ l, j.scratch ≤ k) : headroom l ≤ k := by
  induction l with
  | nil => simp [headroom]
  | cons j js ih =>
      simp only [headroom]
      rw [Nat.max_le]
      constructor
      · exact h j (by simp)
      · have htail : ∀ x ∈ js, x.scratch ≤ k := by
          intro x hx
          exact h x (by simp [hx])
        exact Nat.le_trans (Nat.sub_le _ _) (ih htail)

theorem sorted_minimum (s order : List Job) (hs : s.Pairwise leScratch)
    (hp : s.Perm order) : headroom s ≤ headroom order := by
  induction order generalizing s with
  | nil =>
      have hsnil : s = [] := hp.eq_nil
      subst s
      simp [headroom]
  | cons a rest ih =>
      have ha_mem : a ∈ s := hp.symm.subset (by simp)
      obtain ⟨P, Q, hdecomp⟩ := List.append_of_mem ha_mem
      subst s
      have hsub : List.Sublist (P ++ Q) (P ++ a :: Q) :=
        List.Sublist.middle (List.Sublist.refl (P ++ Q)) a
      have hsPQ : (P ++ Q).Pairwise leScratch := List.Pairwise.sublist hsub hs
      have hmid : (P ++ a :: Q).Perm (a :: (P ++ Q)) := List.perm_middle
      have h2 : (a :: rest).Perm (a :: (P ++ Q)) := hp.symm.trans hmid
      have hperm_rest : (P ++ Q).Perm rest := ((List.perm_cons a).mp h2).symm
      have ih' := ih (P ++ Q) hsPQ hperm_rest
      have hP_le_a : headroom P ≤ a.scratch := by
        apply headroom_le_of_all_scratch_le
        intro x hx
        have hrel : leScratch x a := List.Pairwise.rel_of_mem_append
            (R := fun u v : Job => leScratch u v = true) hs hx (by simp)
        simpa [leScratch] using hrel
      have hQ_minus : headroom Q - (P.map Job.held).sum ≤ headroom rest := by
        have htmp : max (headroom P) (headroom Q - (P.map Job.held).sum) ≤ headroom rest := by
          rw [← headroom_append P Q]
          exact ih'
        exact Nat.le_trans (Nat.le_max_right _ _) htmp
      calc
        headroom (P ++ a :: Q)
            = max (headroom P) (max a.scratch (headroom Q - a.held) - (P.map Job.held).sum) := by
                rw [headroom_append]
                simp only [headroom]
        _ ≤ max a.scratch (headroom rest - a.held) := by
            rw [Nat.max_le]
            constructor
            · exact Nat.le_trans hP_le_a (Nat.le_max_left _ _)
            · rw [max_sub_right]
              rw [Nat.max_le]
              constructor
              · exact Nat.le_trans (Nat.sub_le _ _) (Nat.le_max_left _ _)
              · have htmp : (headroom Q - a.held) - (P.map Job.held).sum =
                    (headroom Q - (P.map Job.held).sum) - a.held := by omega
                rw [htmp]
                exact Nat.le_trans (Nat.sub_le_sub_right hQ_minus a.held) (Nat.le_max_right _ _)

end Nov26.LeastScratch

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.LeastScratchCompletionOrder.Goal := by
  intro jobs order hperm
  have hsorted : (jobs.mergeSort Nov26.LeastScratch.leScratch).Pairwise Nov26.LeastScratch.leScratch := by
    exact List.pairwise_mergeSort Nov26.LeastScratch.leScratch_trans Nov26.LeastScratch.leScratch_total jobs
  have hp : (jobs.mergeSort Nov26.LeastScratch.leScratch).Perm order :=
    (List.mergeSort_perm jobs Nov26.LeastScratch.leScratch).trans hperm
  exact Nov26.LeastScratch.sorted_minimum (jobs.mergeSort Nov26.LeastScratch.leScratch) order hsorted hp
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_GrantAdmission_LeastScratchCompletionOrder
