import TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling.CheapestReadersFanIn.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MergeScheduling_CheapestReadersFanIn

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling


set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

private theorem pairwise_head_le {s : List Nat}
    (hs : List.Pairwise (fun a b => decide (a ≤ b) = true) s)
    {x : Nat} (hx : x ∈ s) (hne : s ≠ []) : s.head hne ≤ x := by
  match s, hs with
  | [], _ => contradiction
  | y :: ys, hs =>
      simp [List.head_cons]
      rcases (List.mem_cons.mp hx) with rfl | hx
      · exact Nat.le_refl _
      · have hy : ∀ z ∈ ys, decide (y ≤ z) = true := (List.pairwise_cons.mp hs).1
        have := hy x hx
        simpa using this

private theorem mem_split {a : Nat} {t : List Nat} (hx : a ∈ t) :
    ∃ t1 t2, t = t1 ++ a :: t2 := by
  induction t with
  | nil => simp at hx
  | cons x xs ih =>
      rcases List.mem_cons.mp hx with rfl | hx
      · exact ⟨[], xs, rfl⟩
      · rcases ih hx with ⟨t1, t2, ht⟩
        exact ⟨x :: t1, t2, by simp [ht]⟩

private theorem sum_take_le_sum_take_of_pairwise_perm : ∀ (k : Nat) (s t : List Nat),
    List.Pairwise (fun a b => decide (a ≤ b) = true) s → s.Perm t →
      (s.take k).sum ≤ (t.take k).sum := by
  intro k
  induction k with
  | zero => intro s t hs hp; simp
  | succ k ih =>
      intro s t hs hp
      cases s with
      | nil =>
          simp at hp
          subst hp
          simp
      | cons a as =>
          have hs_tail : List.Pairwise (fun a b => decide (a ≤ b) = true) as :=
            (List.pairwise_cons.mp hs).2
          have ha_mem_t : a ∈ t := hp.subset (by simp)
          rcases mem_split ha_mem_t with ⟨t1, t2, ht⟩
          have hp' : (a :: as).Perm (t1 ++ a :: t2) := by
            simpa [ht] using hp
          have hp_as : as.Perm (t1 ++ t2) := by
            have hmid : (t1 ++ a :: t2).Perm (a :: (t1 ++ t2)) := List.perm_middle
            exact List.Perm.cons_inv (hp'.trans hmid)
          have ih' := ih as (t1 ++ t2) hs_tail hp_as
          by_cases hk : t1.length < k + 1
          · have hk_le : t1.length ≤ k := Nat.lt_succ_iff.mp hk
            have htake_t : (t1 ++ a :: t2).take (k + 1) =
                t1 ++ a :: t2.take (k - t1.length) := by
              rw [List.take_append]
              rw [List.take_of_length_le (by omega : t1.length ≤ k + 1)]
              have hsub : k + 1 - t1.length = (k - t1.length) + 1 := by omega
              rw [hsub]
              simp [List.take]
            have htake_u : (t1 ++ t2).take k = t1 ++ t2.take (k - t1.length) := by
              rw [List.take_append]
              rw [List.take_of_length_le hk_le]
            have htake_s : (a :: as).take (k + 1) = a :: as.take k := by simp [List.take]
            rw [htake_s]
            rw [ht, htake_t]
            rw [htake_u] at ih'
            simp only [List.sum_append, List.sum_cons] at ih' ⊢
            omega
          · have hk_ge : k + 1 ≤ t1.length := Nat.le_of_not_gt hk
            have htake_t : (t1 ++ a :: t2).take (k + 1) = t1.take (k + 1) :=
              List.take_append_of_le_length hk_ge
            have htake_u : (t1 ++ t2).take k = t1.take k :=
              List.take_append_of_le_length (by omega)
            have hklt : k < t1.length := by omega
            have htake_succ : t1.take (k + 1) = t1.take k ++ [t1[k]] :=
              List.take_succ_eq_append_getElem hklt
            have ha_le_all : ∀ x ∈ t, a ≤ x := by
              intro x hx
              have hxas : x ∈ a :: as := hp.symm.subset hx
              exact pairwise_head_le hs hxas (by simp)
            have hle_a : a ≤ t1[k] := by
              apply ha_le_all
              have : t1[k] ∈ t1 := List.getElem_mem hklt
              simpa [ht] using (List.mem_append_left this : t1[k] ∈ t1 ++ a :: t2)
            have htake_s : (a :: as).take (k + 1) = a :: as.take k := by simp [List.take]
            rw [htake_s]
            rw [ht, htake_t, htake_succ, List.sum_append, List.sum_singleton]
            simp only [List.sum_cons]
            rw [htake_u] at ih'
            omega

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling.CheapestReadersFanIn.Goal := by
  intro p q k mem fd out hp hq
  constructor
  · have hq1 : (q.take k).length + 1 ≤ fd := hq.1
    simp only [List.length_take] at hq1 ⊢
    have h2 : (p.mergeSort (fun a b => decide (a ≤ b))).length = p.length :=
      (List.mergeSort_perm p (fun a b => decide (a ≤ b))).length_eq
    have h3 : p.length = q.length := hp.length_eq
    omega
  · have hq2 : (q.take k).sum + out ≤ mem := hq.2
    have hperm : (p.mergeSort (fun a b => decide (a ≤ b))).Perm q :=
      (List.mergeSort_perm p (fun a b => decide (a ≤ b))).trans hp
    have hpairwise : List.Pairwise (fun a b => decide (a ≤ b) = true)
        (p.mergeSort (fun a b => decide (a ≤ b))) :=
      List.pairwise_mergeSort
        (fun a b c hab hbc => by
          have hab' : a ≤ b := by simpa using hab
          have hbc' : b ≤ c := by simpa using hbc
          simp [hab', hbc', Nat.le_trans hab' hbc'])
        (fun a b => by
          by_cases h : a ≤ b
          · simp [h]
          · have hba : b ≤ a := Nat.le_of_not_ge h
            simp [h, hba]) p
    have hsum : ((p.mergeSort (fun a b => decide (a ≤ b))).take k).sum ≤ (q.take k).sum :=
      sum_take_le_sum_take_of_pairwise_perm k
        (p.mergeSort (fun a b => decide (a ≤ b))) q hpairwise hperm
    omega
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MergeScheduling_CheapestReadersFanIn
