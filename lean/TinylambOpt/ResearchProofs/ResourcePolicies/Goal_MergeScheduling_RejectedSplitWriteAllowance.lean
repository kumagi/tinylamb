import TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling
import TinylambOpt.ResearchProofs.ResourcePolicies.MergeScheduling.GeometricRepartitionTraffic
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling.RejectedSplitWriteAllowance.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MergeScheduling_RejectedSplitWriteAllowance

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

private theorem shrinking_all_le : ∀ (head : Nat) (rest : List Nat),
    shrinking (head :: rest) → ∀ x ∈ (head :: rest), x ≤ head := by
  intro head rest hs x hx
  match rest, hs with
  | [], _ =>
      simp at hx
      omega
  | b :: tl, hs =>
      unfold shrinking at hs
      rcases hs with ⟨hab, htail⟩
      simp at hx
      rcases hx with rfl | rfl | hx
      · omega
      · omega
      · have hble : b ≤ head := by omega
        have hxle : x ≤ b := shrinking_all_le b tl htail x (by simp [hx])
        omega

private theorem getLast?_mem {α : Type} : ∀ {l : List α} {x : α}, l.getLast? = some x → x ∈ l := by
  intro l x h
  match l with
  | [] => simp at h
  | [y] =>
      simp [List.getLast?] at h
      subst h
      simp
  | y :: z :: zs =>
      have h' : (y :: z :: zs).getLast? = (z :: zs).getLast? := by
        simp [List.getLast?]
      rw [h'] at h
      have := getLast?_mem h
      simp [List.mem_cons, this]

theorem goal : RejectedSplitWriteAllowance.Goal := by
  intro a rest last hshr hlast
  have hsum : (a :: rest).sum ≤ 5 * a :=
    TinylambOpt.ResearchProofs.ResourcePolicies.MergeScheduling.GeometricRepartitionTraffic.shrinking_sum_le_five_mul_head (a :: rest) hshr a rfl
  have hmem : last ∈ (a :: rest) := getLast?_mem hlast
  have hle : last ≤ a := shrinking_all_le a rest hshr last hmem
  show (a :: rest).sum + last ≤ 6 * a
  omega

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MergeScheduling_RejectedSplitWriteAllowance
