import TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling
import TinylambOpt.ResearchProofs.ResourcePolicies.MergeScheduling.GeometricRepartitionTraffic

namespace TinylambOpt.ResearchProofs.ResourcePolicies.MergeScheduling.RejectedSplitWriteAllowance

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
    GeometricRepartitionTraffic.shrinking_sum_le_five_mul_head (a :: rest) hshr a rfl
  have hmem : last ∈ (a :: rest) := getLast?_mem hlast
  have hle : last ≤ a := shrinking_all_le a rest hshr last hmem
  show (a :: rest).sum + last ≤ 6 * a
  omega

end TinylambOpt.ResearchProofs.ResourcePolicies.MergeScheduling.RejectedSplitWriteAllowance
