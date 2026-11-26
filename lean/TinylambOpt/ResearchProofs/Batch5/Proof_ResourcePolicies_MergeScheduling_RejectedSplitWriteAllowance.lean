import TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.MergeScheduling.Goal_RejectedSplitWriteAllowance

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling


/-! Soundness of `RejectedSplitWriteAllowance`: one extra rejected-split pass
    fits inside the six-head-byte allowance for a shrinking repartition. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

private theorem shrinking_sum_le_five_mul_head : ∀ (xs : List Nat),
    shrinking xs → ∀ a, xs = a :: t → xs.sum ≤ 5 * a := by
  intro xs hs
  match xs, hs with
  | [], hs => intro a h; cases h
  | [a], hs =>
      intro a' h
      simp at h
      rcases h with ⟨rfl, rfl⟩
      simp [List.sum_cons]
      omega
  | a :: b :: rest, hs =>
      simp [shrinking] at hs
      rcases hs with ⟨hab, htail⟩
      intro a' h
      simp at h
      rcases h with ⟨rfl, rfl⟩
      have htail_sum : (b :: rest).sum ≤ 5 * b :=
        shrinking_sum_le_five_mul_head (b :: rest) htail b rfl
      simp [List.sum_cons] at htail_sum ⊢
      omega

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
      have hmem := getLast?_mem h
      simp [List.mem_cons, hmem]

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling.RejectedSplitWriteAllowance.Goal := by
  intro a rest last hshr hlast
  have hsum : (a :: rest).sum ≤ 5 * a := shrinking_sum_le_five_mul_head (a :: rest) hshr a rfl
  have hmem : last ∈ (a :: rest) := getLast?_mem hlast
  have hle : last ≤ a := shrinking_all_le a rest hshr last hmem
  show (a :: rest).sum + last ≤ 6 * a
  omega
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.MergeScheduling.Goal_RejectedSplitWriteAllowance
