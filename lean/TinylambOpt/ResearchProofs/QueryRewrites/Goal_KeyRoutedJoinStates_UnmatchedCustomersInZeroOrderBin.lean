import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

/-!
# Goal_KeyRoutedJoinStates_UnmatchedCustomersInZeroOrderBin

For unique customer keys, the zero-order bin is the complement of matched
customer keys: counting zero subtotals equals the number of customers whose key
never appears in the orders list.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_UnmatchedCustomersInZeroOrderBin

open TinylambOpt.ResearchGoals.QueryRewrites

private theorem count_zero_map_count (customers orders : List Nat) :
    (customers.map orders.count).count 0 = (customers.filter (fun c => orders.count c == 0)).length := by
  induction customers with
  | nil => simp
  | cons c cs ih =>
      by_cases h : orders.count c = 0
      · simp [h, ih]
      · have hne : (orders.count c == 0) = false := by simp [h]
        simp [h, hne, ih]

private theorem nodup_eraseDups (xs : List Nat) (hnd : xs.Nodup) : xs.eraseDups = xs := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      change List.Pairwise (fun a b : Nat => a ≠ b) (x :: xs) at hnd
      rw [List.pairwise_cons] at hnd
      have hnot : ¬ x ∈ xs := by
        intro hx
        exact hnd.1 x hx rfl
      have hndxs : xs.Nodup := by
        simpa [List.Nodup] using hnd.2
      have hf : (List.filter (fun b : Nat => !b == x) xs) = xs := by
        apply List.filter_eq_self.mpr
        intro b hb
        have hbx : b ≠ x := by
          intro heq
          apply hnot
          simpa [heq] using hb
        simp [hbx]
      rw [List.eraseDups_cons, hf]
      simp [ih hndxs]

private theorem length_filter_complement (xs : List Nat) (p : Nat → Bool) :
    (xs.filter p).length + (xs.filter (fun x => !p x)).length = xs.length := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      cases h : p x <;> simp [h, ih] <;> omega

theorem sound :
    TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.UnmatchedCustomersInZeroOrderBin.Goal := by
  intro customers orders hnodup
  change (customers.map orders.count).count 0 = customers.length - (members customers orders).length
  rw [count_zero_map_count]
  rw [members, nodup_eraseDups customers hnodup]
  have hpred : (fun c : Nat => orders.count c == 0) = (fun c => !orders.contains c) := by
    funext c
    by_cases h : c ∈ orders
    · have hc : orders.count c ≠ 0 := by
        intro hz
        exact (List.count_eq_zero.mp hz) h
      simp [h, hc]
    · have hc : orders.count c = 0 := List.count_eq_zero.mpr h
      simp [h, hc]
  rw [hpred]
  have hlen := length_filter_complement customers (fun c : Nat => orders.contains c)
  rw [← hlen]
  exact (Nat.add_sub_cancel_left _ _).symm

end TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_UnmatchedCustomersInZeroOrderBin
