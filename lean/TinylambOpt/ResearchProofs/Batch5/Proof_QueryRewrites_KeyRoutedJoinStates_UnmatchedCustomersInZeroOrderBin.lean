import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.KeyRoutedJoinStates.Goal_UnmatchedCustomersInZeroOrderBin

open TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates


/-! With unique customer keys, the zero-order bin is the complement of the
    matched-customer set. This also covers the case where the order filter
    removes every row, because the matched list is then empty. -/

open TinylambOpt.ResearchGoals.QueryRewrites

/-- Deduplicating a list that already has no duplicate leaves it unchanged. -/
theorem eraseDups_eq_self_of_nodup {l : List Nat} (h : l.Nodup) : l.eraseDups = l := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      have hxnot : x ∉ xs := (List.nodup_cons.mp h).1
      have htailNodup : xs.Nodup := (List.nodup_cons.mp h).2
      have hfilter : xs.filter (fun b => !b == x) = xs := by
        apply List.filter_eq_self.mpr
        intro b hb
        have hne : b ≠ x := by
          intro heq
          exact hxnot (heq ▸ hb)
        simp [hne]
      simp [List.eraseDups_cons, hfilter, ih htailNodup]

/-- Counting zero in a mapped Nat list is the same as counting the inputs whose
    mapped value equals zero. -/
theorem map_count_zero_eq_filter_length (xs : List Nat) (f : Nat → Nat) :
    (xs.map f).count 0 = (xs.filter (fun x => f x == 0)).length := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.count_cons]
      rw [List.filter_cons]
      by_cases h : f x == 0
      · simp [h, ih, beq_iff_eq]
      · simp [h, ih, beq_iff_eq]

/-- The zero-count filter has length equal to the remaining complement after
    removing the matched keys from the original list. -/
theorem zeroFilter_length_eq_sub (xs : List Nat) (orders : List Nat) :
    (xs.filter (fun k => orders.count k == 0)).length =
      xs.length - (xs.filter orders.contains).length := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      rw [List.filter_cons, List.filter_cons]
      by_cases hx : x ∈ orders
      · have hmem : orders.contains x = true := (List.contains_iff_mem).mpr hx
        have hcntpos : orders.count x ≠ 0 := by
          intro hz
          exact (List.count_eq_zero.mp hz) hx
        have hle : (List.filter orders.contains xs).length ≤ xs.length := List.length_filter_le _ _
        simp [hmem, hx, hcntpos, ih]
      · have hcnt : orders.count x = 0 := (List.count_eq_zero).mpr hx
        have hle : (List.filter orders.contains xs).length ≤ xs.length := List.length_filter_le _ _
        simp [hx, hcnt, ih]
        omega

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.UnmatchedCustomersInZeroOrderBin.Goal := by
  intro customers orders hnd
  rw [map_count_zero_eq_filter_length]
  have hmem : members customers orders = customers.filter orders.contains := by
    dsimp [members]
    rw [eraseDups_eq_self_of_nodup hnd]
  rw [hmem]
  exact zeroFilter_length_eq_sub customers orders
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.KeyRoutedJoinStates.Goal_UnmatchedCustomersInZeroOrderBin
