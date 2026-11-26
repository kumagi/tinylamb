import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates
import TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping

namespace TinylambOpt.ResearchProofs.QueryRewrites.PartitionedAggregateStates.Goal_DistinctAddressMultiplicityAfterProjection

open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates


/-! Replacing each repeated address-group profit by one frequency row with its
multiplicity preserves the sorted output bag. The frequency rows are ordered by
profit and have distinct keys, so expanding them in that order yields the same
nondecreasing list as sorting the original profits. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.QueryRewrites

/-- Filtering a list by equality with a key is a run of that key. -/
theorem filter_eq_replicate_count {α : Type} [BEq α] [LawfulBEq α]
    (k : α) (xs : List α) : xs.filter (fun x => x == k) = List.replicate (xs.count k) k := by
  rw [List.eq_replicate_iff]
  constructor
  · exact (List.count_eq_length_filter (a := k) (l := xs)).symm
  · intro b hb
    exact beq_iff_eq.mp (List.mem_filter.mp hb).2

/-- The frequency table expands back to the original multiset. -/
theorem frequency_expansion_perm (xs : List Int) :
    xs.Perm ((frequency xs).flatMap (fun p : Int × Nat => List.replicate p.2 p.1)) := by
  have hp := TinylambOpt.perm_buckets_of_nodup (key := fun k : Int => k)
    (ks := xs.eraseDups) (r := xs)
    (TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping.nodup_eraseDups xs)
    (by intro row hrow; exact List.mem_eraseDups.mpr hrow)
  simpa only [frequency, List.flatMap_map, filter_eq_replicate_count] using hp

/-- Expanding a list of value/multiplicity rows ordered by value keeps the
expanded list ordered by value. -/
theorem pairwise_flatMap_replicate :
    ∀ rows : List (Int × Nat),
      rows.Pairwise (fun p q => p.1 ≤ q.1) →
      (rows.flatMap (fun p => List.replicate p.2 p.1)).Pairwise (fun a b => a ≤ b)
  | [], _ => by simp
  | p :: ps, hp => by
      rw [List.pairwise_cons] at hp
      rw [List.flatMap_cons, List.pairwise_append]
      refine ⟨?_, pairwise_flatMap_replicate ps hp.2, ?_⟩
      · exact List.pairwise_replicate.mpr (Or.inr (by omega))
      · intro a ha b hb
        have ha_eq : a = p.1 := (List.mem_replicate.mp ha).2
        obtain ⟨q, hq, hbq⟩ := List.mem_flatMap.mp hb
        have hb_eq : b = q.1 := (List.mem_replicate.mp hbq).2
        simpa [ha_eq, hb_eq] using hp.1 q hq

/-- Sorting by value gives a pairwise ordered list of frequency rows. -/
theorem pairwise_frequency_sorted (xs : List Int) :
    ((frequency xs).mergeSort (fun p q : Int × Nat => decide (p.1 ≤ q.1))).Pairwise
      (fun p q => p.1 ≤ q.1) := by
  have h := List.pairwise_mergeSort
    (le := fun p q : Int × Nat => decide (p.1 ≤ q.1))
    (by intro a b c hab hbc; simp at hab hbc ⊢; omega)
    (by intro a b; simp; omega) (frequency xs)
  exact h.imp (by intro a b hab; simpa using hab)

/-- Sorting Int values by the default comparator gives a pairwise ordered list. -/
theorem pairwise_sorted (xs : List Int) : (xs.mergeSort).Pairwise (fun a b => a ≤ b) := by
  have h := List.pairwise_mergeSort
    (le := fun a b : Int => decide (a ≤ b))
    (by intro a b c hab hbc; simp at hab hbc ⊢; omega)
    (by intro a b; simp; omega) xs
  exact h.imp (by intro a b hab; simpa using hab)

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.DistinctAddressMultiplicityAfterProjection.Goal := by
  intro profits
  let cmp : Int × Nat → Int × Nat → Bool := fun p q => decide (p.1 ≤ q.1)
  let sortedFreq := (frequency profits).mergeSort cmp
  let expanded := sortedFreq.flatMap (fun p : Int × Nat => List.replicate p.2 p.1)
  have hfreqPerm : expanded.Perm profits := by
    have hsortPerm : sortedFreq.Perm (frequency profits) := by
      dsimp [sortedFreq, cmp]
      exact List.mergeSort_perm (frequency profits) (fun p q : Int × Nat => decide (p.1 ≤ q.1))
    exact (hsortPerm.flatMap_right (fun p : Int × Nat => List.replicate p.2 p.1)).trans
      (frequency_expansion_perm profits).symm
  have hsortedExp : expanded.Pairwise (fun a b => a ≤ b) := by
    dsimp [expanded, sortedFreq, cmp]
    exact pairwise_flatMap_replicate _ (pairwise_frequency_sorted profits)
  have hperm : expanded.Perm profits.mergeSort :=
    hfreqPerm.trans (List.mergeSort_perm profits (fun a b : Int => decide (a ≤ b))).symm
  change expanded = profits.mergeSort
  exact List.Perm.eq_of_pairwise
    (by intro a b _ _ hab hba; omega)
    hsortedExp (pairwise_sorted profits) hperm
end TinylambOpt.ResearchProofs.QueryRewrites.PartitionedAggregateStates.Goal_DistinctAddressMultiplicityAfterProjection
