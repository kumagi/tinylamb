import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates
import TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.PartitionedAggregateStates.Goal_NullableStateAverageFromCountedReturnBuckets

open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates


/-! Counting each non-null value once and recording its multiplicity recovers
both the exact SUM and the non-null COUNT of a nullable aggregate. The proof
expands the frequency table back into the original multiset and uses only
permutation invariance of sum and length. -/

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

/-- The sum of a constant Int run is its length times the value. -/
theorem sum_replicate_int (n : Nat) (v : Int) : (List.replicate n v).sum = (n : Int) * v := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.replicate_succ, List.sum_cons, ih]
      simp [Int.add_mul, Int.add_comm]

/-- Summing key times multiplicity over a frequency table equals the sum of
the expanded key runs. -/
theorem weighted_sum_eq_expanded_sum (ks : List Int) (cnt : Int → Nat) :
    ((ks.map (fun k => (k, cnt k))).map (fun p => p.1 * (p.2 : Int))).sum =
      (ks.flatMap (fun k => List.replicate (cnt k) k)).sum := by
  induction ks with
  | nil => rfl
  | cons k ks ih =>
      simp only [List.map_cons, List.sum_cons, List.flatMap_cons, List.sum_append]
      rw [ih, sum_replicate_int, Int.mul_comm k (cnt k : Int)]

/-- The sum of multiplicities in a frequency table equals the length of the
expanded key runs. -/
theorem count_sum_eq_expanded_length (ks : List Int) (cnt : Int → Nat) :
    (ks.map cnt).sum = (ks.flatMap (fun k => List.replicate (cnt k) k)).length := by
  induction ks with
  | nil => rfl
  | cons k ks ih =>
      simp only [List.map_cons, List.sum_cons, List.flatMap_cons, List.length_append,
        List.length_replicate]
      rw [ih]

/-- A permutation of Int lists preserves their sum. -/
theorem perm_sum_int {l₁ l₂ : List Int} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons x p ih => simpa using ih
  | swap x y xs => simp [Int.add_comm, Int.add_left_comm]
  | trans p₁ p₂ ih₁ ih₂ => exact ih₁.trans ih₂

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.NullableStateAverageFromCountedReturnBuckets.Goal := by
  intro xs
  let ys := xs.filterMap id
  let expanded := ys.eraseDups.flatMap (fun k => List.replicate (ys.count k) k)
  have hperm : ys.Perm expanded := by
    have hp := TinylambOpt.perm_buckets_of_nodup (key := fun k : Int => k)
      (ks := ys.eraseDups) (r := ys)
      (TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping.nodup_eraseDups ys)
      (by intro row hrow; exact List.mem_eraseDups.mpr hrow)
    simpa only [expanded, filter_eq_replicate_count] using hp
  dsimp
  apply Prod.ext
  · change ((ys.eraseDups.map (fun v => (v, ys.count v))).map
        (fun p : Int × Nat => p.1 * (p.2 : Int))).sum = ys.sum
    rw [weighted_sum_eq_expanded_sum, perm_sum_int hperm]
  · simpa [ys, frequency, Function.comp_def] using
      (count_sum_eq_expanded_length ys.eraseDups (fun k => ys.count k)).trans hperm.length_eq.symm
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.PartitionedAggregateStates.Goal_NullableStateAverageFromCountedReturnBuckets
