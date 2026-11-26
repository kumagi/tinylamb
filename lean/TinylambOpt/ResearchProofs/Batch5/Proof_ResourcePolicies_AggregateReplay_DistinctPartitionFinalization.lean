import TinylambOpt.ResearchGoals.QueryRewrites.Common
import TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.AggregateReplay.Goal_DistinctPartitionFinalization

open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay


/-! Scratch proof: routing distinct values to their hash bucket and freeing
    each local state after finalization preserves global COUNT DISTINCT,
    because every distinct non-NULL value is counted exactly once. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

/-- Filtering NULL-able rows by `Option.any` and then projecting non-NULL
    values is the same as projecting first and filtering the values. -/
theorem filterMap_filter_any (vs : List (Option Int)) (p : Int → Bool) :
    (vs.filter (fun v => v.any p)).filterMap id = (vs.filterMap id).filter p := by
  induction vs with
  | nil => simp
  | cons v vs ih =>
      cases v with
      | none => simp [ih]
      | some x =>
          by_cases hx : p x
          · simp [hx, ih]
          · simp [hx, ih]

/-- A value in `filterMap id vs` corresponds to `some x ∈ vs`. -/
theorem some_mem_of_mem_filterMap {vs : List (Option Int)} {x : Int}
    (hx : x ∈ vs.filterMap id) : some x ∈ vs := by
  rcases List.mem_filterMap.mp hx with ⟨v, hv, hvx⟩
  cases v with
  | none => simp at hvx
  | some y =>
      have hyx : y = x := by simpa using hvx
      subst x
      exact hv

/-- Deduplication removes all repeated elements (strong induction, since the
    deduplicated tail is a filtered sublist). -/
theorem nodup_eraseDups_aux : ∀ n (xs : List Int), xs.length = n → xs.eraseDups.Nodup := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro xs hlen
      cases xs with
      | nil => simp
      | cons x xs =>
          rw [List.eraseDups_cons]
          apply List.nodup_cons.mpr
          constructor
          · intro hx
            have hxfil : x ∈ xs.filter (fun b : Int => ! b == x) := List.mem_eraseDups.mp hx
            have hbool : (! x == x) = true := (List.mem_filter.mp hxfil).2
            simp at hbool
          · have hsmall : (xs.filter (fun b : Int => ! b == x)).length < n := by
              simp only [List.length_cons] at hlen
              have hle := List.length_filter_le (fun b : Int => ! b == x) xs
              omega
            exact ih _ hsmall (xs.filter (fun b : Int => ! b == x)) rfl

/-- Deduplication removes all repeated elements. -/
theorem nodup_eraseDups (xs : List Int) : xs.eraseDups.Nodup :=
  nodup_eraseDups_aux xs.length xs rfl

/-- Deduplication commutes with filtering at the level of cardinality: the
    two nodup lists have the same elements, hence are permutations. -/
theorem eraseDups_filter_length (xs : List Int) (p : Int → Bool) :
    ((xs.filter p).eraseDups).length = ((xs.eraseDups).filter p).length := by
  apply List.Perm.length_eq
  refine (List.perm_ext_iff_of_nodup (nodup_eraseDups (xs.filter p)) ?_).mpr ?_
  · have hpw : (xs.eraseDups).Pairwise (fun a b : Int => a ≠ b) := by
      simpa [List.nodup_iff_pairwise_ne] using nodup_eraseDups xs
    simpa [List.nodup_iff_pairwise_ne] using List.Pairwise.filter p hpw
  · intro a
    simp [List.mem_filter]

/-- Sum of pointwise sums of two functions over a list. -/
theorem sum_map_add {α : Type} (l : List α) (f g : α → Nat) :
    (l.map (fun a => f a + g a)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Nat.add_assoc, Nat.add_left_comm]

/-- Sum of an all-zero map. -/
theorem sum_map_zero {α : Type} (l : List α) : (l.map (fun _ => (0 : Nat))).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih => simpa using ih

/-- Exactly one bucket index in `range n` matches a bucket below `n`. -/
theorem sum_indicator_range {n b : Nat} (hb : b < n) :
    ((List.range n).map (fun i => if b == i then 1 else 0)).sum = 1 := by
  induction n with
  | zero => omega
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.sum_append]
      simp only [List.map_singleton, List.sum_singleton]
      by_cases hbn : b == n
      · have hzero : ((List.range n).map (fun i => if b == i then 1 else 0)).sum = 0 := by
          have hm : (List.range n).map (fun i => if b == i then 1 else 0) =
              (List.range n).map (fun _ : Nat => (0 : Nat)) := by
            apply List.map_congr_left
            intro i hi
            have hin : i < n := List.mem_range.mp hi
            have hbval : b = n := by simpa using hbn
            have hneq : b ≠ i := by omega
            simp [hneq]
          rw [hm]
          exact sum_map_zero (List.range n)
        rw [hzero]
        simp [hbn]
      · have hbne : b ≠ n := by simpa using hbn
        have hblt : b < n := by omega
        rw [ih hblt]
        simp [hbn]

/-- Every element is counted once by its bucket partition. -/
theorem sum_partition_count (E : List Int) (f : Int → Nat) (n : Nat)
    (h : ∀ x ∈ E, f x < n) :
    ((List.range n).map (fun b => (E.filter (fun x => f x == b)).length)).sum = E.length := by
  induction E with
  | nil =>
      simpa using sum_map_zero (List.range n)
  | cons x xs ih =>
      have hxlt : f x < n := h x (by simp)
      have htail : ∀ y ∈ xs, f y < n := by
        intro y hy
        exact h y (by simp [hy])
      calc
        ((List.range n).map (fun b => ((x :: xs).filter (fun y => f y == b)).length)).sum =
            ((List.range n).map (fun b => (if f x == b then 1 else 0) +
              (xs.filter (fun y => f y == b)).length)).sum := by
                apply congrArg List.sum
                apply List.map_congr_left
                intro b hb
                by_cases hfb : f x == b <;> simp [hfb, Nat.add_comm]
        _ = ((List.range n).map (fun b => if f x == b then 1 else 0)).sum +
            ((List.range n).map (fun b => (xs.filter (fun y => f y == b)).length)).sum :=
              sum_map_add (List.range n) _ _
        _ = 1 + xs.length := by
              rw [ih htail]
              rw [show ((List.range n).map (fun b => if f x == b then 1 else 0)).sum =
                ((List.range n).map (fun i => if f x == i then 1 else 0)).sum from rfl]
              rw [sum_indicator_range hxlt]
        _ = (x :: xs).length := by simp [Nat.add_comm]

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay.DistinctPartitionFinalization.Goal := by
  intro vs bucketOf n hbound
  let xs : List Int := vs.filterMap id
  have hrewrite : ∀ b ∈ List.range n,
      distinctCount (vs.filter (fun v => v.any (fun x => bucketOf x == b))) =
        (xs.filter (fun x => bucketOf x == b)).eraseDups.length := by
    intro b hb
    unfold distinctCount
    rw [filterMap_filter_any]
  calc
    ((List.range n).map (fun b => distinctCount
      (vs.filter (fun v => v.any (fun x => bucketOf x == b))))).sum =
        ((List.range n).map (fun b => (xs.filter (fun x => bucketOf x == b)).eraseDups.length)).sum := by
          apply congrArg List.sum
          exact List.map_congr_left hrewrite
    _ = ((List.range n).map (fun b => (xs.eraseDups.filter (fun x => bucketOf x == b)).length)).sum := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro b hb
          exact eraseDups_filter_length xs (fun x => bucketOf x == b)
    _ = xs.eraseDups.length := by
          apply sum_partition_count xs.eraseDups bucketOf n
          intro x hx
          have hxmem : x ∈ xs := List.mem_eraseDups.mp hx
          exact hbound x (some_mem_of_mem_filterMap hxmem)
    _ = distinctCount vs := by rfl
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.AggregateReplay.Goal_DistinctPartitionFinalization
