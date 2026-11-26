import TinylambOpt.ResearchProofs.StorageExecution.SE_BagKernels_Common

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved (10 claims, including the following)

- `RepeatedSlices.AntiRevenue` — Compute revenue of all left rows whose key is absent
  on the right.
- `RepeatedSlices.AppendedSuffixes` — `∀ a b k, bag.mass (a++b) k = bag.mass a k +
  bag.mass b k + a.length*b.count k`
- `RepeatedSlices.DistinctOutput` — Output live grouping keys once even if physical
  codes alias.
-/

set_option maxHeartbeats 0

open TinylambOpt.ResearchProofs.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels

namespace RepeatedSlicesProofs

open RepeatedSlices

theorem expand_nil : bag.expand [] = [] := rfl

theorem expand_cons : ∀ (y : Nat) (r : List Nat), bag.expand (y :: r) = (y :: r) ++ bag.expand r := by
  intro y r
  show ((List.range (r.length + 1)).flatMap fun i => (y :: r).drop i)
      = (y :: r) ++ ((List.range r.length).flatMap fun i => r.drop i)
  rw [List.range_succ_eq_map]
  rw [List.flatMap_cons]
  rw [List.flatMap_map Nat.succ (fun i => (y :: r).drop i) (List.range r.length)]
  simp only [Nat.succ_eq_add_one, List.drop_succ_cons, List.drop_zero]

theorem zipIdx_append : ∀ (a b : List Nat) (s : Nat),
    (a ++ b).zipIdx s = a.zipIdx s ++ b.zipIdx (s + a.length) := by
  intro a b; induction a with
  | nil => intro s; rfl
  | cons z t ih =>
    intro s
    rw [List.cons_append, List.zipIdx_cons, List.zipIdx_cons, ih (s + 1)]
    rw [show s + 1 + t.length = s + (t.length + 1) from by omega]
    rw [List.length_cons]
    exact rfl

theorem zipIdx_mass_shift (k : Nat) : ∀ (l : List Nat) (s : Nat),
    (((l.zipIdx s).filter (fun e => e.1 == k)).map (fun e => e.2 + 1)).sum
      = bag.mass l k + s * l.count k := by
  intro l; induction l with
  | nil => intro s; exact rfl
  | cons z t ih =>
    intro s
    have hb : ((z, s).1 == k) = (z == k) := rfl
    have hb0 : ((z, 0).1 == k) = (z == k) := rfl
    have hm : bag.mass (z :: t) k
        = ((( (z, 0) :: t.zipIdx 1).filter (fun e => e.1 == k)).map (fun e => e.2 + 1)).sum := by
      show ((( (z :: t).zipIdx 0).filter (fun e => e.1 == k)).map (fun e => e.2 + 1)).sum = _
      rw [List.zipIdx_cons]
    rw [hm, List.zipIdx_cons, List.filter_cons, List.filter_cons, hb, hb0, List.count_cons]
    by_cases hz : (z == k) = true
    · rw [if_pos hz, if_pos hz, if_pos hz, List.map_cons, List.map_cons, List.sum_cons,
        List.sum_cons, ih (s + 1), ih 1]
      simp only [Nat.add_mul, Nat.mul_add, Nat.mul_one, Nat.add_zero, Nat.zero_add,
        Nat.reduceAdd]
      omega
    · rw [if_neg hz, if_neg hz, if_neg hz, ih (s + 1), ih 1]
      simp only [Nat.add_mul, Nat.mul_add, Nat.mul_one, Nat.add_zero, Nat.zero_add,
        Nat.reduceAdd]
      omega

theorem wf : WellFormed bag := by
  refine' ⟨_, _⟩
  · intro p; induction p with
    | nil => intro k; rw [expand_nil, List.count_nil]; exact rfl
    | cons z t ih =>
      intro k
      rw [expand_cons, List.count_append, List.count_cons, ih]
      have hb0 : ((z, 0).1 == k) = (z == k) := rfl
      have hm : bag.mass (z :: t) k
          = ((( (z, 0) :: t.zipIdx 1).filter (fun e => e.1 == k)).map (fun e => e.2 + 1)).sum := by
        show ((( (z :: t).zipIdx 0).filter (fun e => e.1 == k)).map (fun e => e.2 + 1)).sum = _
        rw [List.zipIdx_cons]
      rw [hm, List.filter_cons, hb0]
      by_cases hz : (z == k) = true
      · rw [if_pos hz, if_pos hz, List.map_cons, List.sum_cons, zipIdx_mass_shift k t 1]
        simp only [Nat.mul_one, Nat.add_zero, Nat.reduceAdd]
        omega
      · rw [if_neg hz, if_neg hz, zipIdx_mass_shift k t 1]
        simp only [Nat.mul_one, Nat.add_zero, Nat.reduceAdd]
        omega
  · intro p; induction p with
    | nil => intro k hk; rw [expand_nil] at hk; exact absurd hk List.not_mem_nil
    | cons z t ih =>
      intro k hk
      rw [expand_cons] at hk
      rcases List.mem_append.mp hk with h | h
      · exact h
      · exact List.mem_cons_of_mem z (ih k h)

theorem sound_DistinctOutput : DistinctOutput.Goal := wf_distinctOutput bag wf
theorem sound_FilteredMoment : FilteredMoment.Goal := wf_filteredMoment bag wf
theorem sound_OrderStatistic : OrderStatistic.Goal := wf_orderStatistic bag wf
theorem sound_JoinFanout : JoinFanout.Goal := wf_joinFanout bag wf
theorem sound_AntiRevenue : AntiRevenue.Goal := wf_antiRevenue bag wf
theorem sound_SelfPairs : SelfPairs.Goal := wf_selfPairs bag wf
theorem sound_IntersectAll : IntersectAll.Goal := wf_intersectAll bag wf
theorem sound_OuterWeight : OuterWeight.Goal := wf_outerWeight bag wf

theorem expand_len (xs : List Nat) : (bag.expand xs).length = xs.length * (xs.length + 1) / 2 := by
  induction xs with
  | nil => rfl
  | cons z t ih =>
    rw [expand_cons, List.length_append, List.length_cons, ih, tri_succ t.length]
    omega

theorem sound_AppendedSuffixes : AppendedSuffixes.Goal := by
  intro a b k
  show ((((a ++ b).zipIdx 0).filter (fun e => e.1 == k)).map (fun e => e.2 + 1)).sum
      = bag.mass a k + bag.mass b k + a.length * b.count k
  rw [zipIdx_append, List.filter_append, List.map_append, List.sum_append,
    zipIdx_mass_shift k a 0, zipIdx_mass_shift k b (0 + a.length)]
  simp only [Nat.zero_add, Nat.zero_mul, Nat.add_zero]
  omega

theorem sound_TriangularSize : TriangularSize.Goal := by
  intro xs
  rw [weighted_eq bag wf xs (fun _ => 1), sum_map_const, Nat.mul_one, expand_len]

end RepeatedSlicesProofs
