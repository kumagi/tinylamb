import TinylambOpt.ResearchProofs.StorageExecution.SE_BagKernels_Common

open TinylambOpt.ResearchProofs.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels

namespace EdgeDegreesProofs

open EdgeDegrees

theorem count_flatMap_pair : ∀ (es : List (Nat × Nat)) (k : Nat),
    ((es.flatMap fun (a,b) => [a,b]) : List Nat).count k =
      List.count k (es.map Prod.fst) + List.count k (es.map Prod.snd) := by
  intro es
  induction es with
  | nil => intro k; simp
  | cons e es ih =>
      intro k
      cases e with
      | mk a b =>
          rw [List.flatMap_cons, List.count_append, ih, List.map_cons, List.map_cons,
            List.count_cons, List.count_cons, List.count_cons, List.count_cons,
            List.count_nil, Nat.zero_add]
          by_cases h1 : (a == k) = true <;> by_cases h2 : (b == k) = true <;>
            simp [h1, h2] <;> omega

theorem wf : WellFormed bag := by
  constructor
  · exact count_flatMap_pair
  · intro es k hk
    induction es with
    | nil => exact absurd hk (by simp [bag])
    | cons e es ih =>
        cases e with
        | mk a b =>
            change k ∈ List.flatMap (fun (u,v) => [u,v]) ((a, b) :: es) at hk
            rw [List.flatMap_cons] at hk
            rw [List.mem_append] at hk
            rcases hk with hhead | htail
            · have hdisj : k = a ∨ k = b := by simpa using hhead
              show k ∈ List.map Prod.fst ((a, b) :: es) ++ List.map Prod.snd ((a, b) :: es)
              rw [List.mem_append]
              rcases hdisj with h1 | h2
              · subst h1
                left
                exact List.Mem.head (List.map Prod.fst es)
              · subst h2
                right
                exact List.Mem.head (List.map Prod.snd es)
            · have hi2 : k ∈ List.map Prod.fst es ++ List.map Prod.snd es := ih htail
              show k ∈ List.map Prod.fst ((a, b) :: es) ++ List.map Prod.snd ((a, b) :: es)
              rw [List.mem_append]
              rcases (List.mem_append.mp hi2) with h | h
              · left
                exact List.Mem.tail a h
              · right
                exact List.Mem.tail b h

theorem cut_sum : ∀ (es : List (Nat × Nat)) (inside : Nat → Bool),
    (((es.flatMap fun (a,b) => [a,b]) : List Nat).map fun v => (inside v).toNat).sum =
      2 * es.countP (fun p => inside p.1 && inside p.2) +
        es.countP (fun p => Bool.xor (inside p.1) (inside p.2)) := by
  intro es
  induction es with
  | nil => intro inside; rfl
  | cons e es ih =>
      intro inside
      cases e with
      | mk a b =>
          rw [List.flatMap_cons, List.map_append, List.sum_append, ih,
            List.countP_cons, List.countP_cons]
          cases ha : inside a <;> cases hb : inside b <;>
            simp [ha, hb, Nat.mul_add, Nat.mul_one, Nat.mul_zero, Nat.add_zero, Nat.zero_add]
          <;> omega

theorem sound_CutWeight : CutWeight.Goal := by
  intro es inside
  rw [weighted_eq bag wf]
  exact cut_sum es inside

theorem sound_LoopDegree : LoopDegree.Goal := by
  intro es x
  show List.count x (x :: List.map Prod.fst es) + List.count x (x :: List.map Prod.snd es) =
    (List.count x (List.map Prod.fst es) + List.count x (List.map Prod.snd es)) + 2
  rw [List.count_cons, List.count_cons, if_pos (by simp : (x == x) = true)]
  omega

theorem sound_DistinctOutput : DistinctOutput.Goal := wf_distinctOutput bag wf
theorem sound_FilteredMoment : FilteredMoment.Goal := wf_filteredMoment bag wf
theorem sound_OrderStatistic : OrderStatistic.Goal := wf_orderStatistic bag wf
theorem sound_JoinFanout : JoinFanout.Goal := wf_joinFanout bag wf
theorem sound_AntiRevenue : AntiRevenue.Goal := wf_antiRevenue bag wf
theorem sound_SelfPairs : SelfPairs.Goal := wf_selfPairs bag wf
theorem sound_IntersectAll : IntersectAll.Goal := wf_intersectAll bag wf
theorem sound_OuterWeight : OuterWeight.Goal := wf_outerWeight bag wf

end EdgeDegreesProofs
