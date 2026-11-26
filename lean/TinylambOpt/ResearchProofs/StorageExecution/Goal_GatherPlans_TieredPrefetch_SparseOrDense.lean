import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_TieredPrefetch_SparseOrDense

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.TieredPrefetch

/-! The optimizer takes the cheapest member of the exhaustive list of covers.
Both the all-singleton plan and the single bounding extent are members of that
list, so their costs are upper bounds for the chosen plan.  This argument does
not depend on the shape of the price function, hence it applies to the tiered
prefetch price in particular. -/

private theorem pairwise_head?_le {α : Type} {le : α → α → Bool}
    (hrefl : ∀ x, le x x = true)
    {l : List α} {a b : α} (hpw : l.Pairwise (fun x y => le x y = true))
    (hhead : l.head? = some a) (hmem : b ∈ l) : le a b = true := by
  cases l with
  | nil => simp at hmem
  | cons x xs =>
    simp at hhead
    subst hhead
    rw [List.pairwise_cons] at hpw
    rcases List.mem_cons.mp hmem with rfl | hmem
    · exact hrefl _
    · exact hpw.1 b hmem

private theorem choose_le (f : Price) (xs : List Nat) (ps : List Extent)
    (hmem : ps ∈ covers (points xs)) :
    cost f (choose f xs) ≤ cost f ps := by
  unfold choose
  let le := fun a b => decide (cost f a ≤ cost f b)
  have hpair : List.Pairwise (fun a b => le a b = true)
      ((covers (points xs)).mergeSort le) := by
    apply List.pairwise_mergeSort
    · intro a b c hab hbc
      simp only [le, decide_eq_true_eq] at hab hbc ⊢
      exact Nat.le_trans hab hbc
    · intro a b
      simp only [le, Bool.or_eq_true, decide_eq_true_eq]
      exact Nat.le_total (cost f a) (cost f b)
  have hmem' : ps ∈ (covers (points xs)).mergeSort le := List.mem_mergeSort.mpr hmem
  cases h : (covers (points xs)).mergeSort le with
  | nil => simp [h] at hmem'
  | cons hd tl =>
    simp only [h, List.head?_cons, Option.getD_some]
    have hle := pairwise_head?_le (fun x => by simp [le]) hpair
      (show ((covers (points xs)).mergeSort le).head? = some hd by simp [h]) hmem'
    simpa [le] using hle

private theorem points_ne_nil (xs : List Nat) (hxs : xs ≠ []) : points xs ≠ [] := by
  cases xs with
  | nil => contradiction
  | cons x xs =>
      intro h
      have hlen := congrArg List.length h
      simp [points, List.eraseDups_cons, List.length_mergeSort] at hlen

private theorem singleton_cover (ks : List Nat) :
    ks.map (fun k => (k, k)) ∈ covers ks := by
  induction ks with
  | nil => simp [covers]
  | cons x xs ih =>
      rw [covers]
      refine List.mem_flatMap.mpr ⟨xs.map (fun k => (k, k)), ih, ?_⟩
      cases xs with
      | nil => simp
      | cons y ys => simp

private theorem bounding_cover (ks : List Nat) (hks : ks ≠ []) :
    [(ks.head?.getD 0, ks.getLast?.getD 0)] ∈ covers ks := by
  induction ks with
  | nil => contradiction
  | cons x xs ih =>
      cases xs with
      | nil => simp [covers]
      | cons y ys =>
          have hq : [((y :: ys).head?.getD 0, (y :: ys).getLast?.getD 0)] ∈
              covers (y :: ys) := ih (by simp)
          rw [covers]
          refine List.mem_flatMap.mpr
            ⟨[((y :: ys).head?.getD 0, (y :: ys).getLast?.getD 0)], hq, ?_⟩
          simp

theorem sound_SparseOrDense : TieredPrefetch.SparseOrDense.Goal := by
  intro limit spill xs hxs
  dsimp only [sparseOrDense]
  let f : Price := price limit spill
  let ks := points xs
  change cost f (choose f xs) ≤ min ((ks.map (fun k => f k k)).sum)
    (f (ks.head?.getD 0) (ks.getLast?.getD 0))
  have hks : ks ≠ [] := by
    dsimp [ks]
    exact points_ne_nil xs hxs
  have hsparse : cost f (choose f xs) ≤ (ks.map (fun k => f k k)).sum := by
    have hle := choose_le f xs (ks.map (fun k => (k, k))) (by
      dsimp [ks]
      exact singleton_cover (points xs))
    have heq : cost f (ks.map (fun k => (k, k))) = (ks.map (fun k => f k k)).sum := by
      induction ks with
      | nil => simp [cost]
      | cons k ks ih => simp [cost, Function.comp_def, ih]
    simpa [heq] using hle
  have hdense : cost f (choose f xs) ≤ f (ks.head?.getD 0) (ks.getLast?.getD 0) := by
    have hle := choose_le f xs [(ks.head?.getD 0, ks.getLast?.getD 0)] (by
      dsimp [ks]
      exact bounding_cover (points xs) (points_ne_nil xs hxs))
    simpa [cost] using hle
  exact Nat.le_min.mpr ⟨hsparse, hdense⟩

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_TieredPrefetch_SparseOrDense
