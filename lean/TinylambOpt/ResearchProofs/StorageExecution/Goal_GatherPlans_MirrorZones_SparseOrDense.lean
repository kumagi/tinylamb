import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

/-!
Late materialization can gather payloads at selected row positions while preserving the
selection's order and multiplicities.

Proved

- `MirrorZones.SparseOrDense` — `∀ (zone penalty : Nat), sparseOrDense (price zone
  penalty)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_MirrorZones_SparseOrDense

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.MirrorZones

-- The plan that reads every point singly is enumerated by `covers`.
private theorem singles_mem_covers : ∀ ks : List Nat, ks.map (fun k => (k, k)) ∈ covers ks := by
  intro ks
  induction ks with
  | nil => simp [covers]
  | cons x tl ih =>
      cases tl with
      | nil => simp [covers]
      | cons y ys =>
          rw [covers]
          rw [List.mem_flatMap]
          refine ⟨(y :: ys).map (fun k => (k, k)), ?_, ?_⟩
          · simpa using ih
          · simp

-- The single spanning extent from the first to the last point is enumerated too.
private theorem span_mem_covers : ∀ ks : List Nat, ks ≠ [] →
    [(ks.head?.getD 0, ks.getLast?.getD 0)] ∈ covers ks := by
  intro ks hne
  induction ks with
  | nil => contradiction
  | cons x tl ih =>
      cases tl with
      | nil => simp [covers]
      | cons y ys =>
          have htne : y :: ys ≠ [] := by simp
          have hih := ih htne
          simp only [List.head?_cons, Option.getD_some] at hih
          rw [covers]
          rw [List.mem_flatMap]
          refine ⟨[(y, (y :: ys).getLast?.getD 0)], hih, ?_⟩
          simp [List.getLast?_cons]

-- `choose` is the head of a cost-sorted list of all covers, so its cost is
-- bounded by the cost of any particular enumerated cover.
private theorem choose_le_cost {f : Price} {xs : List Nat} {ps : List Extent}
    (hps : ps ∈ covers (points xs)) : cost f (choose f xs) ≤ cost f ps := by
  unfold choose
  let cs := covers (points xs)
  let cmp : List Extent → List Extent → Bool := fun a b => decide (cost f a ≤ cost f b)
  change cost f ((cs.mergeSort cmp).head?.getD []) ≤ cost f ps
  have hpsms : ps ∈ cs.mergeSort cmp := (List.mem_mergeSort).2 hps
  have hsorted : List.Pairwise (fun a b => cmp a b = true) (cs.mergeSort cmp) :=
    List.pairwise_mergeSort (le := cmp)
      (trans := by
        intro a b c hab hbc
        simp only [cmp, decide_eq_true_eq] at hab hbc ⊢
        exact Nat.le_trans hab hbc)
      (total := by
        intro a b
        by_cases h : cost f a ≤ cost f b
        · simp [cmp, h]
        · have h2 : cost f b ≤ cost f a := Nat.le_of_not_ge h
          simp [cmp, h, h2])
      cs
  cases hms : cs.mergeSort cmp with
  | nil => simp [hms] at hpsms
  | cons e tl =>
      rw [hms] at hsorted hpsms
      have hhead : ∀ b ∈ tl, cost f e ≤ cost f b := by
        intro b hb
        have hb' := (List.pairwise_cons.mp hsorted).1 b hb
        simpa only [cmp, decide_eq_true_eq] using hb'
      change cost f e ≤ cost f ps
      rcases List.mem_cons.mp hpsms with rfl | hpsms
      · exact Nat.le_refl _
      · exact hhead ps hpsms

theorem sound_SparseOrDense : MirrorZones.SparseOrDense.Goal := by
  intro zone penalty xs hxs
  rw [Nat.le_min]
  constructor
  · have hs : (points xs).map (fun k => (k, k)) ∈ covers (points xs) :=
      singles_mem_covers (points xs)
    have hle := choose_le_cost (f := price zone penalty) (xs := xs)
      (ps := (points xs).map (fun k => (k, k))) hs
    simpa [cost, Function.comp_def] using hle
  · have hne : points xs ≠ [] := by
      cases xs with
      | nil => contradiction
      | cons x tl =>
          intro hnil
          have hxmem : x ∈ points (x :: tl) := by simp [points]
          simp [hnil] at hxmem
    have hs : [((points xs).head?.getD 0, (points xs).getLast?.getD 0)] ∈ covers (points xs) :=
      span_mem_covers (points xs) hne
    have hle := choose_le_cost (f := price zone penalty) (xs := xs)
      (ps := [((points xs).head?.getD 0, (points xs).getLast?.getD 0)]) hs
    simpa [cost] using hle

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_MirrorZones_SparseOrDense
