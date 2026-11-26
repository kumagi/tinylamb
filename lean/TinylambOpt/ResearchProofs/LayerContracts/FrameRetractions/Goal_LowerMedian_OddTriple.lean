import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_LowerMedian

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Proved

- `LowerMedian.OddTriple` — `∀ x y z, x ≤ y → y ≤ z → value model [z,x,y] = some y`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_LowerMedian_OddTriple

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open List

theorem pairwise_ms (l : List Nat) : Pairwise (· ≤ ·) (l.mergeSort (fun a b => decide (a ≤ b))) := by
  have key : Pairwise (fun a b : Nat => (decide (a ≤ b)) = true) (l.mergeSort (fun a b => decide (a ≤ b))) :=
    pairwise_mergeSort (le := fun a b : Nat => decide (a ≤ b))
      (by
        intro a b c ha hb
        simp only [decide_eq_true_eq] at ha hb ⊢
        omega)
      (by
        intro a b
        cases Nat.le_total a b with
        | inl h => simp [h]
        | inr h => simp [h]) l
  simpa only [decide_eq_true_eq] using key

theorem getD_eq_getElem {xs : List Nat} {n d : Nat} (h : n < xs.length) :
    xs[n]?.getD d = xs[n] := by
  rw [List.getElem?_eq_getElem h, Option.getD_some]

theorem sound : LowerMedian.OddTriple.Goal := by
  intro x y z hx hy
  have hp : Pairwise (· ≤ ·) [x, y, z] := by
    refine Pairwise.cons ?_ (Pairwise.cons ?_ (Pairwise.cons ?_ Pairwise.nil))
    · intro b hb
      simp only [List.mem_cons, List.mem_nil_iff, or_false] at hb
      rcases hb with hb | hb
      · subst b
        exact hx
      · subst b
        exact Nat.le_trans hx hy
    · intro b hb
      have hbz : b = z := by simpa using hb
      subst b
      exact hy
    · intro b hb
      simp at hb
  have hperm : [z, x, y] ~ [x, y, z] :=
    ((List.Perm.swap z x [y]).symm).trans (List.Perm.cons x ((List.Perm.swap z y []).symm))
  have heq : [z, x, y].mergeSort (fun a b => decide (a ≤ b)) = [x, y, z] :=
    Perm.eq_of_pairwise (fun p q _ _ hpq hqp => Nat.le_antisymm hpq hqp)
      (pairwise_ms [z, x, y]) hp
      ((mergeSort_perm [z, x, y] (fun a b => decide (a ≤ b))).trans hperm)
  unfold value LowerMedian.model
  simp only [List.isEmpty_cons, Bool.false_eq_true, ↓reduceIte]
  apply Option.some.inj
  unfold lowerMedian
  rw [heq, show ((([z, x, y] : List Nat).length - 1) / 2) = 1 from by simp]
  rw [getD_eq_getElem (by simp : (1 : Nat) < [x, y, z].length)]
  simp

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_LowerMedian_OddTriple
