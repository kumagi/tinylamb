import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `MonotoneBreaks.ParallelMergeFree` — `∀ a b, a.Pairwise (· ≤ ·) → b.Pairwise (· ≤ ·)
  → (∀ x∈a, ∀ y∈b, x≤y) → (agg.reference (a++b)).all (fun v => v.2.2==0)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_MonotoneBreaks_ParallelMergeFree

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.MonotoneBreaks

private def smallLists : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 =>
      let prev := smallLists n
      prev ++ prev.flatMap (fun xs => (List.range 4).map (fun v => xs ++ [v]))

private def checkSmall (n : Nat) : Bool :=
  (smallLists n).all (fun a => (smallLists n).all (fun b =>
    if _h : a.Pairwise (· ≤ ·) ∧ b.Pairwise (· ≤ ·) ∧ (∀ x ∈ a, ∀ y ∈ b, x ≤ y) then
      decide ((agg.reference (a ++ b)).all (fun v => v.2.2 == 0) = true)
    else true))

#eval checkSmall 0
#eval checkSmall 2
#eval checkSmall 3
#eval checkSmall 4

private theorem countDescZero : ∀ xs : List Nat, xs.Pairwise (· ≤ ·) →
    (edges xs).countP (fun (x, y) => y < x) = 0 := by
  intro xs h
  induction xs with
  | nil => simp [edges]
  | cons x xs ih =>
      cases xs with
      | nil => simp [edges]
      | cons y ys =>
          have hp := List.pairwise_cons.mp h
          have hxy : x ≤ y := hp.1 y (by simp)
          have htail : (y :: ys).Pairwise (· ≤ ·) := hp.2
          have ih' := ih htail
          have hedge : edges (x :: y :: ys) = (x, y) :: edges (y :: ys) := by
            simp [edges]
          rw [hedge]
          simp [hxy, ih']

theorem sound_ParallelMergeFree : MonotoneBreaks.ParallelMergeFree.Goal := by
  intro a b ha hb hcross
  have hp : (a ++ b).Pairwise (· ≤ ·) := (List.pairwise_append).2 ⟨ha, hb, hcross⟩
  have hcnt : (edges (a ++ b)).countP (fun (x, y) => y < x) = 0 :=
    countDescZero (a ++ b) hp
  cases hh : (a ++ b).head? with
  | none => simp [agg, hh]
  | some x => simp [agg, hh, hcnt]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_MonotoneBreaks_ParallelMergeFree
