import TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2

/-!
# Positive-baseline filter before top-k (TPC-DS Q74)

If at least `k` valid candidates already lie below the ordinal fence `tau`, the first
`k` entries of the sorted full list are exactly the first `k` entries of the sorted
fenced list. The executor may discard out-of-fence candidates early and defer wide
customer attribute lookups to the survivors.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q74_PositiveBaselineFilterBeforeTopKGoal

open TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2
open TinylambOpt.ResearchGoals.QueryRewrites

/-- A downward-closed predicate selects a prefix of a sorted list. -/
theorem filter_prefix {α : Type} (le : α → α → Prop) (p : α → Bool)
    (hclosed : ∀ a b, le a b → p b = true → p a = true) :
    ∀ l : List α, l.Pairwise le → l.filter p = l.take (l.filter p).length := by
  intro l hp
  induction l with
  | nil => simp
  | cons a t ih =>
      have hp' := List.pairwise_cons.mp hp
      by_cases ha : p a = true
      · have iht := ih hp'.2
        rw [List.filter_cons, ite_eq_left ha]
        simp only [List.length_cons]
        change a :: t.filter p = a :: t.take (t.filter p).length
        exact congrArg (List.cons a) iht
      · have hz : t.filter p = [] := by
          rw [List.filter_eq_nil_iff]
          intro b hb hpb
          exact ha (hclosed a b (hp'.1 b hb) hpb)
        simp [ha, hz]

/-- `mergeSort` on Nat produces the nondecreasing pairwise ordering. -/
theorem sorted_pairwise (l : List Nat) :
    l.mergeSort.Pairwise (fun a b : Nat => decide (a ≤ b) = true) := by
  have htrans : ∀ a b c : Nat, decide (a ≤ b) = true → decide (b ≤ c) = true →
      decide (a ≤ c) = true := by
    intros a b c hab hbc
    simp at hab hbc ⊢
    omega
  have htotal : ∀ a b : Nat, (decide (a ≤ b) || decide (b ≤ a)) = true := by
    intro a b
    by_cases h : a ≤ b
    · simp [h]
    · have h' : b ≤ a := by omega
      simp [h, h']
  simpa using (List.pairwise_mergeSort htrans htotal l)

theorem sound : Q74_PositiveBaselineFilterBeforeTopKGoal := by
  intro cands k tau
  dsimp only
  intro hk
  let P : Nat → Bool := fun x => decide (x ≤ tau)
  let A := cands.filter (fun r => decide (0 < r.2.1 ∧ 0 < r.2.2)) |>.map Prod.fst
  change best k (A.filter P) = best k A
  have hcount : k ≤ (A.filter P).length := hk
  let sortedA := A.mergeSort
  let sortedP := (A.filter P).mergeSort
  have hmin : k ≤ (sortedA.filter P).length := by
    have hp := (List.mergeSort_perm (le := fun a b : Nat => decide (a ≤ b)) A).filter P
    have hlen := hp.length_eq
    dsimp [sortedA]
    omega
  have hprefix : sortedA.filter P = sortedA.take (sortedA.filter P).length :=
    filter_prefix (fun a b : Nat => a ≤ b) P
      (fun a b hab hpb => by simp [P] at hpb ⊢; omega) sortedA
      (by simpa [sortedA] using sorted_pairwise A)
  have hsortP : sortedP = sortedA.filter P := by
    have hsortedP : sortedP.Pairwise (fun a b : Nat => decide (a ≤ b) = true) := by
      dsimp [sortedP]
      exact List.pairwise_mergeSort
        (fun a b c hab hbc => by simp only [decide_eq_true_eq] at hab hbc ⊢; omega)
        (fun a b => by simp only [Bool.or_eq_true, decide_eq_true_eq]; omega)
        (A.filter P)
    have hpairLeft : sortedP.Pairwise (fun a b : Nat => a ≤ b) :=
      hsortedP.imp (fun h => by simpa using h)
    have hpairRight : (sortedA.filter P).Pairwise (fun a b : Nat => a ≤ b) :=
      (sorted_pairwise A).filter P |>.imp (fun h => by simpa using h)
    have hperm : (List.filter P A).Perm (List.filter P sortedA) :=
      (List.mergeSort_perm (le := fun a b : Nat => decide (a ≤ b)) A).symm.filter P
    have hperm' : sortedP.Perm (sortedA.filter P) :=
      (List.mergeSort_perm (le := fun a b : Nat => decide (a ≤ b)) (A.filter P)).trans hperm
    exact List.Perm.eq_of_pairwise
      (l₁ := sortedP) (l₂ := sortedA.filter P) (le := fun a b : Nat => a ≤ b)
      (fun a b _ _ hab hba => Nat.le_antisymm hab hba) hpairLeft hpairRight hperm'
  have htake : sortedP.take k = sortedA.take k := by
    rw [hsortP, hprefix, List.take_take, Nat.min_eq_left hmin]
  simpa [best, sortedA, sortedP] using htake

end TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q74_PositiveBaselineFilterBeforeTopKGoal
