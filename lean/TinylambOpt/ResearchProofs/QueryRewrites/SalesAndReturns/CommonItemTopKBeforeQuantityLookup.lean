import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns
import TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping

/-!
# CommonItemTopKBeforeQuantityLookup

The ordered prefix of item keys common to all three channels can be selected separately
from their quantities under the stated key assumptions.

The executor can locate the first 100 distinct matching keys and defer loading
quantities until afterward.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false


namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.CommonItemTopKBeforeQuantityLookup
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

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

theorem mergeSort_eq_of_perm {l₁ l₂ : List Nat} (hp : List.Perm l₁ l₂) :
    l₁.mergeSort = l₂.mergeSort := by
  have hp' : List.Perm l₁.mergeSort l₂.mergeSort :=
    ((List.mergeSort_perm (le := fun a b : Nat => decide (a ≤ b)) l₁).trans hp).trans (List.mergeSort_perm (le := fun a b : Nat => decide (a ≤ b)) l₂).symm
  exact List.Perm.eq_of_pairwise
    (by
      intro a b ha hb hab hba
      simp at hab hba
      exact Nat.le_antisymm hab hba)
    (sorted_pairwise l₁) (sorted_pairwise l₂) hp'

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.CommonItemTopKBeforeQuantityLookup.Goal := by
  intro sr cr wr stop qty
  dsimp only
  intro h
  let P : Nat → Bool := fun k => k ≤ stop
  let A := members (members (sr.filter P) (cr.filter P)) (wr.filter P)
  let C := members (members sr cr) wr
  have hmemA : ∀ k, k ∈ A ↔ k ∈ C ∧ P k = true := by
    intro k
    simp [A, C, P, members, List.mem_eraseDups, List.mem_filter]
    constructor
    · intro hleft
      have hleft' := And.left hleft
      have hwr := And.left (And.right hleft)
      have hk := And.right (And.right hleft)
      have hA := And.left hleft'
      have hcr := And.left (And.right hleft')
      have hsr := And.left hA
      exact And.intro (And.intro (And.intro hsr hcr) hwr) (by omega)
    · intro hright
      have hright' := And.left hright
      have hk := And.right hright
      have hwr := And.right hright'
      have hsr := And.left (And.left hright')
      have hcr := And.right (And.left hright')
      exact And.intro (And.intro (And.intro hsr (by omega)) (And.intro hcr (by omega))) (And.intro hwr (by omega))
  have hnodA : A.Nodup := by
    dsimp [A, members]
    exact (KeyGrouping.nodup_eraseDups _).filter _
  have hnodCF : (C.filter P).Nodup := by
    dsimp [C, members]
    exact ((KeyGrouping.nodup_eraseDups _).filter _).filter _
  have hpermA : List.Perm A (C.filter P) :=
    (List.perm_ext_iff_of_nodup hnodA hnodCF).2 (by
      intro k
      simpa using hmemA k)
  have hsortA : A.mergeSort = (C.filter P).mergeSort := mergeSort_eq_of_perm hpermA
  have hsortFilter : (C.filter P).mergeSort = C.mergeSort.filter P := by
    have hp : List.Perm (C.filter P).mergeSort (C.mergeSort.filter P) :=
      ((List.mergeSort_perm (le := fun a b : Nat => decide (a ≤ b)) (C.filter P)).trans
        ((List.mergeSort_perm (le := fun a b : Nat => decide (a ≤ b)) C).symm.filter P))
    have hleft := sorted_pairwise (C.filter P)
    have hright : (C.mergeSort.filter P).Pairwise (fun a b : Nat => decide (a ≤ b) = true) :=
      (sorted_pairwise C).filter P
    exact List.Perm.eq_of_pairwise
      (by
        intro a b ha hb hab hba
        simp at hab hba
        exact Nat.le_antisymm hab hba)
      hleft hright hp
  have hclosedP : ∀ a b : Nat, a ≤ b → P b = true → P a = true := by
    intro a b hab hpb
    simp [P] at hab hpb ⊢
    omega
  have hprefS : C.mergeSort.filter P = C.mergeSort.take (C.mergeSort.filter P).length :=
    filter_prefix (fun a b : Nat => a ≤ b) P hclosedP C.mergeSort (by
      have hs := sorted_pairwise C
      simpa using hs)
  have hlenCP : 100 ≤ (C.filter P).length := by
    simpa [C, P] using h
  have hlenSF : 100 ≤ (C.mergeSort.filter P).length := by
    have heq := ((List.mergeSort_perm (le := fun a b : Nat => decide (a ≤ b)) C).filter P).length_eq
    omega
  have htakeSF : (C.mergeSort.filter P).take 100 = C.mergeSort.take 100 := by
    rw [hprefS, List.take_take, Nat.min_eq_left hlenSF]
  calc
    (A.mergeSort.take 100).map (fun k => (k, qty k))
        = ((C.filter P).mergeSort.take 100).map (fun k => (k, qty k)) := by rw [hsortA]
    _ = ((C.mergeSort.filter P).take 100).map (fun k => (k, qty k)) := by rw [hsortFilter]
    _ = (C.mergeSort.take 100).map (fun k => (k, qty k)) := by rw [htakeSF]
    _ = (C.mergeSort.map (fun k => (k, qty k))).take 100 := by rw [List.map_take]
end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.CommonItemTopKBeforeQuantityLookup
