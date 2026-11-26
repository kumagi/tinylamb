import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

/-!
Ordered SQL scans and limits depend on preserving key order and ties across physical
clusters.

Proved

- `XorClass.MergeShards` — A stable merge of independently clustered shards recovers
  the global physical order.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_XorClass_MergeShards

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.XorClass

private abbrev Rw := Nat × Nat

--------------------------------------------------------------------------------
-- Key-dependent part: `XorClass.key (x,y) = (x ^^^ y, x)` is injective by XOR
-- cancellation, hence the lexicographic comparison on keys is a total order on
-- rows (with equal rows as the only ties).

-- The merge comparator of `mergeShards`, specialized to this key.
private def leKey (x y : Rw) : Bool :=
  let a := key x
  let b := key y
  a.1 < b.1 || (a.1 == b.1 && a.2 ≤ b.2)

-- The comparator used by `perm` for this key (ties by original ordinal).
private def cmpKey (ri rj : Rw × Nat) : Bool :=
  let a := key ri.1
  let b := key rj.1
  a.1 < b.1 || (a.1 == b.1 && (a.2 < b.2 || (a.2 == b.2 && ri.2 ≤ rj.2)))

-- Bool comparators are converted with `of_decide_eq_true` / `decide_eq_true`:
-- `simp [decide_eq_true_eq]` cannot fire here because the synthesized
-- `Decidable` instances still mention the unreduced `key` application.
private theorem leKey_iff (x y : Rw) : leKey x y = true ↔
    (key x).1 < (key y).1 ∨ ((key x).1 = (key y).1 ∧ (key x).2 ≤ (key y).2) := by
  dsimp [leKey]
  constructor
  · intro h
    simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq] at h
    rcases h with h | ⟨heq, hle⟩
    · exact Or.inl (of_decide_eq_true h)
    · exact Or.inr ⟨heq, of_decide_eq_true hle⟩
  · intro h
    simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq]
    rcases h with h | ⟨heq, hle⟩
    · exact Or.inl (decide_eq_true h)
    · exact Or.inr ⟨heq, decide_eq_true hle⟩

private theorem cmpKey_iff (ri rj : Rw × Nat) : cmpKey ri rj = true ↔
    (key ri.1).1 < (key rj.1).1 ∨
      ((key ri.1).1 = (key rj.1).1 ∧
        ((key ri.1).2 < (key rj.1).2 ∨
          ((key ri.1).2 = (key rj.1).2 ∧ ri.2 ≤ rj.2))) := by
  dsimp [cmpKey]
  constructor
  · intro h
    simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq] at h
    rcases h with h | ⟨heq, h | ⟨heq2, hord⟩⟩
    · exact Or.inl (of_decide_eq_true h)
    · exact Or.inr ⟨heq, Or.inl (of_decide_eq_true h)⟩
    · exact Or.inr ⟨heq, Or.inr ⟨heq2, of_decide_eq_true hord⟩⟩
  · intro h
    simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq]
    rcases h with h | ⟨heq, h | ⟨heq2, hord⟩⟩
    · exact Or.inl (decide_eq_true h)
    · exact Or.inr ⟨heq, Or.inl (decide_eq_true h)⟩
    · exact Or.inr ⟨heq, Or.inr ⟨heq2, decide_eq_true hord⟩⟩

private theorem leKey_trans : ∀ a b c : Rw, leKey a b = true → leKey b c = true → leKey a c = true := by
  intro a b c hab hbc
  rw [leKey_iff] at hab hbc ⊢
  omega

private theorem leKey_total : ∀ a b : Rw, (leKey a b || leKey b a) = true := by
  intro a b
  rw [Bool.or_eq_true, leKey_iff, leKey_iff]
  omega

private theorem leKey_antisymm {a b : Rw} (hab : leKey a b = true) (hba : leKey b a = true) : a = b := by
  rw [leKey_iff] at hab hba
  have h1 : (key a).1 = (key b).1 := by omega
  have h2 : (key a).2 = (key b).2 := by omega
  dsimp [key] at h1 h2
  ext
  · exact h2
  · have hx := congrArg (fun t : Nat => t ^^^ a.1) h1
    rw [← h2] at hx
    calc
      a.2 = (a.1 ^^^ a.2) ^^^ a.1 := by
        calc
          a.2 = (a.1 ^^^ a.1) ^^^ a.2 := by simp
          _ = (a.1 ^^^ a.2) ^^^ a.1 := by ac_rfl
      _ = (a.1 ^^^ b.2) ^^^ a.1 := hx
      _ = b.2 := by
        calc
          (a.1 ^^^ b.2) ^^^ a.1 = (a.1 ^^^ a.1) ^^^ b.2 := by ac_rfl
          _ = b.2 := by simp

private theorem cmpKey_leKey {a b : Rw × Nat} (h : cmpKey a b = true) : leKey a.1 b.1 = true := by
  rw [cmpKey_iff] at h
  rw [leKey_iff]
  omega

private theorem cmpKey_trans (a b c : Rw × Nat) (hab : cmpKey a b = true)
    (hbc : cmpKey b c = true) : cmpKey a c = true := by
  rw [cmpKey_iff] at hab hbc ⊢
  omega

private theorem cmpKey_total (a b : Rw × Nat) : (cmpKey a b || cmpKey b a) = true := by
  rw [Bool.or_eq_true, cmpKey_iff, cmpKey_iff]
  omega

--------------------------------------------------------------------------------
-- The remainder is key-independent: total-order sortedness + permutation.
-- (Same argument as the Lexicographic / Diagonal MergeShards proofs.)

private theorem getD_zipIdx (rs : List Rw) (ri : Rw × Nat) (h : ri ∈ rs.zipIdx) :
    rs[ri.2]?.getD (0, 0) = ri.1 := by
  rcases ri with ⟨r, i⟩
  have hm := List.mem_zipIdx h
  simp at hm
  have hg : rs[i]? = some rs[i] := List.getElem?_eq_getElem hm.1
  rw [hg]
  simpa using hm.2.symm

private theorem map_fst_zipIdx (rs : List Rw) : (rs.zipIdx.map Prod.fst) = rs := by
  induction rs with
  | nil => simp
  | cons r rs ih => simp [List.zipIdx_cons]

private theorem physical_eq_map_fst (rs : List Rw) :
    physical key rs = (rs.zipIdx.mergeSort cmpKey).map Prod.fst := by
  unfold physical perm
  change ((rs.zipIdx.mergeSort cmpKey).map Prod.snd).map (fun i => rs[i]?.getD (0, 0)) =
    (rs.zipIdx.mergeSort cmpKey).map Prod.fst
  simp only [List.map_map]
  apply List.map_congr_left
  intro ri hri
  exact getD_zipIdx rs ri ((List.mem_mergeSort (a := ri) (l := rs.zipIdx) (le := cmpKey)).1 hri)

private theorem physical_perm (rs : List Rw) : (physical key rs).Perm rs := by
  rw [physical_eq_map_fst]
  have h := (List.mergeSort_perm (rs.zipIdx) cmpKey).map Prod.fst
  simpa [map_fst_zipIdx] using h

private theorem physical_sorted (rs : List Rw) :
    (physical key rs).Pairwise (fun x y => leKey x y = true) := by
  rw [physical_eq_map_fst]
  have hs : (rs.zipIdx.mergeSort cmpKey).Pairwise (fun a b => leKey a.1 b.1 = true) := by
    apply (List.pairwise_mergeSort cmpKey_trans cmpKey_total (rs.zipIdx)).imp
    intro a b h
    exact cmpKey_leKey h
  exact hs.map Prod.fst (by intro a b h; exact h)

private theorem cons_append_perm (y : Rw) (xs ys : List Rw) :
    (y :: (xs ++ ys)).Perm (xs ++ y :: ys) := by
  induction xs with
  | nil => rfl
  | cons a xs ih =>
      exact (List.Perm.swap a y (xs ++ ys)).trans (List.Perm.cons a ih)

private theorem merge_perm_append : ∀ (l1 l2 : List Rw), (l1.merge l2 leKey).Perm (l1 ++ l2)
  | [], l2 => by simp
  | x :: xs, [] => by simp
  | x :: xs, y :: ys => by
      by_cases h : leKey x y = true
      · simp [h]
        exact merge_perm_append xs (y :: ys)
      · simp [h]
        exact (List.Perm.cons y (merge_perm_append (x :: xs) ys)).trans
          ((List.Perm.swap x y (xs ++ ys)).trans
            (List.Perm.cons x (cons_append_perm y xs ys)))

private theorem sorted_eq_of_perm : ∀ (l1 l2 : List Rw),
    l1.Pairwise (fun x y => leKey x y = true) →
    l2.Pairwise (fun x y => leKey x y = true) →
    l1.Perm l2 → l1 = l2
  | [], [], _, _, _ => rfl
  | [], _ :: _, _, _, hp => by simp at hp
  | _ :: _, [], _, _, hp => by simp at hp
  | x :: xs, z :: zs, h1, h2, hp => by
      have hpair1 : (∀ y ∈ xs, leKey x y = true) ∧ xs.Pairwise (fun a b => leKey a b = true) := by
        simpa using h1
      have hpair2 : (∀ y ∈ zs, leKey z y = true) ∧ zs.Pairwise (fun a b => leKey a b = true) := by
        simpa using h2
      have hxmem : x ∈ z :: zs := hp.mem_iff.mp (by simp)
      have hzmem : z ∈ x :: xs := hp.symm.mem_iff.mp (by simp)
      have hzx : leKey z x = true := by
        by_cases hz : z = x
        · rw [hz, leKey_iff]
          exact Or.inr ⟨rfl, Nat.le_refl _⟩
        · have hz' : x ≠ z := fun h => hz h.symm
          exact hpair2.1 x (by simpa [hz'] using hxmem)
      have hxz : leKey x z = true := by
        by_cases hz : z = x
        · rw [hz, leKey_iff]
          exact Or.inr ⟨rfl, Nat.le_refl _⟩
        · exact hpair1.1 z (by simpa [hz] using hzmem)
      have hzx' : z = x := leKey_antisymm hzx hxz
      subst z
      exact congrArg (List.cons x) (sorted_eq_of_perm xs zs hpair1.2 hpair2.2 (List.Perm.cons_inv hp))

-- Truth check on small inputs (all `true`).
private def checkOne (a b : List Rw) : Bool :=
  List.merge (physical key a) (physical key b) leKey == physical key (a ++ b)

#eval checkOne [(2, 1), (1, 1), (2, 0)] [(1, 1), (0, 5), (2, 0)]
#eval checkOne [(0, 0), (1, 1), (1, 2)] [(1, 1), (0, 0), (2, 3)]
#eval checkOne [] [(0, 0), (0, 0)]
#eval checkOne [(5, 1), (3, 1), (9, 1)] [(4, 1), (1, 1)]
#eval checkOne [(0, 3), (4, 0), (2, 2)] [(3, 3), (1, 0), (7, 7)]
#eval checkOne [(3, 0), (1, 2), (2, 1), (0, 3)] [(2, 0), (3, 1), (1, 1)]

theorem sound_MergeShards : XorClass.MergeShards.Goal := by
  intro a b
  change List.merge (physical key a) (physical key b) leKey = physical key (a ++ b)
  apply sorted_eq_of_perm
  · exact List.pairwise_merge leKey_trans leKey_total (physical key a) (physical key b)
      (physical_sorted a) (physical_sorted b)
  · exact physical_sorted (a ++ b)
  · exact (merge_perm_append (physical key a) (physical key b)).trans
      ((List.Perm.append (physical_perm a) (physical_perm b)).trans (physical_perm (a ++ b)).symm)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_XorClass_MergeShards
