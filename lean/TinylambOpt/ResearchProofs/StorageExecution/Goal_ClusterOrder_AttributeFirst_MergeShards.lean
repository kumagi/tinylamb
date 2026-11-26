import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_AttributeFirst_MergeShards

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.AttributeFirst

private abbrev Rw := Nat × Nat

-- The merge comparator of `mergeShards`, specialized to this key.
private def leKey (x y : Rw) : Bool :=
  let a := key x
  let b := key y
  a.1 < b.1 || (a.1 == b.1 && a.2 ≤ b.2)

-- The comparator used by `perm` for this key (key = swap, ties by original ordinal).
private def cmpKey (ri rj : Rw × Nat) : Bool :=
  let a := key ri.1
  let b := key rj.1
  a.1 < b.1 || (a.1 == b.1 && (a.2 < b.2 || (a.2 == b.2 && ri.2 ≤ rj.2)))

private theorem leKey_iff (x y : Rw) :
    leKey x y = true ↔ (key x).1 < (key y).1 ∨ ((key x).1 = (key y).1 ∧ (key x).2 ≤ (key y).2) := by
  rcases x with ⟨x1, x2⟩
  rcases y with ⟨y1, y2⟩
  unfold leKey key
  simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq]
  by_cases h2 : x2 < y2 <;> by_cases he : x2 = y2 <;> by_cases h1 : x1 ≤ y1 <;> simp_all

private theorem leKey_trans : ∀ a b c : Rw, leKey a b = true → leKey b c = true → leKey a c = true := by
  intro a b c hab hbc
  rw [leKey_iff] at hab hbc ⊢
  rcases a with ⟨a1, a2⟩
  rcases b with ⟨b1, b2⟩
  rcases c with ⟨c1, c2⟩
  simp [key] at hab hbc ⊢
  omega

private theorem leKey_total : ∀ a b : Rw, (leKey a b || leKey b a) = true := by
  intro a b
  rw [Bool.or_eq_true]
  rw [leKey_iff, leKey_iff]
  rcases a with ⟨a1, a2⟩
  rcases b with ⟨b1, b2⟩
  simp [key]
  omega

private theorem leKey_antisymm : ∀ {a b : Rw}, leKey a b = true → leKey b a = true → a = b := by
  intro a b hab hba
  rw [leKey_iff] at hab hba
  rcases a with ⟨a1, a2⟩
  rcases b with ⟨b1, b2⟩
  simp [key] at hab hba
  ext <;> omega

private theorem cmpKey_leKey {a b : Rw × Nat} (h : cmpKey a b = true) : leKey a.1 b.1 = true := by
  unfold cmpKey key at h
  unfold leKey key
  simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h ⊢
  rcases h with hlt | ⟨hf, hlt2 | ⟨hs, _⟩⟩
  · exact Or.inl hlt
  · exact Or.inr ⟨hf, by omega⟩
  · exact Or.inr ⟨hf, by omega⟩

private theorem cmpKey_trans (a b c : Rw × Nat) (hab : cmpKey a b = true)
    (hbc : cmpKey b c = true) : cmpKey a c = true := by
  unfold cmpKey key at hab hbc ⊢
  simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at hab hbc ⊢
  rcases hab with hlt | ⟨habf, hlt2 | ⟨habs, habi⟩⟩
  · rcases hbc with hlt' | ⟨hbcf, hlt2' | ⟨hbcs, hbci⟩⟩
    · exact Or.inl (by omega)
    · exact Or.inl (by omega)
    · exact Or.inl (by omega)
  · rcases hbc with hlt' | ⟨hbcf, hlt2' | ⟨hbcs, hbci⟩⟩
    · exact Or.inl (by omega)
    · exact Or.inr ⟨by omega, Or.inl (by omega)⟩
    · exact Or.inr ⟨by omega, Or.inl (by omega)⟩
  · rcases hbc with hlt' | ⟨hbcf, hlt2' | ⟨hbcs, hbci⟩⟩
    · exact Or.inl (by omega)
    · exact Or.inr ⟨by omega, Or.inl (by omega)⟩
    · exact Or.inr ⟨by omega, Or.inr ⟨by omega, by omega⟩⟩

private theorem cmpKey_total (a b : Rw × Nat) : (cmpKey a b || cmpKey b a) = true := by
  unfold cmpKey key
  simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq]
  by_cases h : a.1.2 < b.1.2
  · exact Or.inl (Or.inl h)
  · by_cases h' : b.1.2 < a.1.2
    · exact Or.inr (Or.inl h')
    · have hf : a.1.2 = b.1.2 := by omega
      by_cases h2 : a.1.1 < b.1.1
      · exact Or.inl (Or.inr ⟨hf, Or.inl h2⟩)
      · by_cases h2' : b.1.1 < a.1.1
        · exact Or.inr (Or.inr ⟨hf.symm, Or.inl h2'⟩)
        · have hs : a.1.1 = b.1.1 := by omega
          by_cases hi : a.2 ≤ b.2
          · exact Or.inl (Or.inr ⟨hf, Or.inr ⟨hs, hi⟩⟩)
          · exact Or.inr (Or.inr ⟨hf.symm, Or.inr ⟨hs.symm, by omega⟩⟩)

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
          exact Or.inr ⟨rfl, by omega⟩
        · have hz' : x ≠ z := fun h => hz h.symm
          exact hpair2.1 x (by simpa [hz'] using hxmem)
      have hxz : leKey x z = true := by
        by_cases hz : z = x
        · rw [hz, leKey_iff]
          exact Or.inr ⟨rfl, by omega⟩
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

theorem sound_MergeShards : AttributeFirst.MergeShards.Goal := by
  intro a b
  change List.merge (physical key a) (physical key b) leKey = physical key (a ++ b)
  apply sorted_eq_of_perm
  · exact List.pairwise_merge leKey_trans leKey_total (physical key a) (physical key b)
      (physical_sorted a) (physical_sorted b)
  · exact physical_sorted (a ++ b)
  · exact (merge_perm_append (physical key a) (physical key b)).trans
      ((List.Perm.append (physical_perm a) (physical_perm b)).trans (physical_perm (a ++ b)).symm)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_AttributeFirst_MergeShards
