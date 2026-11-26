import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_UnorderedPair_MergeShards

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.UnorderedPair

private abbrev Rw := Nat × Nat
private abbrev Ann := Rw × Nat

private def leRow (x y : Rw) : Bool := x.1 < y.1 || (x.1 == y.1 && x.2 ≤ y.2)

private def cmpKey (x y : Rw) : Bool := leRow (key x) (key y)

-- The comparator used by `perm`: key, then original ordinal.
private def cmpAnn (ri rj : Ann) : Bool :=
  let a := key ri.1
  let b := key rj.1
  a.1 < b.1 || (a.1 == b.1 && (a.2 < b.2 || (a.2 == b.2 && ri.2 ≤ rj.2)))

private theorem leRow_antisymm {a b : Rw} : leRow a b = true → leRow b a = true → a = b := by
  intro hab hba
  simp [leRow] at hab hba
  ext <;> omega

private theorem cmpAnn_leRow {a b : Ann} (h : cmpAnn a b = true) :
    leRow (key a.1) (key b.1) = true := by
  unfold cmpAnn at h
  unfold leRow
  simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h ⊢
  rcases h with hlt | ⟨hf, hlt2 | ⟨hs, _⟩⟩
  · exact Or.inl hlt
  · exact Or.inr ⟨hf, by omega⟩
  · exact Or.inr ⟨hf, by omega⟩

private theorem cmpAnn_ord_le_of_key_eq {a b : Ann} (hk : key a.1 = key b.1)
    (h : cmpAnn a b = true) : a.2 ≤ b.2 := by
  unfold cmpAnn at h
  simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h
  rcases h with hlt | ⟨hf, hlt2 | ⟨hs, hi⟩⟩
  · have hf' : (key a.1).1 = (key b.1).1 := by rw [hk]
    omega
  · have hs' : (key a.1).2 = (key b.1).2 := by rw [hk]
    omega
  · exact hi

private theorem cmpAnn_trans (a b c : Ann) (hab : cmpAnn a b = true)
    (hbc : cmpAnn b c = true) : cmpAnn a c = true := by
  unfold cmpAnn at hab hbc ⊢
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

private theorem cmpAnn_total (a b : Ann) : (cmpAnn a b || cmpAnn b a) = true := by
  unfold cmpAnn
  simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq]
  by_cases h : (key a.1).1 < (key b.1).1
  · exact Or.inl (Or.inl h)
  · by_cases h' : (key b.1).1 < (key a.1).1
    · exact Or.inr (Or.inl h')
    · have hf : (key a.1).1 = (key b.1).1 := by omega
      by_cases h2 : (key a.1).2 < (key b.1).2
      · exact Or.inl (Or.inr ⟨hf, Or.inl h2⟩)
      · by_cases h2' : (key b.1).2 < (key a.1).2
        · exact Or.inr (Or.inr ⟨hf.symm, Or.inl h2'⟩)
        · have hs : (key a.1).2 = (key b.1).2 := by omega
          by_cases hi : a.2 ≤ b.2
          · exact Or.inl (Or.inr ⟨hf, Or.inr ⟨hs, hi⟩⟩)
          · exact Or.inr (Or.inr ⟨hf.symm, Or.inr ⟨hs.symm, by omega⟩⟩)

private theorem cmpAnn_shift (n : Nat) (x y : Ann) :
    cmpAnn (x.1, x.2 + n) (y.1, y.2 + n) = cmpAnn x y := by
  unfold cmpAnn
  simp [Nat.add_le_add_iff_right]

private theorem zipIdx_offset {α : Type} (rs : List α) (n : Nat) :
    rs.zipIdx n = rs.zipIdx.map (fun p => (p.1, p.2 + n)) := by
  induction rs generalizing n with
  | nil => simp
  | cons r rs ih =>
      rw [List.zipIdx_cons, List.zipIdx_cons]
      simp only [List.map_cons, Nat.zero_add]
      congr 1
      rw [ih (n + 1), ih 1]
      simp only [List.map_map]
      apply List.map_congr_left
      intro p hp
      ext <;> simp <;> omega

private theorem snd_zipIdx_ge (rs : List Rw) (n k : Nat) :
    k ∈ (rs.zipIdx n).map Prod.snd → n ≤ k := by
  intro h
  induction rs generalizing n with
  | nil => simp at h
  | cons r rs ih =>
      rw [List.zipIdx_cons] at h
      simp only [List.map_cons, List.mem_cons] at h
      rcases h with rfl | hk
      · omega
      · have := ih (n + 1) hk
        omega

private theorem snd_zipIdx_lt (rs : List Rw) (n k : Nat) :
    k ∈ (rs.zipIdx n).map Prod.snd → k < n + rs.length := by
  intro h
  induction rs generalizing n with
  | nil => simp at h
  | cons r rs ih =>
      rw [List.zipIdx_cons] at h
      simp only [List.map_cons, List.mem_cons, List.length_cons] at h ⊢
      rcases h with rfl | hk
      · omega
      · have := ih (n + 1) hk
        omega

private theorem snd_zipIdx_nodup (rs : List Rw) (n : Nat) :
    ((rs.zipIdx n).map Prod.snd).Nodup := by
  induction rs generalizing n with
  | nil => simp
  | cons r rs ih =>
      rw [List.zipIdx_cons]
      simp only [List.map_cons, List.nodup_cons]
      constructor
      · intro hmem
        have hge := snd_zipIdx_ge rs (n + 1) n hmem
        omega
      · exact ih (n + 1)

private theorem getD_zipIdx (rs : List Rw) (ri : Ann) (h : ri ∈ rs.zipIdx) :
    rs[ri.2]?.getD (0, 0) = ri.1 := by
  rcases ri with ⟨r, i⟩
  have hm := List.mem_zipIdx h
  simp at hm
  have hg : rs[i]? = some rs[i] := List.getElem?_eq_getElem hm.1
  rw [hg]
  simpa using hm.2.symm

private theorem physical_eq_map_fst (rs : List Rw) :
    physical key rs = (rs.zipIdx.mergeSort cmpAnn).map Prod.fst := by
  unfold physical perm
  change ((rs.zipIdx.mergeSort cmpAnn).map Prod.snd).map (fun i => rs[i]?.getD (0, 0)) =
    (rs.zipIdx.mergeSort cmpAnn).map Prod.fst
  simp only [List.map_map]
  apply List.map_congr_left
  intro ri hri
  exact getD_zipIdx rs ri ((List.mem_mergeSort (a := ri) (l := rs.zipIdx) (le := cmpAnn)).1 hri)

private theorem cons_append_perm {α : Type} (y : α) (xs ys : List α) :
    (y :: (xs ++ ys)).Perm (xs ++ y :: ys) := by
  induction xs with
  | nil => rfl
  | cons a xs ih =>
      exact (List.Perm.swap a y (xs ++ ys)).trans (List.Perm.cons a ih)

private theorem merge_perm_append : ∀ (l1 l2 : List Ann), (l1.merge l2 cmpAnn).Perm (l1 ++ l2)
  | [], l2 => by simp
  | x :: xs, [] => by simp
  | x :: xs, y :: ys => by
      by_cases h : cmpAnn x y = true
      · simp [h]
        exact merge_perm_append xs (y :: ys)
      · simp [h]
        exact (List.Perm.cons y (merge_perm_append (x :: xs) ys)).trans
          ((List.Perm.swap x y (xs ++ ys)).trans
            (List.Perm.cons x (cons_append_perm y xs ys)))

-- Uniqueness of a sorted annotated list: ordinals are unique in the lists we use,
-- so equal keys and equal ordinals force equal elements even though `key` is not injective.
private theorem sorted_eq_of_perm : ∀ (l1 l2 : List Ann),
    l1.Pairwise (fun x y => cmpAnn x y = true) →
    l2.Pairwise (fun x y => cmpAnn x y = true) →
    l1.Perm l2 →
    (l1.map Prod.snd).Nodup →
    l1 = l2
  | [], [], _, _, _, _ => rfl
  | [], _ :: _, _, _, hp, _ => by simp at hp
  | _ :: _, [], _, _, hp, _ => by simp at hp
  | x :: xs, z :: zs, h1, h2, hp, hn => by
      have hpair1 : (∀ y ∈ xs, cmpAnn x y = true) ∧ xs.Pairwise (fun a b => cmpAnn a b = true) := by
        simpa using h1
      have hpair2 : (∀ y ∈ zs, cmpAnn z y = true) ∧ zs.Pairwise (fun a b => cmpAnn a b = true) := by
        simpa using h2
      have hxmem : x ∈ z :: zs := hp.mem_iff.mp (by simp)
      have hzmem : z ∈ x :: xs := hp.symm.mem_iff.mp (by simp)
      have hzx : cmpAnn z x = true := by
        by_cases hz : z = x
        · simp [hz, cmpAnn]
        · have hz' : x ≠ z := fun h => hz h.symm
          exact hpair2.1 x (by simpa [hz'] using hxmem)
      have hxz : cmpAnn x z = true := by
        by_cases hz : z = x
        · simp [hz, cmpAnn]
        · exact hpair1.1 z (by simpa [hz] using hzmem)
      have hkey : key x.1 = key z.1 := leRow_antisymm (cmpAnn_leRow hxz) (cmpAnn_leRow hzx)
      have hordxz : x.2 ≤ z.2 := cmpAnn_ord_le_of_key_eq hkey hxz
      have hordzx : z.2 ≤ x.2 := cmpAnn_ord_le_of_key_eq hkey.symm hzx
      have hord : x.2 = z.2 := by omega
      have hzx' : z = x := by
        by_cases hne : z = x
        · exact hne
        · have hztail : z ∈ xs := by simpa [hne] using hzmem
          simp only [List.map_cons, List.nodup_cons] at hn
          exfalso
          apply hn.1
          rw [hord]
          exact List.mem_map.mpr ⟨z, hztail, rfl⟩
      subst z
      simp only [List.map_cons, List.nodup_cons] at hn
      exact congrArg (List.cons x) (sorted_eq_of_perm xs zs hpair1.2 hpair2.2 (List.Perm.cons_inv hp) hn.2)

private theorem cmpAnn_eq_cmpKey {x y : Ann} (hxy : x.2 < y.2) :
    cmpAnn x y = cmpKey x.1 y.1 := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    change leRow (key x.1) (key y.1) = true
    exact cmpAnn_leRow h
  · intro h
    change leRow (key x.1) (key y.1) = true at h
    unfold cmpAnn
    unfold leRow at h
    simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h ⊢
    rcases h with hlt | ⟨hf, hge⟩
    · exact Or.inl hlt
    · by_cases h2 : (key x.1).2 < (key y.1).2
      · exact Or.inr ⟨hf, Or.inl h2⟩
      · exact Or.inr ⟨hf, Or.inr ⟨by omega, by omega⟩⟩

private theorem merge_ann_fst : ∀ (l1 l2 : List Ann),
    (∀ x ∈ l1, ∀ y ∈ l2, x.2 < y.2) →
    (List.merge l1 l2 cmpAnn).map Prod.fst =
      List.merge (l1.map Prod.fst) (l2.map Prod.fst) cmpKey
  | [], l2, _ => by simp
  | l1, [], _ => by simp
  | x :: xs, y :: ys, h => by
      have hxy : cmpAnn x y = cmpKey x.1 y.1 :=
        cmpAnn_eq_cmpKey (h x (by simp) y (by simp))
      by_cases hc : cmpAnn x y = true
      · have hck : cmpKey x.1 y.1 = true := by rw [← hxy]; exact hc
        simp [hc, hck]
        exact merge_ann_fst xs (y :: ys) (by
          intro z hz w hw
          exact h z (by simp [hz]) w hw)
      · have hcf : cmpAnn x y = false := Bool.eq_false_of_ne_true hc
        have hck : cmpKey x.1 y.1 = false := by rw [← hxy]; exact hcf
        simp [hcf, hck]
        exact merge_ann_fst (x :: xs) ys (by
          intro z hz w hw
          exact h z hz w (by simp [hw]))

private theorem mergeSort_zipIdx_shift (rs : List Rw) (n : Nat) :
    (rs.zipIdx n).mergeSort cmpAnn =
      ((rs.zipIdx.mergeSort cmpAnn).map (fun p => (p.1, p.2 + n))) := by
  rw [zipIdx_offset]
  apply sorted_eq_of_perm
  · exact List.pairwise_mergeSort cmpAnn_trans cmpAnn_total (rs.zipIdx.map (fun p => (p.1, p.2 + n)))
  · have hs := List.pairwise_mergeSort cmpAnn_trans cmpAnn_total rs.zipIdx
    have hs' : ((rs.zipIdx.mergeSort cmpAnn).map (fun p : Ann => (p.1, p.2 + n))).Pairwise
        (fun x y => cmpAnn x y = true) := by
      apply hs.map
      intro x y hxy
      change cmpAnn (x.1, x.2 + n) (y.1, y.2 + n) = true
      rw [cmpAnn_shift]
      exact hxy
    exact hs'
  · exact (List.mergeSort_perm (rs.zipIdx.map (fun p => (p.1, p.2 + n))) cmpAnn).trans
      ((List.mergeSort_perm rs.zipIdx cmpAnn).map (fun p => (p.1, p.2 + n))).symm
  · have h := (List.mergeSort_perm (rs.zipIdx.map (fun p => (p.1, p.2 + n))) cmpAnn).map Prod.snd
    exact (h.nodup_iff).2 (by
      rw [← zipIdx_offset]
      exact snd_zipIdx_nodup rs n)

private theorem physical_shift (rs : List Rw) (n : Nat) :
    ((rs.zipIdx n).mergeSort cmpAnn).map Prod.fst = physical key rs := by
  rw [mergeSort_zipIdx_shift]
  simpa [Function.comp_def] using (physical_eq_map_fst rs).symm

-- Truth check on small inputs (all `true`), including symmetric key collisions.
#eval List.merge (physical key [(1, 2), (2, 1), (0, 0)]) (physical key [(2, 1), (1, 2), (3, 3)]) cmpKey ==
  physical key ([(1, 2), (2, 1), (0, 0)] ++ [(2, 1), (1, 2), (3, 3)])
#eval List.merge (physical key ([] : List Rw)) (physical key [(0, 0), (0, 0)]) cmpKey ==
  physical key ([] ++ [(0, 0), (0, 0)] : List Rw)

theorem sound_MergeShards : UnorderedPair.MergeShards.Goal := by
  intro a b
  change List.merge (physical key a) (physical key b) cmpKey = physical key (a ++ b)
  have hzip : (a ++ b).zipIdx = a.zipIdx ++ b.zipIdx a.length := by
    simpa using (List.zipIdx_append (xs := a) (ys := b) (k := 0))
  have hAperm : (a.zipIdx.mergeSort cmpAnn).Perm a.zipIdx := List.mergeSort_perm _ _
  have hBperm : ((b.zipIdx a.length).mergeSort cmpAnn).Perm (b.zipIdx a.length) := List.mergeSort_perm _ _
  have hmergePerm :
      (List.merge (a.zipIdx.mergeSort cmpAnn) ((b.zipIdx a.length).mergeSort cmpAnn) cmpAnn).Perm
        (a ++ b).zipIdx := by
    exact (merge_perm_append (a.zipIdx.mergeSort cmpAnn) ((b.zipIdx a.length).mergeSort cmpAnn)).trans
      ((List.Perm.append hAperm hBperm).trans (by simp [hzip]))
  have hmergeNodup :
      ((List.merge (a.zipIdx.mergeSort cmpAnn) ((b.zipIdx a.length).mergeSort cmpAnn) cmpAnn).map Prod.snd).Nodup := by
    have h := hmergePerm.map Prod.snd
    exact (h.nodup_iff).2 (snd_zipIdx_nodup (a ++ b) 0)
  have hEqAnn :
      List.merge (a.zipIdx.mergeSort cmpAnn) ((b.zipIdx a.length).mergeSort cmpAnn) cmpAnn =
        (a ++ b).zipIdx.mergeSort cmpAnn := by
    apply sorted_eq_of_perm
    · exact List.pairwise_merge cmpAnn_trans cmpAnn_total _ _
        (List.pairwise_mergeSort cmpAnn_trans cmpAnn_total (a.zipIdx))
        (List.pairwise_mergeSort cmpAnn_trans cmpAnn_total (b.zipIdx a.length))
    · exact List.pairwise_mergeSort cmpAnn_trans cmpAnn_total ((a ++ b).zipIdx)
    · exact hmergePerm.trans (List.mergeSort_perm ((a ++ b).zipIdx) cmpAnn).symm
    · exact hmergeNodup
  rw [← physical_shift b a.length]
  rw [physical_eq_map_fst a, physical_eq_map_fst (a ++ b)]
  rw [← merge_ann_fst (a.zipIdx.mergeSort cmpAnn) ((b.zipIdx a.length).mergeSort cmpAnn)]
  · rw [hEqAnn]
  · intro x hx y hy
    have hx' : x ∈ a.zipIdx := (List.mem_mergeSort (a := x) (l := a.zipIdx) (le := cmpAnn)).1 hx
    have hy' : y ∈ b.zipIdx a.length :=
      (List.mem_mergeSort (a := y) (l := b.zipIdx a.length) (le := cmpAnn)).1 hy
    have hxmem2 : x.2 ∈ a.zipIdx.map Prod.snd := List.mem_map.mpr ⟨x, hx', rfl⟩
    have hymem2 : y.2 ∈ (b.zipIdx a.length).map Prod.snd := List.mem_map.mpr ⟨y, hy', rfl⟩
    have hxl0 : x.2 < 0 + a.length := snd_zipIdx_lt a 0 x.2 (by simpa using hxmem2)
    have hxl : x.2 < a.length := by simpa using hxl0
    have hyg : a.length ≤ y.2 := snd_zipIdx_ge b a.length y.2 hymem2
    omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_UnorderedPair_MergeShards
