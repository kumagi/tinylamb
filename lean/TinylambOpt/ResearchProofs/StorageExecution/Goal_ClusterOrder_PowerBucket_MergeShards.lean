import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

/-!
Ordered SQL scans and limits depend on preserving key order and ties across physical
clusters.

Proved

- `PowerBucket.MergeShards` — A stable merge of independently clustered shards recovers
  the global physical order.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_PowerBucket_MergeShards

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.PowerBucket

private abbrev Rw := Nat × Nat
private abbrev Ann := Rw × Nat

private def leRow (x y : Rw) : Bool := x.1 < y.1 || (x.1 == y.1 && x.2 ≤ y.2)

-- The merge comparator of `mergeShards`: lexicographic comparison of keys.
-- `key (x,y) = (width x, y)` is NOT injective (width 4 = width 5 = 3), so
-- key equality alone cannot identify rows; `perm` breaks ties by ordinal.
private def leKey (x y : Rw) : Bool := leRow (key x) (key y)

-- The comparator used by `perm`: key, then original ordinal.
private def cmpAnn (ri rj : Ann) : Bool :=
  let a := key ri.1
  let b := key rj.1
  a.1 < b.1 || (a.1 == b.1 && (a.2 < b.2 || (a.2 == b.2 && ri.2 ≤ rj.2)))

--------------------------------------------------------------------------------
-- Total-order facts about the key comparator and the ordinal comparator.

private theorem leRow_trans : ∀ a b c : Rw, leRow a b = true → leRow b c = true → leRow a c = true := by
  intro a b c hab hbc
  simp [leRow] at hab hbc ⊢
  omega

private theorem leRow_total : ∀ a b : Rw, (leRow a b || leRow b a) = true := by
  intro a b
  simp [leRow]
  omega

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
  · have : (key a.1).1 = (key b.1).1 := by rw [hk]
    omega
  · have : (key a.1).2 = (key b.1).2 := by rw [hk]
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

-- Shifting both ordinals by the same amount preserves `cmpAnn`.
private theorem cmpAnn_shift (n : Nat) (ri rj : Ann) :
    cmpAnn (ri.1, ri.2 + n) (rj.1, rj.2 + n) = cmpAnn ri rj := by
  unfold cmpAnn
  simp [Nat.add_le_add_iff_right]

private theorem leKey_cmpAnn_of_lt {x y : Ann} (hxy : x.2 < y.2)
    (h : leKey x.1 y.1 = true) : cmpAnn x y = true := by
  unfold leKey leRow at h
  simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h
  unfold cmpAnn
  simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq]
  rcases h with hlt | ⟨hf, hle⟩
  · exact Or.inl hlt
  · rcases Nat.lt_or_eq_of_le hle with hlt2 | heq2
    · exact Or.inr ⟨hf, Or.inl hlt2⟩
    · exact Or.inr ⟨hf, Or.inr ⟨heq2, by omega⟩⟩

private theorem cmpAnn_eq_leKey {x y : Ann} (hxy : x.2 < y.2) :
    cmpAnn x y = leKey x.1 y.1 := by
  exact Bool.eq_iff_iff.mpr ⟨fun h => cmpAnn_leRow h, fun h => leKey_cmpAnn_of_lt hxy h⟩

--------------------------------------------------------------------------------
-- Ordinal bookkeeping for `zipIdx`.

private theorem snd_zipIdx_ge (rs : List Rw) (n k : Nat) :
    k ∈ (rs.zipIdx n).map Prod.snd → n ≤ k := by
  induction rs generalizing n with
  | nil => simp
  | cons r rs ih =>
      intro h
      rw [List.zipIdx_cons] at h
      simp only [List.map_cons, Prod.snd, List.mem_cons] at h
      rcases h with rfl | hk
      · omega
      · have := ih (n + 1) hk
        omega

private theorem snd_zipIdx_lt (rs : List Rw) (n k : Nat) :
    k ∈ (rs.zipIdx n).map Prod.snd → k < n + rs.length := by
  induction rs generalizing n with
  | nil => simp
  | cons r rs ih =>
      intro h
      rw [List.zipIdx_cons] at h
      simp only [List.map_cons, Prod.snd, List.mem_cons] at h
      simp only [List.length_cons]
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
      simp only [List.map_cons, Prod.snd, List.nodup_cons]
      constructor
      · intro hmem
        have hge := snd_zipIdx_ge rs (n + 1) n hmem
        omega
      · exact ih (n + 1)

private theorem zipIdx_shift (rs : List Rw) (n : Nat) :
    rs.zipIdx n = (rs.zipIdx 0).map (fun ri : Ann => (ri.1, ri.2 + n)) := by
  induction rs generalizing n with
  | nil => simp
  | cons r rs ih =>
      rw [List.zipIdx_cons, List.zipIdx_cons]
      simp only [List.map_cons, Nat.zero_add]
      have h1 : (rs.zipIdx 1).map (fun ri : Ann => (ri.1, ri.2 + n)) =
          (rs.zipIdx 0).map (fun ri : Ann => (ri.1, ri.2 + (n + 1))) := by
        rw [ih 1]
        simp only [List.map_map]
        apply List.map_congr_left
        intro ri hri
        simp [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
      rw [ih (n + 1), ← h1]

private theorem getD_zipIdx (rs : List Rw) (ri : Ann) (h : ri ∈ rs.zipIdx) :
    rs[ri.2]?.getD (0, 0) = ri.1 := by
  rcases ri with ⟨r, i⟩
  have hm := List.mem_zipIdx h
  simp at hm
  have hg : rs[i]? = some rs[i] := List.getElem?_eq_getElem hm.1
  rw [hg]
  simpa [hm.2]

private theorem map_fst_zipIdx (rs : List Rw) : (rs.zipIdx.map Prod.fst) = rs := by
  induction rs with
  | nil => simp
  | cons r rs ih => simp [List.zipIdx_cons]

private theorem physical_eq_map_fst (rs : List Rw) :
    physical key rs = (rs.zipIdx.mergeSort cmpAnn).map Prod.fst := by
  unfold physical perm
  change ((rs.zipIdx.mergeSort cmpAnn).map Prod.snd).map (fun i => rs[i]?.getD (0, 0)) =
    (rs.zipIdx.mergeSort cmpAnn).map Prod.fst
  simp only [List.map_map]
  apply List.map_congr_left
  intro ri hri
  exact getD_zipIdx rs ri ((List.mem_mergeSort (a := ri) (l := rs.zipIdx) (le := cmpAnn)).1 hri)

private theorem physical_perm (rs : List Rw) : (physical key rs).Perm rs := by
  rw [physical_eq_map_fst]
  have h := (List.mergeSort_perm (rs.zipIdx) cmpAnn).map Prod.fst
  simpa [map_fst_zipIdx] using h

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

-- Uniqueness of a list sorted under `cmpAnn` with nodup ordinals. `cmpAnn` is
-- antisymmetric only on annotated rows (equal keys are separated by ordinal).
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
        rcases List.mem_cons.mp hzmem with hzxEq | hztail
        · exact hzxEq
        · exfalso
          simp only [List.map_cons, Prod.snd, List.nodup_cons] at hn
          apply hn.1
          rw [hord]
          exact List.mem_map.mpr ⟨z, hztail, rfl⟩
      subst z
      simp only [List.map_cons, Prod.snd, List.nodup_cons] at hn
      exact congrArg (List.cons x) (sorted_eq_of_perm xs zs hpair1.2 hpair2.2 (List.Perm.cons_inv hp) hn.2)

-- `mergeSort` commutes with a uniform shift of all ordinals (projected to rows).
private theorem mergeSort_shift_eq (rs : List Rw) (n : Nat) :
    ((rs.zipIdx 0).map (fun ri : Ann => (ri.1, ri.2 + n))).mergeSort cmpAnn =
      ((rs.zipIdx 0).mergeSort cmpAnn).map (fun ri : Ann => (ri.1, ri.2 + n)) := by
  apply sorted_eq_of_perm
  · exact List.pairwise_mergeSort cmpAnn_trans cmpAnn_total
      ((rs.zipIdx 0).map (fun ri : Ann => (ri.1, ri.2 + n)))
  · have hp := List.pairwise_mergeSort cmpAnn_trans cmpAnn_total (rs.zipIdx 0)
    exact List.Pairwise.map (fun ri : Ann => (ri.1, ri.2 + n)) (by
      intro a b h
      rw [cmpAnn_shift n a b]
      exact h) hp
  · exact (List.mergeSort_perm ((rs.zipIdx 0).map (fun ri : Ann => (ri.1, ri.2 + n))) cmpAnn).trans
      ((List.mergeSort_perm (rs.zipIdx 0) cmpAnn).map
        (fun ri : Ann => (ri.1, ri.2 + n))).symm
  · have hn := (List.mergeSort_perm ((rs.zipIdx 0).map (fun ri : Ann => (ri.1, ri.2 + n))) cmpAnn).map Prod.snd
    exact (hn.nodup_iff).2 (by
      rw [← zipIdx_shift rs n]
      exact snd_zipIdx_nodup rs n)

-- Shifting all ordinals of `rs.zipIdx` does not change the physical row order.
private theorem physical_eq_map_fst_shift (rs : List Rw) (n : Nat) :
    physical key rs = ((rs.zipIdx n).mergeSort cmpAnn).map Prod.fst := by
  rw [physical_eq_map_fst, zipIdx_shift rs n]
  have hmap : ((rs.zipIdx.mergeSort cmpAnn).map (fun ri : Ann => (ri.1, ri.2 + n))).map Prod.fst =
      (rs.zipIdx.mergeSort cmpAnn).map Prod.fst := by
    simp only [List.map_map]
    apply List.map_congr_left
    intro ri hri
    rfl
  rw [← hmap]
  exact (congrArg (fun l : List Ann => l.map Prod.fst) (mergeSort_shift_eq rs n)).symm

private theorem merge_ann_fst : ∀ (l1 l2 : List Ann),
    (∀ x ∈ l1, ∀ y ∈ l2, x.2 < y.2) →
    (List.merge l1 l2 cmpAnn).map Prod.fst =
      List.merge (l1.map Prod.fst) (l2.map Prod.fst) leKey
  | [], l2, _ => by simp
  | l1, [], _ => by simp
  | x :: xs, y :: ys, h => by
      have hxy : cmpAnn x y = leKey x.1 y.1 :=
        cmpAnn_eq_leKey (h x (by simp) y (by simp))
      by_cases hc : cmpAnn x y = true
      · have hc' : leKey x.1 y.1 = true := by rwa [← hxy]
        simp [hc, hc']
        exact merge_ann_fst xs (y :: ys) (by
          intro z hz w hw
          exact h z (by simp [hz]) w hw)
      · have hc' : leKey x.1 y.1 = false := by
          rw [← hxy]
          exact Bool.eq_false_of_ne_true hc
        simp [hc, hc']
        exact merge_ann_fst (x :: xs) ys (by
          intro z hz w hw
          exact h z hz w (by simp [hw]))

--------------------------------------------------------------------------------
-- Truth check on small inputs (all `true`), including key collisions caused by
-- `width` not being injective (width 4 = width 5 = width 6 = width 7 = 3).

private def checkOne (a b : List Rw) : Bool :=
  List.merge (physical key a) (physical key b) leKey == physical key (a ++ b)

#eval checkOne [(5, 0), (4, 0), (7, 0)] [(6, 0), (5, 0)]
#eval checkOne [(2, 1), (3, 1), (0, 0)] [(3, 1), (1, 2), (7, 0)]
#eval checkOne [] [(0, 0), (0, 0)]
#eval checkOne [(7, 3), (4, 1), (2, 2)] [(5, 1), (3, 0), (0, 9)]

private def pool : List Rw := [(0, 0), (1, 0), (2, 0), (3, 0), (4, 0), (0, 1), (1, 1), (2, 1)]
private def smallLists : List (List Rw) :=
  [] :: (pool.map (fun x => [x]) ++ List.flatMap (fun x => pool.map (fun y => [x, y])) pool)
private def exhaustive : Bool :=
  (List.flatMap (fun a => smallLists.map (fun b => checkOne a b)) smallLists).all id
#eval exhaustive
example : exhaustive = true := by native_decide

theorem sound_MergeShards : PowerBucket.MergeShards.Goal := by
  intro a b
  change List.merge (physical key a) (physical key b) leKey = physical key (a ++ b)
  have hzip : (a ++ b).zipIdx = a.zipIdx ++ b.zipIdx a.length := by
    simpa using (List.zipIdx_append (xs := a) (ys := b) (k := 0))
  have hAperm : (a.zipIdx.mergeSort cmpAnn).Perm a.zipIdx := List.mergeSort_perm _ _
  have hBperm : ((b.zipIdx a.length).mergeSort cmpAnn).Perm (b.zipIdx a.length) := List.mergeSort_perm _ _
  have hCperm : ((a ++ b).zipIdx.mergeSort cmpAnn).Perm (a ++ b).zipIdx := List.mergeSort_perm _ _
  have hmergePerm :
      (List.merge (a.zipIdx.mergeSort cmpAnn) ((b.zipIdx a.length).mergeSort cmpAnn) cmpAnn).Perm
        (a ++ b).zipIdx := by
    exact (merge_perm_append (a.zipIdx.mergeSort cmpAnn) ((b.zipIdx a.length).mergeSort cmpAnn)).trans
      ((List.Perm.append hAperm hBperm).trans (by simpa [hzip]))
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
    · exact hmergePerm.trans hCperm.symm
    · exact hmergeNodup
  rw [physical_eq_map_fst a, physical_eq_map_fst_shift b a.length, physical_eq_map_fst (a ++ b)]
  rw [← merge_ann_fst (a.zipIdx.mergeSort cmpAnn) ((b.zipIdx a.length).mergeSort cmpAnn)]
  · rw [hEqAnn]
  · intro x hx y hy
    have hx' : x ∈ a.zipIdx := (List.mem_mergeSort (a := x) (l := a.zipIdx) (le := cmpAnn)).1 hx
    have hy' : y ∈ b.zipIdx a.length := (List.mem_mergeSort (a := y) (l := b.zipIdx a.length) (le := cmpAnn)).1 hy
    have hxmem : x.2 ∈ (a.zipIdx 0).map Prod.snd := List.mem_map.mpr ⟨x, hx', rfl⟩
    have hymem : y.2 ∈ (b.zipIdx a.length).map Prod.snd := List.mem_map.mpr ⟨y, hy', rfl⟩
    have hxl : x.2 < a.length := by simpa using snd_zipIdx_lt a 0 x.2 hxmem
    have hyg : a.length ≤ y.2 := snd_zipIdx_ge b a.length y.2 hymem
    omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_PowerBucket_MergeShards
