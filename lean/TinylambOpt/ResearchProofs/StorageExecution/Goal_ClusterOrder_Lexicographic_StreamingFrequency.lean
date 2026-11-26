import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_Lexicographic_StreamingFrequency

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.Lexicographic

private theorem perm_length (rs : List Row) : (perm id rs).length = rs.length := by
  simp [perm]

private theorem rowAt_of_mem_zipIdx {rs : List Row} {x : Row × Nat} (hx : x ∈ rs.zipIdx) :
    rs[x.2]?.getD (0, 0) = x.1 := by
  have hget : rs[x.2]? = some x.1 := List.mem_zipIdx_iff_getElem?.mp hx
  simp [hget]

private theorem perm_perm_range (rs : List Row) : (perm id rs).Perm (List.range rs.length) := by
  unfold perm
  let cmp := fun (x : Row × Nat) (y : Row × Nat) =>
    let a := id x.1
    let b := id y.1
    a.1 < b.1 || (a.1 == b.1 && (a.2 < b.2 || (a.2 == b.2 && x.2 ≤ y.2)))
  have hmap := (List.mergeSort_perm rs.zipIdx cmp).map Prod.snd
  have hz : rs.zipIdx.map Prod.snd = List.range rs.length := by
    rw [List.zipIdx_map_snd]
    exact List.range_eq_range'.symm
  simpa [hz, cmp] using hmap

private theorem physical_perm (rs : List Row) : (physical id rs).Perm rs := by
  unfold physical
  have h1 := (perm_perm_range rs).map (fun i => rs[i]?.getD (0, 0))
  have h2 : (List.range rs.length).map (fun i => rs[i]?.getD (0, 0)) = rs := by
    symm
    apply List.ext_getElem
    · simp
    · intro i h1 h2
      simp only [List.length_map, List.length_range] at h2
      simp [List.getElem?_eq_getElem h2]
  simpa [h2] using h1

private def tagLe (x y : Row × Nat) : Bool :=
  x.1.1 < y.1.1 ||
    (x.1.1 == y.1.1 && (x.1.2 < y.1.2 || (x.1.2 == y.1.2 && x.2 ≤ y.2)))

private theorem tagLe_trans (x y z : Row × Nat) :
    tagLe x y = true → tagLe y z = true → tagLe x z = true := by
  intro h1 h2
  simp only [tagLe, decide_eq_true_eq, Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq] at h1 h2 ⊢
  omega

private theorem tagLe_total (x y : Row × Nat) : (tagLe x y || tagLe y x) = true := by
  simp only [tagLe, decide_eq_true_eq, Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq]
  omega

private theorem pairwise_of_tagLe {rs : List Row} {L : List (Row × Nat)}
    (hmem : ∀ x ∈ L, x ∈ rs.zipIdx)
    (hs : L.Pairwise (fun a b : Row × Nat => tagLe a b = true)) :
    L.Pairwise
      (fun a b : Row × Nat => (rs[a.2]?.getD (0, 0)).1 ≤ (rs[b.2]?.getD (0, 0)).1) := by
  induction hs with
  | nil => simp
  | @cons a l hhead htail ih =>
      apply List.Pairwise.cons
      · intro b hb
        have ht : tagLe a b = true := hhead b hb
        have ha_mem : a ∈ rs.zipIdx := hmem a List.mem_cons_self
        have hb_mem : b ∈ rs.zipIdx := hmem b (List.mem_cons_of_mem a hb)
        have hrowa := rowAt_of_mem_zipIdx ha_mem
        have hrowb := rowAt_of_mem_zipIdx hb_mem
        simp only [tagLe, decide_eq_true_eq, Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq] at ht
        have hab : a.1.1 ≤ b.1.1 := by omega
        simpa [hrowa, hrowb] using hab
      · exact ih (fun x hx => hmem x (List.mem_cons_of_mem a hx))

private theorem sorted_merged_fst (rs : List Row) :
    ((rs.zipIdx.mergeSort tagLe).map (fun p : Row × Nat => (rs[p.2]?.getD (0, 0)).1)).Pairwise
      (fun a b : Nat => a ≤ b) := by
  rw [List.pairwise_map]
  exact pairwise_of_tagLe (rs := rs)
    (fun x hx => (List.mergeSort_perm rs.zipIdx tagLe).mem_iff.mp hx)
    (List.pairwise_mergeSort tagLe_trans tagLe_total rs.zipIdx)

private theorem col_pairwise (rs : List Row) :
    ((physical id rs).map Prod.fst).Pairwise (fun a b : Nat => a ≤ b) := by
  unfold physical perm
  rw [List.map_map, List.map_map]
  exact sorted_merged_fst rs

private theorem takeWhile_eq_count_of_ge (l : List Nat) (hs : l.Pairwise (fun a b : Nat => a ≤ b))
    (v : Nat) (hge : ∀ x ∈ l, v ≤ x) :
    (l.takeWhile (fun x => x == v)).length = l.count v := by
  induction l with
  | nil => simp
  | cons b bs ih =>
      have hbge : v ≤ b := hge b List.mem_cons_self
      have hbsge : ∀ x ∈ bs, v ≤ x := fun x hx => hge x (List.mem_cons_of_mem b hx)
      have hhead : ∀ c ∈ bs, b ≤ c := (List.pairwise_cons.mp hs).1
      have hstail : bs.Pairwise (fun a c : Nat => a ≤ c) := (List.pairwise_cons.mp hs).2
      by_cases hb : b == v
      · simp [List.takeWhile_cons, hb, List.count_cons, ih hstail hbsge]
      · have hbne : b ≠ v := by
          intro heq
          exact hb (by simp [heq])
        have hbgt : v < b := Nat.lt_of_le_of_ne hbge (Ne.symm hbne)
        have hnone : ¬v ∈ bs := by
          intro hv
          have hbv := hhead v hv
          omega
        have hcnt : bs.count v = 0 := (List.count_eq_zero).mpr hnone
        simp [List.takeWhile_cons, hb, List.count_cons, hcnt]

private theorem run_eq_count_sorted (l : List Nat) (hs : l.Pairwise (fun a b : Nat => a ≤ b)) (v : Nat) :
    (l.dropWhile (fun x => decide (x < v)) |>.takeWhile (fun x => x == v)).length = l.count v := by
  induction l generalizing v with
  | nil => simp
  | cons a xs ih =>
      have hhead : ∀ b ∈ xs, a ≤ b := (List.pairwise_cons.mp hs).1
      have hstail : xs.Pairwise (fun a b : Nat => a ≤ b) := (List.pairwise_cons.mp hs).2
      by_cases hav : a < v
      · have hne : (a == v) = false := by
          have hane : a ≠ v := by omega
          simp [hane]
        simp [List.dropWhile_cons, hav, List.count_cons, hne, ih hstail v]
      · have hge : v ≤ a := Nat.le_of_not_gt hav
        by_cases haeq : a == v
        · have haeq' : a = v := by simpa using haeq
          have hge_xs : ∀ x ∈ xs, v ≤ x := by
            intro x hx
            have hax := hhead x hx
            omega
          have htail := takeWhile_eq_count_of_ge xs hstail v hge_xs
          simp [List.dropWhile_cons, hav, List.takeWhile_cons, haeq, List.count_cons, haeq, htail]
        · have hane : a ≠ v := by
            intro heq
            exact haeq (by simp [heq])
          have hnone : ¬v ∈ a :: xs := by
            intro hv
            simp only [List.mem_cons] at hv
            rcases hv with rfl | hx
            · omega
            · have hax := hhead v hx
              omega
          have hcnt : (a :: xs).count v = 0 := (List.count_eq_zero).mpr hnone
          simp [List.dropWhile_cons, hav, List.takeWhile_cons, haeq, hcnt]

theorem sound_StreamingFrequency : StreamingFrequency.Goal := by
  intro rs v
  dsimp only
  let col := (physical id rs).map Prod.fst
  have hs : col.Pairwise (fun a b : Nat => a ≤ b) := by
    dsimp [col]
    exact col_pairwise rs
  have hp : col.Perm (rs.map Prod.fst) := by
    dsimp [col]
    exact (physical_perm rs).map Prod.fst
  rw [← hp.count v]
  exact run_eq_count_sorted col hs v

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_Lexicographic_StreamingFrequency
