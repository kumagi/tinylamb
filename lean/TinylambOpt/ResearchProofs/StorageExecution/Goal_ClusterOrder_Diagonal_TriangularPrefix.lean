import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_Diagonal_TriangularPrefix

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.Diagonal

private def tagLe (key : Key) (x y : Row × Nat) : Bool :=
  let a := key x.1
  let b := key y.1
  decide (a.1 < b.1 || (a.1 == b.1 && (a.2 < b.2 || (a.2 == b.2 && x.2 ≤ y.2))))

private theorem tagLe_trans (key : Key) (x y z : Row × Nat) :
    tagLe key x y = true → tagLe key y z = true → tagLe key x z = true := by
  intro h1 h2
  simp only [tagLe, decide_eq_true_eq, Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq] at h1 h2 ⊢
  omega

private theorem tagLe_total (key : Key) (x y : Row × Nat) :
    (tagLe key x y || tagLe key y x) = true := by
  simp only [tagLe, decide_eq_true_eq, Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq]
  omega

private theorem rowAt_of_mem_zipIdx {rs : List Row} {x : Row × Nat} (hx : x ∈ rs.zipIdx) :
    rs[x.2]?.getD (0, 0) = x.1 := by
  have hget : rs[x.2]? = some x.1 := List.mem_zipIdx_iff_getElem?.mp hx
  simp [hget]

private theorem list_eq_map_range_getD {α : Type} (l : List α) (d : α) :
    l = (List.range l.length).map (fun i => l[i]?.getD d) := by
  apply List.ext_getElem
  · simp
  · intro i h1 h2
    simp only [List.length_map, List.length_range] at h2
    simp [List.getElem?_eq_getElem h2]

private theorem perm_perm_range (rs : List Row) : (perm key rs).Perm (List.range rs.length) := by
  unfold perm
  let cmp := fun (x : Row × Nat) (y : Row × Nat) =>
    let a := key x.1; let b := key y.1
    a.1 < b.1 || (a.1 == b.1 && (a.2 < b.2 || (a.2 == b.2 && x.2 ≤ y.2)))
  have hmap := (List.mergeSort_perm rs.zipIdx cmp).map Prod.snd
  have hz : rs.zipIdx.map Prod.snd = List.range rs.length := by
    rw [List.zipIdx_map_snd]
    exact List.range_eq_range'.symm
  simpa [hz, cmp] using hmap

private theorem physical_perm (rs : List Row) : (physical key rs).Perm rs := by
  unfold physical
  have h1 := (perm_perm_range rs).map (fun i => rs[i]?.getD (0, 0))
  have h2 : (List.range rs.length).map (fun i => rs[i]?.getD (0, 0)) = rs :=
    (list_eq_map_range_getD rs (0, 0)).symm
  simpa [h2] using h1

private theorem tagged_sums_pairwise (rs : List Row) (L : List (Row × Nat))
    (hall : ∀ x ∈ L, x ∈ rs.zipIdx)
    (hs : L.Pairwise (fun a b => tagLe key a b = true)) :
    L.Pairwise (fun a b : Row × Nat =>
      (rs[a.2]?.getD (0, 0)).1 + (rs[a.2]?.getD (0, 0)).2 ≤
      (rs[b.2]?.getD (0, 0)).1 + (rs[b.2]?.getD (0, 0)).2) := by
  induction hs with
  | nil => simp
  | @cons a l hhead htail ih =>
      apply List.Pairwise.cons
      · intro b hb
        have ht : tagLe key a b = true := hhead b hb
        have ha_mem : a ∈ rs.zipIdx := hall a List.mem_cons_self
        have hb_mem : b ∈ rs.zipIdx := hall b (List.mem_cons_of_mem a hb)
        have hrowa := rowAt_of_mem_zipIdx ha_mem
        have hrowb := rowAt_of_mem_zipIdx hb_mem
        rw [hrowa, hrowb]
        simp only [tagLe] at ht
        simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq] at ht
        simp only [decide_eq_true_eq] at ht
        simp [key] at ht
        rcases ht with hlt | ⟨heq, _⟩
        · exact Nat.le_of_lt hlt
        · exact Nat.le_of_eq heq
      · exact ih (fun x hx => hall x (List.mem_cons_of_mem a hx))

private theorem sorted_sums (rs : List Row) :
    (physical key rs).Pairwise (fun a b : Row => a.1 + a.2 ≤ b.1 + b.2) := by
  unfold physical
  rw [List.pairwise_map]
  unfold perm
  rw [List.pairwise_map]
  have hs := List.pairwise_mergeSort (tagLe_trans key) (tagLe_total key) rs.zipIdx
  have hperm := List.mergeSort_perm rs.zipIdx (tagLe key)
  have hall : ∀ x ∈ rs.zipIdx.mergeSort (tagLe key), x ∈ rs.zipIdx :=
    fun x hx => hperm.mem_iff.mp hx
  exact tagged_sums_pairwise rs (rs.zipIdx.mergeSort (tagLe key)) hall hs

private theorem takeWhile_count_sums (l : List Row)
    (hs : l.Pairwise (fun a b : Row => a.1 + a.2 ≤ b.1 + b.2)) (t : Nat) :
    (l.takeWhile (fun p : Row => decide (p.1 + p.2 < t))).length =
      l.countP (fun p : Row => decide (p.1 + p.2 < t)) := by
  induction l with
  | nil => simp
  | cons a xs ih =>
      have hhead : ∀ b ∈ xs, a.1 + a.2 ≤ b.1 + b.2 := (List.pairwise_cons.mp hs).1
      have hstail : xs.Pairwise (fun a b : Row => a.1 + a.2 ≤ b.1 + b.2) :=
        (List.pairwise_cons.mp hs).2
      have ihtail := ih hstail
      by_cases ha : a.1 + a.2 < t
      · simp [List.takeWhile_cons, ha, List.countP_cons, ihtail]
      · have hcnt : xs.countP (fun p : Row => decide (p.1 + p.2 < t)) = 0 := by
          rw [List.countP_eq_zero]
          intro b hb
          simp only [decide_eq_true_eq]
          have hge := hhead b hb
          omega
        simp [List.takeWhile_cons, ha, List.countP_cons, hcnt]

theorem sound_TriangularPrefix : TriangularPrefix.Goal := by
  intro rs t
  dsimp only
  let p : Row → Bool := fun r => decide (r.1 + r.2 < t)
  have hperm := physical_perm rs
  rw [← hperm.countP_eq p]
  exact takeWhile_count_sums (physical key rs) (sorted_sums rs) t

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_Diagonal_TriangularPrefix
