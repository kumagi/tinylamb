import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

/-!
Ordered SQL scans and limits depend on preserving key order and ties across physical
clusters.

Proved

- `AttributeFirst.AttributeCut` — `∀ rs v, let ps:=physical key rs; (ps.takeWhile (fun
  p => p.2<v)).length=rs.countP (fun p => p.2<v)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_AttributeFirst_AttributeCut

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.AttributeFirst

private theorem rowAt_of_mem_zipIdx {rs : List Row} {x : Row × Nat} (hx : x ∈ rs.zipIdx) :
    rs[x.2]?.getD (0, 0) = x.1 := by
  have hget : rs[x.2]? = some x.1 := List.mem_zipIdx_iff_getElem?.mp hx
  simp [hget]

private theorem perm_perm_range (rs : List Row) : (perm key rs).Perm (List.range rs.length) := by
  unfold perm
  let cmp := fun (x : Row × Nat) (y : Row × Nat) =>
    let a := key x.1
    let b := key y.1
    a.1 < b.1 || (a.1 == b.1 && (a.2 < b.2 || (a.2 == b.2 && x.2 ≤ y.2)))
  have hmap := (List.mergeSort_perm rs.zipIdx cmp).map Prod.snd
  have hz : rs.zipIdx.map Prod.snd = List.range rs.length := by
    rw [List.zipIdx_map_snd]
    exact List.range_eq_range'.symm
  simpa [hz, cmp] using hmap

private theorem physical_perm (rs : List Row) : (physical key rs).Perm rs := by
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
  decide (x.1.2 < y.1.2) ||
    (x.1.2 == y.1.2 && (decide (x.1.1 < y.1.1) ||
      (x.1.1 == y.1.1 && decide (x.2 ≤ y.2))))

private theorem cmp_eq_tagLe :
    (fun x y : Row × Nat =>
      let a := key x.1
      let b := key y.1
      a.1 < b.1 || (a.1 == b.1 && (a.2 < b.2 || (a.2 == b.2 && x.2 ≤ y.2)))) = tagLe := by
  rfl

private theorem tagLe_trans (x y z : Row × Nat) :
    tagLe x y = true → tagLe y z = true → tagLe x z = true := by
  intro h1 h2
  simp only [tagLe, Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h1 h2 ⊢
  omega

private theorem tagLe_total (x y : Row × Nat) : (tagLe x y || tagLe y x) = true := by
  simp only [tagLe, Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq]
  omega

private theorem sorted_merged_snd (rs : List Row) :
    ((rs.zipIdx.mergeSort tagLe).map (fun p : Row × Nat => (rs[p.2]?.getD (0, 0)).2)).Pairwise
      (fun a b : Nat => a ≤ b) := by
  rw [List.pairwise_map]
  have hs := List.pairwise_mergeSort tagLe_trans tagLe_total rs.zipIdx
  apply hs.imp_of_mem
  intro a b ha hb hab
  have ha' : a ∈ rs.zipIdx := by simpa using ha
  have hb' : b ∈ rs.zipIdx := by simpa using hb
  have hrowa := rowAt_of_mem_zipIdx ha'
  have hrowb := rowAt_of_mem_zipIdx hb'
  simp only [tagLe, Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at hab
  have hab' : a.1.2 ≤ b.1.2 := by omega
  simpa [hrowa, hrowb] using hab'

private theorem snd_pairwise (rs : List Row) :
    ((physical key rs).map Prod.snd).Pairwise (fun a b : Nat => a ≤ b) := by
  unfold physical
  unfold perm
  rw [cmp_eq_tagLe]
  simp only [List.map_map, Function.comp_apply]
  exact sorted_merged_snd rs

private theorem takeWhile_lt_count (l : List Row)
    (hs : (l.map Prod.snd).Pairwise (fun a b : Nat => a ≤ b)) (v : Nat) :
    (l.takeWhile (fun p => p.2 < v)).length = l.countP (fun p => p.2 < v) := by
  induction l with
  | nil => simp
  | cons a xs ih =>
      have hhead_snd : ∀ x ∈ xs.map Prod.snd, a.2 ≤ x := (List.pairwise_cons.mp hs).1
      have hstail : (xs.map Prod.snd).Pairwise (fun a b : Nat => a ≤ b) := (List.pairwise_cons.mp hs).2
      by_cases hav : a.2 < v
      · simp [List.takeWhile_cons, hav, List.countP_cons, ih hstail]
      · have hge : v ≤ a.2 := Nat.le_of_not_gt hav
        have hnone : ∀ x ∈ xs, ¬ x.2 < v := by
          intro x hx hxlt
          have hxmem : x.2 ∈ xs.map Prod.snd := List.mem_map.mpr ⟨x, hx, rfl⟩
          have hax := hhead_snd x.2 hxmem
          omega
        have hcnt : xs.countP (fun p => p.2 < v) = 0 := (List.countP_eq_zero).mpr (by
          intro x hx hxv
          have hxlt : x.2 < v := by simpa using hxv
          exact hnone x hx hxlt)
        simp [List.takeWhile_cons, hav, List.countP_cons, hcnt]

theorem sound_AttributeCut : AttributeCut.Goal := by
  intro rs v
  dsimp only
  let ps := physical key rs
  have hs : (ps.map Prod.snd).Pairwise (fun a b : Nat => a ≤ b) := by
    dsimp [ps]
    exact snd_pairwise rs
  have htake : (ps.takeWhile (fun p => p.2 < v)).length = ps.countP (fun p => p.2 < v) :=
    takeWhile_lt_count ps hs v
  have hperm : ps.Perm rs := by
    dsimp [ps]
    exact physical_perm rs
  rw [← hperm.countP_eq (fun p : Row => p.2 < v)]
  exact htake

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_AttributeFirst_AttributeCut
