import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

/-!
Ordered SQL scans and limits depend on preserving key order and ties across physical
clusters.

Proved

- `RatioClass.PatchByOrdinal` — Apply an update to the old physical layout via the
  inverse permutation before reclustering.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_RatioClass_PatchByOrdinal

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.RatioClass

/-! `patchByOrdinal` is generic in the clustering key: it only needs the fact
    that `perm key rs` is a permutation of `List.range rs.length`, hence is
    nodup and contains every valid ordinal. The proof is therefore identical
    to the Lexicographic case and independent of the RatioClass key. -/

private theorem set_map_getD_of_mem (idx : List Nat) (rs : List Row) (i : Nat) (v : Row)
    (hi : i < rs.length) (hnd : idx.Nodup) (himem : i ∈ idx) :
    (idx.map (fun j => rs[j]?.getD (0, 0))).set (idx.idxOf i) v =
      idx.map (fun j => (rs.set i v)[j]?.getD (0, 0)) := by
  induction idx with
  | nil => simp at himem
  | cons j js ih =>
      rw [List.nodup_cons] at hnd
      simp only [List.mem_cons] at himem
      by_cases hji : j = i
      · subst i
        simp [List.getElem?_set_self hi]
        intro k hk
        have hkj : k ≠ j := by
          intro h
          subst k
          exact hnd.1 hk
        rw [List.getElem?_set_ne (Ne.symm hkj)]
      · have hnot : (j == i) = false := by
          simp [hji]
        simp [List.idxOf_cons, hnot, List.set_cons_succ]
        constructor
        · rw [List.getElem?_set_ne (Ne.symm hji)]
        · apply ih hnd.2
          rcases himem with rfl | h
          · exact (hji rfl).elim
          · exact h

private theorem perm_nodup_mem (key : Key) (rs : List Row) (i : Nat) (hi : i < rs.length) :
    (perm key rs).Nodup ∧ i ∈ perm key rs := by
  have hperm0 : (rs.zipIdx.mergeSort (fun (x : Row × Nat) (y : Row × Nat) =>
      let a := key x.1; let b := key y.1
      a.1 < b.1 || (a.1 == b.1 && (a.2 < b.2 || (a.2 == b.2 && x.2 ≤ y.2))))).Perm rs.zipIdx :=
    List.mergeSort_perm _ _
  have hperm1 := List.Perm.map Prod.snd hperm0
  have hperm : (perm key rs).Perm (List.range rs.length) := by
    unfold perm
    simpa [List.zipIdx_map_snd, List.range'_eq_map_range] using hperm1
  constructor
  · exact (hperm.nodup_iff).2 List.nodup_range
  · rw [hperm.mem_iff]
    simpa using hi

theorem sound_PatchByOrdinal : RatioClass.PatchByOrdinal.Goal := by
  intro rs i v hi
  have hndmem := perm_nodup_mem RatioClass.key rs i hi
  unfold physical
  exact set_map_getD_of_mem (perm RatioClass.key rs) rs i v hi hndmem.1 hndmem.2

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_RatioClass_PatchByOrdinal
