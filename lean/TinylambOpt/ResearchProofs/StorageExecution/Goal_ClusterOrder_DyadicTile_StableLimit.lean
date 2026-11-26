import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
import TinylambOpt.ResearchProofs.Compression.Wire

/-!
Ordered SQL scans and limits depend on preserving key order and ties across physical
clusters.

Proved

- `DyadicTile.StableLimit` — A filtered LIMIT in original order can be reconstructed
  from selected ordinals.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_DyadicTile_StableLimit

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.DyadicTile
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchProofs.Compression.Wire

-- The generic permutation/serialization facts used by `stableLimit` do not
-- depend on the clustering key: `perm key rs` is always a permutation of
-- `List.range rs.length` and `ordinal` simply reads back that permutation
-- from the sidecar. So the Lexicographic argument applies verbatim here.

private theorem perm_range (rs : List Row) : (perm key rs).Perm (List.range rs.length) := by
  unfold perm
  have h := List.mergeSort_perm (rs.zipIdx) (fun (x : Row × Nat) (y : Row × Nat) =>
    let a := key x.1; let b := key y.1
    a.1 < b.1 || (a.1 == b.1 && (a.2 < b.2 || (a.2 == b.2 && x.2 ≤ y.2))))
  have hm := h.map Prod.snd
  simpa [List.range_eq_range'] using hm

private theorem perm_length (rs : List Row) : (perm key rs).length = rs.length := by
  simpa using (perm_range rs).length_eq

private theorem perm_mem_lt (rs : List Row) {j : Nat} (hj : j ∈ perm key rs) : j < rs.length := by
  have h := (perm_range rs).subset hj
  simpa using h

private theorem ordinal_eq_perm (rs : List Row) (i : Nat) (hi : i < rs.length) :
    ordinal key rs i = (perm key rs)[i]'(by rwa [perm_length]) := by
  let w := width (rs.length - 1)
  let ps := perm key rs
  have hpslen : ps.length = rs.length := perm_length rs
  have hbound : ∀ x ∈ ps, x < 2 ^ w := by
    intro x hx
    have hxlt : x < rs.length := perm_mem_lt rs hx
    by_cases hn : rs.length = 0
    · simp [hn] at hxlt
    have hpred : x ≤ rs.length - 1 := by omega
    have htop : rs.length - 1 < 2 ^ w := by
      dsimp [w]
      have hh := (sound_MinimumWidth [rs.length - 1] (width (rs.length - 1))).mpr (by simp [peak])
      simpa [peak] using hh
    omega
  have hbytes : ∀ b ∈ pack w ps, b < 256 := by
    intro b hb
    simp only [pack, little] at hb
    rcases List.mem_map.mp hb with ⟨k, hk, rfl⟩
    exact Nat.mod_lt _ (by decide)
  have hget : ∀ (i : Nat) (hi' : i < ps.length), get (pack w ps) w i = ps[i]'hi' := by
    intro i hi'
    have hp := sound_PackedInverse w ps hbound
    have h := congrArg (fun l : List Nat => l[i]?) hp
    have hleft : ((List.range ps.length).map (get (pack w ps) w))[i]? =
        some (get (pack w ps) w i) := by simp [hi']
    have hright : ps[i]? = some (ps[i]'hi') := by simp [hi']
    rw [hleft, hright] at h
    exact Option.some.inj h
  have hi' : i < ps.length := by rw [hpslen]; exact hi
  unfold ordinal side
  change takeAt (pack w ps) w i = ps[i]'hi'
  rw [sound_LocalRead (pack w ps) w i hbytes]
  exact hget i hi'

private theorem tagged_eq (rs : List Row) :
    (physical key rs).zipIdx.map (fun (r, i) => (r, ordinal key rs i)) =
    (perm key rs).map (fun j => (rs[j]?.getD (0, 0), j)) := by
  apply List.ext_getElem?
  intro i
  by_cases hi : i < rs.length
  · have hps : i < (perm key rs).length := by rwa [perm_length]
    have hpi : (perm key rs)[i]'hps < rs.length :=
      perm_mem_lt rs (List.get_mem (perm key rs) ⟨i, hps⟩)
    have hphys : (physical key rs)[i]? = some (rs[(perm key rs)[i]'hps]?.getD (0, 0)) := by
      simp [physical, hps]
    have hval : rs[(perm key rs)[i]'hps]?.getD (0, 0) = rs[(perm key rs)[i]'hps] := by
      simp [hpi]
    have hord : ordinal key rs i = (perm key rs)[i]'hps := ordinal_eq_perm rs i hi
    simp [List.getElem?_map, hphys, hval, hord, hps]
  · have hlen1 : ((physical key rs).zipIdx.map (fun (r, i) => (r, ordinal key rs i))).length = rs.length := by
      simp [physical, perm_length]
    have hlen2 : ((perm key rs).map (fun j => (rs[j]?.getD (0, 0), j))).length = rs.length := by
      simp [perm_length]
    simp [hlen1, hlen2, hi]

private theorem tagged_perm (rs : List Row) :
    ((physical key rs).zipIdx.map (fun (r, i) => (r, ordinal key rs i))).Perm (rs.zipIdx) := by
  rw [tagged_eq]
  have hp := (perm_range rs).map (fun j => (rs[j]?.getD (0, 0), j))
  have hrange : (List.range rs.length).map (fun j => (rs[j]?.getD (0, 0), j)) = rs.zipIdx := by
    apply List.ext_getElem?
    intro i
    by_cases hi : i < rs.length
    · simp [hi]
    · simp [hi]
  rwa [hrange] at hp

private theorem raw_perm (rs : List Row) (p : Row → Bool) :
    ((physical key rs).zipIdx.filterMap (fun (r, i) => if p r then some (ordinal key rs i) else none)).Perm
      (rs.zipIdx.filterMap (fun (r, j) => if p r then some j else none)) := by
  let f : Row × Nat → Option Nat := fun x => if p x.1 then some x.2 else none
  have h := (tagged_perm rs).filterMap f
  have hl : ((physical key rs).zipIdx.map (fun (r, i) => (r, ordinal key rs i))).filterMap f =
      (physical key rs).zipIdx.filterMap (fun (r, i) => if p r then some (ordinal key rs i) else none) := by
    simp [f, Function.comp_def]
  rw [hl] at h
  simpa [f] using h

private theorem ids_perm (rs : List Row) (p : Row → Bool) :
    (((physical key rs).zipIdx.filterMap (fun (r, i) => if p r then some (ordinal key rs i) else none)).mergeSort).Perm
      (rs.zipIdx.filterMap (fun (r, j) => if p r then some j else none)) := by
  exact (List.mergeSort_perm _ _).trans (raw_perm rs p)

private theorem ids_pairwise (rs : List Row) (p : Row → Bool) :
    (((physical key rs).zipIdx.filterMap (fun (r, i) => if p r then some (ordinal key rs i) else none)).mergeSort).Pairwise
      (fun a b : Nat => a ≤ b) := by
  have htrans : ∀ a b c : Nat, decide (a ≤ b) → decide (b ≤ c) → decide (a ≤ c) := by
    intro a b c hab hbc
    simp at hab hbc ⊢
    omega
  have htotal : ∀ a b : Nat, (decide (a ≤ b) || decide (b ≤ a)) = true := by
    intro a b
    by_cases h : a ≤ b
    · simp [h]
    · have hba : b ≤ a := by omega
      simp [h, hba]
  have h := List.pairwise_mergeSort (le := fun a b : Nat => decide (a ≤ b))
    htrans htotal
    ((physical key rs).zipIdx.filterMap (fun (r, i) => if p r then some (ordinal key rs i) else none))
  simpa using h

private theorem zipIdx_pairwise (i : Nat) (rs : List Row) :
    (rs.zipIdx i).Pairwise (fun x y : Row × Nat => x.2 ≤ y.2) := by
  induction rs generalizing i with
  | nil => simp
  | cons r rs ih =>
      rw [List.zipIdx_cons]
      simp only [List.pairwise_cons]
      constructor
      · intro y hy
        have hy' := List.le_snd_of_mem_zipIdx hy
        omega
      · exact ih (i + 1)

private theorem rawSel_pairwise (rs : List Row) (p : Row → Bool) :
    (rs.zipIdx.filterMap (fun (r, j) => if p r then some j else none)).Pairwise
      (fun a b : Nat => a ≤ b) := by
  apply (zipIdx_pairwise 0 rs).filterMap
  intro a b hab j hj k hk
  by_cases hpa : p a.1
  · have hj' : a.2 = j := by simpa [hpa] using hj
    by_cases hpb : p b.1
    · have hk' : b.2 = k := by simpa [hpb] using hk
      omega
    · simp [hpb] at hk
  · simp [hpa] at hj

private theorem filterMap_eq_filter_map (xs : List (Row × Nat)) (p : Row → Bool) :
    xs.filterMap (fun (r, j) => if p r then some j else none) =
      (xs.filter (fun x => p x.1)).map Prod.snd := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      rcases x with ⟨r, j⟩
      by_cases hpr : p r <;> simp [hpr, ih]

private theorem filter_map_fst (xs : List (Row × Nat)) (p : Row → Bool) :
    (xs.filter (fun x => p x.1)).map Prod.fst = (xs.map Prod.fst).filter p := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      rcases x with ⟨r, j⟩
      by_cases hpr : p r <;> simp [hpr, ih]

private theorem sel_map_eq (rs : List Row) (p : Row → Bool) :
    (rs.zipIdx.filterMap (fun (r, j) => if p r then some j else none)).map
        (fun j => rs[j]?.getD (0, 0)) = rs.filter p := by
  rw [filterMap_eq_filter_map]
  have hcong : ((rs.zipIdx.filter (fun x => p x.1)).map Prod.snd).map
      (fun j => rs[j]?.getD (0, 0)) = (rs.zipIdx.filter (fun x => p x.1)).map Prod.fst := by
    rw [List.map_map]
    apply List.map_congr_left
    intro x hx
    have hmem : x ∈ rs.zipIdx := (List.mem_filter.mp hx).1
    have hg : rs[x.2]? = some x.1 := (List.mem_zipIdx_iff_getElem?).1 hmem
    dsimp
    rw [hg]
    simp
  rw [hcong]
  simpa using (filter_map_fst rs.zipIdx p)

theorem sound_StableLimit : DyadicTile.StableLimit.Goal := by
  intro rs p k
  let rawIds := (physical key rs).zipIdx.filterMap (fun (r, i) => if p r then some (ordinal key rs i) else none)
  let rawSel := rs.zipIdx.filterMap (fun (r, j) => if p r then some j else none)
  have hidseq : rawIds.mergeSort = rawSel := by
    dsimp [rawIds, rawSel]
    exact List.Perm.eq_of_pairwise (le := fun a b : Nat => a ≤ b)
      (by intro a b ha hb hab hba; omega)
      (ids_pairwise rs p) (rawSel_pairwise rs p) (ids_perm rs p)
  change (rawIds.mergeSort.take k).map (fun j => rs[j]?.getD (0, 0)) = (rs.filter p).take k
  rw [hidseq]
  have hmap := congrArg (fun l : List Row => l.take k) (sel_map_eq rs p)
  simpa using hmap

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_DyadicTile_StableLimit
