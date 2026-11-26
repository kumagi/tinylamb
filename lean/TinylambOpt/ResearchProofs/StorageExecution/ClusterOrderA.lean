import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
import TinylambOpt.ResearchProofs.Compression.Wire

namespace TinylambOpt.ResearchProofs.StorageExecution.ClusterOrderA

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchProofs.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! Shared permutation and wire-format facts. -/

private theorem ca_lt_two_pow_width (n : Nat) : n < 2 ^ width n := by
  unfold width
  by_cases h : n == 0
  · simp at h
    subst h
    decide
  · simp [h]
    exact Nat.lt_log2_self

private theorem ca_mem_perm_lt (key : Key) (rs : List Row) {i : Nat}
    (hi : i ∈ perm key rs) : i < rs.length := by
  unfold perm at hi
  simp only [List.mem_map, Prod.exists, exists_and_right, exists_eq_right] at hi
  obtain ⟨r, hr⟩ := hi
  rcases hr with ⟨b, hb⟩
  have hb2 : ((r, b), i) ∈ rs.zipIdx := (List.mergeSort_perm _ _).mem_iff.mp hb
  have hmem := List.mem_zipIdx' hb2
  exact hmem.1

private theorem ca_perm_length (key : Key) (rs : List Row) : (perm key rs).length = rs.length := by
  unfold perm
  simp

private theorem ca_perm_bound (key : Key) (rs : List Row) :
    ∀ x ∈ perm key rs, x < 2 ^ width (rs.length - 1) := by
  intro x hx
  have hxlt : x < rs.length := ca_mem_perm_lt key rs hx
  have h := ca_lt_two_pow_width (rs.length - 1)
  omega

private theorem ca_pack_bytes_lt (w : Nat) (xs : List Nat) :
    ∀ b ∈ pack w xs, b < 256 := by
  intro b hb
  unfold pack little at hb
  simp only [List.mem_map, List.mem_range] at hb
  obtain ⟨i, _, rfl⟩ := hb
  exact Nat.mod_lt _ (by decide)

private theorem ca_ordinal_eq_getD (key : Key) (rs : List Row) (i : Nat)
    (hi : i < rs.length) :
    ordinal key rs i = (perm key rs)[i]?.getD 0 := by
  unfold ordinal side
  let w := width (rs.length - 1)
  let xs := perm key rs
  have hbound : ∀ x ∈ xs, x < 2 ^ w := ca_perm_bound key rs
  have hbs : ∀ b ∈ pack w xs, b < 256 := ca_pack_bytes_lt w xs
  rw [sound_LocalRead (pack w xs) w i hbs]
  have hpi := sound_PackedInverse w xs hbound
  have hi' : i < xs.length := by rw [ca_perm_length]; exact hi
  have hmap := congrArg (fun l => l[i]?) hpi
  simp only [List.getElem?_map, List.getElem?_range, hi', ↓reduceIte, Option.map_some] at hmap
  rw [← hmap]
  simp

private theorem ca_physical_length (key : Key) (rs : List Row) :
    (physical key rs).length = rs.length := by
  unfold physical
  simp [ca_perm_length]

private theorem ca_perm_perm_range (key : Key) (rs : List Row) :
    (perm key rs).Perm (List.range rs.length) := by
  unfold perm
  let cmp := fun (x : Row × Nat) (y : Row × Nat) =>
    let a := key x.1; let b := key y.1
    a.1 < b.1 || (a.1 == b.1 && (a.2 < b.2 || (a.2 == b.2 && x.2 ≤ y.2)))
  have hsort : (rs.zipIdx.mergeSort cmp).Perm rs.zipIdx := List.mergeSort_perm _ _
  have hmap : ((rs.zipIdx.mergeSort cmp).map Prod.snd).Perm (rs.zipIdx.map Prod.snd) :=
    hsort.map Prod.snd
  have hz : rs.zipIdx.map Prod.snd = List.range rs.length := by
    simp [List.zipIdx_map_snd, ← List.range_eq_range']
  simpa [hz] using hmap

private theorem ca_perm_mem_iff (key : Key) (rs : List Row) (i : Nat) :
    i ∈ perm key rs ↔ i < rs.length := by
  constructor
  · exact ca_mem_perm_lt key rs
  · intro hi
    exact (ca_perm_perm_range key rs).symm.mem_iff.mp (List.mem_range.mpr hi)

private theorem ca_physical_perm (key : Key) (rs : List Row) : (physical key rs).Perm rs := by
  unfold physical
  have hp : (perm key rs).Perm (List.range rs.length) := ca_perm_perm_range key rs
  have h1 : ((perm key rs).map (fun i => rs[i]?.getD (0,0))).Perm
      ((List.range rs.length).map (fun i => rs[i]?.getD (0,0))) := hp.map _
  have h2 : ((List.range rs.length).map (fun i => rs[i]?.getD (0,0))) = rs := by
    symm
    apply List.ext_getElem
    · simp
    · intro i h1 h2
      simp only [List.length_map, List.length_range] at h2
      simp [List.getElem?_eq_getElem h2]
  simpa [h2] using h1

private theorem ca_side_length (key : Key) (rs : List Row) :
    (side key rs).length = orderBytes rs.length := by
  unfold side orderBytes pack little
  simp [ca_perm_length]

private theorem ca_layoutBytes_eq (key : Key) (rs : List Row) :
    layoutBytes key rs = 1 + orderBytes rs.length + both (physical key rs) := by
  unfold layoutBytes
  rw [ca_side_length]

private theorem ca_sidecarBudget (key : Key) : sidecarBudget key := by
  intro rs
  rw [ca_layoutBytes_eq]
  constructor <;> intro h <;> omega

private theorem ca_patchByOrdinal (key : Key) : patchByOrdinal key := by
  intro rs i v hi
  let ids := perm key rs
  have hpn : ids.length = rs.length := ca_perm_length key rs
  have hmem : i ∈ ids := (ca_perm_mem_iff key rs i).mpr hi
  have hidx : ids.idxOf i < ids.length := List.idxOf_lt_length_of_mem hmem
  have hget : ids[ids.idxOf i]'hidx = i := List.getElem_idxOf hidx
  unfold physical
  change (ids.map (fun j => rs[j]?.getD (0,0))).set (ids.idxOf i) v =
    ids.map (fun j => (rs.set i v)[j]?.getD (0,0))
  apply List.ext_getElem
  · simp [List.length_set, List.length_map, hpn]
  · intro k hk1 hk2
    simp only [List.length_set, List.length_map, hpn] at hk1
    simp only [List.getElem_set, List.getElem_map]
    by_cases hk : k = ids.idxOf i
    · subst k
      simp [hget, hi]
    · rw [if_neg hk]

private theorem ca_payloadAlignment (key : Key) : payloadAlignment key := by
  intro rs payload hlen
  have hp : (perm key rs).Perm (List.range rs.length) := ca_perm_perm_range key rs
  have hL : (physical key rs).zip ((perm key rs).map (fun i => payload[i]?.getD 0)) =
      (perm key rs).map (fun i => (rs[i]?.getD (0,0), payload[i]?.getD 0)) := by
    unfold physical
    rw [List.zip_map']
  have hR : rs.zip payload = (List.range rs.length).map (fun i => (rs[i]?.getD (0,0), payload[i]?.getD 0)) := by
    apply List.ext_getElem
    · simp [hlen]
    · intro i h1 h2
      simp only [List.length_zip, List.length_map, List.length_range] at h1 h2
      have hi : i < rs.length := by omega
      have hip : i < payload.length := by omega
      simp [List.getElem_zip, List.getElem?_eq_getElem hi, List.getElem?_eq_getElem hip]
  rw [hL, hR]
  exact hp.map (fun i => (rs[i]?.getD (0,0), payload[i]?.getD 0))

end TinylambOpt.ResearchProofs.StorageExecution.ClusterOrderA
