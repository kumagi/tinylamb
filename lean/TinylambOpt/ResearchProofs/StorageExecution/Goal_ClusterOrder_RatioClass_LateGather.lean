import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
import TinylambOpt.ResearchProofs.Compression.Wire

/-!
Ordered SQL scans and limits depend on preserving key order and ties across physical
clusters.

Proved

- `RatioClass.LateGather` — Restore the projected column through the serialized ordinal
  map.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_RatioClass_LateGather

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchProofs.Compression.Wire

/-! The `lateGather` property is generic in the clustering key, so the same
    packed-ordinal / stable-gather argument used for the Lexicographic key
    applies verbatim to `RatioClass.key`. -/

private theorem lg_lt_two_pow_width (n : Nat) : n < 2 ^ width n := by
  unfold width
  by_cases h : n == 0
  · simp at h
    subst h
    decide
  · simp [h]
    have hn : 0 < n := Nat.pos_of_ne_zero (by simpa using h)
    have h1 : n < 2 ^ (n.log2 + 1) := Nat.lt_log2_self
    simpa [Nat.pow_succ, Nat.mul_comm, Nat.mul_left_comm] using h1

private theorem lg_mem_perm_lt (key : Key) (rs : List Row) {i : Nat}
    (hi : i ∈ perm key rs) : i < rs.length := by
  unfold perm at hi
  simp only [List.mem_map, Prod.exists, exists_eq_right] at hi
  obtain ⟨r, hr⟩ := hi
  rcases hr with ⟨b, hb⟩
  have hb2 : ((r, b), i) ∈ rs.zipIdx := (List.mergeSort_perm _ _).mem_iff.mp hb
  have hmem := List.mem_zipIdx hb2
  simpa using hmem.2.1

private theorem lg_perm_length (key : Key) (rs : List Row) :
    (perm key rs).length = rs.length := by
  unfold perm
  simp

private theorem lg_perm_bound (key : Key) (rs : List Row) :
    ∀ x ∈ perm key rs, x < 2 ^ width (rs.length - 1) := by
  intro x hx
  have hxlt : x < rs.length := lg_mem_perm_lt key rs hx
  have : rs.length - 1 < 2 ^ width (rs.length - 1) := lg_lt_two_pow_width (rs.length - 1)
  omega

private theorem lg_pack_bytes_lt (w : Nat) (xs : List Nat) :
    ∀ b ∈ pack w xs, b < 256 := by
  intro b hb
  unfold pack little at hb
  simp only [List.mem_map, List.mem_range] at hb
  obtain ⟨i, _, rfl⟩ := hb
  exact Nat.mod_lt _ (by decide)

private theorem lg_ordinal_eq_getElem (key : Key) (rs : List Row) (i : Nat)
    (hi : i < rs.length) :
    ordinal key rs i = (perm key rs)[i]'(by rw [lg_perm_length]; exact hi) := by
  unfold ordinal side
  let w := width (rs.length - 1)
  let xs := perm key rs
  have hbound : ∀ x ∈ xs, x < 2 ^ w := lg_perm_bound key rs
  have hbs : ∀ b ∈ pack w xs, b < 256 := lg_pack_bytes_lt w xs
  rw [sound_LocalRead (pack w xs) w i hbs]
  have hpi := sound_PackedInverse w xs hbound
  have hi' : i < xs.length := by rw [lg_perm_length]; exact hi
  have hmap := congrArg (fun l => l[i]?) hpi
  simp only [List.getElem?_map, List.getElem?_range, hi', Option.map_some] at hmap
  have hget : get (pack w xs) w i = xs[i]'hi' := by
    have := hmap.symm
    simp [List.getElem?_eq_getElem hi'] at this
    exact this.symm
  exact hget

private theorem lg_ordinal_eq_getD (key : Key) (rs : List Row) (i : Nat)
    (hi : i < rs.length) :
    ordinal key rs i = (perm key rs)[i]?.getD 0 := by
  rw [lg_ordinal_eq_getElem key rs i hi]
  rw [List.getElem?_eq_getElem (by rw [lg_perm_length]; exact hi)]
  simp

private theorem lg_perm_perm_range (key : Key) (rs : List Row) :
    (perm key rs).Perm (List.range rs.length) := by
  unfold perm
  let cmp := fun (x : Row × Nat) (y : Row × Nat) =>
    let a := key x.1
    let b := key y.1
    a.1 < b.1 || (a.1 == b.1 && (a.2 < b.2 || (a.2 == b.2 && x.2 ≤ y.2)))
  have hsort : (rs.zipIdx.mergeSort cmp).Perm rs.zipIdx := List.mergeSort_perm _ _
  have hmap : ((rs.zipIdx.mergeSort cmp).map Prod.snd).Perm (rs.zipIdx.map Prod.snd) :=
    hsort.map Prod.snd
  have hz : rs.zipIdx.map Prod.snd = List.range rs.length := by
    simp [List.zipIdx_map_snd, ← List.range_eq_range']
  simpa [hz] using hmap

private theorem lg_pairwise_fst_le_antisymm {l1 l2 : List (Nat × Nat)}
    (hp : l1.Perm l2)
    (h1 : l1.Pairwise (fun a b => a.1 ≤ b.1))
    (h2 : l2.Pairwise (fun a b => a.1 ≤ b.1))
    (hk : ∀ x ∈ l1, ∀ y ∈ l1, x.1 = y.1 → x = y) : l1 = l2 := by
  induction h1 generalizing l2 with
  | nil => exact hp.symm.eq_nil.symm
  | @cons a l ha hl ih =>
    cases l2 with
    | nil => exact absurd hp.eq_nil (by simp)
    | cons b l2 =>
      cases h2 with
      | cons hba_all htl =>
        have hbl : b ∈ a :: l := hp.symm.mem_iff.mp List.mem_cons_self
        have hab : a.1 ≤ b.1 := by
          cases hbl with
          | head => exact Nat.le_refl _
          | tail _ hm => exact ha b hm
        have hal2 : a ∈ b :: l2 := hp.mem_iff.mp List.mem_cons_self
        have hba : b.1 ≤ a.1 := by
          cases hal2 with
          | head => exact Nat.le_refl _
          | tail _ hm => exact hba_all a hm
        have hfst : a.1 = b.1 := Nat.le_antisymm hab hba
        have hab' : a = b := hk a List.mem_cons_self b (hp.symm.mem_iff.mp List.mem_cons_self) hfst
        subst hab'
        exact congrArg (List.cons a) (ih (List.Perm.cons_inv hp) htl (by
          intro x hx y hy hxy
          exact hk x (List.mem_cons_of_mem a hx) y (List.mem_cons_of_mem a hy) hxy))

private theorem lg_range_tagged_pairwise {n : Nat} (f : Nat → Nat) :
    ((List.range n).map (fun j => (j, f j))).Pairwise (fun a b => a.1 ≤ b.1) := by
  rw [List.pairwise_map]
  exact List.pairwise_lt_range.imp (fun h => Nat.le_of_lt h)

private theorem lg_range_tagged_fst_injective {n : Nat} (f : Nat → Nat) :
    ∀ x ∈ (List.range n).map (fun j => (j, f j)),
      ∀ y ∈ (List.range n).map (fun j => (j, f j)), x.1 = y.1 → x = y := by
  intro x hx y hy hxy
  simp only [List.mem_map, List.mem_range] at hx hy
  obtain ⟨i, _, rfl⟩ := hx
  obtain ⟨j, _, rfl⟩ := hy
  simp at hxy
  simp [hxy]

private theorem lg_list_eq_map_range_getD {α : Type} (l : List α) (d : α) :
    l = (List.range l.length).map (fun i => l[i]?.getD d) := by
  apply List.ext_getElem
  · simp
  · intro i h1 h2
    simp only [List.length_map, List.length_range] at h2
    simp [List.getElem?_eq_getElem h2]

private theorem lg_lateGather (key : Key) : lateGather key := by
  intro rs project
  unfold unravel
  let n := rs.length
  let f := fun (j : Nat) => project (rs[j]?.getD (0,0))
  let L := (List.range n).map (fun i => (ordinal key rs i, ((physical key rs).map project)[i]?.getD 0))
  let M := (List.range n).map (fun j => (j, f j))
  have hpperm : (perm key rs).Perm (List.range n) := lg_perm_perm_range key rs
  have hpn : (perm key rs).length = n := lg_perm_length key rs
  have hpermLM : L.Perm M := by
    have hL : L = (perm key rs).map (fun j => (j, project (rs[j]?.getD (0,0)))) := by
      apply List.ext_getElem
      · simp [L, hpn]
      · intro i h1 h2
        simp [L] at h1
        have hi : i < n := h1
        have hip : i < (perm key rs).length := by omega
        simp only [L, List.getElem_map, List.getElem_range]
        apply Prod.ext
        · simp [lg_ordinal_eq_getD key rs i hi, List.getElem?_eq_getElem hip]
        · simp only [List.getElem?_map, physical, List.getElem?_map]
          have hpi : (perm key rs)[i]? = some ((perm key rs)[i]'hip) := List.getElem?_eq_getElem hip
          simp [hpi]
    rw [hL]
    exact hpperm.map (fun j => (j, project (rs[j]?.getD (0,0))))
  have hpairM : M.Pairwise (fun a b => a.1 ≤ b.1) := lg_range_tagged_pairwise f
  have hfstM : ∀ x ∈ M, ∀ y ∈ M, x.1 = y.1 → x = y := lg_range_tagged_fst_injective f
  let le : (Nat × Nat) → (Nat × Nat) → Bool := fun a b => decide (a.1 ≤ b.1)
  have htrans : ∀ (a b c : Nat × Nat), le a b = true → le b c = true → le a c = true := by
    intro a b c hab hbc
    simp only [le, decide_eq_true_eq] at hab hbc ⊢
    omega
  have htotal : ∀ (a b : Nat × Nat), (le a b || le b a) = true := by
    intro a b
    simp only [le, Bool.or_eq_true, decide_eq_true_eq]
    omega
  have hsorted : (L.mergeSort le).Pairwise (fun a b => le a b = true) := List.pairwise_mergeSort htrans htotal L
  have hpermSort : (L.mergeSort le).Perm L := List.mergeSort_perm L le
  have hpermSortM : (L.mergeSort le).Perm M := hpermSort.trans hpermLM
  have hsorted' : (L.mergeSort le).Pairwise (fun a b => a.1 ≤ b.1) :=
    hsorted.imp (by intro a b h; simpa [le, decide_eq_true_eq] using h)
  have hfstSort : ∀ x ∈ L.mergeSort le, ∀ y ∈ L.mergeSort le, x.1 = y.1 → x = y := by
    intro x hx y hy hxy
    exact hfstM x (hpermSortM.mem_iff.mp hx) y (hpermSortM.mem_iff.mp hy) hxy
  have heq : L.mergeSort le = M := lg_pairwise_fst_le_antisymm hpermSortM hsorted' hpairM hfstSort
  rw [show (L.mergeSort (fun a b => decide (a.1 ≤ b.1))) = L.mergeSort le from rfl, heq]
  have hrs : (List.range rs.length).map (fun j => rs[j]?.getD (0,0)) = rs :=
    (lg_list_eq_map_range_getD rs (0,0)).symm
  simpa [M, f, n, List.map_map, Function.comp_def] using congrArg (List.map project) hrs

theorem sound_LateGather : RatioClass.LateGather.Goal := by
  exact lg_lateGather RatioClass.key

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_RatioClass_LateGather
