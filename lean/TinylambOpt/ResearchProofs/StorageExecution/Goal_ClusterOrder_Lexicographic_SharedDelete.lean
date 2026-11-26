import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
import TinylambOpt.ResearchProofs.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_Lexicographic_SharedDelete

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.Lexicographic
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchProofs.Compression.Wire

private theorem lt_two_pow_width (n : Nat) : n < 2 ^ width n := by
  unfold width
  by_cases h : n == 0
  · simp at h; subst h; decide
  · simp [h]
    have hn : 0 < n := Nat.pos_of_ne_zero (by simpa using h)
    have h1 : n < 2 ^ (n.log2 + 1) := Nat.lt_log2_self
    simpa [Nat.pow_succ, Nat.mul_comm, Nat.mul_left_comm] using h1

private theorem mem_perm_lt (key : Key) (rs : List Row) {i : Nat}
    (hi : i ∈ perm key rs) : i < rs.length := by
  unfold perm at hi
  simp only [List.mem_map, Prod.exists, exists_and_right, exists_eq_right] at hi
  obtain ⟨r, hr⟩ := hi
  rcases hr with ⟨b, hb⟩
  have hb2 : ((r, b), i) ∈ rs.zipIdx := (List.mergeSort_perm _ _).mem_iff.mp hb
  have hmem := List.mem_zipIdx hb2
  simpa using hmem.2.1

private theorem perm_length (key : Key) (rs : List Row) : (perm key rs).length = rs.length := by
  unfold perm
  simp

private theorem perm_bound (key : Key) (rs : List Row) :
    ∀ x ∈ perm key rs, x < 2 ^ width (rs.length - 1) := by
  intro x hx
  have hxlt : x < rs.length := mem_perm_lt key rs hx
  have : rs.length - 1 < 2 ^ width (rs.length - 1) := lt_two_pow_width (rs.length - 1)
  omega

private theorem pack_bytes_lt (w : Nat) (xs : List Nat) :
    ∀ b ∈ pack w xs, b < 256 := by
  intro b hb
  unfold pack little at hb
  simp only [List.mem_map, List.mem_range] at hb
  obtain ⟨i, _, rfl⟩ := hb
  exact Nat.mod_lt _ (by decide)

private theorem ordinal_eq_getElem (key : Key) (rs : List Row) (i : Nat)
    (hi : i < rs.length) :
    ordinal key rs i = (perm key rs)[i]'(by rw [perm_length]; exact hi) := by
  unfold ordinal side
  let w := width (rs.length - 1)
  let xs := perm key rs
  have hbound : ∀ x ∈ xs, x < 2 ^ w := perm_bound key rs
  have hbs : ∀ b ∈ pack w xs, b < 256 := pack_bytes_lt w xs
  rw [sound_LocalRead (pack w xs) w i hbs]
  have hpi := sound_PackedInverse w xs hbound
  have hi' : i < xs.length := by rw [perm_length]; exact hi
  have hmap := congrArg (fun l => l[i]?) hpi
  simp only [List.getElem?_map, List.getElem?_range, hi', Option.map_some] at hmap
  have hget : get (pack w xs) w i = xs[i]'hi' := by
    have := hmap.symm
    simp [List.getElem?_eq_getElem hi'] at this
    exact this.symm
  exact hget

private theorem ordinal_eq_getD (key : Key) (rs : List Row) (i : Nat)
    (hi : i < rs.length) :
    ordinal key rs i = (perm key rs)[i]?.getD 0 := by
  rw [ordinal_eq_getElem key rs i hi]
  rw [List.getElem?_eq_getElem (by rw [perm_length]; exact hi)]
  simp

private theorem perm_perm_range (key : Key) (rs : List Row) :
    (perm key rs).Perm (List.range rs.length) := by
  unfold perm
  let cmp := fun (x : Row × Nat) (y : Row × Nat) =>
    let a := key x.1; let b := key y.1
    a.1 < b.1 || (a.1==b.1 && (a.2 < b.2 || (a.2==b.2 && x.2≤y.2)))
  have hsort : (rs.zipIdx.mergeSort cmp).Perm rs.zipIdx := List.mergeSort_perm _ _
  have hmap : ((rs.zipIdx.mergeSort cmp).map Prod.snd).Perm (rs.zipIdx.map Prod.snd) :=
    hsort.map Prod.snd
  have hz : rs.zipIdx.map Prod.snd = List.range rs.length := by
    simp [List.zipIdx_map_snd, ← List.range_eq_range']
  simpa [hz] using hmap

private theorem list_eq_map_range_getD {α : Type} (l : List α) (d : α) :
    l = (List.range l.length).map (fun i => l[i]?.getD d) := by
  apply List.ext_getElem
  · simp
  · intro i h1 h2
    simp only [List.length_map, List.length_range] at h2
    simp [List.getElem?_eq_getElem h2]

/-- Filtering the indexed map of a list by a predicate on the list entries picks
exactly the entries of the filtered list, regardless of the starting index. -/
private theorem filter_zipIdx_map_offset {α : Type} (p : List Nat) (f : Nat → Bool) (g : Nat → α) (k : Nat) :
    (((p.map g).zipIdx k).filter (fun (_, i) => f (p[i - k]?.getD 0))).map Prod.fst = (p.filter f).map g := by
  induction p generalizing k with
  | nil => simp
  | cons x xs ih =>
    have htail : List.filter (fun z : α × Nat => f ((x :: xs)[z.snd - k]?.getD 0)) ((List.map g xs).zipIdx (k + 1))
        = List.filter (fun z : α × Nat => f (xs[z.snd - (k + 1)]?.getD 0)) ((List.map g xs).zipIdx (k + 1)) := by
      apply List.filter_congr
      intro z hz
      have hlo : k + 1 ≤ z.snd := (List.mem_zipIdx (α := α) hz).1
      have hsub : z.snd - k = (z.snd - (k + 1)) + 1 := by omega
      simp [hsub]
    by_cases hx : f x = true
    · simp [hx, htail, ih (k + 1)]
    · have hx' : f x = false := by cases h : f x <;> simp_all
      simp [hx', htail, ih (k + 1)]

theorem sound_SharedDelete :
    TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.Lexicographic.SharedDelete.Goal := by
  intro rs ds
  let n := rs.length
  let p := perm key rs
  let g : Nat → Row := fun j => rs[j]?.getD (0,0)
  let f : Nat → Bool := fun j => !ds.contains j
  have hp : p.Perm (List.range n) := perm_perm_range key rs
  have hpn : p.length = n := perm_length key rs

  have hL0 : ((physical key rs).zipIdx.filter (fun (_, i) => f (ordinal key rs i))).map Prod.fst
      = ((p.map g).zipIdx.filter (fun (_, i) => f (p[i]?.getD 0))).map Prod.fst := by
    unfold physical
    apply congrArg (fun l : List (Row × Nat) => l.map Prod.fst)
    apply List.filter_congr
    intro z hz
    have hzlt : z.2 < (p.map g).length := by simpa [p, g] using (List.mem_zipIdx (α := Row) hz).2.1
    have hi : z.2 < rs.length := by simpa [p, g, hpn] using hzlt
    simp [f, p, ordinal_eq_getD key rs z.2 hi]
  have hL : ((physical key rs).zipIdx.filter (fun (_, i) => f (ordinal key rs i))).map Prod.fst
      = (p.filter f).map g := by
    rw [hL0]
    simpa using filter_zipIdx_map_offset p f g 0

  have hrs : rs = (List.range n).map g := by
    simpa [n, g] using (list_eq_map_range_getD rs (0,0))
  have hR0 : ((rs.zipIdx.filter (fun (_, i) => f i)).map Prod.fst)
      = ((((List.range n).map g).zipIdx.filter (fun (_, i) => f i)).map Prod.fst) := by
    rw [hrs]
  have hR1 : ((((List.range n).map g).zipIdx.filter (fun (_, i) => f (((List.range n)[i]?.getD 0)))).map Prod.fst)
      = ((((List.range n).map g).zipIdx.filter (fun (_, i) => f i)).map Prod.fst) := by
    apply congrArg (fun l : List (Row × Nat) => l.map Prod.fst)
    apply List.filter_congr
    intro z hz
    have hzlt : z.2 < ((List.range n).map g).length := by simpa using (List.mem_zipIdx (α := Row) hz).2.1
    have hi : z.2 < n := by simpa using hzlt
    simp [List.getElem?_range hi]
  have hR : ((rs.zipIdx.filter (fun (_, i) => f i)).map Prod.fst)
      = ((List.range n).filter f).map g := by
    rw [hR0]
    have hgen := filter_zipIdx_map_offset (List.range n) f g 0
    simp only [Nat.sub_zero] at hgen
    rw [hR1] at hgen
    exact hgen

  rw [hL, hR]
  exact (hp.filter f).map g

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_Lexicographic_SharedDelete
