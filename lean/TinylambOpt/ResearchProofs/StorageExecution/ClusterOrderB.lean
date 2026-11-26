import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
import TinylambOpt.ResearchProofs.Compression.Wire

namespace TinylambOpt.ResearchProofs.StorageExecution.ClusterOrderB
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchProofs.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! Shared permutation, serialization, and stable-order facts. -/

private theorem width_eq (n : Nat) (hn : n ≠ 0) : width n = n.log2 + 1 := by
  unfold width
  simp [hn]

private theorem lt_two_pow_width (n : Nat) : n < 2 ^ width n := by
  by_cases hn : n = 0
  · subst n
    simp [width]
  · rw [width_eq n hn]
    exact Nat.lt_log2_self

private theorem mem_perm_lt (key : Key) (rs : List Row) {i : Nat}
    (hi : i ∈ perm key rs) : i < rs.length := by
  unfold perm at hi
  simp only [List.mem_map, Prod.exists, exists_and_right, exists_eq_right] at hi
  obtain ⟨r, hr⟩ := hi
  rcases hr with ⟨b, hb⟩
  have hb2 : ((r, b), i) ∈ rs.zipIdx := (List.mergeSort_perm _ _).mem_iff.mp hb
  simpa using (List.mem_zipIdx hb2).2.1

private theorem perm_length (key : Key) (rs : List Row) :
    (perm key rs).length = rs.length := by
  simp [perm]

private theorem perm_bound (key : Key) (rs : List Row) :
    ∀ x ∈ perm key rs, x < 2 ^ width (rs.length - 1) := by
  intro x hx
  have hxlt : x < rs.length := mem_perm_lt key rs hx
  have := lt_two_pow_width (rs.length - 1)
  omega

private theorem pack_bytes_lt (w : Nat) (xs : List Nat) :
    ∀ b ∈ pack w xs, b < 256 := by
  intro b hb
  unfold pack little at hb
  simp only [List.mem_map, List.mem_range] at hb
  obtain ⟨i, _, rfl⟩ := hb
  exact Nat.mod_lt _ (by decide)

private theorem ordinal_eq_getD (key : Key) (rs : List Row) (i : Nat)
    (hi : i < rs.length) :
    ordinal key rs i = (perm key rs)[i]?.getD 0 := by
  unfold ordinal side
  set w := width (rs.length - 1)
  set xs := perm key rs
  have hbound : ∀ x ∈ xs, x < 2 ^ w := perm_bound key rs
  have hbs : ∀ b ∈ pack w xs, b < 256 := pack_bytes_lt w xs
  have hpi := sound_PackedInverse w xs hbound
  have hlocal := sound_LocalRead (pack w xs) w i hbs
  have hi' : i < xs.length := by rw [perm_length]; exact hi
  have hidx : (List.range xs.length)[i]? = some ((perm key rs)[i]?.getD 0) := by
    rw [List.getElem?_range hi', List.getElem?_eq_getElem hi']
    simp [List.getElem?_eq_getElem hi']
  rw [hlocal, hpi]
  exact List.getElem?_eq_getElem hi'

private theorem physical_length (key : Key) (rs : List Row) :
    (physical key rs).length = rs.length := by
  simp [physical, perm_length]

private theorem perm_perm_range (key : Key) (rs : List Row) :
    (perm key rs).Perm (List.range rs.length) := by
  unfold perm
  let cmp := fun (x : Row × Nat) (y : Row × Nat) =>
    let a := key x.1; let b := key y.1
    a.1 < b.1 || (a.1==b.1 && (a.2 < b.2 || (a.2==b.2 && x.2≤y.2)))
  have hmap := (List.mergeSort_perm rs.zipIdx cmp).map Prod.snd
  have hz : rs.zipIdx.map Prod.snd = List.range rs.length := by
    rw [List.zipIdx_map_snd]
    exact List.range_eq_range'.symm
  simpa [hz] using hmap

private theorem perm_nodup (key : Key) (rs : List Row) : (perm key rs).Nodup :=
  (perm_perm_range key rs).nodup_iff.mpr (List.nodup_range)

private theorem perm_mem_iff (key : Key) (rs : List Row) (i : Nat) :
    i ∈ perm key rs ↔ i < rs.length := by
  constructor
  · exact mem_perm_lt key rs
  · intro hi
    exact (perm_perm_range key rs).symm.mem_iff.mp (List.mem_range.mpr hi)

private theorem little_length (n k : Nat) : (little n k).length = k := by
  simp [little]

private theorem side_length (key : Key) (rs : List Row) :
    (side key rs).length = orderBytes rs.length := by
  simp [side, orderBytes, pack, little_length, perm_length]

private theorem layoutBytes_eq (key : Key) (rs : List Row) :
    layoutBytes key rs = 1 + orderBytes rs.length + both (physical key rs) := by
  unfold layoutBytes side orderBytes pack
  rw [little_length, perm_length]

private theorem list_eq_map_range_getD {α : Type} (l : List α) (d : α) :
    l = (List.range l.length).map (fun i => l[i]?.getD d) := by
  apply List.ext_getElem
  · simp
  · intro i h1 h2
    simp only [List.length_map, List.length_range] at h2
    simp [List.getElem?_eq_getElem h2]

private theorem physical_perm (key : Key) (rs : List Row) : (physical key rs).Perm rs := by
  unfold physical
  have h1 := (perm_perm_range key rs).map (fun i => rs[i]?.getD (0,0))
  have h2 : ((List.range rs.length).map (fun i => rs[i]?.getD (0,0))) = rs :=
    (list_eq_map_range_getD rs (0,0)).symm
  simpa [h2] using h1

private def tagLe (key : Key) (x y : Row × Nat) : Bool :=
  let a := key x.1; let b := key y.1
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

private def rowLe (x y : Row) : Prop := x.1 ≤ y.1
private def keyRel (key : Key) (x y : Row) : Prop :=
  let a := key x; let b := key y
  a.1 < b.1 ∨ (a.1 = b.1 ∧ (a.2 < b.2 ∨ (a.2 = b.2 ∧ rowLe x y)))

private theorem physical_pairwise_key (key : Key) (rs : List Row) :
    (physical key rs).Pairwise (keyRel key) := by
  have hlen : (rs.zipIdx.mergeSort (tagLe key)).length = rs.length := by
    rw [(List.mergeSort_perm _ _).length_eq]
    simp
  have hmem_all : ∀ z ∈ rs.zipIdx.mergeSort (tagLe key), z ∈ rs.zipIdx :=
    (List.mergeSort_perm _ _).mem_iff.mp
  have hrow : ∀ z ∈ rs.zipIdx.mergeSort (tagLe key),
      rs[z.2]?.getD (0,0) = z.1 := by
    intro z hz
    have hzm := hmem_all z hz
    have hz' := List.mem_zipIdx hzm
    rw [List.getElem?_eq_getElem (by simpa using hz'.2.1)]
    exact hz'.2.2.symm
  unfold physical perm
  rw [List.pairwise_map, List.pairwise_map]
  apply hs.imp
  intro x y h
  have hxm := hmem_all x (by
    have := List.pairwise_iff_getElem.mp hs
    exact this.1 0 (by
      have := (List.mergeSort_perm rs.zipIdx (tagLe key)).length_eq
      simp at this
      omega) x (by
        have := (List.pairwise_iff_getElem.mp hs).1
        exact this.1 0 (by simp [hlen]) x (by simp [hlen]) (by omega)))
  have hym := hmem_all y (by
    have := (List.pairwise_iff_getElem.mp hs).1
    exact this.1 0 (by simp [hlen]) y (by simp [hlen]) (by omega))
  have hxrow := hrow x (by
    have := (List.pairwise_iff_getElem.mp hs).1
    exact this.1 0 (by simp [hlen]) x (by simp [hlen]) (by omega))
  have hyrow := hrow y (by
    have := (List.pairwise_iff_getElem.mp hs).1
    exact this.1 0 (by simp [hlen]) y (by simp [hlen]) (by omega))
  rw [← hxrow, ← hyrow]
  simpa [tagLe, keyRel, rowLe, decide_eq_true_eq, Bool.or_eq_true,
    Bool.and_eq_true, beq_iff_eq] using h

private theorem sidecarBudget_true (key : Key) : sidecarBudget key := by
  intro rs
  rw [layoutBytes_eq]
  constructor <;> intro h <;> omega

/-! The generic serialization and stable-permutation goals. -/

private theorem pairwise_fst_le_antisymm {l1 l2 : List (Nat × Nat)}
    (hp : l1.Perm l2)
    (h1 : l1.Pairwise (fun a b => a.1 ≤ b.1))
    (h2 : l2.Pairwise (fun a b => a.1 ≤ b.1))
    (hk : ∀ x ∈ l1, ∀ y ∈ l1, x.1 = y.1 → x = y) : l1 = l2 := by
  induction h1 generalizing l2 with
  | nil => exact (hp.symm.eq_nil).symm
  | @cons a l ha hl ih =>
    cases l2 with
    | nil => exact absurd hp.eq_nil (by simp)
    | cons b l2 =>
      have hbl : b ∈ a :: l := hp.symm.mem_iff.mp List.mem_cons_self
      have hab : a.1 ≤ b.1 := by
        cases hbl with
        | head => exact Nat.le_refl _
        | tail _ hm => exact ha b hm
      have hal2 : a ∈ b :: l2 := hp.mem_iff.mp List.mem_cons_self
      have hba : b.1 ≤ a.1 := by
        cases hal2 with
        | head => exact Nat.le_refl _
        | tail _ hm => exact (List.pairwise_cons.mp h2).1 a hm
      have hfst : a.1 = b.1 := Nat.le_antisymm hab hba
      have hab' : a = b := hk a List.mem_cons_self b (hp.symm.mem_iff.mp List.mem_cons_self) hfst
      subst hab'
      exact congrArg (List.cons a) (ih (List.Perm.cons_inv hp) (List.pairwise_cons.mp h2).2 (by
        intro x hx y hy hxy
        exact hk x (List.mem_cons_of_mem a hx) y (List.mem_cons_of_mem a hy) hxy))

private theorem range_tagged_pairwise {n : Nat} (f : Nat → Nat) :
    ((List.range n).map (fun j => (j, f j))).Pairwise (fun a b => a.1 ≤ b.1) := by
  rw [List.pairwise_map]
  exact List.pairwise_lt_range.imp (fun h => Nat.le_of_lt h)

private theorem range_tagged_fst_injective {n : Nat} (f : Nat → Nat) :
    ∀ x ∈ (List.range n).map (fun j => (j, f j)),
      ∀ y ∈ (List.range n).map (fun j => (j, f j)), x.1 = y.1 → x = y := by
  intro x hx y hy hxy
  simp only [List.mem_map, List.mem_range] at hx hy
  obtain ⟨i, _, rfl⟩ := hx
  obtain ⟨j, _, rfl⟩ := hy
  simp at hxy
  simp [hxy]

private theorem lateGather_true (key : Key) : lateGather key := by
  intro rs project
  unfold unravel
  set n := rs.length
  set f := fun j => project (rs[j]?.getD (0,0))
  set L := (List.range n).map (fun i => (ordinal key rs i, ((physical key rs).map project)[i]?.getD 0))
  set M := (List.range n).map (fun j => (j, f j))
  have hpn : (perm key rs).length = n := perm_length key rs
  have hpermLM : L.Perm M := by
    have hL : L = (perm key rs).map (fun j => (j, project (rs[j]?.getD (0,0)))) := by
      apply List.ext_getElem
      · simp [hpn]
      · intro i h1 h2
        simp only [List.length_map, List.length_range] at h1
        have hi : i < n := h1
        have hip : i < (perm key rs).length := by omega
        simp only [List.getElem_map, List.getElem_range]
        constructor
        · simp [ordinal_eq_getD key rs i hi]
        · simp only [List.getElem?_map, physical, List.getElem?_map]
          have hpi : (perm key rs)[i]? = some ((perm key rs)[i]'hip) := List.getElem?_eq_getElem hip
          simp [hpi, ordinal_eq_getD key rs i hi]
    rw [hL]
    exact (perm_perm_range key rs).map (fun j => (j, project (rs[j]?.getD (0,0))))
  have hpairM : M.Pairwise (fun a b => a.1 ≤ b.1) := range_tagged_pairwise f
  have hfstM : ∀ x ∈ M, ∀ y ∈ M, x.1 = y.1 → x = y := range_tagged_fst_injective f
  let le : (Nat × Nat) → (Nat × Nat) → Bool := fun a b => decide (a.1 ≤ b.1)
  have htrans : ∀ (a b c : Nat × Nat), le a b = true → le b c = true → le a c = true := by
    intro a b c hab hbc; simp only [le, decide_eq_true_eq] at hab hbc ⊢; omega
  have htotal : ∀ (a b : Nat × Nat), (le a b || le b a) = true := by
    intro a b; simp only [le, Bool.or_eq_true, decide_eq_true_eq]; omega
  have hsorted : (L.mergeSort le).Pairwise (fun a b => a.1 ≤ b.1) :=
    (List.pairwise_mergeSort htrans htotal L).imp (by intro a b h; simpa [le, decide_eq_true_eq] using h)
  have hpermSortM : (L.mergeSort le).Perm M :=
    (List.mergeSort_perm L le).trans hpermLM
  have hfstSort : ∀ x ∈ L.mergeSort le, ∀ y ∈ L.mergeSort le, x.1 = y.1 → x = y := by
    intro x hx y hy hxy
    exact hfstM x (hpermSortM.mem_iff.mp hx) y (hpermSortM.mem_iff.mp hy) hxy
  have heq : L.mergeSort le = M := pairwise_fst_le_antisymm hpermSortM hsorted hpairM hfstSort
  rw [show (L.mergeSort (fun a b => decide (a.1 ≤ b.1))) = L.mergeSort le from rfl, heq]
  simp [M, f, List.map_map]

private theorem filterMap_map_eq_zip {α β γ : Type} (p : List α) (f : α → β) (g : α → γ) :
    (p.map f).zip (p.map g) = p.map (fun x => (f x, g x)) := by
  induction p with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem payloadAlignment_true (key : Key) : payloadAlignment key := by
  intro rs payload hlen
  have hp : (perm key rs).Perm (List.range rs.length) := perm_perm_range key rs
  have hL : (physical key rs).zip ((perm key rs).map (fun i => payload[i]?.getD 0)) =
      (perm key rs).map (fun i => (rs[i]?.getD (0,0), payload[i]?.getD 0)) := by
    unfold physical
    rw [filterMap_map_eq_zip]
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

private theorem filterMap_congr' {α β : Type} {l : List α} {f g : α → Option β}
    (h : ∀ x ∈ l, f x = g x) : l.filterMap f = l.filterMap g := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
    simp only [List.filterMap_cons]
    rw [h x List.mem_cons_self, ih (fun y hy => h y (List.mem_cons_of_mem x hy))]

private theorem filter_cons_of_neg' {α : Type} {p : α → Bool} {x : α} {xs : List α}
    (h : p x = false) : xs.filter p = (x :: xs).filter p := by
  simp [List.filter_cons, h]

private theorem filter_cons_of_pos' {α : Type} {p : α → Bool} {x : α} {xs : List α}
    (h : p x = true) : (x :: xs).filter p = x :: xs.filter p := by
  simp [List.filter_cons, h]

private theorem filterMap_range_eq_filter_map {n : Nat} (p : List Nat) (f : Nat → Bool) (g : Nat → Row)
    (hpn : p.length = n) :
    (List.range n).filterMap (fun i => if f (p[i]?.getD 0) then some (g (p[i]?.getD 0)) else none)
    = (p.filter f).map g := by
  induction p generalizing n with
  | nil =>
    simp at hpn
    subst hpn
    simp
  | cons x xs ih =>
    cases n with
    | zero => simp at hpn
    | succ n =>
      have hxs : xs.length = n := by simp at hpn; exact hpn
      have htail : (List.range n).filterMap (fun i => if f (xs[i]?.getD 0) then some (g (xs[i]?.getD 0)) else none)
          = (xs.filter f).map g := ih hxs
      by_cases hf : f x = true
      · simp only [List.range_succ, List.filterMap_cons, List.getElem?_cons_zero,
          Option.getD_some, hf, ite_true, List.map_cons]
        congr 1
        exact ((congrArg (List.map g) (filter_cons_of_pos' (xs := xs) hf)).symm.trans htail).symm
      · have hf' : f x = false := by cases h : f x <;> simp_all
        simp only [List.range_succ, List.filterMap_cons, List.getElem?_cons_zero,
          Option.getD_some, hf', Bool.false_eq_true, ite_false, List.map_cons]
        exact ((congrArg (List.map g) (filter_cons_of_neg' (xs := xs) hf')).symm.trans htail).symm

private theorem list_eq_nil_of_length_eq_zero {α : Type} {l : List α} (h : l.length = 0) : l = [] := by
  cases l with
  | nil => rfl
  | cons x xs => simp at h

private theorem zipIdx_filterMap_ordinal (key : Key) (rs : List Row) (ds : List Nat) :
    ((physical key rs).zipIdx.filter (fun (_,i) => !ds.contains (ordinal key rs i))).map Prod.fst
      = ((perm key rs).filter (fun j => !ds.contains j)).map (fun j => rs[j]?.getD (0,0)) := by
  unfold physical
  let p' := perm key rs
  let q : Nat → Row := fun j => rs[j]?.getD (0,0)
  let f : Nat → Bool := fun j => !ds.contains j
  let g : Nat → Row := q
  have hL0 : ((p'.map q).zipIdx.filter (fun (_,i) => !ds.contains (ordinal key rs i))).map Prod.fst
      = (List.range rs.length).filterMap (fun i => if f (p'[i]?.getD 0) then some (q (p'[i]?.getD 0)) else none) := by
    induction p' generalizing rs with
    | nil =>
      have hp : (perm key rs).length = 0 := hp0
      have hperm0 : perm key rs = [] := list_eq_nil_of_length_eq_zero hp
      subst p'
      have hrs0 : rs = [] := by
        apply list_eq_nil_of_length_eq_zero
        unfold physical
        rw [hperm0]
        rfl
      subst rs
      rfl
    | cons a as ih =>
      cases rs with
      | nil =>
        have hperm0 : perm key ([] : List Row) = [] := by simp [perm]
        have has0 : as = [] := by
          have hh := perm_length key ([] : List Row)
          simp only [List.length_nil] at hh
          have haslen : as.length = 0 := by
            change as.length = (p' : List Nat) |>.length
            rw [hh]
          exact list_eq_nil_of_length_eq_zero haslen
        subst as
        rfl
      | cons r rest =>
        have has : as.length = rest.length := by
          change as.length = (p' : List Nat) |>.length - 1
          change (perm key (r :: rest)).length = rest.length + 1 at hp
          omega
        simp only [List.zipIdx_cons, List.filter_cons, List.map_cons, List.range_succ,
          List.filterMap_cons, List.getElem?_cons_zero, Option.getD_some]
        by_cases hf : f a = true
        · simp only [hf, ite_true]
          rw [ih rest has]
          simp [q, f, List.getElem?_cons_succ]
        · have hf' : f a = false := by cases h : f a <;> simp_all
          simp only [hf', Bool.false_eq_true, ite_false]
          rw [ih rest has]
          simp [q, f, List.getElem?_cons_succ]
  dsimp only [p', q, f] at hL0 ⊢
  rw [hL0, filterMap_range_eq_filter_map p' f q (perm_length key rs)]

private theorem sharedDelete_true (key : Key) : sharedDelete key := by
  intro rs ds
  rw [zipIdx_filterMap_ordinal, rs_zipIdx_filter]
  exact ((perm_perm_range key rs).filter (fun j => !ds.contains j)).map (fun j => rs[j]?.getD (0,0))

private theorem rs_zipIdx_filter (rs : List Row) (ds : List Nat) :
    (rs.zipIdx.filter (fun (_,i) => !ds.contains i)).map Prod.fst
      = ((List.range rs.length).filter (fun j => !ds.contains j)).map (fun j => rs[j]?.getD (0,0)) := by
  induction rs with
  | nil => simp
  | cons r rest ih =>
    simp only [List.zipIdx_cons, List.filter_cons, List.map_cons, List.range_succ,
      List.filterMap_cons, List.getElem?_cons_zero, Option.getD_some]
    by_cases hf : !ds.contains 0 = true
    · simp only [hf, ite_true]
      rw [ih]
      apply List.map_congr_left
      intro i hi
      simp [List.getElem?_cons_succ]
    · have hf' : !ds.contains 0 = false := by cases h : !ds.contains 0 <;> simp_all
      simp only [hf', Bool.false_eq_true, ite_false]
      rw [ih]
      apply List.map_congr_left
      intro i hi
      simp [List.getElem?_cons_succ]

private theorem patchByOrdinal_true (key : Key) : patchByOrdinal key := by
  intro rs i v hi
  set ids := perm key rs
  have hpn : ids.length = rs.length := perm_length key rs
  have hmem : i ∈ ids := (perm_mem_iff key rs i).mpr hi
  have hidx : ids.idxOf i < ids.length := List.idxOf_lt_length_of_mem hmem
  have hget : ids[ids.idxOf i]'hidx = i := List.getElem_idxOf hidx
  unfold physical
  apply List.ext_getElem
  · simp [List.length_set, List.length_map, hpn]
  · intro k hk1 hk2
    simp only [List.length_set, List.length_map, hpn] at hk1
    simp only [List.getElem_set, List.getElem_map]
    by_cases hk : k = ids.idxOf i
    · subst k
      simp [hget]
    · simp [hk]

private theorem nat_le_antisymm_list {l1 l2 : List Nat}
    (hp : l1.Perm l2) (h1 : l1.Pairwise (· ≤ ·)) (h2 : l2.Pairwise (· ≤ ·)) : l1 = l2 := by
  induction h1 generalizing l2 with
  | nil => exact (hp.symm.eq_nil).symm
  | @cons a l ha hl ih =>
    cases l2 with
    | nil => exact absurd hp.eq_nil (by simp)
    | cons b l2 =>
      have hbl : b ∈ a :: l := hp.symm.mem_iff.mp List.mem_cons_self
      have hab : a ≤ b := by
        cases hbl with
        | head => exact Nat.le_refl _
        | tail _ hm => exact ha b hm
      have hal2 : a ∈ b :: l2 := hp.mem_iff.mp List.mem_cons_self
      have hba : b ≤ a := by
        cases hal2 with
        | head => exact Nat.le_refl _
        | tail _ hm => exact (List.pairwise_cons.mp h2).1 a hm
      have heq : a = b := Nat.le_antisymm hab hba
      subst b
      exact congrArg (List.cons a) (ih (List.Perm.cons_inv hp) (List.pairwise_cons.mp h2).2)

private theorem stableLimit_true (key : Key) : stableLimit key := by
  intro rs p k
  set n := rs.length
  set ph := physical key rs
  let le : Nat → Nat → Bool := fun a b => decide (a ≤ b)
  set ids := ((ph.zipIdx.filterMap (fun (r,i) => if p r then some (ordinal key rs i) else none)).mergeSort le)
  have hsorted : ids.Pairwise (· ≤ ·) :=
    (List.pairwise_mergeSort
      (by intro a b c h1 h2; simp only [le, decide_eq_true_eq] at h1 h2 ⊢; omega)
      (by intro a b; simp only [le, Bool.or_eq_true, decide_eq_true_eq]; omega) _).imp
      (by intro a b h; simpa [le, decide_eq_true_eq] using h)
  have hrange_sorted : ((List.range n).filter (fun j => p (rs[j]?.getD (0,0)))).Pairwise (· ≤ ·) :=
    (List.pairwise_lt_range.imp (fun h => Nat.le_of_lt h)).filter _
  have hids_perm : ids.Perm ((List.range n).filter (fun j => p (rs[j]?.getD (0,0)))) := by
    have hms : ids.Perm ((ph.zipIdx.filterMap (fun (r,i) => if p r then some (ordinal key rs i) else none))) :=
      List.mergeSort_perm _ _
    refine hms.trans ?_
    unfold physical
    let p' := perm key rs
    let q := fun j => rs[j]?.getD (0,0)
    let pr := fun j => p (q j)
    have hconv : ((p'.map q).zipIdx.filterMap (fun (r, i) => if p r then some (ordinal key rs i) else none))
      = (List.range n).filterMap (fun i => if pr (p'[i]?.getD 0) then some (p'[i]?.getD 0) else none) := by
      induction p' generalizing rs with
      | nil => simp [physical]
      | cons a as ih =>
        cases rs with
        | nil => simp [physical] at *
        | cons r rest =>
          have hn : 1 + as.length = 1 + rest.length := by
            simpa [p', perm_length] using perm_length key (r :: rest)
          have has : as.length = rest.length := by omega
          simp only [List.zipIdx_cons, List.filterMap_cons, List.range_succ,
            List.getElem?_cons_zero, Option.getD_some]
          by_cases hpa : pr a = true
          · simp only [hpa, ite_true, ordinal_eq_getD key (r :: rest) 0 (by decide)]
            rw [ih rest has]
            simp [q, pr, List.getElem?_cons_succ]
          · have hpa' : pr a = false := by cases h : pr a <;> simp_all
            simp only [hpa', Bool.false_eq_true, ite_false]
            rw [ih rest has]
            apply List.filterMap_congr
            intro i hi
            have hi0 : i < rest.length := List.mem_range.mp hi
            simp [q, pr, List.getElem?_cons_succ, ordinal_eq_getD key (r :: rest) (i+1) (by simpa using hi0)]
    rw [hconv]
    have h2 := filterMap_range_eq_filter_map n p' pr id (perm_length key rs)
    simp only [id] at h2
    rw [h2, List.map_id]
    exact (perm_perm_range key rs).filter pr
  have hids_eq : ids = (List.range n).filter (fun j => p (rs[j]?.getD (0,0))) :=
    nat_le_antisymm_list hids_perm hsorted hrange_sorted
  rw [hids_eq]
  conv_rhs => rw [← list_eq_map_range_getD rs (0,0)]
  rw [List.filter_map, List.map_take]
  apply List.map_congr_left
  intro i hi
  simp [Function.comp]

end TinylambOpt.ResearchProofs.StorageExecution.ClusterOrderB

