import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
import TinylambOpt.ResearchProofs.Compression.Wire

namespace TinylambOpt.ResearchProofs.StorageExecution.ClusterOrder

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchProofs.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! ## Common helpers -/

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

private theorem physical_length (key : Key) (rs : List Row) :
    (physical key rs).length = rs.length := by
  unfold physical
  simp [perm_length]

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

private theorem perm_nodup (key : Key) (rs : List Row) : (perm key rs).Nodup :=
  (perm_perm_range key rs).nodup_iff.mpr (List.nodup_range (n := rs.length))

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
  unfold side orderBytes
  simp [pack, little_length, perm_length]

private theorem layoutBytes_eq (key : Key) (rs : List Row) :
    layoutBytes key rs = 1 + orderBytes rs.length + both (physical key rs) := by
  unfold layoutBytes
  rw [side_length]
  try omega

theorem sidecarBudget_true (key : Key) : sidecarBudget key := by
  intro rs
  rw [layoutBytes_eq]
  constructor <;> intro h <;> omega

private theorem list_eq_map_range_getD {α : Type} (l : List α) (d : α) :
    l = (List.range l.length).map (fun i => l[i]?.getD d) := by
  apply List.ext_getElem
  · simp
  · intro i h1 h2
    simp only [List.length_map, List.length_range] at h2
    simp [List.getElem?_eq_getElem h2]

private theorem physical_perm (key : Key) (rs : List Row) : (physical key rs).Perm rs := by
  unfold physical
  have hp : (perm key rs).Perm (List.range rs.length) := perm_perm_range key rs
  have h1 : ((perm key rs).map (fun i => rs[i]?.getD (0,0))).Perm
      ((List.range rs.length).map (fun i => rs[i]?.getD (0,0))) := hp.map _
  have h2 : ((List.range rs.length).map (fun i => rs[i]?.getD (0,0))) = rs := by
    symm
    exact list_eq_map_range_getD rs (0,0)
  simpa [h2] using h1


/-! ## LateGather -/

private theorem pairwise_fst_le_antisymm {l1 l2 : List (Nat × Nat)}
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

theorem lateGather_true (key : Key) : lateGather key := by
  intro rs project
  unfold unravel
  let n := rs.length
  let f : Nat → Nat := fun j => project (rs[j]?.getD ((0,0) : Row))
  let L : List (Nat × Nat) := (List.range n).map (fun i => (ordinal key rs i, ((physical key rs).map project)[i]?.getD (0:Nat)))
  let M : List (Nat × Nat) := (List.range n).map (fun j => (j, f j))
  have hpperm : (perm key rs).Perm (List.range n) := perm_perm_range key rs
  have hpn : (perm key rs).length = n := perm_length key rs
  have hpermLM : L.Perm M := by
    have hL : L = (perm key rs).map (fun j => (j, project (rs[j]?.getD (0,0)))) := by
      apply List.ext_getElem
      · simp [L, hpn]
      · intro i h1 h2
        simp only [L, List.length_map, List.length_range] at h1 h2 ⊢
        have hi : i < n := by simpa using h1
        have hip : i < (perm key rs).length := by rwa [hpn]
        simp only [List.getElem_map, List.getElem_range]
        refine Prod.ext ?_ ?_
        · exact ordinal_eq_getElem key rs i hi
        · simp only [physical, List.getElem?_map, List.getElem?_eq_getElem hip, Option.map_some,
            Option.getD_some]
    rw [hL]
    exact hpperm.map (fun j => (j, project (rs[j]?.getD (0,0))))
  have hpairM : M.Pairwise (fun a b => a.1 ≤ b.1) := range_tagged_pairwise f
  have hfstM : ∀ x ∈ M, ∀ y ∈ M, x.1 = y.1 → x = y := range_tagged_fst_injective f
  let le : (Nat × Nat) → (Nat × Nat) → Bool := fun a b => decide (a.1 ≤ b.1)
  have htrans : ∀ (a b c : Nat × Nat), le a b = true → le b c = true → le a c = true := by
    intro a b c hab hbc; simp only [le, decide_eq_true_eq] at hab hbc ⊢; omega
  have htotal : ∀ (a b : Nat × Nat), (le a b || le b a) = true := by
    intro a b; simp only [le, Bool.or_eq_true, decide_eq_true_eq]; omega
  have hsorted : (L.mergeSort le).Pairwise (fun a b => le a b = true) := List.pairwise_mergeSort htrans htotal L
  have hpermSort : (L.mergeSort le).Perm L := List.mergeSort_perm L le
  have hpermSortM : (L.mergeSort le).Perm M := hpermSort.trans hpermLM
  have hsorted' : (L.mergeSort le).Pairwise (fun a b => a.1 ≤ b.1) := hsorted.imp (by intro a b h; simpa [le, decide_eq_true_eq] using h)
  have hfstSort : ∀ x ∈ L.mergeSort le, ∀ y ∈ L.mergeSort le, x.1 = y.1 → x = y := by
    intro x hx y hy hxy
    exact hfstM x (hpermSortM.mem_iff.mp hx) y (hpermSortM.mem_iff.mp hy) hxy
  have heq : L.mergeSort le = M := pairwise_fst_le_antisymm hpermSortM hsorted' hpairM hfstSort
  rw [show (L.mergeSort (fun a b => decide (a.1 ≤ b.1))) = L.mergeSort le from rfl, heq]
  simp only [M, f, List.map_map, Function.comp]
  have hrs : rs = (List.range n).map (fun i => rs[i]?.getD (0,0)) := list_eq_map_range_getD rs (0,0)
  rw [hrs]
  simp [List.map_map, Function.comp]


/-! ## PayloadAlignment -/

theorem payloadAlignment_true (key : Key) : payloadAlignment key := by
  intro rs payload hlen
  have hp : (perm key rs).Perm (List.range rs.length) := perm_perm_range key rs
  have hL : (physical key rs).zip ((perm key rs).map (fun i => payload[i]?.getD 0)) =
      (perm key rs).map (fun i => (rs[i]?.getD (0,0), payload[i]?.getD 0)) := by
    unfold physical
    simp only [List.zip_map, List.map_map, Function.comp]
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


/-! ## SharedDelete -/

private theorem filterMap_range_eq_filter_map (n : Nat) (p : List Nat) (f : Nat → Bool) (g : Nat → Row)
    (hpn : p.length = n) :
    (List.range n).filterMap (fun i => if f (p[i]?.getD 0) then some (g (p[i]?.getD 0)) else none)
    = (p.filter f).map g := by
  subst hpn
  induction p with
  | nil => simp
  | cons x xs ih =>
    by_cases hf : f x = true
    · simp only [List.range_succ_eq_map, List.filterMap_cons, List.getElem?_cons_zero, Option.getD_some,
        List.filterMap_map, Function.comp, List.getElem?_cons_succ, List.filter_cons, List.map_cons, hf,
        ih, ite_true]
    · simp only [List.range_succ_eq_map, List.filterMap_cons, List.getElem?_cons_zero, Option.getD_some,
        List.filterMap_map, Function.comp, List.getElem?_cons_succ, List.filter_cons, List.map_cons, hf,
        ih, Bool.false_eq_true, ite_false]

private theorem zipIdx_filter_map_fst {α β : Type} (l : List α) (q : Nat → Bool) (g : Nat → β) :
    ((l.zipIdx.filter (fun x => q x.2)).map (fun x => g x.2)) =
    (List.range l.length).filterMap (fun i => if q i then some (g i) else none) := by
  induction l generalizing q g with
  | nil => simp
  | cons a l ih =>
    simp only [List.zipIdx_cons, List.filter_cons, List.map_cons, List.length_cons,
      List.range_succ_eq_map, List.filterMap_cons, Nat.zero_add]
    by_cases hq : q 0 = true
    · simp only [hq, ite_true, List.filterMap_map, Function.comp, Nat.succ_eq_add_one]
      congr 1
      have ih' := ih (fun i => q (i+1)) (fun i => g (i+1))
      simp only at ih'
      convert ih' using 2 <;> simp [Nat.add_comm]
    · have hq' : q 0 = false := by cases h : q 0 <;> simp_all
      simp only [hq', Bool.false_eq_true, ite_false, List.filterMap_map, Function.comp,
        Nat.succ_eq_add_one]
      have ih' := ih (fun i => q (i+1)) (fun i => g (i+1))
      convert ih' using 2 <;> simp [Nat.add_comm]

theorem sharedDelete_true (key : Key) : sharedDelete key := by
  intro rs ds
  let n := rs.length
  let p := perm key rs
  have hp : p.Perm (List.range n) := perm_perm_range key rs
  have hpn : p.length = n := perm_length key rs
  let pred : Nat → Bool := fun j => !ds.contains j
  let f : Nat → Row := fun j => rs[j]?.getD (0,0)
  -- RHS = (range n | filter pred).map f
  have hR : ((rs.zipIdx.filter (fun x => pred x.2)).map Prod.fst)
      = ((List.range n).filter pred).map f := by
    apply List.ext_getElem
    · simp only [List.length_map, List.length_filter, List.length_zipIdx, List.length_range]
      rw [← List.countP_eq_length_filter, ← List.countP_eq_length_filter]
      simp [List.countP_eq_length_filter, Function.comp]
    · intro k hk1 hk2
      simp only [List.getElem_map, List.getElem_filter, List.getElem_zipIdx, List.getElem_range]
  -- LHS = (p | filter pred).map f
  have hL : ((physical key rs).zipIdx.filter (fun x => pred (ordinal key rs x.2))).map Prod.fst
      = (p.filter pred).map f := by
    unfold physical
    -- convert to filterMap over range n
    have hfm : ((List.map (fun j => rs[j]?.getD (0,0)) p).zipIdx.filter (fun x => pred (ordinal key rs x.2))).map Prod.fst
        = (List.range n).filterMap (fun i => if pred (p[i]?.getD 0) then some (f (p[i]?.getD 0)) else none) := by
      apply List.ext_getElem
      · simp only [List.length_map, List.length_filter, List.length_zipIdx, List.length_filterMap, List.length_range, hpn]
        rw [← List.countP_eq_length_filter, List.length_filterMap_eq_countP]
        simp only [List.countP_map, Function.comp, List.getElem_zipIdx]
        apply List.countP_congr
        intro i hi
        simp only [List.mem_range] at hi
        simp [ordinal_eq_getD key rs i hi, hpn, hi]
      · intro k hk1 hk2
        simp only [List.getElem_map, List.getElem_filter]
        have hk1' : k < (List.filter (fun x => pred (ordinal key rs x.2)) ((p.map f).zipIdx)).length := by
          simpa [List.length_map] using hk1
        simp only [List.getElem_filter, List.getElem_zipIdx, List.getElem_map]
        -- rhs
        have hrhs : (List.range n).filterMap (fun i => if pred (p[i]?.getD 0) then some (f (p[i]?.getD 0)) else none)
            = ((List.range n).filter (fun i => pred (p[i]?.getD 0))).map (fun i => f (p[i]?.getD 0)) := by
          rw [List.filterMap_eq_map (fun a h => by by_cases hp : pred (p[a]?.getD 0) = true <;> simp [hp])]
          rfl
        rw [hrhs] at hk2
        simp only [List.getElem_map, List.getElem_filter, List.getElem_range] at hk2 ⊢
        -- index correspondence
        have hlen_eq : ((List.range n).filter (fun i => pred (ordinal key rs i))).length =
            ((List.range n).filter (fun i => pred (p[i]?.getD 0))).length := by
          rw [← List.countP_eq_length_filter, ← List.countP_eq_length_filter]
          apply List.countP_congr
          intro i hi
          simp [ordinal_eq_getD key rs i (List.mem_range.mp hi)]
        have hkA : k < ((List.range n).filter (fun i => pred (ordinal key rs i))).length := by
          have : k < (List.filter (fun x => pred (ordinal key rs x.2)) ((p.map f).zipIdx)).length := hk1'
          simpa [List.length_filter, List.length_zipIdx, List.length_map, hpn, ← List.countP_eq_length_filter] using this
        have hkB : k < ((List.range n).filter (fun i => pred (p[i]?.getD 0))).length := by omega
        have hidx : ((List.range n).filter (fun i => pred (ordinal key rs i)))[k]'hkA =
            ((List.range n).filter (fun i => pred (p[i]?.getD 0)))[k]'hkB := by
          have aux : ∀ m (hm : m ≤ n) k
              (hkA : k < ((List.range m).filter (fun i => pred (ordinal key rs i))).length)
              (hkB : k < ((List.range m).filter (fun i => pred (p[i]?.getD 0))).length),
              ((List.range m).filter (fun i => pred (ordinal key rs i)))[k]'hkA =
              ((List.range m).filter (fun i => pred (p[i]?.getD 0)))[k]'hkB := by
            intro m hm k hkA hkB
            induction m generalizing k with
            | zero => simp at hkA
            | succ m ih =>
              have hpe : pred (ordinal key rs m) = pred (p[m]?.getD 0) := by
                by_cases hmn : m < n <;> simp [ordinal_eq_getD key rs m, hmn]
              simp only [List.range_succ, List.filter_append, List.filter_singleton]
              by_cases hpm : pred (ordinal key rs m) = true
              · have hpm2 : pred (p[m]?.getD 0) = true := by rwa [← hpe]
                simp only [hpm, hpm2, ↓reduceIte, List.length_append, List.length_singleton]
                have hleq : ((List.range m).filter (fun i => pred (ordinal key rs i))).length =
                    ((List.range m).filter (fun i => pred (p[i]?.getD 0))).length := by
                  rw [← List.countP_eq_length_filter, ← List.countP_eq_length_filter]
                  exact List.countP_congr (fun i hi => by simp [ordinal_eq_getD key rs i (by have := List.mem_range.mp hi; omega)])
                by_cases hklt : k < ((List.range m).filter (fun i => pred (ordinal key rs i))).length
                · rw [List.getElem_append_left hklt, List.getElem_append_left (by omega)]
                  exact ih (by omega) k hklt (by omega)
                · have hkA' : k = ((List.range m).filter (fun i => pred (ordinal key rs i))).length := by
                    simp only [List.length_append, List.length_singleton] at hkA; omega
                  have hkB' : k = ((List.range m).filter (fun i => pred (p[i]?.getD 0))).length := by omega
                  simp [hkA', hkB']
              · have hpm' : pred (ordinal key rs m) = false := by cases h : pred (ordinal key rs m) <;> simp_all
                have hpm2' : pred (p[m]?.getD 0) = false := by rwa [← hpe]
                simp only [hpm', hpm2', Bool.false_eq_true, ↓reduceIte, List.append_nil]
                exact ih (by omega) k hkA hkB
          exact aux n le_rfl k hkA hkB
        -- map element equality
        simp only [List.getElem_filter, List.getElem_range] at hidx hk2 ⊢
        simp [hidx]
    rw [hfm]
    exact filterMap_range_eq_filter_map n p pred f hpn
  rw [hL, hR]
  exact (hp.filter pred).map f

/-! ## PatchByOrdinal -/

theorem patchByOrdinal_true (key : Key) : patchByOrdinal key := by
  intro rs i v hi
  let ids := perm key rs
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
    · subst hk
      simp only [List.getElem_set, List.getElem_map, ite_true]
      simp [hget]
    · simp only [List.getElem_set, hk, ite_false, List.getElem_map]
      have hkne : ids[k]'(by omega) ≠ i := by
        intro heq
        apply hk
        have hnodup := perm_nodup key rs
        have hk' : k < ids.length := by omega
        have hi' : ids.idxOf i < ids.length := by omega
        exact (hnodup.getElem_inj_iff hk' hi').mp (by rw [hget]; exact heq)
        exact this.mp heq
      simp [List.getElem?_eq_getElem (by omega : ids[k] < (rs.set i v).length),
            List.getElem?_eq_getElem (by omega : ids[k] < rs.length),
            List.getElem_set_ne hkne]

/-! ## StableLimit -/

private theorem nat_le_antisymm_list {l1 l2 : List Nat}
    (hp : l1.Perm l2) (h1 : l1.Pairwise (· ≤ ·)) (h2 : l2.Pairwise (· ≤ ·)) : l1 = l2 := by
  induction h1 generalizing l2 with
  | nil => exact hp.symm.eq_nil.symm
  | @cons a l ha hl ih =>
    cases l2 with
    | nil => exact absurd hp.eq_nil (by simp)
    | cons b l2 =>
      have hbl : b ∈ a :: l := hp.symm.mem_iff.mp List.mem_cons_self
      have hab : a ≤ b := by cases hbl with | head => exact Nat.le_refl _ | tail _ hm => exact ha b hm
      have hal2 : a ∈ b :: l2 := hp.mem_iff.mp List.mem_cons_self
      cases h2 with
      | cons hba_all htl =>
        have hba : b ≤ a := by cases hal2 with | head => exact Nat.le_refl _ | tail _ hm => exact hba_all a hm
        have hab' := Nat.le_antisymm hab hba
        subst hab'
        exact congrArg (List.cons a) (ih (List.Perm.cons_inv hp) htl)

theorem stableLimit_true (key : Key) : stableLimit key := by
  intro rs p k
  let n := rs.length
  let ph := physical key rs
  let le : Nat → Nat → Bool := fun a b => decide (a ≤ b)
  let ids := ((ph.zipIdx.filterMap (fun (r,i) => if p r then some (ordinal key rs i) else none)).mergeSort le)
  have hsorted : ids.Pairwise (· ≤ ·) := by
    have hpair : ids.Pairwise (fun a b => le a b = true) :=
      List.pairwise_mergeSort (by intro a b c h1 h2; simp only [le, decide_eq_true_eq] at h1 h2 ⊢; omega)
        (by intro a b; simp only [le, Bool.or_eq_true, decide_eq_true_eq]; omega) _
    exact hpair.imp (by intro a b h; simpa [le, decide_eq_true_eq] using h)
  have hrange_sorted : ((List.range n).filter (fun j => p (rs[j]?.getD (0,0)))).Pairwise (· ≤ ·) :=
    (List.pairwise_lt_range.imp (fun h => Nat.le_of_lt h)).filter _
  have hids_perm : ids.Perm ((List.range n).filter (fun j => p (rs[j]?.getD (0,0)))) := by
    have hms : ids.Perm ((ph.zipIdx.filterMap (fun (r,i) => if p r then some (ordinal key rs i) else none))) :=
      List.mergeSort_perm _ _
    refine hms.trans ?_
    have hpperm : (perm key rs).Perm (List.range n) := perm_perm_range key rs
    have hpn : (perm key rs).length = n := perm_length key rs
    have hconv : ((List.map (fun j => rs[j]?.getD (0, 0)) (perm key rs)).zipIdx.filterMap
        (fun (r, i) => if p r then some (ordinal key rs i) else none))
      = (List.range n).filterMap (fun i =>
          if p (rs[(perm key rs)[i]?.getD 0]?.getD (0,0)) then some ((perm key rs)[i]?.getD 0) else none) := by
      apply List.ext_getElem
      · simp only [List.length_filterMap_eq_countP, List.length_zipIdx, List.length_map, List.length_range, hpn]
        simp only [List.countP_map, Function.comp, List.getElem_zipIdx]
        congr 1
        ext i
        simp only [List.mem_range]
        constructor
        · intro hi
          simp [ordinal_eq_getD key rs i hi, hi]
        · intro hi
          simp [hi]
      · intro i h1 h2
        simp only [List.length_filterMap_eq_countP] at h1 h2
        rw [List.getElem_filterMap, List.getElem_filterMap]
        simp only [List.getElem_zipIdx, List.getElem_map, List.getElem_range]
        have hi : i < n := by simpa [List.length_filterMap_eq_countP, List.length_range] using h2
        simp [ordinal_eq_getD key rs i hi]
    show (ph.zipIdx.filterMap (fun (r,i) => if p r then some (ordinal key rs i) else none)).Perm _
    simp only [ph, physical]
    rw [hconv]
    have h2 : (List.range n).filterMap (fun i =>
        if p (rs[(perm key rs)[i]?.getD 0]?.getD (0,0)) then some ((perm key rs)[i]?.getD 0) else none)
      = ((perm key rs).filter (fun j => p (rs[j]?.getD (0,0)))).map id := by
      rw [List.map_id]
      have h3 := filterMap_range_eq_filter_map n (perm key rs) (fun j => p (rs[j]?.getD (0,0))) (fun j => j) hpn
      simp at h3
      exact h3
    rw [h2, List.map_id]
    exact hpperm.filter (fun j => p (rs[j]?.getD (0,0)))
  have hids_eq : ids = (List.range n).filter (fun j => p (rs[j]?.getD (0,0))) :=
    nat_le_antisymm_list hids_perm hsorted hrange_sorted
  rw [hids_eq]
  have hR : (rs.filter p) = ((List.range n).filter (fun j => p (rs[j]?.getD (0,0)))).map (fun j => rs[j]?.getD (0,0)) := by
    have hrw := list_eq_map_range_getD rs (0,0)
    rw [hrw, List.filter_map]
    simp [Function.comp]
  rw [hR, List.map_take]

/-! ## MergeShards -/

theorem mergeShards_true (key : Key) : mergeShards key := by
  intro a b
  unfold mergeShards
  let cmp := fun x y : Row => let ka := key x; let kb := key y; decide (ka.1 < kb.1 || (ka.1 == kb.1 && ka.2 ≤ kb.2))
  have htrans : ∀ x y z, cmp x y = true → cmp y z = true → cmp x z = true := by
    intro x y z h1 h2; simp only [cmp, decide_eq_true_eq] at h1 h2 ⊢; omega
  have htotal : ∀ x y, (cmp x y || cmp y x) = true := by
    intro x y; simp only [cmp, Bool.or_eq_true, decide_eq_true_eq]; omega
  have hpermR : (physical key (a ++ b)).Perm (a ++ b) := physical_perm key (a ++ b)
  have hpermL : (List.merge (physical key a) (physical key b) cmp).Perm (a ++ b) :=
    (List.merge_perm_append (physical key a) (physical key b) cmp).trans ((physical_perm key a).append (physical_perm key b))
  -- Prove by strong uniqueness: both Perm of a++b and both sorted by same stable order.
  -- Use a direct index-chasing ext proof via a canonical ordering.
  -- Key observation: merge is stable and mergeSort is stable.
  -- We show equality by proving every element is at the same position.
  have hlen : (List.merge (physical key a) (physical key b) cmp).length = (physical key (a ++ b)).length :=
    hpermL.length_eq.trans hpermR.symm.length_eq
  -- Prove via list equality by showing same Perm and both are "stable sorted".
  -- We use the fact that for the same multiset, the stable sort by (key1,key2) is unique.
  -- Implement stable uniqueness: if two lists are Perm of same base and stable w.r.t. base order for equal keys, equal.
  suffices heq : (List.merge (physical key a) (physical key b) cmp) = (physical key (a ++ b)) by exact heq
  -- Use a general stable-sort uniqueness lemma.
  -- l1 = l2 if: l1.Perm l2, both Pairwise by cmp, and for equal-cmp elements, same relative order.
  have stable_unique : ∀ (l1 l2 base : List Row),
      l1.Perm base → l2.Perm base →
      l1.Pairwise (fun x y => cmp x y = true) → l2.Pairwise (fun x y => cmp x y = true) →
      (∀ x y, x ∈ base → y ∈ base → cmp x y = true → cmp y x = true →
        (List.idxOf l1 x < List.idxOf l1 y ↔ List.idxOf l2 x < List.idxOf l2 y)) →
      l1 = l2 := by
    intro l1 l2 base hb1 hb2 hp1 hp2 hstab
    have hp12 : l1.Perm l2 := hb1.trans hb2.symm
    induction hp1 generalizing l2 base with
    | nil => exact hp12.symm.eq_nil
    | @cons x xs hx hxs ih =>
      cases l2 with
      | nil => exact absurd hp12.eq_nil (by simp)
      | cons y ys =>
        by_cases hxy : x = y
        · subst hxy
          congr 1
          apply ih (base := base)
          · exact List.Perm.cons_inv hb1
          · exact List.Perm.cons_inv hb2
          · exact List.Perm.cons_inv hp12
          · exact hp2.tail
          · intro u v hu hv hcu hcv
            have hux : u ≠ x := fun h => by subst h; exact absurd hu (by
              have := (List.Perm.cons_inv hb1); exact (this.mem_iff.not.mpr (by simp [List.mem_cons_self])))
            have hvx : v ≠ x := fun h => by subst h; exact absurd hv (by
              have := (List.Perm.cons_inv hb1); exact (this.mem_iff.not.mpr (by simp [List.mem_cons_self])))
            have hux2 : u ∈ (x::xs) := List.mem_cons_of_mem x hu
            have hvx2 : v ∈ (x::xs) := List.mem_cons_of_mem x hv
            have := hstab u v hu hv hcu hcv
            simp [List.idxOf_cons, bne_iff_ne, hux, hvx] at this
            exact by constructor <;> intro h <;> omega
        · exfalso
          have hxy1 : cmp x y = true := by
            have : y ∈ x :: xs := hp12.symm.mem_iff.mp List.mem_cons_self
            cases this with | head => exact absurd rfl hxy | tail _ hm => exact hx y hm
          cases hp2 with
          | cons hyx_all htl2 =>
          have hyx1 : cmp y x = true := by
            have : x ∈ y :: ys := hp12.mem_iff.mp List.mem_cons_self
            cases this with | head => exact absurd rfl hxy | tail _ hm => exact hyx_all x hm
          have hxb : x ∈ base := hb1.mem_iff.mp List.mem_cons_self
          have hyb : y ∈ base := hb2.mem_iff.mp List.mem_cons_self
          have h1 := hstab x y hxb hyb hxy1 hyx1
          have h2 := hstab y x hyb hxb hyx1 hxy1
          simp [List.idxOf_cons, hxy, Ne.symm hxy] at h1 h2
          have hy_in_xs : y ∈ xs := by
            have : y ∈ x :: xs := hp12.symm.mem_iff.mp List.mem_cons_self
            cases this with | head => contradiction | tail _ hm => exact hm
          have hx_in_ys : x ∈ ys := by
            have : x ∈ y :: ys := hp12.mem_iff.mp List.mem_cons_self
            cases this with | head => contradiction | tail _ hm => exact hm
          have hiy : 0 < List.idxOf xs y + 1 := Nat.succ_pos _
          have hix : 0 < List.idxOf ys x + 1 := Nat.succ_pos _
          have hlt1 : List.idxOf (x::xs) x < List.idxOf (x::xs) y := by simp [List.idxOf_cons, hxy]; omega
          have hlt2 := h1.mp hlt1
          simp [List.idxOf_cons, Ne.symm hxy] at hlt2
          omega
  apply stable_unique _ _ (a++b) hpermL hpermR
  · -- LHS pairwise by cmp
    apply List.pairwise_merge htrans htotal
    · unfold physical perm
      let cmp2 := fun (x : Row × Nat) (y : Row × Nat) => let ka := key x.1; let kb := key y.1; ka.1 < kb.1 || (ka.1 == kb.1 && (ka.2 < kb.2 || (ka.2 == kb.2 && x.2 ≤ y.2)))
      have hsort := List.pairwise_mergeSort (by intro x y z h1 h2; simp only [cmp2, Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h1 h2 ⊢; omega) (by intro x y; simp only [cmp2, Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq]; omega) a.zipIdx
      rw [List.pairwise_map]; exact hsort.imp (by intro x y h; simp only [cmp, cmp2, Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h ⊢; omega)
    · unfold physical perm
      let cmp2 := fun (x : Row × Nat) (y : Row × Nat) => let ka := key x.1; let kb := key y.1; ka.1 < kb.1 || (ka.1 == kb.1 && (ka.2 < kb.2 || (ka.2 == kb.2 && x.2 ≤ y.2)))
      have hsort := List.pairwise_mergeSort (by intro x y z h1 h2; simp only [cmp2, Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h1 h2 ⊢; omega) (by intro x y; simp only [cmp2, Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq]; omega) b.zipIdx
      rw [List.pairwise_map]; exact hsort.imp (by intro x y h; simp only [cmp, cmp2, Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h ⊢; omega)
  · -- RHS pairwise by cmp
    unfold physical perm
    let cmp2 := fun (x : Row × Nat) (y : Row × Nat) => let ka := key x.1; let kb := key y.1; ka.1 < kb.1 || (ka.1 == kb.1 && (ka.2 < kb.2 || (ka.2 == kb.2 && x.2 ≤ y.2)))
    have hsort := List.pairwise_mergeSort (by intro x y z h1 h2; simp only [cmp2, Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h1 h2 ⊢; omega) (by intro x y; simp only [cmp2, Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq]; omega) (a++b).zipIdx
    rw [List.pairwise_map]; exact hsort.imp (by intro x y h; simp only [cmp, cmp2, Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h ⊢; omega)
  · -- stability
    intro x y hx hy hcxy hcyx
    -- For equal-cmp elements, relative order in both is determined by original position in a++b.
    -- Both merge and mergeSort are stable w.r.t. this.
    constructor
    · intro hlt
      by_contra hge
      simp only [Nat.not_lt, Nat.not_le, not_not] at hge
      cases Nat.eq_or_lt_of_not_lt (by omega : ¬ List.idxOf (physical key (a ++ b)) x < List.idxOf (physical key (a ++ b)) y) with
      | inl heq
      · -- same index in RHS means same element
        have hxR := List.getElem_idxOf (List.idxOf_lt_length_of_mem (hpermR.mem_iff.mp hx))
        have hyR := List.getElem_idxOf (List.idxOf_lt_length_of_mem (hpermR.mem_iff.mp hy))
        rw [heq] at hxR
        have : x = y := hxR.trans hyR.symm
        subst this; exact absurd hlt (by simp)
      · -- y before x in RHS, x before y in LHS, equal cmp - contradicts stability
        -- Stable ordering: for equal (key1,key2), order by position in a++b is preserved.
        -- In a++b, position of x < position of y or vice versa. Both sorts preserve this.
        have hstabR := fun (u v : Row) (hu : u ∈ a++b) (hv : v ∈ a++b) (hcu : cmp u v = true) (hcv : cmp v u = true) =>
          show List.idxOf (a++b) u < List.idxOf (a++b) v → List.idxOf (physical key (a++b)) u < List.idxOf (physical key (a++b)) v from by
            intro huv
            by_contra hg
            simp only [Nat.not_lt, Nat.not_le, not_not] at hg
            rcases Nat.eq_or_lt_of_not_lt (by omega : ¬ List.idxOf (physical key (a++b)) u < List.idxOf (physical key (a++b)) v) with he | hg2
            · have h1 := List.getElem_idxOf (List.idxOf_lt_length_of_mem (hpermR.mem_iff.mp hu))
              have h2 := List.getElem_idxOf (List.idxOf_lt_length_of_mem (hpermR.mem_iff.mp hv))
              rw [he] at h1; have : u = v := h1.trans h2.symm
              subst this; exact absurd huv (by simp)
            · -- v before u in physical, u before v in a++b, equal cmp - contradicts mergeSort stability
              -- mergeSort of zipIdx with rowCmp is stable: equal key1,key2 => use original index
              unfold physical perm at hg2
              simp only [List.idxOf_map] at hg2
              -- This follows from mergeSort stability. Use pairwise.
              have hpair2 : ((a++b).zipIdx.mergeSort (fun (x y : Row × Nat) => let ka := key x.1; let kb := key y.1; ka.1 < kb.1 || (ka.1 == kb.1 && (ka.2 < kb.2 || (ka.2 == kb.2 && x.2 ≤ y.2)))).Pairwise
                  (fun x y => (let ka := key x.1; let kb := key y.1; ka.1 < kb.1 || (ka.1 == kb.1 && (ka.2 < kb.2 || (ka.2 == kb.2 && x.2 ≤ y.2))) = true) :=
                List.pairwise_mergeSort (by intro x y z h1 h2; simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h1 h2 ⊢; omega) (by intro x y; simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq]; omega) _
              -- v before u in mapped list means v's pair before u's pair in sorted zipIdx
              -- and equal key => pair comparator gives v.2 ≤ u.2, i.e. v before u in a++b
              -- contradicting u before v in a++b
              omega
        have hstabL := fun (u v : Row) (hu : u ∈ a++b) (hv : v ∈ a++b) (hcu : cmp u v = true) (hcv : cmp v u = true) =>
          show List.idxOf (a++b) u < List.idxOf (a++b) v → List.idxOf (List.merge (physical key a) (physical key b) cmp) u < List.idxOf (List.merge (physical key a) (physical key b) cmp) v from by
            intro huv
            by_contra hg
            simp only [Nat.not_lt, Nat.not_le, not_not] at hg
            rcases Nat.eq_or_lt_of_not_lt (by omega : ¬ List.idxOf (List.merge (physical key a) (physical key b) cmp) u < List.idxOf (List.merge (physical key a) (physical key b) cmp) v) with he | hg2
            · have h1 := List.getElem_idxOf (List.idxOf_lt_length_of_mem (hpermL.mem_iff.mp hu))
              have h2 := List.getElem_idxOf (List.idxOf_lt_length_of_mem (hpermL.mem_iff.mp hv))
              rw [he] at h1; have : u = v := h1.trans h2.symm
              subst this; exact absurd huv (by simp)
            · -- v before u in merge, u before v in a++b, equal cmp
              -- merge is stable: for equal cmp, takes left (a) before right (b)
              -- within same list, preserves order. So stable w.r.t. a++b.
              omega
        -- derive contradiction
        rcases Nat.lt_trichotomy (List.idxOf (a++b) x) (List.idxOf (a++b) y) with hxyab | hxyab | hxyab
        · exact absurd (hstabR x y hx hy hcxy hcyx hxyab) (by omega)
        · have : x = y := by
            have h1 := List.getElem_idxOf (List.idxOf_lt_length_of_mem hx)
            have h2 := List.getElem_idxOf (List.idxOf_lt_length_of_mem hy)
            rw [hxyab] at h1; exact h1.trans h2.symm
          subst this; exact absurd hlt (by simp)
        · exact absurd (hstabL y x hy hx hcyx hcxy hxyab) (by omega)
    · intro hlt
      by_contra hge
      simp only [Nat.not_lt, Nat.not_le, not_not] at hge
      rcases Nat.eq_or_lt_of_not_lt (by omega : ¬ List.idxOf (List.merge (physical key a) (physical key b) cmp) x < List.idxOf (List.merge (physical key a) (physical key b) cmp) y) with heq | hgt
      · have hxL := List.getElem_idxOf (List.idxOf_lt_length_of_mem (hpermL.mem_iff.mp hx))
        have hyL := List.getElem_idxOf (List.idxOf_lt_length_of_mem (hpermL.mem_iff.mp hy))
        rw [heq] at hxL
        have : x = y := hxL.trans hyL.symm
        subst this; exact absurd hlt (by simp)
      · have hstabR := fun (u v : Row) (hu : u ∈ a++b) (hv : v ∈ a++b) (hcu : cmp u v = true) (hcv : cmp v u = true) =>
          show List.idxOf (a++b) u < List.idxOf (a++b) v → List.idxOf (physical key (a++b)) u < List.idxOf (physical key (a++b)) v from by
            intro huv
            by_contra hg
            simp only [Nat.not_lt, Nat.not_le, not_not] at hg
            rcases Nat.eq_or_lt_of_not_lt (by omega : ¬ List.idxOf (physical key (a++b)) u < List.idxOf (physical key (a++b)) v) with he | hg2
            · have h1 := List.getElem_idxOf (List.idxOf_lt_length_of_mem (hpermR.mem_iff.mp hu))
              have h2 := List.getElem_idxOf (List.idxOf_lt_length_of_mem (hpermR.mem_iff.mp hv))
              rw [he] at h1; have : u = v := h1.trans h2.symm
              subst this; exact absurd huv (by simp)
            · omega
        have hstabL := fun (u v : Row) (hu : u ∈ a++b) (hv : v ∈ a++b) (hcu : cmp u v = true) (hcv : cmp v u = true) =>
          show List.idxOf (a++b) u < List.idxOf (a++b) v → List.idxOf (List.merge (physical key a) (physical key b) cmp) u < List.idxOf (List.merge (physical key a) (physical key b) cmp) v from by
            intro huv
            by_contra hg
            simp only [Nat.not_lt, Nat.not_le, not_not] at hg
            rcases Nat.eq_or_lt_of_not_lt (by omega : ¬ List.idxOf (List.merge (physical key a) (physical key b) cmp) u < List.idxOf (List.merge (physical key a) (physical key b) cmp) v) with he | hg2
            · have h1 := List.getElem_idxOf (List.idxOf_lt_length_of_mem (hpermL.mem_iff.mp hu))
              have h2 := List.getElem_idxOf (List.idxOf_lt_length_of_mem (hpermL.mem_iff.mp hv))
              rw [he] at h1; have : u = v := h1.trans h2.symm
              subst this; exact absurd huv (by simp)
            · omega
        rcases Nat.lt_trichotomy (List.idxOf (a++b) x) (List.idxOf (a++b) y) with hxyab | hxyab | hxyab
        · exact absurd (hstabL x y hx hy hcxy hcyx hxyab) (by omega)
        · have : x = y := by
            have h1 := List.getElem_idxOf (List.idxOf_lt_length_of_mem hx)
            have h2 := List.getElem_idxOf (List.idxOf_lt_length_of_mem hy)
            rw [hxyab] at h1; exact h1.trans h2.symm
          subst this; exact absurd hlt (by simp)
        · exact absurd (hstabR y x hy hx hcyx hcxy hxyab) (by omega)


/-! ## TwoColumnWin refutations -/

theorem refutation_Lexicographic_TwoColumnWin : ¬ Lexicographic.TwoColumnWin.Goal := by
  intro h
  have := h [(0,0),(1,1)] (by decide)
  revert this; native_decide

theorem refutation_AttributeFirst_TwoColumnWin : ¬ AttributeFirst.TwoColumnWin.Goal := by
  intro h
  have := h [(0,0),(1,1)] (by decide)
  revert this; native_decide

theorem refutation_Interleaved_TwoColumnWin : ¬ Interleaved.TwoColumnWin.Goal := by
  intro h
  have := h [(0,0),(1,1)] (by decide)
  revert this; native_decide

theorem refutation_GrayPair_TwoColumnWin : ¬ GrayPair.TwoColumnWin.Goal := by
  intro h
  have := h [(0,0),(1,1)] (by decide)
  revert this; native_decide

theorem refutation_Diagonal_TwoColumnWin : ¬ Diagonal.TwoColumnWin.Goal := by
  intro h
  have := h [(0,0),(1,1)] (by decide)
  revert this; native_decide

theorem refutation_UnorderedPair_TwoColumnWin : ¬ UnorderedPair.TwoColumnWin.Goal := by
  intro h
  have := h [(0,0),(1,1)] (by decide)
  revert this; native_decide

theorem refutation_RatioClass_TwoColumnWin : ¬ RatioClass.TwoColumnWin.Goal := by
  intro h
  have := h [(0,0),(1,1)] (by decide)
  revert this; native_decide

theorem refutation_PowerBucket_TwoColumnWin : ¬ PowerBucket.TwoColumnWin.Goal := by
  intro h
  have := h [(0,0),(1,1)] (by decide)
  revert this; native_decide

theorem refutation_DyadicTile_TwoColumnWin : ¬ DyadicTile.TwoColumnWin.Goal := by
  intro h
  have := h [(0,0),(1,1)] (by decide)
  revert this; native_decide

theorem refutation_XorClass_TwoColumnWin : ¬ XorClass.TwoColumnWin.Goal := by
  intro h
  have := h [(0,0),(1,1)] (by decide)
  revert this; native_decide


/-! ## Easy model-specific goals -/

theorem sound_UnorderedPair_OrientationTies : UnorderedPair.OrientationTies.Goal := by
  intro x y
  simp [UnorderedPair.key, Nat.min_comm, Nat.max_comm]

theorem sound_RatioClass_EquivalentDenominators : RatioClass.EquivalentDenominators.Goal := by
  intro x y z hxy hxz
  have hy : x / (y + 1) = 0 := Nat.div_eq_of_lt (by omega)
  have hz : x / (z + 1) = 0 := Nat.div_eq_of_lt (by omega)
  have hy' : x % (y + 1) = x := Nat.mod_eq_of_lt (by omega)
  have hz' : x % (z + 1) = x := Nat.mod_eq_of_lt (by omega)
  simp [RatioClass.key, hy, hz, hy', hz']

theorem sound_RatioClass_Reconstruction : RatioClass.Reconstruction.Goal := by
  intro x y
  simp only [RatioClass.key]
  have h := Nat.div_add_mod x (y + 1)
  -- h : (y+1) * (x/(y+1)) + x%(y+1) = x
  calc x / (y + 1) * (y + 1) + x % (y + 1)
      = (y + 1) * (x / (y + 1)) + x % (y + 1) := by rw [Nat.mul_comm]
    _ = x := h

theorem sound_DyadicTile_TileDecode : DyadicTile.TileDecode.Goal := by
  intro x y
  constructor
  · have h : y % 16 < 16 := Nat.mod_lt y (by decide)
    simp only [DyadicTile.key]
    omega
  · have h : y % 16 < 16 := Nat.mod_lt y (by decide)
    simp only [DyadicTile.key]
    omega

theorem sound_XorClass_DecodeRight : XorClass.DecodeRight.Goal := by
  intro x y
  simp only [XorClass.key]
  calc (x ^^^ y) ^^^ x
      = (y ^^^ x) ^^^ x := by rw [Nat.xor_comm x y]
    _ = y ^^^ (x ^^^ x) := by rw [Nat.xor_assoc]
    _ = y ^^^ 0 := by rw [Nat.xor_self]
    _ = y := by rw [Nat.xor_zero]

theorem sound_PowerBucket_DenseBitBounds : PowerBucket.DenseBitBounds.Goal := by
  intro rs w
  rw [List.all_eq_true]
  intro x hx
  simp only [List.mem_map, List.mem_filter] at hx
  obtain ⟨p, ⟨hp, hwp⟩, rfl⟩ := hx
  simp only [PowerBucket.key, beq_iff_eq] at hwp
  have hlt : p.1 < 2 ^ width p.1 := lt_two_pow_width p.1
  simp only [decide_eq_true_eq]
  rw [← hwp]
  exact hlt


/-! ## Shared goal instantiations -/

theorem sound_Lexicographic_LateGather : Lexicographic.LateGather.Goal := lateGather_true Lexicographic.key
theorem sound_AttributeFirst_LateGather : AttributeFirst.LateGather.Goal := lateGather_true AttributeFirst.key
theorem sound_Interleaved_LateGather : Interleaved.LateGather.Goal := lateGather_true Interleaved.key
theorem sound_GrayPair_LateGather : GrayPair.LateGather.Goal := lateGather_true GrayPair.key
theorem sound_Diagonal_LateGather : Diagonal.LateGather.Goal := lateGather_true Diagonal.key
theorem sound_UnorderedPair_LateGather : UnorderedPair.LateGather.Goal := lateGather_true UnorderedPair.key
theorem sound_RatioClass_LateGather : RatioClass.LateGather.Goal := lateGather_true RatioClass.key
theorem sound_PowerBucket_LateGather : PowerBucket.LateGather.Goal := lateGather_true PowerBucket.key
theorem sound_DyadicTile_LateGather : DyadicTile.LateGather.Goal := lateGather_true DyadicTile.key
theorem sound_XorClass_LateGather : XorClass.LateGather.Goal := lateGather_true XorClass.key
theorem sound_Lexicographic_StableLimit : Lexicographic.StableLimit.Goal := stableLimit_true Lexicographic.key
theorem sound_AttributeFirst_StableLimit : AttributeFirst.StableLimit.Goal := stableLimit_true AttributeFirst.key
theorem sound_Interleaved_StableLimit : Interleaved.StableLimit.Goal := stableLimit_true Interleaved.key
theorem sound_GrayPair_StableLimit : GrayPair.StableLimit.Goal := stableLimit_true GrayPair.key
theorem sound_Diagonal_StableLimit : Diagonal.StableLimit.Goal := stableLimit_true Diagonal.key
theorem sound_UnorderedPair_StableLimit : UnorderedPair.StableLimit.Goal := stableLimit_true UnorderedPair.key
theorem sound_RatioClass_StableLimit : RatioClass.StableLimit.Goal := stableLimit_true RatioClass.key
theorem sound_PowerBucket_StableLimit : PowerBucket.StableLimit.Goal := stableLimit_true PowerBucket.key
theorem sound_DyadicTile_StableLimit : DyadicTile.StableLimit.Goal := stableLimit_true DyadicTile.key
theorem sound_XorClass_StableLimit : XorClass.StableLimit.Goal := stableLimit_true XorClass.key
theorem sound_Lexicographic_SharedDelete : Lexicographic.SharedDelete.Goal := sharedDelete_true Lexicographic.key
theorem sound_AttributeFirst_SharedDelete : AttributeFirst.SharedDelete.Goal := sharedDelete_true AttributeFirst.key
theorem sound_Interleaved_SharedDelete : Interleaved.SharedDelete.Goal := sharedDelete_true Interleaved.key
theorem sound_GrayPair_SharedDelete : GrayPair.SharedDelete.Goal := sharedDelete_true GrayPair.key
theorem sound_Diagonal_SharedDelete : Diagonal.SharedDelete.Goal := sharedDelete_true Diagonal.key
theorem sound_UnorderedPair_SharedDelete : UnorderedPair.SharedDelete.Goal := sharedDelete_true UnorderedPair.key
theorem sound_RatioClass_SharedDelete : RatioClass.SharedDelete.Goal := sharedDelete_true RatioClass.key
theorem sound_PowerBucket_SharedDelete : PowerBucket.SharedDelete.Goal := sharedDelete_true PowerBucket.key
theorem sound_DyadicTile_SharedDelete : DyadicTile.SharedDelete.Goal := sharedDelete_true DyadicTile.key
theorem sound_XorClass_SharedDelete : XorClass.SharedDelete.Goal := sharedDelete_true XorClass.key
theorem sound_Lexicographic_PayloadAlignment : Lexicographic.PayloadAlignment.Goal := payloadAlignment_true Lexicographic.key
theorem sound_AttributeFirst_PayloadAlignment : AttributeFirst.PayloadAlignment.Goal := payloadAlignment_true AttributeFirst.key
theorem sound_Interleaved_PayloadAlignment : Interleaved.PayloadAlignment.Goal := payloadAlignment_true Interleaved.key
theorem sound_GrayPair_PayloadAlignment : GrayPair.PayloadAlignment.Goal := payloadAlignment_true GrayPair.key
theorem sound_Diagonal_PayloadAlignment : Diagonal.PayloadAlignment.Goal := payloadAlignment_true Diagonal.key
theorem sound_UnorderedPair_PayloadAlignment : UnorderedPair.PayloadAlignment.Goal := payloadAlignment_true UnorderedPair.key
theorem sound_RatioClass_PayloadAlignment : RatioClass.PayloadAlignment.Goal := payloadAlignment_true RatioClass.key
theorem sound_PowerBucket_PayloadAlignment : PowerBucket.PayloadAlignment.Goal := payloadAlignment_true PowerBucket.key
theorem sound_DyadicTile_PayloadAlignment : DyadicTile.PayloadAlignment.Goal := payloadAlignment_true DyadicTile.key
theorem sound_XorClass_PayloadAlignment : XorClass.PayloadAlignment.Goal := payloadAlignment_true XorClass.key
theorem sound_Lexicographic_SidecarBudget : Lexicographic.SidecarBudget.Goal := sidecarBudget_true Lexicographic.key
theorem sound_AttributeFirst_SidecarBudget : AttributeFirst.SidecarBudget.Goal := sidecarBudget_true AttributeFirst.key
theorem sound_Interleaved_SidecarBudget : Interleaved.SidecarBudget.Goal := sidecarBudget_true Interleaved.key
theorem sound_GrayPair_SidecarBudget : GrayPair.SidecarBudget.Goal := sidecarBudget_true GrayPair.key
theorem sound_Diagonal_SidecarBudget : Diagonal.SidecarBudget.Goal := sidecarBudget_true Diagonal.key
theorem sound_UnorderedPair_SidecarBudget : UnorderedPair.SidecarBudget.Goal := sidecarBudget_true UnorderedPair.key
theorem sound_RatioClass_SidecarBudget : RatioClass.SidecarBudget.Goal := sidecarBudget_true RatioClass.key
theorem sound_PowerBucket_SidecarBudget : PowerBucket.SidecarBudget.Goal := sidecarBudget_true PowerBucket.key
theorem sound_DyadicTile_SidecarBudget : DyadicTile.SidecarBudget.Goal := sidecarBudget_true DyadicTile.key
theorem sound_XorClass_SidecarBudget : XorClass.SidecarBudget.Goal := sidecarBudget_true XorClass.key
theorem sound_Lexicographic_PatchByOrdinal : Lexicographic.PatchByOrdinal.Goal := patchByOrdinal_true Lexicographic.key
theorem sound_AttributeFirst_PatchByOrdinal : AttributeFirst.PatchByOrdinal.Goal := patchByOrdinal_true AttributeFirst.key
theorem sound_Interleaved_PatchByOrdinal : Interleaved.PatchByOrdinal.Goal := patchByOrdinal_true Interleaved.key
theorem sound_GrayPair_PatchByOrdinal : GrayPair.PatchByOrdinal.Goal := patchByOrdinal_true GrayPair.key
theorem sound_Diagonal_PatchByOrdinal : Diagonal.PatchByOrdinal.Goal := patchByOrdinal_true Diagonal.key
theorem sound_UnorderedPair_PatchByOrdinal : UnorderedPair.PatchByOrdinal.Goal := patchByOrdinal_true UnorderedPair.key
theorem sound_RatioClass_PatchByOrdinal : RatioClass.PatchByOrdinal.Goal := patchByOrdinal_true RatioClass.key
theorem sound_PowerBucket_PatchByOrdinal : PowerBucket.PatchByOrdinal.Goal := patchByOrdinal_true PowerBucket.key
theorem sound_DyadicTile_PatchByOrdinal : DyadicTile.PatchByOrdinal.Goal := patchByOrdinal_true DyadicTile.key
theorem sound_XorClass_PatchByOrdinal : XorClass.PatchByOrdinal.Goal := patchByOrdinal_true XorClass.key
theorem sound_Lexicographic_MergeShards : Lexicographic.MergeShards.Goal := mergeShards_true Lexicographic.key
theorem sound_AttributeFirst_MergeShards : AttributeFirst.MergeShards.Goal := mergeShards_true AttributeFirst.key
theorem sound_Interleaved_MergeShards : Interleaved.MergeShards.Goal := mergeShards_true Interleaved.key
theorem sound_GrayPair_MergeShards : GrayPair.MergeShards.Goal := mergeShards_true GrayPair.key
theorem sound_Diagonal_MergeShards : Diagonal.MergeShards.Goal := mergeShards_true Diagonal.key
theorem sound_UnorderedPair_MergeShards : UnorderedPair.MergeShards.Goal := mergeShards_true UnorderedPair.key
theorem sound_RatioClass_MergeShards : RatioClass.MergeShards.Goal := mergeShards_true RatioClass.key
theorem sound_PowerBucket_MergeShards : PowerBucket.MergeShards.Goal := mergeShards_true PowerBucket.key
theorem sound_DyadicTile_MergeShards : DyadicTile.MergeShards.Goal := mergeShards_true DyadicTile.key
theorem sound_XorClass_MergeShards : XorClass.MergeShards.Goal := mergeShards_true XorClass.key

/-! ## Model-specific goals -/

-- Lexicographic
theorem sound_Lexicographic_StreamingFrequency : Lexicographic.StreamingFrequency.Goal := by
  intro rs v
  simp only [Lexicographic.StreamingFrequency.Goal]
  -- physical is perm of rs, so counts preserved; physical sorted by fst
  let col := (physical Lexicographic.key rs).map Prod.fst
  have hperm : col.Perm (rs.map Prod.fst) := (physical_perm Lexicographic.key rs).map Prod.fst
  have hcount : col.count v = (rs.map Prod.fst).count v := by
    induction hperm with
    | nil => rfl
    | cons x h ih => simp [List.count_cons, ih]
    | swap x y l => simp [List.count_cons]; omega
    | trans h1 h2 ih1 ih2 => exact ih1.trans ih2
  rw [← hcount]
  -- for sorted list, dropWhile (<v) ++ takeWhile (==v) gives count
  -- prove by general lemma
  have hsorted : col.Pairwise (· ≤ ·) := by
    -- physical is sorted by rowCmp which implies fst ≤
    unfold col physical perm Lexicographic.key
    have hsort : (rs.zipIdx.mergeSort (fun (x y : Row × Nat) => let a := (id x.1); let b := (id y.1); a.1 < b.1 || (a.1==b.1 && (a.2 < b.2 || (a.2==b.2 && x.2≤y.2))))).Pairwise
        (fun x y => (let a := (id x.1); let b := (id y.1); a.1 < b.1 || (a.1==b.1 && (a.2 < b.2 || (a.2==b.2 && x.2≤y.2))) = true) :=
      List.pairwise_mergeSort (by intro x y z h1 h2; simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h1 h2 ⊢; omega) (by intro x y; simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq]; omega) _
    rw [List.pairwise_map, List.pairwise_map]
    exact hsort.imp (by intro x y h; simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq, id] at h ⊢; omega)
  -- count in sorted list = dropWhile(<v).takeWhile(==v).length
  have hcount_sorted : ∀ (l : List Nat), l.Pairwise (· ≤ ·) → (l.dropWhile (· < v) |>.takeWhile (· == v)).length = l.count v := by
    intro l hl
    induction l with
    | nil => simp
    | cons x xs ih =>
      rw [List.pairwise_cons] at hl
      by_cases hx : x < v
      · have hne : (x == v) = false := by simp; omega
        simp [List.dropWhile_cons, hx, List.count_cons, hne, ih hl.2]
      · by_cases hx2 : x == v
        · have hxv : x = v := by simpa using hx2
          subst hxv
          simp only [List.dropWhile_cons, Nat.lt_irrefl, decide_false, Bool.false_eq_true, not_false_eq_true, List.takeWhile_cons, beq_self_eq_true, ite_true, List.length_cons, List.count_cons, beq_self_eq_true]
          congr 1
          -- takeWhile (==v) on xs where all ≥ v
          have hxs : (xs.takeWhile (· == v)) = xs.filter (· == v) := by
            apply List.takeWhile_eq_filter_of_pairwise
            · exact hl.2.imp (by intro a b h; omega)
            · intro a ha
              have : v ≤ a := hl.1 a ha
              simp only [Bool.eq_iff_iff, beq_iff_eq]
              omega
          rw [hxs, List.length_filter]
        · have hxgt : v < x := by
            have : ¬ x < v := hx
            have : x ≠ v := by simpa using hx2
            omega
          have hne : (x == v) = false := by simp; omega
          simp [List.dropWhile_cons, hx, List.takeWhile_cons, hne, List.count_cons, hne, ih hl.2]
  exact hcount_sorted col hsorted

theorem sound_Lexicographic_PrimaryRuns : Lexicographic.PrimaryRuns.Goal := by
  intro rs
  simp only [Lexicographic.PrimaryRuns.Goal]
  let col := (physical Lexicographic.key rs).map Prod.fst
  have hperm : col.Perm (rs.map Prod.fst) := (physical_perm Lexicographic.key rs).map Prod.fst
  have hsorted : col.Pairwise (· ≤ ·) := by
    unfold col physical perm Lexicographic.key
    have hsort : (rs.zipIdx.mergeSort (fun (x y : Row × Nat) => let a := (id x.1); let b := (id y.1); a.1 < b.1 || (a.1==b.1 && (a.2 < b.2 || (a.2==b.2 && x.2≤y.2))))).Pairwise
        (fun x y => (let a := (id x.1); let b := (id y.1); a.1 < b.1 || (a.1==b.1 && (a.2 < b.2 || (a.2==b.2 && x.2≤y.2))) = true) :=
      List.pairwise_mergeSort (by intro x y z h1 h2; simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h1 h2 ⊢; omega) (by intro x y; simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq]; omega) _
    rw [List.pairwise_map, List.pairwise_map]
    exact hsort.imp (by intro x y h; simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq, id] at h ⊢; omega)
  -- rle length on sorted list = number of distinct values = eraseDups length
  have hrle_sorted : ∀ (l : List Nat), l.Pairwise (· ≤ ·) → (rle l).length = l.eraseDups.length := by
    intro l hl
    induction l with
    | nil => simp [rle]
    | cons x xs ih =>
      rw [List.pairwise_cons] at hl
      cases hxs : rle xs with
      | nil =>
        have : xs = [] := by cases xs <;> simp [rle] at hxs
        subst this; simp [rle]
      | cons p ps =>
        have ih' := ih hl.2
        rw [hxs] at ih'
        simp only [rle, hxs]
        by_cases hxp : x == p.1
        · have hxp' : x = p.1 := by simpa using hxp
          subst hxp'
          -- p.1 is head of xs in terms of rle? rle xs = (p.1, p.2)::ps means xs starts with p.1
          have hxs_ne : xs ≠ [] := by intro h; simp [h, rle] at hxs
          have hhead : xs.head hxs_ne = p.1 := by
            cases xs with
            | nil => contradiction
            | cons y ys =>
              simp [rle] at hxs
              split at hxs <;> simp_all
          have hxle : x ≤ xs.head hxs_ne := hl.1 _ (List.head_mem hxs_ne)
          rw [hhead] at hxle
          -- x == p.1 and x ≤ p.1 means equal, so rle merges
          simp [hxp, ih', List.eraseDups_cons]
          -- x is in xs since x == head
          have hmem : p.1 ∈ xs := by rw [← hhead]; exact List.head_mem hxs_ne
          simp [hmem]
        · simp [hxp, ih', List.eraseDups_cons]
          -- x not equal to head of rle xs => x not in xs (since sorted and x ≤ all xs, x == head means in xs; x != head means x < head or different)
          have hmem : x ∉ xs := by
            intro hmem
            have hle : x ≤ xs.head (by intro h; simp [h] at hmem) := hl.1 _ (List.head_mem _)
            have hhead_mem : xs.head (by intro h; simp [h] at hmem) ∈ xs := List.head_mem _
            cases xs with
            | nil => simp at hmem
            | cons y ys =>
              simp [rle] at hxs
              split at hxs
              · next heq =>
                have : x = y := by
                  have hxy : x ≤ y := hl.1 y (by simp)
                  have := by simpa using hxp
                  -- hxp : (x == y) = false after split matched? split matched means y == p.1 was true
                  omega
                simp [this] at hxp
              · next hne =>
                -- hxs gives (x,1)::(y,_)::... = (p.1,p.2)::ps so p.1 = x
                simp at hxs
                exact hxp (by simp [hxs.1])
          simp [hmem]
  rw [hrle_sorted col hsorted]
  -- eraseDups length preserved by Perm
  have herase : col.eraseDups.Perm (rs.map Prod.fst).eraseDups := hperm.eraseDups
  exact herase.length_eq


-- AttributeFirst
theorem sound_AttributeFirst_AttributeCut : AttributeFirst.AttributeCut.Goal := by
  intro rs v
  simp only [AttributeFirst.AttributeCut.Goal]
  let ps := physical AttributeFirst.key rs
  have hperm : ps.Perm rs := physical_perm AttributeFirst.key rs
  -- sorted by snd
  have hsorted : ps.Pairwise (fun a b => a.2 ≤ b.2) := by
    unfold ps physical perm AttributeFirst.key
    have hsort := List.pairwise_mergeSort
      (by intro x y z h1 h2; simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h1 h2 ⊢; omega)
      (by intro x y; simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq]; omega) rs.zipIdx
    rw [List.pairwise_map, List.pairwise_map]
    exact hsort.imp (by intro x y h; simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h ⊢; omega)
  -- takeWhile countP on sorted
  have htake : ∀ (l : List Row), l.Pairwise (fun a b => a.2 ≤ b.2) → (l.takeWhile (fun p => p.2 < v)).length = l.countP (fun p => p.2 < v) := by
    intro l hl
    induction l with
    | nil => simp
    | cons a as ih =>
      rw [List.pairwise_cons] at hl
      by_cases ha : a.2 < v
      · simp [List.takeWhile_cons, ha, List.countP_cons, ih hl.2]
      · have hfalse : (a.2 < v) = false := by omega
        simp only [List.takeWhile_cons, hfalse, Bool.false_eq_true, not_false_eq_true, List.length_nil, List.countP_cons, hfalse]
        have : as.countP (fun p => p.2 < v) = 0 := by
          rw [List.countP_eq_zero]
          intro x hx
          have := hl.1 x hx
          simp; omega
        omega
  rw [htake ps hsorted]
  -- countP preserved by Perm
  have hcp : ps.countP (fun p => p.2 < v) = rs.countP (fun p => p.2 < v) := by
    induction hperm with
    | nil => rfl
    | cons x h ih => simp [List.countP_cons, ih]
    | swap x y l => simp [List.countP_cons]; omega
    | trans h1 h2 ih1 ih2 => exact ih1.trans ih2
  exact hcp

theorem sound_AttributeFirst_AttributeRuns : AttributeFirst.AttributeRuns.Goal := by
  intro rs
  simp only [AttributeFirst.AttributeRuns.Goal]
  let col := (physical AttributeFirst.key rs).map Prod.snd
  have hperm : col.Perm (rs.map Prod.snd) := (physical_perm AttributeFirst.key rs).map Prod.snd
  have hsorted : col.Pairwise (· ≤ ·) := by
    unfold col physical perm AttributeFirst.key
    have hsort := List.pairwise_mergeSort
      (by intro x y z h1 h2; simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h1 h2 ⊢; omega)
      (by intro x y; simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq]; omega) rs.zipIdx
    rw [List.pairwise_map, List.pairwise_map]
    exact hsort.imp (by intro x y h; simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h ⊢; omega)
  -- same rle/eraseDups argument as Lexicographic.PrimaryRuns
  have hrle_sorted : ∀ (l : List Nat), l.Pairwise (· ≤ ·) → (rle l).length = l.eraseDups.length := by
    intro l hl
    induction l with
    | nil => simp [rle]
    | cons x xs ih =>
      rw [List.pairwise_cons] at hl
      cases hxs : rle xs with
      | nil =>
        have : xs = [] := by cases xs <;> simp [rle] at hxs
        subst this; simp [rle]
      | cons p ps =>
        have ih' := ih hl.2; rw [hxs] at ih'
        simp only [rle, hxs]
        by_cases hxp : x == p.1
        · have hxp' : x = p.1 := by simpa using hxp
          subst hxp'
          have hxs_ne : xs ≠ [] := by intro h; simp [h, rle] at hxs
          have hhead : xs.head hxs_ne = p.1 := by
            cases xs with | nil => contradiction | cons y ys => simp [rle] at hxs; split at hxs <;> simp_all
          simp [hxp, ih', List.eraseDups_cons]
          have hmem : p.1 ∈ xs := by rw [← hhead]; exact List.head_mem hxs_ne
          simp [hmem]
        · simp [hxp, ih', List.eraseDups_cons]
          have hmem : x ∉ xs := by
            intro hmem
            cases xs with
            | nil => simp at hmem
            | cons y ys =>
              simp [rle] at hxs
              split at hxs
              · simp at hxs; simp [hxs.1] at hxp
              · simp at hxs; exact hxp (by simp [hxs.1])
          simp [hmem]
  rw [hrle_sorted col hsorted]
  exact (hperm.eraseDups).length_eq

-- Diagonal
theorem sound_Diagonal_TriangularPrefix : Diagonal.TriangularPrefix.Goal := by
  intro rs t
  simp only [Diagonal.TriangularPrefix.Goal]
  let ps := physical Diagonal.key rs
  have hperm : ps.Perm rs := physical_perm Diagonal.key rs
  have hsorted : ps.Pairwise (fun a b => a.1+a.2 ≤ b.1+b.2) := by
    unfold ps physical perm Diagonal.key
    have hsort := List.pairwise_mergeSort
      (by intro x y z h1 h2; simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h1 h2 ⊢; omega)
      (by intro x y; simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq]; omega) rs.zipIdx
    rw [List.pairwise_map, List.pairwise_map]
    exact hsort.imp (by intro x y h; simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h ⊢; omega)
  have htake : ∀ (l : List Row), l.Pairwise (fun a b => a.1+a.2 ≤ b.1+b.2) → (l.takeWhile (fun (x,y) => x+y < t)).length = l.countP (fun (x,y) => x+y < t) := by
    intro l hl
    induction l with
    | nil => simp
    | cons a as ih =>
      rw [List.pairwise_cons] at hl
      by_cases ha : a.1+a.2 < t
      · simp [List.takeWhile_cons, ha, List.countP_cons, ih hl.2]
      · have hfalse : (a.1+a.2 < t) = false := by omega
        simp only [List.takeWhile_cons, hfalse, Bool.false_eq_true, not_false_eq_true, List.length_nil, List.countP_cons, hfalse]
        have : as.countP (fun (x,y) => x+y < t) = 0 := by
          rw [List.countP_eq_zero]; intro x hx; have := hl.1 x hx; simp; omega
        omega
  rw [htake ps hsorted]
  have hcp : ps.countP (fun (x,y) => x+y < t) = rs.countP (fun (x,y) => x+y < t) := by
    induction hperm with
    | nil => rfl
    | cons x h ih => simp [List.countP_cons, ih]
    | swap x y l => simp [List.countP_cons]; omega
    | trans h1 h2 ih1 ih2 => exact ih1.trans ih2
  exact hcp

theorem sound_Diagonal_EqualSumRuns : Diagonal.EqualSumRuns.Goal := by
  intro rs
  simp only [Diagonal.EqualSumRuns.Goal]
  let col := (physical Diagonal.key rs).map (fun (x,y) => x+y)
  have hperm : col.Perm (rs.map (fun (x,y) => x+y)) := (physical_perm Diagonal.key rs).map _
  have hsorted : col.Pairwise (· ≤ ·) := by
    unfold col physical perm Diagonal.key
    have hsort := List.pairwise_mergeSort
      (by intro x y z h1 h2; simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h1 h2 ⊢; omega)
      (by intro x y; simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq]; omega) rs.zipIdx
    rw [List.pairwise_map, List.pairwise_map]
    exact hsort.imp (by intro x y h; simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h ⊢; omega)
  have hrle_sorted : ∀ (l : List Nat), l.Pairwise (· ≤ ·) → (rle l).length = l.eraseDups.length := by
    intro l hl
    induction l with
    | nil => simp [rle]
    | cons x xs ih =>
      rw [List.pairwise_cons] at hl
      cases hxs : rle xs with
      | nil =>
        have : xs = [] := by cases xs <;> simp [rle] at hxs
        subst this; simp [rle]
      | cons p ps =>
        have ih' := ih hl.2; rw [hxs] at ih'
        simp only [rle, hxs]
        by_cases hxp : x == p.1
        · have hxp' : x = p.1 := by simpa using hxp
          subst hxp'
          have hxs_ne : xs ≠ [] := by intro h; simp [h, rle] at hxs
          have hhead : xs.head hxs_ne = p.1 := by
            cases xs with | nil => contradiction | cons y ys => simp [rle] at hxs; split at hxs <;> simp_all
          simp [hxp, ih', List.eraseDups_cons]
          have hmem : p.1 ∈ xs := by rw [← hhead]; exact List.head_mem hxs_ne
          simp [hmem]
        · simp [hxp, ih', List.eraseDups_cons]
          have hmem : x ∉ xs := by
            intro hmem
            cases xs with
            | nil => simp at hmem
            | cons y ys =>
              simp [rle] at hxs
              split at hxs
              · simp at hxs; simp [hxs.1] at hxp
              · simp at hxs; exact hxp (by simp [hxs.1])
          simp [hmem]
  rw [hrle_sorted col hsorted]
  exact (hperm.eraseDups).length_eq

-- UnorderedPair
theorem sound_UnorderedPair_SymmetricMultiplicity : UnorderedPair.SymmetricMultiplicity.Goal := by
  intro rs a b hab
  simp only [UnorderedPair.SymmetricMultiplicity.Goal]
  let ps := physical UnorderedPair.key rs
  have hperm : ps.Perm rs := physical_perm UnorderedPair.key rs
  have hmap_perm : (ps.map UnorderedPair.key).Perm (rs.map UnorderedPair.key) := hperm.map _
  have hcount : (ps.map UnorderedPair.key).count (a,b) = (rs.map UnorderedPair.key).count (a,b) := by
    induction hmap_perm with
    | nil => rfl
    | cons x h ih => simp [List.count_cons, ih]
    | swap x y l => simp [List.count_cons]; omega
    | trans h1 h2 ih1 ih2 => exact ih1.trans ih2
  rw [hcount]
  -- count key (a,b) in rs.map key = rs.count (a,b) + rs.count (b,a) when a<b
  have hkey_ab : UnorderedPair.key (a,b) = (a,b) := by simp [UnorderedPair.key, Nat.min_eq_left (Nat.le_of_lt hab), Nat.max_eq_right (Nat.le_of_lt hab)]
  have hkey_ba : UnorderedPair.key (b,a) = (a,b) := by simp [UnorderedPair.key, Nat.min_eq_right (Nat.le_of_lt hab), Nat.max_eq_left (Nat.le_of_lt hab)]
  -- count (a,b) in map key = count elements where key = (a,b) = count (a,b) + count (b,a)
  have hgen : ∀ (l : List Row), (l.map UnorderedPair.key).count (a,b) = l.count (a,b) + l.count (b,a) := by
    intro l
    induction l with
    | nil => simp
    | cons x xs ih =>
      simp only [List.map_cons, List.count_cons, ih]
      by_cases hx : UnorderedPair.key x = (a,b)
      · have h1 : ((UnorderedPair.key x == (a,b)) = true) := by simp [hx]
        simp only [h1, ite_true]
        -- x is (a,b) or (b,a)
        have hcases : x = (a,b) ∨ x = (b,a) := by
          have := hx
          simp [UnorderedPair.key] at this
          obtain ⟨h1, h2⟩ := Prod.mk.inj this
          have hmin := h1; have hmax := h2
          -- min x.1 x.2 = a, max x.1 x.2 = b
          rcases Nat.le_total x.1 x.2 with hle | hle
          · rw [Nat.min_eq_left hle, Nat.max_eq_right hle] at hmin hmax
            left; ext <;> simp_all
          · rw [Nat.min_eq_right hle, Nat.max_eq_left hle] at hmin hmax
            right; ext <;> simp_all
        rcases hcases with rfl | rfl
        · simp [List.count_cons]
        · simp [List.count_cons, show ((b,a) == (a,b)) = false by simp [beq_iff_eq]; omega, show ((b,a) == (b,a)) = true by simp]
      · have h1 : ((UnorderedPair.key x == (a,b)) = false) := by simp [hx]
        simp only [h1, Bool.false_eq_true, ite_false]
        have hne1 : (x == (a,b)) = false := by
          by_contra h
          simp at h
          subst h
          exact hx hkey_ab
        have hne2 : (x == (b,a)) = false := by
          by_contra h
          simp at h
          subst h
          exact hx hkey_ba
        simp [hne1, hne2]
  exact hgen rs

-- PowerBucket
theorem sound_PowerBucket_WidthRunCount : PowerBucket.WidthRunCount.Goal := by
  intro rs
  simp only [PowerBucket.WidthRunCount.Goal]
  let col := (physical PowerBucket.key rs).map (fun p => width p.1)
  have hperm : col.Perm (rs.map (fun (p : Row) => width p.1)) := (physical_perm PowerBucket.key rs).map _
  have hsorted : col.Pairwise (· ≤ ·) := by
    unfold col physical perm PowerBucket.key
    have hsort := List.pairwise_mergeSort
      (by intro x y z h1 h2; simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h1 h2 ⊢; omega)
      (by intro x y; simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq]; omega) rs.zipIdx
    rw [List.pairwise_map, List.pairwise_map]
    exact hsort.imp (by intro x y h; simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h ⊢; omega)
  have hrle_sorted : ∀ (l : List Nat), l.Pairwise (· ≤ ·) → (rle l).length = l.eraseDups.length := by
    intro l hl
    induction l with
    | nil => simp [rle]
    | cons x xs ih =>
      rw [List.pairwise_cons] at hl
      cases hxs : rle xs with
      | nil =>
        have : xs = [] := by cases xs <;> simp [rle] at hxs
        subst this; simp [rle]
      | cons p ps =>
        have ih' := ih hl.2; rw [hxs] at ih'
        simp only [rle, hxs]
        by_cases hxp : x == p.1
        · have hxp' : x = p.1 := by simpa using hxp
          subst hxp'
          have hxs_ne : xs ≠ [] := by intro h; simp [h, rle] at hxs
          have hhead : xs.head hxs_ne = p.1 := by
            cases xs with | nil => contradiction | cons y ys => simp [rle] at hxs; split at hxs <;> simp_all
          simp [hxp, ih', List.eraseDups_cons]
          have hmem : p.1 ∈ xs := by rw [← hhead]; exact List.head_mem hxs_ne
          simp [hmem]
        · simp [hxp, ih', List.eraseDups_cons]
          have hmem : x ∉ xs := by
            intro hmem
            cases xs with
            | nil => simp at hmem
            | cons y ys =>
              simp [rle] at hxs
              split at hxs
              · simp at hxs; simp [hxs.1] at hxp
              · simp at hxs; exact hxp (by simp [hxs.1])
          simp [hmem]
  rw [hrle_sorted col hsorted]
  exact (hperm.eraseDups).length_eq

-- XorClass
theorem sound_XorClass_ZeroClass : XorClass.ZeroClass.Goal := by
  intro rs
  simp only [XorClass.ZeroClass.Goal]
  let ps := physical XorClass.key rs
  have hperm : ps.Perm rs := physical_perm XorClass.key rs
  have hsorted : ps.Pairwise (fun a b => (XorClass.key a).1 ≤ (XorClass.key b).1) := by
    unfold ps physical perm XorClass.key
    have hsort := List.pairwise_mergeSort
      (by intro x y z h1 h2; simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h1 h2 ⊢; omega)
      (by intro x y; simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq]; omega) rs.zipIdx
    rw [List.pairwise_map, List.pairwise_map]
    exact hsort.imp (by intro x y h; simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h ⊢; omega)
  have htake : ∀ (l : List Row), l.Pairwise (fun a b => (XorClass.key a).1 ≤ (XorClass.key b).1) →
      (l.takeWhile (fun p => (XorClass.key p).1 == 0)).length = l.countP (fun (x,y) => x == y) := by
    intro l hl
    induction l with
    | nil => simp
    | cons a as ih =>
      rw [List.pairwise_cons] at hl
      by_cases ha : (XorClass.key a).1 == 0
      · have ha' : a.1 == a.2 := by simp [XorClass.key] at ha ⊢; exact ha
        simp [List.takeWhile_cons, ha, List.countP_cons, ha', ih hl.2]
      · have ha0 : (XorClass.key a).1 ≠ 0 := by simpa using ha
        have hfalse : ((XorClass.key a).1 == 0) = false := by simp [ha]
        have hfalse2 : (a.1 == a.2) = false := by
          simp [XorClass.key] at ha
          simp; intro heq; subst heq; simp at ha
        simp only [List.takeWhile_cons, hfalse, Bool.false_eq_true, not_false_eq_true, List.length_nil, List.countP_cons, hfalse2]
        have : as.countP (fun (x,y) => x == y) = 0 := by
          rw [List.countP_eq_zero]
          intro x hx
          have hle := hl.1 x hx
          have hkx : (XorClass.key x).1 ≠ 0 := by
            simp [XorClass.key] at hle ha ⊢; omega
          simp [XorClass.key] at hkx ⊢
          intro heq; subst heq; simp at hkx
        omega
  rw [htake ps hsorted]
  have hcp : ps.countP (fun (x,y) => x == y) = rs.countP (fun (x,y) => x == y) := by
    induction hperm with
    | nil => rfl
    | cons x h ih => simp [List.countP_cons, ih]
    | swap x y l => simp [List.countP_cons]; omega
    | trans h1 h2 ih1 ih2 => exact ih1.trans ih2
  exact hcp


-- Interleaved Morton helpers
private theorem or_eq_add_of_and_eq_zero {x y : Nat} (h : x &&& y = 0) : x ||| y = x + y := by
  apply Nat.eq_of_testBit_eq
  intro i
  have ha := Nat.testBit_and x y i
  have ho := Nat.testBit_or x y i
  have h0 : (x &&& y).testBit i = false := by rw [h]; simp
  rw [ho, h0] at *
  simp at ha
  cases hx : x.testBit i <;> cases hy : y.testBit i <;> simp [hx, hy, Nat.testBit_add, *] at * <;>
    simp_all [Nat.testBit, Nat.shiftRight_eq_div_pow, Nat.add_mod, Nat.mul_add_div, Nat.div_add_mod]

private theorem morton_testBit (x y j : Nat) :
    (morton x y).testBit j = (if j % 2 == 0 then x.testBit (j / 2) else y.testBit (j / 2)) := by
  unfold morton
  have hterm : ∀ i, ((if x.testBit i then 2^(2*i) else 0) + (if y.testBit i then 2^(2*i+1) else 0)).testBit j =
      (if j == 2*i then x.testBit i else if j == 2*i+1 then y.testBit i else false) := by
    intro i
    by_cases hx : x.testBit i <;> by_cases hy : y.testBit i <;>
      simp [hx, hy, Nat.testBit_two_pow, Nat.add_comm]
  -- prove by induction over range
  let m := max (width x) (width y)
  let f := fun i => (if x.testBit i then 2^(2*i) else 0) + (if y.testBit i then 2^(2*i+1) else 0)
  have hsum : ((List.range m).map f).sum.testBit j = (List.range m).any (fun i => (f i).testBit j) := by
    have hgen : ∀ (l : List Nat),
        (∀ a ∈ l, ∀ b ∈ l, a ≠ b → f a &&& f b = 0) →
        ((l.map f).sum).testBit j = (l.any (fun i => (f i).testBit j)) := by
      intro l hd
      induction l with
      | nil => simp
      | cons a as ih =>
        rw [List.map_cons, List.sum_cons, List.any_cons]
        have hdisj : f a &&& ((as.map f).sum) = 0 := by
          apply Nat.eq_of_testBit_eq
          intro k
          rw [Nat.testBit_and]
          simp only [Nat.testBit_zero, Bool.and_eq_false_imp, Bool.and_eq_true]
          intro hfa
          -- show as.sum has no bit k
          have hsum_as : ((as.map f).sum).testBit k = (as.any (fun i => (f i).testBit k)) := ih (fun x hx y hy hne => hd x (List.mem_cons_of_mem _ hx) y (List.mem_cons_of_mem _ hy) hne)
          rw [hsum_as, List.any_eq_true, Bool.not_eq_true]
          simp only [List.mem_map, not_exists, not_and]
          intro b hb hfb
          have hne : a ≠ b := by intro h; subst h; exact False.elim (by simp at hb)
          have hdab := hd a List.mem_cons_self b (List.mem_cons_of_mem _ hb) hne
          have := congrArg (fun n => n.testBit k) hdab
          rw [Nat.testBit_and] at this
          simp at this
          exact this hfa
        rw [← or_eq_add_of_and_eq_zero hdisj, Nat.testBit_or]
        congr 1
        exact ih (fun x hx y hy hne => hd x (List.mem_cons_of_mem _ hx) y (List.mem_cons_of_mem _ hy) hne)
    apply hgen
    intro a ha b hb hne
    apply Nat.eq_of_testBit_eq
    intro k
    rw [Nat.testBit_and, hterm a, hterm b]
    simp only [Nat.testBit_zero, Bool.and_eq_false_imp, Bool.and_eq_true]
    intro ha' hb'
    split at ha' <;> split at hb' <;> split at hb' <;> simp_all <;> omega
  rw [hsum]
  simp only [List.any_eq_true, List.mem_range, hterm]
  by_cases hj : j % 2 == 0
  · have hje : j = 2 * (j / 2) := by omega
    constructor
    · intro ⟨i, hi, h⟩
      split at h
      · next he => simp at he; have : i = j/2 := by omega; subst this; simpa using h
      · split at h <;> simp at h
    · intro h
      refine ⟨j/2, ?_, ?_⟩
      · by_contra hge; simp only [Nat.not_lt, Nat.not_le, not_not] at hge
        have hx0 : x.testBit (j/2) = false := Nat.testBit_lt_two_pow (by
          calc x < 2 ^ width x := lt_two_pow_width x
            _ ≤ 2 ^ m := Nat.pow_le_pow_right (by omega) (Nat.le_max_left _ _)
            _ ≤ 2 ^ (j/2) := Nat.pow_le_pow_right (by omega) (by omega))
        simp [hx0] at h
      · simp [hje, h]
  · have hjo : j = 2 * (j / 2) + 1 := by omega
    constructor
    · intro ⟨i, hi, h⟩
      split at h
      · simp at h
      · split at h
        · next he => simp at he; have : i = j/2 := by omega; subst this; simpa using h
        · simp at h
    · intro h
      refine ⟨j/2, ?_, ?_⟩
      · by_contra hge; simp only [Nat.not_lt, Nat.not_le, not_not] at hge
        have hy0 : y.testBit (j/2) = false := Nat.testBit_lt_two_pow (by
          calc y < 2 ^ width y := lt_two_pow_width y
            _ ≤ 2 ^ m := Nat.pow_le_pow_right (by omega) (Nat.le_max_right _ _)
            _ ≤ 2 ^ (j/2) := Nat.pow_le_pow_right (by omega) (by omega))
        simp [hy0] at h
      · simp [hjo, h]

private theorem morton_div_pow (x y h : Nat) : morton x y / 4 ^ h = morton (x / 2 ^ h) (y / 2 ^ h) := by
  apply Nat.eq_of_testBit_eq
  intro i
  rw [Nat.testBit_div_two_pow, show 4^h = 2^(2*h) by rw [show (4:Nat)=2^2 by rfl, Nat.pow_mul]]
  simp only [morton_testBit, Nat.testBit_div_two_pow]
  by_cases hi : i % 2 == 0
  · have h2 : (i + 2*h) % 2 == 0 := by omega
    simp [hi, h2]; congr 1; omega
  · have h2 : (i + 2*h) % 2 == 0 = false := by omega
    simp [hi, h2]; congr 1; omega

private theorem morton_inj : Function.Injective (fun (p : Nat × Nat) => morton p.1 p.2) := by
  intro p q h
  ext
  · apply Nat.eq_of_testBit_eq
    intro i
    have := congrArg (fun n => n.testBit (2*i)) h
    simp only [morton_testBit, Nat.mul_mod_right, beq_self_eq_true, ite_true, Nat.mul_div_right _ (by omega : 0 < 2)] at this
    exact this
  · apply Nat.eq_of_testBit_eq
    intro i
    have := congrArg (fun n => n.testBit (2*i+1)) h
    simp only [morton_testBit] at this
    have h1 : ((2 * i + 1) % 2 == 0) = false := by omega
    have h2 : (2 * i + 1) / 2 = i := by omega
    simp [h1, h2] at this
    exact this

theorem sound_Interleaved_QuadrantPrefix : Interleaved.QuadrantPrefix.Goal := by
  intro x y h
  simp [Interleaved.key, morton_div_pow]

theorem sound_Interleaved_SquareTile : Interleaved.SquareTile.Goal := by
  intro h x y bx by_
  simp only [Interleaved.key]
  rw [morton_div_pow]
  constructor
  · intro heq
    have := @morton_inj (x / 2 ^ h, y / 2 ^ h) (bx, by_) heq
    simpa using this
  · intro ⟨h1, h2⟩; subst h1; subst h2; rfl

-- GrayPair
private theorem gray_inj : Function.Injective (fun x => x ^^^ (x / 2)) := by
  intro a b h
  apply Nat.eq_of_testBit_eq
  intro i
  have hg : ∀ x j, (x ^^^ (x / 2)).testBit j = (x.testBit j ^^ x.testBit (j+1)) := by
    intro x j; simp only [Nat.testBit_xor, Nat.testBit_div_two_pow, Nat.pow_one]
  let m := max (width a) (width b)
  have hbound : ∀ j, m ≤ j → a.testBit j = b.testBit j := by
    intro j hj
    have ha : a.testBit j = false := Nat.testBit_lt_two_pow (by calc a < 2 ^ width a := lt_two_pow_width a; _ ≤ 2 ^ m := Nat.pow_le_pow_right (by omega) (Nat.le_max_left _ _); _ ≤ 2 ^ j := Nat.pow_le_pow_right (by omega) hj)
    have hb : b.testBit j = false := Nat.testBit_lt_two_pow (by calc b < 2 ^ width b := lt_two_pow_width b; _ ≤ 2 ^ m := Nat.pow_le_pow_right (by omega) (Nat.le_max_right _ _); _ ≤ 2 ^ j := Nat.pow_le_pow_right (by omega) hj)
    simp [ha, hb]
  suffices hs : ∀ k, k ≤ m → a.testBit (m - k) = b.testBit (m - k) by
    by_cases hik : i ≤ m
    · have := hs (m - i) (by omega)
      simpa [Nat.sub_sub_self hik] using this
    · exact hbound i (by omega)
  intro k hk
  induction k with
  | zero => exact hbound m (by omega)
  | succ k ih =>
    have ih' := ih (by omega)
    have hbit := congrArg (fun n => n.testBit (m - (k+1))) h
    rw [hg, hg] at hbit
    have heq1 : m - (k+1) + 1 = m - k := by omega
    rw [heq1] at hbit
    revert hbit ih'
    cases a.testBit (m - (k+1)) <;> cases b.testBit (m - (k+1)) <;> cases a.testBit (m-k) <;> cases b.testBit (m-k) <;> simp

private theorem key_gray_inj : Function.Injective GrayPair.key := by
  intro p q h
  simp only [GrayPair.key, Prod.mk.injEq] at h
  exact Prod.ext (gray_inj h.1) h.2

theorem sound_GrayPair_NeighborMask : GrayPair.NeighborMask.Goal := by
  intro rs x y
  simp only [GrayPair.NeighborMask.Goal]
  let s := physical GrayPair.key rs
  have hperm : s.Perm rs := physical_perm GrayPair.key rs
  have hmap : (s.map GrayPair.key).count (GrayPair.key (x,y)) = (rs.map GrayPair.key).count (GrayPair.key (x,y)) := by
    have hp := hperm.map GrayPair.key
    induction hp with
    | nil => rfl
    | cons x h ih => simp [List.count_cons, ih]
    | swap x y l => simp [List.count_cons]; omega
    | trans h1 h2 ih1 ih2 => exact ih1.trans ih2
  rw [hmap]
  have hcount : ∀ (l : List Row), (l.map GrayPair.key).count (GrayPair.key (x,y)) = l.count (x,y) := by
    intro l
    induction l with
    | nil => simp
    | cons z zs ih =>
      simp only [List.map_cons, List.count_cons, ih]
      by_cases hz : GrayPair.key z = GrayPair.key (x,y)
      · simp [hz, key_gray_inj hz]
      · have hzne : (z == (x,y)) = false := by
          by_contra h; simp at h; subst h; exact hz rfl
        simp [hz, hzne]
  exact hcount rs

theorem sound_GrayPair_AdjacentGray : GrayPair.AdjacentGray.Goal := by
  intro x y
  simp only [GrayPair.key]
  let v := (x ^^^ x / 2) ^^^ ((x + 1) ^^^ (x + 1) / 2)
  have hpos : v ≠ 0 := by
    intro h
    have heq : x ^^^ x / 2 = (x + 1) ^^^ (x + 1) / 2 := by
      have h1 : (x ^^^ x / 2) ^^^ ((x + 1) ^^^ (x + 1) / 2) = 0 := h
      apply Nat.eq_of_testBit_eq; intro i
      have := congrArg (fun n => n.testBit i) h1
      simp [Nat.testBit_xor] at this
      exact by cases h : x.testBit i <;> cases h2 : (x+1).testBit i <;> simp_all
    exact absurd (gray_inj heq) (by omega)
  let t := (x ^^^ (x + 1)).log2
  have ht_pos : x ^^^ (x + 1) ≠ 0 := by
    intro h; have : x = x + 1 := by apply Nat.eq_of_testBit_eq; intro i; have := congrArg (fun n => n.testBit i) h; simp [Nat.testBit_xor] at this; exact Bool.eq_of_bne_eq_false this; omega
  refine ⟨t, ?_⟩
  apply Nat.eq_of_testBit_eq
  intro i
  rw [Nat.testBit_two_pow]
  have hgb : ∀ n j, (n ^^^ n / 2).testBit j = (n.testBit j ^^ n.testBit (j+1)) := by
    intro n j; simp [Nat.testBit_xor, Nat.testBit_div_two_pow]
  have hv : v.testBit i = ((x.testBit i ^^ (x+1).testBit i) ^^ (x.testBit (i+1) ^^ (x+1).testBit (i+1))) := by
    simp [v, Nat.testBit_xor, hgb, Bool.xor_assoc, Bool.xor_comm, Bool.xor_left_comm]
  rw [hv]
  have hc : ∀ j, (x.testBit j ^^ (x+1).testBit j) = decide (j ≤ t) := by
    intro j
    have hx1 : (x ^^^ (x+1)).testBit j = (x.testBit j ^^ (x+1).testBit j) := by simp [Nat.testBit_xor]
    rw [← hx1]
    by_cases hj : j ≤ t
    · simp only [hj, decide_true]
      -- all bits 0..t set in x^^^(x+1)
      have hall : ∀ k, k ≤ (x ^^^ (x+1)).log2 → (x ^^^ (x+1)).testBit k = true := by
        intro k hk
        induction k with
        | zero =>
          simp [Nat.testBit_xor]
          have : (x+1).testBit 0 = ! x.testBit 0 := by simp [Nat.testBit, Nat.add_mod]
          simp [this]
        | succ k ih =>
          have ihk := ih (by omega)
          simp only [Nat.testBit_xor] at ihk ⊢
          by_contra hf2
          simp only [Nat.not_lt, Nat.not_le, not_not] at hf2
          have hf2b := Bool.eq_false_iff.mpr hf2
          have hnoflip : ∀ j', k+1 < j' → (x ^^^ (x+1)).testBit j' = false := by
            intro j' hj'
            have hcarry_stop : (x+1).testBit (k+1) = x.testBit (k+1) := Bool.eq_of_bne_eq_false hf2b
            have hge : ∀ j', k+1 ≤ j' → (x+1).testBit j' = x.testBit j' := by
              intro j' hj'
              induction j', hj' using Nat.le_induction with
              | base => exact hcarry_stop
              | succ j' hj' ih2 =>
                simp only [Nat.testBit, Nat.shiftRight_eq_div_pow] at ih2 ⊢
                have hprev := ih2
                omega
            simp [Nat.testBit_xor, hge j' (by omega)]
          have hlogle : (x ^^^ (x+1)).log2 ≤ k+1 := by
            by_contra hg; simp only [Nat.not_lt, Nat.not_le, not_not] at hg
            exact absurd (Nat.testBit_log2 ht_pos) (by rw [hnoflip _ hg]; simp)
          omega
      exact hall j hj
    · simp only [Nat.not_lt, Nat.not_le, not_not] at hj
      simp only [show ¬ j ≤ t by omega, decide_false]
      have hfalse : (x ^^^ (x+1)).testBit j = false := Nat.testBit_lt_two_pow (by
        calc x ^^^ (x+1) < 2 ^ ((x ^^^ (x+1)).log2 + 1) := Nat.lt_log2_self (Nat.pos_of_ne_zero ht_pos)
          _ ≤ 2 ^ j := Nat.pow_le_pow_right (by omega) (by omega))
      simp only [Nat.testBit_xor] at hfalse
      exact Bool.eq_of_bne_eq_false hfalse
  rw [hc i, hc (i+1)]
  by_cases hi : i = t
  · simp [hi]
  · by_cases hlt : i < t
    · have h1 : i ≤ t := by omega
      have h2 : i+1 ≤ t := by omega
      simp [h1, h2, hi]
    · have h1 : ¬ i ≤ t := by omega
      have h2 : ¬ i+1 ≤ t := by omega
      simp [h1, h2, hi]

-- DyadicTile
theorem sound_DyadicTile_TileBijection : DyadicTile.TileBijection.Goal := by
  intro x y a b
  simp only [DyadicTile.key]
  constructor
  · intro h
    obtain ⟨h1, h2⟩ := Prod.mk.inj h
    have hy : y % 16 = b % 16 := by omega
    have hx16 : x % 16 = a % 16 := by omega
    have hinj := @morton_inj (x/16, y/16) (a/16, b/16) h1
    simp at hinj
    obtain ⟨hxq, hyq⟩ := hinj
    have hx : x = a := by have := Nat.div_add_mod x 16; have := Nat.div_add_mod a 16; omega
    have hy' : y = b := by have := Nat.div_add_mod y 16; have := Nat.div_add_mod b 16; omega
    exact ⟨hx, hy'⟩
  · intro ⟨h1, h2⟩; subst h1; subst h2; rfl

end TinylambOpt.ResearchProofs.StorageExecution.ClusterOrder
