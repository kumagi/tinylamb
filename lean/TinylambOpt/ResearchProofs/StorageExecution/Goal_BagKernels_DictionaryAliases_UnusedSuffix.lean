import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `DictionaryAliases.UnusedSuffix` — `∀ d ids ext, (∀ i∈ids, i<d.length) → weighted bag
  (d++ext,ids) id = weighted bag (d,ids) id`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DictionaryAliases_UnusedSuffix

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.DictionaryAliases

private abbrev B := bag

/-! ## Generic list facts -/

private theorem sum_map_zero {α : Type} (l : List α) : (l.map (fun _ => 0)).sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem count_eq_one_of_mem_nodup {α : Type} [BEq α] [LawfulBEq α]
    {l : List α} {a : α} (hn : l.Nodup) (ha : a ∈ l) : l.count a = 1 := by
  induction l with
  | nil => cases ha
  | cons b bs ih =>
      rw [List.nodup_cons] at hn
      simp only [List.mem_cons] at ha
      rw [List.count_cons]
      by_cases hba : b = a
      · subst a
        simp [List.count_eq_zero_of_not_mem hn.1]
      · have ha' : a ∈ bs := by
          cases ha with
          | inl h => exact (hba h.symm).elim
          | inr h => exact h
        have hbeq : (b == a) = false := by simp [hba]
        simp [hbeq, ih hn.2 ha']

private theorem zipIdx_snd_nodup {α : Type} (l : List α) (n : Nat) :
    ((l.zipIdx n).map Prod.snd).Nodup := by
  induction l generalizing n with
  | nil => simp
  | cons x xs ih =>
      rw [List.zipIdx_cons, List.map_cons, List.nodup_cons]
      constructor
      · intro hmem
        rcases List.mem_map.mp hmem with ⟨e, he, heq⟩
        obtain ⟨y, i⟩ := e
        simp only at heq
        subst i
        obtain ⟨hlo, hi, hval⟩ := List.mem_zipIdx (k := n + 1) he
        omega
      · simpa using ih (n + 1)

private theorem idxs_nodup (d : List Nat) (k : Nat) :
    ((d.zipIdx.filter (fun e => e.1 == k)).map Prod.snd).Nodup := by
  have hsub : ((d.zipIdx.filter (fun e : Nat × Nat => e.1 == k)).map Prod.snd).Sublist
      (d.zipIdx.map Prod.snd) :=
    List.Sublist.map Prod.snd List.filter_sublist
  exact hsub.nodup (zipIdx_snd_nodup d 0)

private theorem mem_idxs_iff_getElem? (d : List Nat) (k a : Nat) :
    a ∈ ((d.zipIdx.filter (fun e => e.1 == k)).map Prod.snd) ↔ d[a]? = some k := by
  constructor
  · intro h
    rcases List.mem_map.mp h with ⟨e, he, heq⟩
    obtain ⟨x, i⟩ := e
    simp only at heq
    subst i
    have hf := List.mem_filter.mp he
    have hpred : (x == k) = true := hf.2
    have hzip := List.mem_zipIdx (k := 0) hf.1
    have ha : a < d.length := by simpa using hzip.2.1
    have hx : x = d[a] := by simpa using hzip.2.2
    have hxk : x = k := eq_of_beq hpred
    rw [List.getElem?_eq_getElem ha]
    rw [← hx, hxk]
  · intro hget
    obtain ⟨ha, hval⟩ := List.getElem?_eq_some_iff.mp hget
    apply List.mem_map.mpr
    refine ⟨(k, a), ?_, rfl⟩
    apply List.mem_filter.mpr
    constructor
    · have hzget : (d.zipIdx)[a]? = some (k, a) := by
        rw [List.getElem?_zipIdx]
        simp [List.getElem?_eq_getElem ha, hval]
      obtain ⟨hidx, hidxval⟩ := List.getElem?_eq_some_iff.mp hzget
      rw [← hidxval]
      exact List.getElem_mem hidx
    · simp

private theorem count_idxs_eq_getElem? (d : List Nat) (k a : Nat) :
    ((d.zipIdx.filter (fun e => e.1 == k)).map Prod.snd).count a =
      (decide (d[a]? == some k)).toNat := by
  by_cases h : d[a]? = some k
  · have hb : (d[a]? == some k) = true := by simp [h]
    simp [hb]
    apply count_eq_one_of_mem_nodup (idxs_nodup d k)
    exact (mem_idxs_iff_getElem? d k a).2 h
  · have hb : (d[a]? == some k) = false := by simp [h]
    simp [hb]
    apply List.count_eq_zero_of_not_mem
    intro ha
    exact h ((mem_idxs_iff_getElem? d k a).1 ha)

private theorem sum_count_cons_pair (l : List (Nat × Nat)) (a : Nat) (ids : List Nat) :
    (l.map (fun e => (a :: ids).count e.2)).sum =
      (l.map (fun e => ids.count e.2)).sum + (l.map Prod.snd).count a := by
  induction l with
  | nil => simp
  | cons e es ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      by_cases hia : a = e.2
      · subst a
        simp [List.count_cons_self]
        omega
      · have hne : e.2 ≠ a := by intro h; exact hia h.symm
        simp [hne, hia]
        omega

private theorem mass_cons_head (d ids : List Nat) (a k : Nat) :
    bag.mass (d, a :: ids) k = bag.mass (d, ids) k + (decide (d[a]? == some k)).toNat := by
  have hsum := sum_count_cons_pair (d.zipIdx.filter (fun e : Nat × Nat => e.1 == k)) a ids
  have hcnt := count_idxs_eq_getElem? d k a
  simpa [bag, hcnt] using hsum

/-- An in-bounds lookup into a dictionary is unaffected by an appended suffix. -/
private theorem getElem?_append_of_lt (d ext : List Nat) (i : Nat) (hi : i < d.length) :
    (d ++ ext)[i]? = d[i]? := by
  rw [List.getElem?_append]
  simp [hi]

/-- All ids stay in the original dictionary, so the mass kernel cannot see `ext`. -/
private theorem mass_append_eq (d ids ext : List Nat) (h : ∀ i ∈ ids, i < d.length) (k : Nat) :
    B.mass (d ++ ext, ids) k = B.mass (d, ids) k := by
  induction ids with
  | nil => simp [B, bag, sum_map_zero]
  | cons a ids ih =>
      have ha : a < d.length := h a (by simp)
      have hids : ∀ i ∈ ids, i < d.length := by
        intro i hi
        exact h i (by simp [hi])
      rw [mass_cons_head (d ++ ext) ids a k, mass_cons_head d ids a k, ih hids,
        getElem?_append_of_lt d ext a ha]

/-- A key absent from the dictionary has zero mass. -/
private theorem mass_zero_of_not_mem (d ids : List Nat) (k : Nat) (hk : k ∉ d) :
    B.mass (d, ids) k = 0 := by
  induction ids with
  | nil => simp [B, bag, sum_map_zero]
  | cons a ids ih =>
      rw [mass_cons_head, ih]
      have hget : d[a]? ≠ some k := by
        intro hg
        obtain ⟨ha, hval⟩ := List.getElem?_eq_some_iff.mp hg
        exact hk (by rw [← hval]; exact List.getElem_mem ha)
      have hb : (d[a]? == some k) = false := by simp [hget]
      simp [hb]

private theorem sum_map_zero_of_mem (l : List Nat) (F : Nat → Nat) (h : ∀ x ∈ l, F x = 0) :
    (l.map F).sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hx : F x = 0 := h x (by simp)
      have hxs : (xs.map F).sum = 0 := ih (by intro y hy; exact h y (by simp [hy]))
      simp [hx, hxs]

/-! ## Goal -/

theorem sound_UnusedSuffix : DictionaryAliases.UnusedSuffix.Goal := by
  intro d ids ext h
  change (((d ++ ext).eraseDups.map (fun k => B.mass (d ++ ext, ids) k * k)).sum) =
    ((d.eraseDups.map (fun k => B.mass (d, ids) k * k)).sum)
  have hmass : ∀ k, B.mass (d ++ ext, ids) k = B.mass (d, ids) k := by
    intro k
    exact mass_append_eq d ids ext h k
  simp only [hmass]
  rw [List.eraseDups_append, List.map_append, List.sum_append]
  have hzero : (((ext.removeAll d).eraseDups.map (fun k => B.mass (d, ids) k * k)).sum) = 0 := by
    apply sum_map_zero_of_mem
    intro k hk
    have hk' : k ∈ ext.removeAll d := List.mem_eraseDups.mp hk
    have hknot : k ∉ d := by
      simp [List.removeAll] at hk'
      exact hk'.2
    rw [mass_zero_of_not_mem d ids k hknot]
    simp
  rw [hzero, Nat.add_zero]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DictionaryAliases_UnusedSuffix
