import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DictionaryAliases_DistinctOutput

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.DictionaryAliases

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

private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] (l : List α) :
    l.eraseDups.Nodup := by
  let motive : Nat → Prop := fun n => ∀ l : List α, l.length = n → l.eraseDups.Nodup
  have h : motive l.length := Nat.strongRecOn l.length (fun n ih l hl => by
    cases l with
    | nil => simp [List.eraseDups_nil]
    | cons b bs =>
        rw [List.eraseDups_cons, List.nodup_cons]
        constructor
        · intro hb
          rw [List.mem_eraseDups] at hb
          have hb' := (List.mem_filter.mp hb).2
          simp at hb'
        · apply ih (bs.filter (fun x => !x == b)).length
          · have hle := List.length_filter_le (fun x => !x == b) bs
            simp only [List.length_cons] at hl
            omega
          · rfl)
  exact h l rfl

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

private theorem decide_toNat_eq_if (p : Prop) [Decidable p] :
    (decide p).toNat = if p then 1 else 0 := by
  by_cases hp : p <;> simp [hp]

private theorem sum_map_zero {α : Type} (l : List α) : (l.map (fun _ => 0)).sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem count_expand_eq_mass (d ids : List Nat) (k : Nat) :
    (bag.expand (d, ids)).count k = bag.mass (d, ids) k := by
  induction ids with
  | nil =>
      change (0 : Nat) =
        ((d.zipIdx.filter (fun e : Nat × Nat => e.1 == k)).map (fun _ => 0)).sum
      exact (sum_map_zero _).symm
  | cons a ids ih =>
      change ((a :: ids).filterMap (fun i => d[i]?)).count k = bag.mass (d, a :: ids) k
      rw [List.count_filterMap, List.countP_cons]
      have hih : ids.countP (fun i => d[i]? == some k) = bag.mass (d, ids) k := by
        simpa [bag, List.count_filterMap] using ih
      rw [hih, mass_cons_head, decide_toNat_eq_if]

private theorem mem_keys_of_mass_pos (d ids : List Nat) (k : Nat)
    (h : 0 < bag.mass (d, ids) k) : k ∈ bag.keys (d, ids) := by
  induction ids with
  | nil =>
      change (0 : Nat) < ((d.zipIdx.filter (fun e : Nat × Nat => e.1 == k)).map (fun _ => 0)).sum at h
      rw [sum_map_zero] at h
      omega
  | cons a ids ih =>
      rw [mass_cons_head] at h
      by_cases hk : d[a]? == some k
      · change k ∈ d
        have hget : d[a]? = some k := by simpa using hk
        obtain ⟨ha, hval⟩ := List.getElem?_eq_some_iff.mp hget
        rw [← hval]
        exact List.getElem_mem ha
      · have hpos : 0 < bag.mass (d, ids) k := by
          simp [hk] at h
          omega
        exact ih hpos

private theorem mem_live_iff (p : List Nat × List Nat) (k : Nat) :
    k ∈ live bag p ↔ 0 < bag.mass p k ∧ k ∈ bag.keys p := by
  unfold live
  simp [List.mem_filter, List.mem_eraseDups, and_comm]

private theorem nodup_live (p : List Nat × List Nat) : (live bag p).Nodup := by
  unfold live
  exact List.Pairwise.filter _ (nodup_eraseDups (bag.keys p))

theorem sound_DistinctOutput : DictionaryAliases.DistinctOutput.Goal := by
  intro p
  obtain ⟨d, ids⟩ := p
  rw [List.perm_iff_count]
  intro k
  by_cases h : 0 < bag.mass (d, ids) k
  · have hl : k ∈ live bag (d, ids) :=
      (mem_live_iff (d, ids) k).2 ⟨h, mem_keys_of_mass_pos d ids k h⟩
    have he : k ∈ (bag.expand (d, ids)).eraseDups := by
      rw [List.mem_eraseDups, ← List.count_pos_iff, count_expand_eq_mass]
      exact h
    rw [count_eq_one_of_mem_nodup (nodup_live (d, ids)) hl,
        count_eq_one_of_mem_nodup (nodup_eraseDups (bag.expand (d, ids))) he]
  · have hl : k ∉ live bag (d, ids) := by
      intro hk
      exact h ((mem_live_iff (d, ids) k).1 hk).1
    have he : k ∉ (bag.expand (d, ids)).eraseDups := by
      intro hk
      rw [List.mem_eraseDups] at hk
      exact h (by simpa [count_expand_eq_mass] using (List.count_pos_iff.mpr hk))
    rw [List.count_eq_zero_of_not_mem hl, List.count_eq_zero_of_not_mem he]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DictionaryAliases_DistinctOutput
