import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DeletedOrdinals_DistinctOutput

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.DeletedOrdinals

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

private theorem perm_of_nodup_mem_iff {α : Type} [BEq α] [LawfulBEq α]
    {l₁ l₂ : List α} (h₁ : l₁.Nodup) (h₂ : l₂.Nodup)
    (hm : ∀ a, a ∈ l₁ ↔ a ∈ l₂) : l₁.Perm l₂ := by
  rw [List.perm_iff_count]
  intro a
  by_cases h : a ∈ l₁
  · rw [count_eq_one_of_mem_nodup h₁ h,
        count_eq_one_of_mem_nodup h₂ ((hm a).1 h)]
  · rw [List.count_eq_zero_of_not_mem h,
        List.count_eq_zero_of_not_mem (fun ha => h ((hm a).2 ha))]

private theorem contains_true_iff_mem (l : List Nat) (a : Nat) : l.contains a = true ↔ a ∈ l := by
  induction l with
  | nil => simp
  | cons b bs ih => by_cases h : b = a <;> simp [h]

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

private def idxs (xs : List Nat) (k : Nat) : List Nat :=
  ((xs.zipIdx.filter (fun e => e.1 == k)).map Prod.snd)

private theorem idxs_nodup (xs : List Nat) (k : Nat) : (idxs xs k).Nodup := by
  have hsub : (idxs xs k).Sublist ((xs.zipIdx).map Prod.snd) :=
    List.Sublist.map Prod.snd List.filter_sublist
  exact hsub.nodup (zipIdx_snd_nodup xs 0)

private theorem mem_idxs_iff_getElem? (xs : List Nat) (k a : Nat) :
    a ∈ idxs xs k ↔ xs[a]? = some k := by
  unfold idxs
  constructor
  · intro h
    rcases List.mem_map.mp h with ⟨e, he, heq⟩
    obtain ⟨y, i⟩ := e
    simp only at heq
    subst i
    have hf := List.mem_filter.mp he
    have hpred : (y == k) = true := hf.2
    have hzip := List.mem_zipIdx (xs := xs) (k := 0) hf.1
    have ha : a < xs.length := by simpa using hzip.2.1
    have hy : y = xs[a] := by simpa using hzip.2.2
    have hyk : y = k := eq_of_beq hpred
    rw [List.getElem?_eq_getElem ha]
    rw [← hy, hyk]
  · intro hget
    obtain ⟨ha, hval⟩ := List.getElem?_eq_some_iff.mp hget
    apply List.mem_map.mpr
    refine ⟨(k, a), ?_, rfl⟩
    apply List.mem_filter.mpr
    constructor
    · have hzget : (xs.zipIdx)[a]? = some (k, a) := by
        rw [List.getElem?_zipIdx]
        simp [List.getElem?_eq_getElem ha, hval]
      obtain ⟨hidx, hidxval⟩ := List.getElem?_eq_some_iff.mp hzget
      rw [← hidxval]
      exact List.getElem_mem hidx
    · simp

private theorem zipIdx_filter_fst (xs : List Nat) (n k : Nat) :
    ((xs.zipIdx n).filter (fun e => e.1 == k)).map Prod.fst = xs.filter (fun x => x == k) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      rw [List.zipIdx_cons]
      by_cases h : a == k <;> simp [h, ih]

private theorem idxs_length (xs : List Nat) (k : Nat) : (idxs xs k).length = xs.count k := by
  have h := congrArg List.length (zipIdx_filter_fst xs 0 k)
  simpa [idxs, List.count_eq_length_filter] using h

private theorem mem_zipIdx_filter_snd_iff (xs ds : List Nat) (k j : Nat) :
    j ∈ ((xs.zipIdx.filter (fun e => !ds.contains e.2 && e.1 == k)).map Prod.snd) ↔
      j ∈ (idxs xs k).filter (fun i => !ds.contains i) := by
  constructor
  · intro hj
    rcases List.mem_map.mp hj with ⟨e, he, heq⟩
    obtain ⟨v, i⟩ := e
    simp only at heq
    subst i
    have hf := List.mem_filter.mp he
    have hp : (!ds.contains j && (v == k)) = true := by simpa only using hf.2
    have hjnotds : j ∉ ds := by
      have hn : (!ds.contains j) = true := (Bool.and_eq_true_iff.mp hp).1
      simpa using hn
    have hvk : v = k := eq_of_beq (Bool.and_eq_true_iff.mp hp).2
    have hzip := List.mem_zipIdx (xs := xs) (k := 0) hf.1
    have hjlen : j < xs.length := by simpa using hzip.2.1
    have hvget : v = xs[j] := by simpa using hzip.2.2
    exact List.mem_filter.mpr ⟨(mem_idxs_iff_getElem? xs k j).2 (by
      rw [List.getElem?_eq_getElem hjlen, ← hvget, hvk]), by simpa using hjnotds⟩
  · intro hj
    have hf := List.mem_filter.mp hj
    have hidx := (mem_idxs_iff_getElem? xs k j).1 hf.1
    obtain ⟨hjlen, hjval⟩ := List.getElem?_eq_some_iff.mp hidx
    have hjnotds : j ∉ ds := by simpa using hf.2
    have hnot : ds.contains j = false := by simpa using hjnotds
    apply List.mem_map.mpr
    refine ⟨(k, j), ?_, rfl⟩
    apply List.mem_filter.mpr
    constructor
    · have hzget : (xs.zipIdx)[j]? = some (k, j) := by
        rw [List.getElem?_zipIdx]
        simp [List.getElem?_eq_getElem hjlen, hjval]
      obtain ⟨hidx', hidxval⟩ := List.getElem?_eq_some_iff.mp hzget
      rw [← hidxval]
      exact List.getElem_mem hidx'
    · simpa [hnot]

private theorem mem_ds_filter_iff (xs ds : List Nat) (k j : Nat) :
    j ∈ (ds.eraseDups.filter (fun i => xs[i]? == some k)) ↔
      j ∈ (idxs xs k).filter (fun i => ds.contains i) := by
  constructor
  · intro hj
    have hf := List.mem_filter.mp hj
    have hjds : j ∈ ds := (List.mem_eraseDups).1 hf.1
    apply List.mem_filter.mpr
    constructor
    · exact (mem_idxs_iff_getElem? xs k j).2 (by simpa using hf.2)
    · exact (contains_true_iff_mem ds j).2 hjds
  · intro hj
    have hf := List.mem_filter.mp hj
    have hidx := (mem_idxs_iff_getElem? xs k j).1 hf.1
    have hjds : j ∈ ds := (contains_true_iff_mem ds j).1 hf.2
    exact List.mem_filter.mpr ⟨(List.mem_eraseDups).2 hjds, by simp [hidx]⟩

private theorem count_map_fst_filter (l : List (Nat × Nat)) (P : Nat × Nat → Bool) (k : Nat) :
    ((l.filter P).map Prod.fst).count k = (l.filter (fun e => P e && e.1 == k)).length := by
  induction l with
  | nil => simp
  | cons e es ih =>
      by_cases hp : P e = true
      · have hfilter : (e :: es).filter P = e :: es.filter P := by simp [hp]
        rw [hfilter, List.map_cons, List.count_cons, ih]
        by_cases hk : (e.1 == k) = true <;> simp [hp, hk]
      · have hfilter : (e :: es).filter P = es.filter P := by simp [hp]
        rw [hfilter, ih]
        simp [hp]

private theorem countP_not_add (l : List Nat) (p : Nat → Bool) :
    l.countP (fun x => !p x) + l.countP p = l.length := by
  induction l with
  | nil => simp
  | cons a as ih =>
      by_cases h : p a = true <;> simp [h] <;> omega

private theorem count_expand_eq_idxs_filter (xs ds : List Nat) (k : Nat) :
    (bag.expand (xs, ds)).count k = ((idxs xs k).filter (fun i => !ds.contains i)).length := by
  change (((xs.zipIdx.filter (fun e => !ds.contains e.2)).map Prod.fst).count k) = _
  rw [count_map_fst_filter]
  have hperm : ((xs.zipIdx.filter (fun e => !ds.contains e.2 && e.1 == k)).map Prod.snd).Perm
      ((idxs xs k).filter (fun i => !ds.contains i)) := by
    apply perm_of_nodup_mem_iff
    · have hsub : ((xs.zipIdx.filter (fun e => !ds.contains e.2 && e.1 == k)).map Prod.snd).Sublist
          ((xs.zipIdx).map Prod.snd) :=
        List.Sublist.map Prod.snd List.filter_sublist
      exact hsub.nodup (zipIdx_snd_nodup xs 0)
    · exact (idxs_nodup xs k).filter _
    · exact mem_zipIdx_filter_snd_iff xs ds k
  rw [← hperm.length_eq]
  simp

private theorem count_del_eq_idxs_filter (xs ds : List Nat) (k : Nat) :
    ds.eraseDups.countP (fun i => xs[i]? == some k) =
      ((idxs xs k).filter (fun i => ds.contains i)).length := by
  have hperm : (ds.eraseDups.filter (fun i => xs[i]? == some k)).Perm
      ((idxs xs k).filter (fun i => ds.contains i)) := by
    apply perm_of_nodup_mem_iff
    · exact (nodup_eraseDups ds).filter _
    · exact (idxs_nodup xs k).filter _
    · exact mem_ds_filter_iff xs ds k
  rw [List.countP_eq_length_filter]
  exact hperm.length_eq

private theorem count_expand_eq_mass (xs ds : List Nat) (k : Nat) :
    (bag.expand (xs, ds)).count k = bag.mass (xs, ds) k := by
  rw [count_expand_eq_idxs_filter]
  change ((idxs xs k).filter (fun i => !ds.contains i)).length =
    xs.count k - ds.eraseDups.countP (fun i => xs[i]? == some k)
  rw [← idxs_length, count_del_eq_idxs_filter]
  have hnotfilter : ((idxs xs k).filter (fun i => !ds.contains i)).length =
      (idxs xs k).countP (fun i => !ds.contains i) := by
    rw [← List.countP_eq_length_filter]
  have hyesfilter : ((idxs xs k).filter (fun i => ds.contains i)).length =
      (idxs xs k).countP (fun i => ds.contains i) := by
    rw [← List.countP_eq_length_filter]
  rw [hnotfilter, hyesfilter]
  have h := countP_not_add (idxs xs k) (fun i => ds.contains i)
  omega

private theorem mem_keys_of_mass_pos (xs ds : List Nat) (k : Nat)
    (h : 0 < bag.mass (xs, ds) k) : k ∈ bag.keys (xs, ds) := by
  change k ∈ xs
  have hpos : 0 < (bag.expand (xs, ds)).count k := by
    simpa [count_expand_eq_mass] using h
  have hmem : k ∈ bag.expand (xs, ds) := List.count_pos_iff.mp hpos
  change k ∈ (xs.zipIdx.filter (fun e => !ds.contains e.2)).map Prod.fst at hmem
  rcases List.mem_map.mp hmem with ⟨e, he, heq⟩
  obtain ⟨v, i⟩ := e
  simp only at heq
  have hzip := List.mem_zipIdx (xs := xs) (k := 0) (List.mem_filter.mp he).1
  have hvi : v = xs[i] := by simpa using hzip.2.2
  have hki : k = xs[i] := heq.symm.trans hvi
  rw [hki]
  exact List.getElem_mem (by simpa using hzip.2.1)

private theorem mem_live_iff (p : List Nat × List Nat) (k : Nat) :
    k ∈ live bag p ↔ 0 < bag.mass p k ∧ k ∈ bag.keys p := by
  unfold live
  simp [List.mem_filter, List.mem_eraseDups, and_comm]

private theorem nodup_live (p : List Nat × List Nat) : (live bag p).Nodup := by
  unfold live
  exact List.Pairwise.filter _ (nodup_eraseDups (bag.keys p))

theorem sound_DistinctOutput : DeletedOrdinals.DistinctOutput.Goal := by
  intro p
  obtain ⟨xs, ds⟩ := p
  rw [List.perm_iff_count]
  intro k
  by_cases h : 0 < bag.mass (xs, ds) k
  · have hl : k ∈ live bag (xs, ds) :=
      (mem_live_iff (xs, ds) k).2 ⟨h, mem_keys_of_mass_pos xs ds k h⟩
    have he : k ∈ (bag.expand (xs, ds)).eraseDups := by
      rw [List.mem_eraseDups, ← List.count_pos_iff, count_expand_eq_mass]
      exact h
    rw [count_eq_one_of_mem_nodup (nodup_live (xs, ds)) hl,
        count_eq_one_of_mem_nodup (nodup_eraseDups (bag.expand (xs, ds))) he]
  · have hl : k ∉ live bag (xs, ds) := by
      intro hk
      exact h ((mem_live_iff (xs, ds) k).1 hk).1
    have he : k ∉ (bag.expand (xs, ds)).eraseDups := by
      intro hk
      rw [List.mem_eraseDups] at hk
      exact h (by simpa [count_expand_eq_mass] using (List.count_pos_iff.mpr hk))
    rw [List.count_eq_zero_of_not_mem hl, List.count_eq_zero_of_not_mem he]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DeletedOrdinals_DistinctOutput
