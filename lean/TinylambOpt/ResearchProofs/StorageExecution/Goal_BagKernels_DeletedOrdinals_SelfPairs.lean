import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DeletedOrdinals_SelfPairs

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.DeletedOrdinals

-- Truth was first checked on small inputs by kernel reduction (empty lists,
-- repeated keys, duplicate/out-of-range delete ordinals); all checks passed.

/-! ## Generic list facts -/

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

private theorem perm_sum {l₁ l₂ : List Nat} (p : l₁.Perm l₂) (f : Nat → Nat) :
    (l₁.map f).sum = (l₂.map f).sum := by
  induction p with
  | nil => simp
  | cons x p ih => simpa using ih
  | swap x y l => simp [Nat.add_left_comm]
  | trans p₁ p₂ ih₁ ih₂ => exact ih₁.trans ih₂

private theorem sum_map_congr {l : List Nat} {f g : Nat → Nat}
    (h : ∀ k ∈ l, f k = g k) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a t ih =>
      simp [h a (by simp), ih (fun k hk => h k (by simp [hk]))]

private theorem sum_map_eq_update {l : List Nat} (hnd : l.Nodup) (a c : Nat)
    (f g : Nat → Nat) (ha : a ∈ l)
    (hf : ∀ k, k ∈ l → k ≠ a → f k = g k) (hfa : f a = g a + c) :
    (l.map f).sum = (l.map g).sum + c := by
  induction l with
  | nil => simp at ha
  | cons b t ih =>
      rw [List.nodup_cons] at hnd
      rcases hnd with ⟨hbnot, htnd⟩
      simp at ha
      rcases ha with rfl | hat
      · have hcong : ∀ k ∈ t, f k = g k := by
          intro k hk
          apply hf k (by simp [hk])
          intro hka
          apply hbnot
          simpa [hka] using hk
        rw [List.map_cons, List.map_cons, List.sum_cons, List.sum_cons, hfa,
          sum_map_congr hcong]
        omega
      · have hbne : b ≠ a := by
          intro h
          subst b
          exact hbnot hat
        have hhead : f b = g b := hf b (by simp) hbne
        have htail := ih htnd hat (fun k hk hka => hf k (by simp [hk]) hka)
        simp [hhead, htail, Nat.add_assoc]

/-! ## The counting identity `comb ∘ count` = suffix-pair count -/

private theorem comb_succ (n : Nat) : comb (n + 1) = comb n + n := by
  unfold comb
  simp only [Nat.add_sub_cancel]
  have h1 : n * (n - 1) = n * n - n := by
    rw [Nat.mul_sub_left_distrib]
    simp
  have h2 : (n + 1) * n = n * n + n := by
    rw [Nat.add_mul]
    simp
  have hnn : n ≤ n * n := by
    cases n with
    | zero => simp
    | succ m =>
        rw [Nat.succ_mul]
        omega
  have hmul : (n + 1) * n = n * (n - 1) + n * 2 := by omega
  rw [hmul]
  exact Nat.add_mul_div_right (n * (n - 1)) n (by decide : 0 < 2)

private def pairSum (d : Nat) : List Nat → Nat
  | [] => 0
  | a :: l => (l.drop d).count a + pairSum d l

private theorem pairSum_append_singleton (l : List Nat) (a : Nat) :
    pairSum 0 (l ++ [a]) = pairSum 0 l + l.count a := by
  induction l with
  | nil => simp [pairSum]
  | cons b t ih =>
      simp [pairSum, ih, List.count_append, List.count_cons,
        Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
      by_cases h : a = b <;> simp [h, eq_comm]

private theorem zipIdx_add (l : List Nat) (d : Nat) :
    l.zipIdx d = (l.zipIdx).map (fun p => (p.1, p.2 + d)) := by
  induction l generalizing d with
  | nil => simp
  | cons a t ih =>
      rw [List.zipIdx_cons (i := d), List.zipIdx_cons (i := 0)]
      simp only [List.map_cons]
      congr
      · simp
      · rw [ih (d + 1), ih 1]
        simp only [List.map_map]
        congr 1
        funext p
        cases p with
        | mk x j => ac_rfl

private theorem pairSum_eq_rhs (l : List Nat) :
    pairSum 0 l = (l.zipIdx.map (fun (x,i) => (l.drop (i+1)).count x)).sum := by
  induction l with
  | nil => simp [pairSum]
  | cons a t ih =>
      rw [List.zipIdx_cons]
      rw [zipIdx_add]
      simp [pairSum, List.drop_succ_cons, ih, Function.comp_def, List.map_map]

private theorem singleton_removeAll_nil (l : List Nat) (a : Nat) (ha : a ∈ l) :
    [a].removeAll l = [] := by
  induction l with
  | nil => simp at ha
  | cons b t ih =>
      by_cases hba : b = a
      · subst b
        simp [List.removeAll_cons]
      · have hab : a ≠ b := by intro h; exact hba h.symm
        rw [List.removeAll_cons]
        have hfil : List.filter (fun x => !x == b) [a] = [a] := by simp [hab]
        rw [hfil]
        exact ih (by
          simp at ha
          rcases ha with h | h
          · exact (hab h).elim
          · exact h)

private theorem singleton_removeAll_self (l : List Nat) (a : Nat) (ha : a ∉ l) :
    [a].removeAll l = [a] := by
  induction l with
  | nil => simp
  | cons b t ih =>
      by_cases hba : b = a
      · subst b
        simp at ha
      · have hab : a ≠ b := by intro h; exact hba h.symm
        rw [List.removeAll_cons]
        have hfil : List.filter (fun x => !x == b) [a] = [a] := by simp [hab]
        rw [hfil]
        exact ih (by intro h; apply ha; simp [h])

private theorem eraseDups_append_singleton_of_mem (l : List Nat) (a : Nat) (ha : a ∈ l) :
    (l ++ [a]).eraseDups = l.eraseDups := by
  rw [List.eraseDups_append, singleton_removeAll_nil l a ha]
  simp

private theorem eraseDups_append_singleton_of_not_mem (l : List Nat) (a : Nat) (ha : a ∉ l) :
    (l ++ [a]).eraseDups = l.eraseDups ++ [a] := by
  rw [List.eraseDups_append, singleton_removeAll_self l a ha]
  simp [List.eraseDups_cons]

private theorem combSum_append_singleton (l : List Nat) (a : Nat) :
    (((l ++ [a]).eraseDups.map (fun k => comb ((l ++ [a]).count k))).sum) =
    ((l.eraseDups.map (fun k => comb (l.count k))).sum) + l.count a := by
  by_cases ha : a ∈ l
  · rw [eraseDups_append_singleton_of_mem l a ha]
    have hmem : a ∈ l.eraseDups := by simpa using ha
    have hc_a : (l ++ [a]).count a = l.count a + 1 := by simp [List.count_append]
    have hfa : comb ((l ++ [a]).count a) = comb (l.count a) + l.count a := by
      rw [hc_a, comb_succ]
    have hf : ∀ k, k ∈ l.eraseDups → k ≠ a →
        comb ((l ++ [a]).count k) = comb (l.count k) := by
      intro k hk hne
      have hne' : ¬ a = k := by intro h; exact hne h.symm
      have hc_k : (l ++ [a]).count k = l.count k := by
        simp [List.count_append, hne']
      rw [hc_k]
    exact sum_map_eq_update (nodup_eraseDups l) a (l.count a)
      (fun k => comb ((l ++ [a]).count k)) (fun k => comb (l.count k)) hmem hf hfa
  · have hc0 : l.count a = 0 := (List.count_eq_zero).mpr ha
    rw [eraseDups_append_singleton_of_not_mem l a ha, List.map_append, List.sum_append]
    have hcong : ∀ k ∈ l.eraseDups,
        comb ((l ++ [a]).count k) = comb (l.count k) := by
      intro k hk
      have hkl : k ∈ l := by simpa using hk
      have hka : k ≠ a := by intro h; subst k; exact ha hkl
      have hka' : ¬ a = k := by intro h; exact hka h.symm
      have hc_k : (l ++ [a]).count k = l.count k := by
        simp [List.count_append, hka']
      rw [hc_k]
    rw [sum_map_congr hcong]
    simp [hc0, comb]

private theorem comb_count_identity (l : List Nat) :
    ((l.eraseDups.map (fun k => comb (l.count k))).sum) =
    (l.zipIdx.map (fun (x,i) => (l.drop (i+1)).count x)).sum := by
  rw [← pairSum_eq_rhs]
  have main : ∀ n : Nat, ∀ l : List Nat, l.length = n →
      ((l.eraseDups.map (fun k => comb (l.count k))).sum) = pairSum 0 l := by
    intro n
    refine Nat.strongRecOn n ?_
    intro n ih l hlen
    by_cases hnil : l = []
    · subst l
      simp [pairSum]
    · let a := l.getLast hnil
      let t := l.dropLast
      have hdecomp : t ++ [a] = l := List.dropLast_concat_getLast hnil
      have htlen : t.length < n := by
        rw [← hlen]
        have hpos : 0 < l.length := by
          rw [List.length_pos_iff_exists_cons]
          exact List.exists_cons_of_ne_nil hnil
        rw [List.length_dropLast]
        omega
      have ih' := ih t.length htlen t rfl
      rw [← hdecomp, combSum_append_singleton, ih', pairSum_append_singleton]
  exact main l.length l rfl

/-! ## The deleted-ordinal bag: mass = count of live physical rows -/

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

private theorem live_perm_expand (xs ds : List Nat) :
    (live bag (xs, ds)).Perm (bag.expand (xs, ds)).eraseDups := by
  apply perm_of_nodup_mem_iff (nodup_live (xs, ds)) (nodup_eraseDups (bag.expand (xs, ds)))
  intro k
  constructor
  · intro hk
    have hm : 0 < bag.mass (xs, ds) k := ((mem_live_iff (xs, ds) k).1 hk).1
    rw [List.mem_eraseDups, ← List.count_pos_iff, count_expand_eq_mass]
    exact hm
  · intro hk
    rw [List.mem_eraseDups] at hk
    have hm : 0 < bag.mass (xs, ds) k := by
      rw [← count_expand_eq_mass]
      exact List.count_pos_iff.mpr hk
    exact (mem_live_iff (xs, ds) k).2 ⟨hm, mem_keys_of_mass_pos xs ds k hm⟩

theorem sound_SelfPairs : DeletedOrdinals.SelfPairs.Goal := by
  intro p
  obtain ⟨xs, ds⟩ := p
  rw [perm_sum (live_perm_expand xs ds) (fun k => comb (bag.mass (xs, ds) k))]
  have hcong : ∀ k ∈ (bag.expand (xs, ds)).eraseDups,
      comb (bag.mass (xs, ds) k) = comb ((bag.expand (xs, ds)).count k) := by
    intro k hk
    rw [count_expand_eq_mass]
  rw [sum_map_congr hcong]
  exact comb_count_identity (bag.expand (xs, ds))

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DeletedOrdinals_SelfPairs
