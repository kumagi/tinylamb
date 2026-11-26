import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `MultiplicityRuns.SelfPairs` — Count unordered equal-key pairs without materializing
  a self-join.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_MultiplicityRuns_SelfPairs

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.MultiplicityRuns

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

private theorem nodup_eraseDups_filter (l : List Nat) (f : Nat → Bool) :
    (l.filter f).eraseDups.Nodup := by
  induction l generalizing f with
  | nil => simp
  | cons a t ih =>
      by_cases ha : f a = true
      · rw [List.filter_cons_of_pos ha, List.eraseDups_cons]
        apply List.nodup_cons.mpr
        constructor
        · simp
        · rw [List.filter_filter]
          exact ih (fun b => !b == a && f b)
      · rw [List.filter_cons_of_neg ha]
        exact ih f

private theorem nodup_eraseDups (l : List Nat) : l.eraseDups.Nodup := by
  have hf : List.filter (fun _ : Nat => true) l = l := by
    induction l with
    | nil => simp
    | cons a t ih => simp [ih]
  rw [← hf]
  exact nodup_eraseDups_filter l (fun _ => true)

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

-- Specialization to the run-length bag.

private theorem mass_eq_count (rs : List (Nat × Nat)) (k : Nat) :
    bag.mass rs k = (bag.expand rs).count k := by
  induction rs with
  | nil => simp [bag]
  | cons r t ih =>
      rcases r with ⟨x,n⟩
      rw [show bag.mass ((x,n)::t) k = (if x = k then n else 0) + bag.mass t k by
            by_cases h : x = k
            · subst k; simp [bag]
            · simp [bag, h]]
      rw [show (bag.expand ((x,n)::t)).count k =
            (if x = k then n else 0) + (bag.expand t).count k by
            simp [bag, List.count_replicate]]
      rw [ih]

private theorem mem_keys_of_mem_expand (rs : List (Nat × Nat)) (k : Nat)
    (hk : k ∈ bag.expand rs) : k ∈ rs.map Prod.fst := by
  induction rs with
  | nil => simp [bag] at hk
  | cons r t ih =>
      rcases r with ⟨x,n⟩
      simp only [bag, List.flatMap_cons] at hk
      rw [List.mem_append] at hk
      rcases hk with h | h
      · have hkx : k = x := List.eq_of_mem_replicate h
        subst k
        simp
      · simpa using (Or.inr (ih h) : k = x ∨ k ∈ t.map Prod.fst)

private theorem live_mem_iff (rs : List (Nat × Nat)) (k : Nat) :
    k ∈ live bag rs ↔ k ∈ (bag.expand rs).eraseDups := by
  simp only [live, List.mem_filter, List.mem_eraseDups]
  rw [mass_eq_count]
  constructor
  · rintro ⟨hkkeys, hkpos⟩
    have hkpos' : 0 < (bag.expand rs).count k := by simpa using hkpos
    by_cases hk : k ∈ bag.expand rs
    · exact hk
    · have hc0 : (bag.expand rs).count k = 0 := (List.count_eq_zero).mpr hk
      omega
  · intro hk
    constructor
    · have hkkeys := mem_keys_of_mem_expand rs k hk
      simpa [bag] using hkkeys
    · have hcpos : 0 < (bag.expand rs).count k := by
        have hcne : (bag.expand rs).count k ≠ 0 := by
          intro h
          exact (List.count_eq_zero.mp h) hk
        exact Nat.pos_of_ne_zero hcne
      simp [hcpos]

private theorem nodup_filter {l : List Nat} (hnd : l.Nodup) (f : Nat → Bool) :
    (l.filter f).Nodup := by
  induction l generalizing f with
  | nil => simp
  | cons a t ih =>
      rw [List.nodup_cons] at hnd
      rcases hnd with ⟨hanot, htnd⟩
      by_cases ha : f a = true
      · rw [List.filter_cons_of_pos ha]
        exact List.nodup_cons.mpr ⟨by
          intro h
          exact hanot (List.mem_filter.mp h).1, ih htnd f⟩
      · rw [List.filter_cons_of_neg ha]
        exact ih htnd f

private theorem live_nodup (rs : List (Nat × Nat)) : (live bag rs).Nodup := by
  simpa [live] using
    nodup_filter (nodup_eraseDups (bag.keys rs)) (fun k => decide (0 < bag.mass rs k))

private theorem perm_sum {l₁ l₂ : List Nat} (p : l₁.Perm l₂) (f : Nat → Nat) :
    (l₁.map f).sum = (l₂.map f).sum := by
  induction p with
  | nil => simp
  | cons x p ih => simpa using ih
  | swap x y l => simp [Nat.add_left_comm]
  | trans p₁ p₂ ih₁ ih₂ => exact ih₁.trans ih₂

private theorem not_mem_erase_of_nodup (a : Nat) {l : List Nat} (hnd : l.Nodup) :
    a ∉ l.erase a := by
  induction l with
  | nil => simp
  | cons b t ih =>
      rw [List.nodup_cons] at hnd
      rcases hnd with ⟨hbnot, htnd⟩
      by_cases hba : b = a
      · subst b
        simpa using hbnot
      · have hab : a ≠ b := by intro h; exact hba h.symm
        have hta : a ∉ t.erase a := ih htnd
        rw [List.erase_cons_tail (by simp [hba])]
        simp [hab, hta]

private theorem perm_of_nodup_mem {l₁ l₂ : List Nat} (h₁ : l₁.Nodup) (h₂ : l₂.Nodup)
    (hmem : ∀ a, a ∈ l₁ ↔ a ∈ l₂) : l₁.Perm l₂ := by
  induction l₁ generalizing l₂ with
  | nil =>
      have hnil : l₂ = [] := by
        rw [List.eq_nil_iff_forall_not_mem]
        intro a ha
        simpa using (hmem a).mpr ha
      rw [hnil]
  | cons a t ih =>
      have ha2 : a ∈ l₂ := (hmem a).mp (by simp)
      rw [List.nodup_cons] at h₁
      rcases h₁ with ⟨ha_not, ht_nd⟩
      have hp : l₂.Perm (a :: l₂.erase a) := List.perm_cons_erase ha2
      have htperm : t.Perm (l₂.erase a) := by
        apply ih ht_nd (List.Nodup.erase a h₂)
        intro b
        by_cases hba : b = a
        · subst b
          have hright : a ∉ l₂.erase a := not_mem_erase_of_nodup a h₂
          simp [ha_not, hright]
        · have hbmem := hp.mem_iff (a := b)
          simp [hba] at hbmem
          simpa [hba, hbmem] using hmem b
      exact (List.Perm.cons a htperm).trans hp.symm

theorem sound_SelfPairs : MultiplicityRuns.SelfPairs.Goal := by
  intro rs
  simp only [mass_eq_count]
  have hperm : (live bag rs).Perm (bag.expand rs).eraseDups :=
    perm_of_nodup_mem (live_nodup rs) (nodup_eraseDups (bag.expand rs)) (live_mem_iff rs)
  rw [perm_sum hperm (fun k => comb ((bag.expand rs).count k))]
  exact comb_count_identity (bag.expand rs)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_MultiplicityRuns_SelfPairs
