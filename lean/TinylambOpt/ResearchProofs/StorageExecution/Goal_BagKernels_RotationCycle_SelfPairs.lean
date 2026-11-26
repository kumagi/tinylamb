import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RotationCycle_SelfPairs

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.RotationCycle

private theorem decide_toNat_eq (P : Prop) [Decidable P] : (decide P).toNat = if P then 1 else 0 := by
  by_cases h : P <;> simp [h]

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

-- rotation arithmetic

private theorem rotation_mod_iff (m s k i : Nat) (hk : k ≤ m) (hi : i < m + 1) :
    (s + i) % (m + 1) = k ↔
      (k + (m + 1) - s % (m + 1)) % (m + 1) = i := by
  have hm : 0 < m + 1 := by omega
  have hklt : k < m + 1 := by omega
  have hs : s % (m + 1) < m + 1 := Nat.mod_lt _ hm
  have hsi : (s + i) % (m + 1) = (s % (m + 1) + i) % (m + 1) := by
    calc
      (s + i) % (m + 1) = (s % (m + 1) + i % (m + 1)) % (m + 1) :=
        Nat.add_mod s i (m + 1)
      _ = (s % (m + 1) + i) % (m + 1) := by rw [Nat.mod_eq_of_lt hi]
  constructor
  · intro h
    have hEq : (s % (m + 1) + i) % (m + 1) = k := by
      rw [hsi] at h
      exact h
    by_cases hlt : s % (m + 1) + i < m + 1
    · have hk_eq : s % (m + 1) + i = k := by
        simpa [Nat.mod_eq_of_lt hlt] using hEq
      have hsub : k + (m + 1) - s % (m + 1) = i + (m + 1) := by omega
      rw [hsub]
      calc
        (i + (m + 1)) % (m + 1) =
            (i % (m + 1) + (m + 1) % (m + 1)) % (m + 1) :=
          Nat.add_mod i (m + 1) (m + 1)
        _ = i := by simp [Nat.mod_eq_of_lt hi]
    · have hge : m + 1 ≤ s % (m + 1) + i := by omega
      have hmod_eq : (s % (m + 1) + i) % (m + 1) =
          s % (m + 1) + i - (m + 1) := by
        rw [Nat.mod_eq_sub_mod hge]
        exact Nat.mod_eq_of_lt (by omega)
      have hk_eq : s % (m + 1) + i - (m + 1) = k := by
        simpa [hmod_eq] using hEq
      have hsub : k + (m + 1) - s % (m + 1) = i := by omega
      rw [hsub]
      exact Nat.mod_eq_of_lt hi
  · intro hd
    by_cases hle : s % (m + 1) ≤ k
    · have hge : m + 1 ≤ k + (m + 1) - s % (m + 1) := by omega
      have hmod_eq : (k + (m + 1) - s % (m + 1)) % (m + 1) =
          k - s % (m + 1) := by
        rw [Nat.mod_eq_sub_mod hge]
        have hsub : k + (m + 1) - s % (m + 1) - (m + 1) = k - s % (m + 1) := by omega
        rw [hsub]
        exact Nat.mod_eq_of_lt (by omega)
      have hki : k - s % (m + 1) = i := by simpa [hmod_eq] using hd
      have hk_eq : k = s % (m + 1) + i := by omega
      have hsumlt : s % (m + 1) + i < m + 1 := by omega
      rw [hsi, hk_eq, Nat.mod_eq_of_lt hsumlt]
    · have hlt : k + (m + 1) - s % (m + 1) < m + 1 := by omega
      have hsum_eq : s % (m + 1) + i = k + (m + 1) := by
        have : k + (m + 1) - s % (m + 1) = i := by
          simpa [Nat.mod_eq_of_lt hlt] using hd
        omega
      have hge : m + 1 ≤ s % (m + 1) + i := by omega
      have hmod_eq : (s % (m + 1) + i) % (m + 1) =
          s % (m + 1) + i - (m + 1) := by
        rw [Nat.mod_eq_sub_mod hge]
        exact Nat.mod_eq_of_lt (by omega)
      rw [hsi, hmod_eq]
      omega

private theorem rotation_mod_iff_all (m s k i : Nat) (hk : k ≤ m) :
    (s + i) % (m + 1) = k ↔
      i % (m + 1) = (k + (m + 1) - s % (m + 1)) % (m + 1) := by
  have hm : 0 < m + 1 := by omega
  have hi : i % (m + 1) < m + 1 := Nat.mod_lt _ hm
  have hsi : (s + i) % (m + 1) = (s + i % (m + 1)) % (m + 1) := by
    calc
      (s + i) % (m + 1) = (s % (m + 1) + i % (m + 1)) % (m + 1) := Nat.add_mod s i (m + 1)
      _ = (s + i % (m + 1)) % (m + 1) := by
        rw [Nat.add_mod s (i % (m + 1)) (m + 1)]
        simp
  rw [hsi]
  constructor
  · intro h
    exact ((rotation_mod_iff m s k (i % (m + 1)) hk hi).1 h).symm
  · intro h
    exact (rotation_mod_iff m s k (i % (m + 1)) hk hi).2 h.symm

-- residue counting lemmas (from Atlas_EncodedPages_G2/G3)

private theorem div_eq_one_of_bounds (t p : Nat) (hp : 0 < p)
    (hge : p ≤ t) (hlt : t < 2*p) : t / p = 1 := by
  have hlt2 : t / p < 2 := (Nat.div_lt_iff_lt_mul hp).mpr hlt
  have hnle : ¬ t / p ≤ 0 := by
    intro h
    have hh := (Nat.div_le_iff_le_mul hp).mp h
    omega
  omega

private theorem mod_step_count (n p r : Nat) (hp : 0 < p) (hr : r < p) :
    (n - r + p - 1) / p = n / p + if r < n % p then 1 else 0 := by
  have hd := Nat.div_add_mod n p
  have hs := Nat.mod_lt n hp
  by_cases hnr : n < r
  · have hdiv : n / p = 0 := Nat.div_eq_of_lt (by omega)
    have hm : n - r + p - 1 = p - 1 := by omega
    have hnot : ¬ r < n % p := by omega
    have hpdiv : (p - 1) / p = 0 := Nat.div_eq_of_lt (Nat.sub_lt hp (by omega : 0 < 1))
    simp [hm, hdiv, hnot, hpdiv]
  · have hrn : r ≤ n := by omega
    have hsub : n - r + p - 1 = n / p * p + (n % p + p - 1 - r) := by
      grind
    rw [hsub, Nat.mul_comm (n / p) p]
    rw [Nat.mul_add_div hp (n / p) (n % p + p - 1 - r)]
    by_cases hlt : r < n % p
    · have hge : p ≤ n % p + p - 1 - r := by omega
      have hlt2 : n % p + p - 1 - r < 2 * p := by omega
      have hdiv2 : (n % p + p - 1 - r) / p = 1 := div_eq_one_of_bounds _ _ hp hge hlt2
      simp [hlt, hdiv2]
    · have hle : n % p ≤ r := by omega
      have hlt2 : n % p + p - 1 - r < p := by omega
      have hdiv2 : (n % p + p - 1 - r) / p = 0 := Nat.div_eq_of_lt hlt2
      simp [hlt, hdiv2]

private theorem residue_count_eq_zero_of_ge (N r p : Nat) (hr : N ≤ r) (hp : 0 < p) :
    (N - r + p - 1) / p = 0 := by
  have hsub : N - r = 0 := by omega
  rw [hsub, Nat.zero_add]
  exact Nat.div_eq_of_lt (Nat.sub_lt hp (by omega : 0 < 1))

private theorem count_residue_mod (n p r : Nat) (hp : 0 < p) (hr : r < p) :
    ((List.range n).filter (fun i => i % p == r)).length = (n - r + p - 1) / p := by
  induction n with
  | zero =>
      rw [residue_count_eq_zero_of_ge 0 r p (by omega) hp]
      simp
  | succ n ih =>
      rw [List.range_succ, List.filter_append, List.length_append, ih]
      have hstep : (n + 1 - r + p - 1) / p = (n - r + p - 1) / p + if n % p == r then 1 else 0 := by
        rw [mod_step_count (n+1) p r hp hr, mod_step_count n p r hp hr]
        have hm : (n+1)%p = if n%p + 1 < p then n%p+1 else 0 := by
          by_cases hp1 : p = 1
          · subst p
            have hz : n % 1 = 0 := by
              have := Nat.mod_lt n (by omega : 0 < 1)
              omega
            simp [hz]
          · have hp' : 1 < p := by omega
            have h1 : (n + 1) % p = (n % p + 1 % p) % p := Nat.add_mod n 1 p
            have h1p : 1 % p = 1 := Nat.mod_eq_of_lt hp'
            rw [h1, h1p]
            by_cases h : n % p + 1 < p
            · rw [Nat.mod_eq_of_lt h]
              simp [h]
            · have heq : n % p + 1 = p := by
                have hs' := Nat.mod_lt n hp
                omega
              rw [heq, Nat.mod_self]
              simp
        have hd : (n+1)/p = n/p + if n%p + 1 < p then 0 else 1 := by
          have hv := Nat.div_add_mod n p
          have hs' := Nat.mod_lt n hp
          by_cases h : n % p + 1 < p
          · have hEq : n + 1 = p * (n / p) + (n % p + 1) := by grind
            rw [hEq, Nat.mul_add_div hp (n / p) (n % p + 1), Nat.div_eq_of_lt h]
            simp [h]
          · have heq : n % p + 1 = p := by omega
            have hEq : n + 1 = p * (n / p + 1) := by grind
            rw [hEq, Nat.mul_comm p (n / p + 1), Nat.mul_div_left _ hp]
            simp [h]
        rw [hm, hd]
        by_cases hn : n % p == r
        · have hnp : n % p = r := Nat.eq_of_beq_eq_true hn
          have hltold : ¬ r < n % p := by omega
          by_cases hs : r + 1 < p
          · have hltnew : r < r + 1 := by omega
            simp [hnp, hs, hltnew]
          · have hltnew : ¬ r < 0 := by omega
            simp [hnp, hs, hltnew]
        · have hne0 : n % p ≠ r := by intro he; exact hn (by rw [he]; simp)
          have hltold : r < n % p ∨ n % p < r := by
            by_cases hrlt : r < n % p
            · exact Or.inl hrlt
            · right; omega
          by_cases h : n % p + 1 < p
          · rcases hltold with hlt | hgt
            · have hltnew : r < n % p + 1 := by omega
              simp [hn, h, hlt, hltnew]
            · have hltnew : ¬ r < n % p + 1 := by omega
              have hnlt : ¬ r < n % p := by omega
              simp [hn, h, hnlt, hltnew]
          · have heq : n % p + 1 = p := by
              have hs' := Nat.mod_lt n hp
              omega
            have hlt : r < n % p := by omega
            have hltnew : ¬ r < 0 := by omega
            simp [hn, h, hlt, hltnew]
      rw [hstep]
      by_cases hbeq : n % p == r <;> simp [hbeq]

-- mass equals count for RotationCycle

private theorem count_map_eq_filter (f : Nat → Nat) (l : List Nat) (k : Nat) :
    (l.map f).count k = (l.filter (fun i => f i == k)).length := by
  rw [List.count_eq_countP, List.countP_map, List.countP_eq_length_filter]
  rfl

private theorem mass_eq_count (n m s k : Nat) :
    bag.mass (n, m, s) k = (bag.expand (n, m, s)).count k := by
  by_cases hk : k ≤ m
  · let M := m + 1
    have hM : 0 < M := by dsimp [M]; omega
    have hklt : k < M := by dsimp [M]; omega
    let d := (k + M - s % M) % M
    have hdlt : d < M := Nat.mod_lt _ hM
    have hmass : bag.mass (n, m, s) k = n / M + (decide (d < n % M)).toNat := by
      simp [bag, hk, M, d]
    have hcount : (bag.expand (n, m, s)).count k =
        ((List.range n).filter (fun i => i % M == d)).length := by
      simp only [bag, M]
      rw [count_map_eq_filter]
      apply congrArg List.length
      apply List.filter_congr
      intro i hi
      by_cases h1 : (s + i) % (m + 1) = k
      · have h2 : i % (m + 1) = (k + (m + 1) - s % (m + 1)) % (m + 1) :=
          (rotation_mod_iff_all m s k i hk).1 h1
        simp [h1, h2, M, d]
      · have h2 : i % (m + 1) ≠ (k + (m + 1) - s % (m + 1)) % (m + 1) := by
          intro he
          apply h1
          exact (rotation_mod_iff_all m s k i hk).2 he
        have hb1 : ((s + i) % (m + 1) == k) = false := by simp [h1]
        have hb2 : (i % (m + 1) == (k + (m + 1) - s % (m + 1)) % (m + 1)) = false := by simp [h2]
        rw [hb1, hb2]
    rw [hmass, hcount, count_residue_mod n M d hM hdlt, mod_step_count n M d hM hdlt]
    rw [decide_toNat_eq]
  · have hkgt : m < k := by omega
    have hz : bag.mass (n, m, s) k = 0 := by simp [bag, hk]
    rw [hz]
    symm
    apply (List.count_eq_zero).mpr
    intro hmem
    simp only [bag, List.mem_map, List.mem_range] at hmem
    rcases hmem with ⟨i, hi, rfl⟩
    have hmodlt := Nat.mod_lt (s + i) (by omega : 0 < m + 1)
    omega

private theorem live_nodup (n m s : Nat) : (live bag (n, m, s)).Nodup := by
  unfold live
  exact nodup_filter (nodup_eraseDups (bag.keys (n, m, s))) (fun k => decide (0 < bag.mass (n, m, s) k))

private theorem live_mem_iff (n m s k : Nat) :
    k ∈ live bag (n, m, s) ↔ k ∈ (bag.expand (n, m, s)).eraseDups := by
  unfold live
  simp only [List.mem_filter, List.mem_eraseDups]
  constructor
  · rintro ⟨hkkeys, hkpos⟩
    have hkpos' : 0 < (bag.expand (n, m, s)).count k := by
      simpa [mass_eq_count] using hkpos
    by_cases hmem : k ∈ bag.expand (n, m, s)
    · exact hmem
    · have hc0 : (bag.expand (n, m, s)).count k = 0 := (List.count_eq_zero).mpr hmem
      omega
  · intro hmem
    constructor
    · have hklt : k < m + 1 := by
        simp only [bag, List.mem_map, List.mem_range] at hmem
        rcases hmem with ⟨i, hi, hval⟩
        have hmodlt := Nat.mod_lt (s + i) (by omega : 0 < m + 1)
        omega
      simpa [bag, List.mem_eraseDups] using hklt
    · have hcpos : 0 < (bag.expand (n, m, s)).count k := by
        have hcne : (bag.expand (n, m, s)).count k ≠ 0 := by
          intro h
          exact (List.count_eq_zero.mp h) hmem
        exact Nat.pos_of_ne_zero hcne
      simpa [mass_eq_count] using hcpos

theorem sound_SelfPairs : RotationCycle.SelfPairs.Goal := by
  intro p
  obtain ⟨n, m, s⟩ := p
  simp only [mass_eq_count]
  have hperm : (live bag (n, m, s)).Perm (bag.expand (n, m, s)).eraseDups :=
    perm_of_nodup_mem (live_nodup n m s) (nodup_eraseDups (bag.expand (n, m, s))) (live_mem_iff n m s)
  rw [perm_sum hperm (fun k => comb ((bag.expand (n, m, s)).count k))]
  exact comb_count_identity (bag.expand (n, m, s))

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RotationCycle_SelfPairs
