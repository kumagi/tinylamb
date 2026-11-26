import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_ExceptionOverlay_DistinctOutput

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.ExceptionOverlay

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

private theorem beq_toNat_comm (a b : Nat) : (a == b).toNat = (b == a).toNat := by
  by_cases h : a = b
  · subst b
    rfl
  · have hne : b ≠ a := by intro hba; exact h hba.symm
    rw [beq_eq_false_iff_ne.mpr h, beq_eq_false_iff_ne.mpr hne]

private theorem count_singleton (a b : Nat) : List.count a [b] = (a == b).toNat := by
  by_cases h : a = b
  · subst b
    rw [List.count_cons]
    simp
  · rw [List.count_cons]
    have hne : b ≠ a := by intro hba; exact h hba.symm
    rw [beq_eq_false_iff_ne.mpr hne, beq_eq_false_iff_ne.mpr h]
    rfl

private theorem filter_false_eq_nil {α : Type} (l : List α) : l.filter (fun _ => false) = [] := by
  induction l with
  | nil => rfl
  | cons a as ih => simp [ih]

private theorem mem_fst_iff_lookup_ne_none (es : List (Nat × Nat)) (i : Nat) :
    i ∈ es.map Prod.fst ↔ es.lookup i != none := by
  induction es with
  | nil => simp
  | cons r es ih =>
      obtain ⟨p, x⟩ := r
      by_cases h : i == p
      · have hip : i = p := eq_of_beq h
        subst p
        simp
      · have hipne : i ≠ p := by
          intro h'
          apply h
          subst h'
          simp
        simp [List.lookup_cons, h, ih, hipne]

private theorem mem_snd_of_lookup_eq_some (es : List (Nat × Nat)) (i k : Nat)
    (h : es.lookup i = some k) : k ∈ es.map Prod.snd := by
  induction es with
  | nil => simp at h
  | cons r es ih =>
      obtain ⟨p, x⟩ := r
      by_cases hi : i == p
      · have hip : i = p := eq_of_beq hi
        subst p
        simp at h
        subst k
        simp
      · simp [List.lookup_cons, hi] at h
        rcases List.mem_map.mp (ih h) with ⟨a, ha⟩
        exact List.mem_map.mpr ⟨a, by simp [ha.1], ha.2⟩

private theorem filter_lt_succ_countP (ds : List Nat) (hn : ds.Nodup) (n : Nat) (P : Nat → Bool) :
    (ds.filter (fun x => x < n + 1)).countP P =
      (ds.filter (fun x => x < n)).countP P + (if n ∈ ds then P n else false).toNat := by
  induction ds with
  | nil => simp
  | cons a as ih =>
      rw [List.nodup_cons] at hn
      by_cases hlt : a < n
      · have hlt' : a < n + 1 := by omega
        have hna : n ≠ a := by omega
        have hmem : n ∈ a :: as ↔ n ∈ as := by simp [hna]
        simp [hlt, hlt', hmem]
        rw [List.countP_cons, List.countP_cons, ih hn.2]
        by_cases hm : n ∈ as <;> simp [hm] <;> omega
      · by_cases ha : a = n
        · subst a
          simp
          rw [List.countP_cons, ih hn.2]
          simp [hn.1]
          cases P n <;> simp
        · have hnot : ¬ a < n + 1 := by omega
          have hna : n ≠ a := by omega
          have hmem : n ∈ a :: as ↔ n ∈ as := by simp [hna]
          simp [hnot, hlt, hmem]
          simpa using ih hn.2

private theorem filter_lt_succ_length (ds : List Nat) (hn : ds.Nodup) (n : Nat) :
    (ds.filter (fun x => x < n + 1)).length =
      (ds.filter (fun x => x < n)).length + (if n ∈ ds then 1 else 0) := by
  have h := filter_lt_succ_countP ds hn n (fun _ => true)
  by_cases hm : n ∈ ds <;> simpa [hm] using h

private theorem filter_lt_length_le (ds : List Nat) (hn : ds.Nodup) (n : Nat) :
    (ds.filter (fun x => x < n)).length ≤ n := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [filter_lt_succ_length ds hn n]
      by_cases hm : n ∈ ds <;> simp [hm] <;> omega

-- The count of an expanded ordinal equals the multiplicity kernel's mass.
private theorem count_expand_eq_mass (n d : Nat) (es : List (Nat × Nat)) (k : Nat) :
    ((List.range n).map (fun i => (es.lookup i).getD d)).count k = bag.mass (n, d, es) k := by
  induction n with
  | zero =>
      simp [bag]
      rw [filter_false_eq_nil]
      rfl
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.count_append, ih]
      have hnds : ((es.map Prod.fst).eraseDups).Nodup := nodup_eraseDups (es.map Prod.fst)
      have hlenle : ((es.map Prod.fst).eraseDups.filter (fun x => x < n)).length ≤ n :=
        filter_lt_length_le ((es.map Prod.fst).eraseDups) hnds n
      have hlen := filter_lt_succ_length ((es.map Prod.fst).eraseDups) hnds n
      have hcnt := filter_lt_succ_countP ((es.map Prod.fst).eraseDups) hnds n
        (fun i => es.lookup i == some k)
      have hmemds : n ∈ (es.map Prod.fst).eraseDups ↔ es.lookup n != none := by
        rw [List.mem_eraseDups, mem_fst_iff_lookup_ne_none]
      generalize hv : es.lookup n = vo
      cases vo with
      | none =>
          have hnnot : n ∉ (es.map Prod.fst).eraseDups := by
            rw [hmemds]
            simp [hv]
          simp [hnnot] at hlen hcnt
          dsimp [bag]
          rw [hlen, hcnt]
          simp [hv]
          rw [count_singleton]
          clear ih hmemds hnds hlen hcnt
          by_cases hkd : k = d <;> simp [hkd] <;> omega
      | some x =>
          have hnmem : n ∈ (es.map Prod.fst).eraseDups := by
            rw [hmemds]
            simp [hv]
          simp [hnmem] at hlen hcnt
          dsimp [bag]
          rw [hlen, hcnt]
          simp [hv]
          rw [count_singleton]
          rw [beq_toNat_comm k x]
          clear ih hmemds hnds hlen hcnt
          by_cases hkd : k = d <;> simp [hkd] <;> omega

private theorem exists_of_countP_pos {α : Type} (l : List α) (p : α → Bool)
    (h : 0 < l.countP p) : ∃ a, a ∈ l ∧ p a = true := by
  induction l with
  | nil => simp at h
  | cons a as ih =>
      rw [List.countP_cons] at h
      by_cases hp : p a = true
      · exact ⟨a, by simp, hp⟩
      · have hp' : p a = false := by cases hpa : p a <;> simp_all
        rw [hp'] at h
        simp only [Bool.false_eq_true, ↓reduceIte, Nat.add_zero] at h
        rcases ih h with ⟨b, hb, hpb⟩
        exact ⟨b, by simp [hb], hpb⟩

private theorem mem_keys_of_mass_pos (n d : Nat) (es : List (Nat × Nat)) (k : Nat)
    (h : 0 < bag.mass (n, d, es) k) : k ∈ bag.keys (n, d, es) := by
  change k ∈ d :: es.map Prod.snd
  by_cases hkd : k = d
  · subst k
    simp
  · right
    dsimp [bag] at h
    have hpos : 0 < ((es.map Prod.fst).eraseDups.filter (fun x => x < n)).countP
        (fun i => es.lookup i == some k) := by
      simpa [hkd] using h
    rcases exists_of_countP_pos _ _ hpos with ⟨i, hi, hlk⟩
    have hlopt : es.lookup i = some k := by simpa using hlk
    exact mem_snd_of_lookup_eq_some es i k hlopt

private theorem mem_live_iff (p : Nat × Nat × List (Nat × Nat)) (k : Nat) :
    k ∈ live bag p ↔ 0 < bag.mass p k ∧ k ∈ bag.keys p := by
  unfold live
  simp [List.mem_filter, List.mem_eraseDups, and_comm]

private theorem nodup_live (p : Nat × Nat × List (Nat × Nat)) : (live bag p).Nodup := by
  unfold live
  exact List.Pairwise.filter _ (nodup_eraseDups (bag.keys p))

theorem sound_DistinctOutput : ExceptionOverlay.DistinctOutput.Goal := by
  intro p
  obtain ⟨n, d, es⟩ := p
  rw [List.perm_iff_count]
  intro k
  by_cases h : 0 < bag.mass (n, d, es) k
  · have hl : k ∈ live bag (n, d, es) :=
      (mem_live_iff (n, d, es) k).2 ⟨h, mem_keys_of_mass_pos n d es k h⟩
    have he : k ∈ (bag.expand (n, d, es)).eraseDups := by
      rw [List.mem_eraseDups, ← List.count_pos_iff]
      change 0 < ((List.range n).map (fun i => (es.lookup i).getD d)).count k
      rw [count_expand_eq_mass]
      exact h
    rw [count_eq_one_of_mem_nodup (nodup_live (n, d, es)) hl,
        count_eq_one_of_mem_nodup (nodup_eraseDups (bag.expand (n, d, es))) he]
  · have hl : k ∉ live bag (n, d, es) := by
      intro hk
      exact h ((mem_live_iff (n, d, es) k).1 hk).1
    have he : k ∉ (bag.expand (n, d, es)).eraseDups := by
      intro hk
      rw [List.mem_eraseDups] at hk
      change k ∈ ((List.range n).map (fun i => (es.lookup i).getD d)) at hk
      have hpos : 0 < ((List.range n).map (fun i => (es.lookup i).getD d)).count k :=
        List.count_pos_iff.mpr hk
      rw [count_expand_eq_mass] at hpos
      exact h hpos
    rw [List.count_eq_zero_of_not_mem hl, List.count_eq_zero_of_not_mem he]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_ExceptionOverlay_DistinctOutput
