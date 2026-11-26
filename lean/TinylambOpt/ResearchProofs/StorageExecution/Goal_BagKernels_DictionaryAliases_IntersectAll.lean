import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DictionaryAliases_IntersectAll

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.DictionaryAliases

-- Small-input truth check: intersection multiplicity agrees with the expanded bags.
private def sample : List (List Nat × List Nat) :=
  [[], [0], [1], [0, 0], [1, 1], [0, 1], [2, 0, 1], [3, 0, 3]].flatMap
    (fun d => [[], [0], [1], [2], [3], [0, 0], [0, 1], [1, 0], [0, 1, 2], [3, 0], [2, 2]].map
      (fun i => (d, i)))

example : sample.all (fun a => sample.all (fun c => (List.range 5).all (fun k =>
    ((live bag a).flatMap (fun x => List.replicate (min (bag.mass a x) (bag.mass c x)) x)).count k ==
      min ((bag.expand a).count k) ((bag.expand c).count k)))) := by
  decide

private theorem sum_map_add {α : Type} (l : List α) (f g : α → Nat) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Nat.add_assoc, Nat.add_left_comm]

private theorem sum_map_zero {α : Type} (l : List α) : (l.map (fun _ => 0)).sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem count_flatMap_replicate (xs : List Nat) (f : Nat → Nat) (k : Nat) :
    (xs.flatMap (fun x => List.replicate (f x) x)).count k =
      (xs.map (fun x => if x = k then f x else 0)).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih, List.count_replicate, beq_iff_eq]

private theorem nodup_filter {α : Type} [DecidableEq α] (l : List α) (hn : l.Nodup) (p : α → Bool) :
    (l.filter p).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.nodup_cons] at hn
      simp only [List.filter_cons]
      by_cases hp : p a = true
      · simp [hp]
        constructor
        · intro hm
          exact hn.1 (by simpa using hm)
        · exact ih hn.2
      · simp [hp]
        exact ih hn.2

private theorem nodup_eraseDups_self (l : List Nat) : l.eraseDups.Nodup := by
  suffices H : ∀ n (l : List Nat), l.length ≤ n → l.eraseDups.Nodup from H l.length l (by omega)
  intro n
  induction n with
  | zero =>
      intro l h
      cases l with
      | nil => simp
      | cons a as => simp at h
  | succ n ih =>
      intro l h
      cases l with
      | nil => simp [List.eraseDups]
      | cons a as =>
          have hlen : as.length ≤ n := by simpa using h
          rw [List.eraseDups_cons]
          have hf : (as.filter (fun b => !b == a)).length ≤ n := by
            have := List.length_filter_le (fun b : Nat => !b == a) as
            omega
          have ht : ((as.filter (fun b => !b == a)).eraseDups).Nodup := ih _ hf
          rw [List.nodup_cons]
          constructor
          · intro hm
            rw [List.mem_eraseDups, List.mem_filter] at hm
            simp at hm
          · exact ht

private theorem sum_eq_single_of_nodup (xs : List Nat) (hn : xs.Nodup) (k : Nat) (f : Nat → Nat) :
    (xs.map (fun x => if x = k then f x else 0)).sum = if k ∈ xs then f k else 0 := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      rw [List.nodup_cons] at hn
      simp only [List.map_cons, List.sum_cons]
      by_cases hxk : x = k
      · subst x
        have hnot : k ∉ xs := hn.1
        simpa [hnot] using (ih hn.2)
      · have hi := ih hn.2
        have hkx : k ≠ x := fun h => hxk h.symm
        by_cases hk : k ∈ xs <;> simp [hxk, hkx, hk, hi]

-- Number of `zipIdx` slots with value `k` and index `i`, with a generalized
-- starting offset so that `List.zipIdx_cons` applies cleanly in the induction.
private theorem idx_count_eq_aux (d : List Nat) (s i k : Nat) :
    (List.map (fun e : Nat × Nat => if i = e.2 then 1 else 0)
      (List.filter (fun e : Nat × Nat => e.1 == k) (d.zipIdx s))).sum =
      if s ≤ i ∧ i < s + d.length + 1 ∧ d[i - s]? = some k then 1 else 0 := by
  induction d generalizing s with
  | nil => simp
  | cons a ds ih =>
      rw [List.zipIdx_cons]
      by_cases ha : a == k
      · by_cases his : i = s
        · subst i
          simp [ha]
          rw [ih (s + 1)]
          have hf : ¬s + 1 ≤ s := by omega
          have ht : s < s + (ds.length + 1) + 1 := by omega
          simp [hf, ht]
          have ha' : a = k := by simpa [beq_iff_eq] using ha
          exact ha'
        · by_cases hsi : s ≤ i
          · have hsli : s + 1 ≤ i := by omega
            have hsub : i - s - 1 = i - (s + 1) := by omega
            have hsucc : i - s = (i - s - 1) + 1 := by omega
            rw [hsucc, List.getElem?_cons_succ]
            simp [ha, ih, his, hsi, hsli, hsub, Nat.add_comm, Nat.add_left_comm]
          · have hnot : ¬s + 1 ≤ i := by omega
            simp [ha, ih, his, hsi, hnot]
      · by_cases his : i = s
        · subst i
          simp [ha]
          rw [ih (s + 1)]
          have hf : ¬s + 1 ≤ s := by omega
          have ht : s < s + (ds.length + 1) + 1 := by omega
          simp [hf, ht]
          have ha' : a ≠ k := by simpa [beq_iff_eq] using ha
          exact ha'
        · by_cases hsi : s ≤ i
          · have hsli : s + 1 ≤ i := by omega
            have hsub : i - s - 1 = i - (s + 1) := by omega
            have hsucc : i - s = (i - s - 1) + 1 := by omega
            rw [hsucc, List.getElem?_cons_succ]
            simp [ha, ih, hsi, hsli, hsub, Nat.add_comm, Nat.add_left_comm]
          · have hnot : ¬s + 1 ≤ i := by omega
            simp [ha, ih, hsi, hnot]

private theorem idx_count_eq_prop (d : List Nat) (i k : Nat) :
    (List.map (fun e : Nat × Nat => if i = e.2 then 1 else 0)
      (List.filter (fun e : Nat × Nat => e.1 == k) d.zipIdx)).sum =
      if d[i]? = some k then 1 else 0 := by
  have h := idx_count_eq_aux d 0 i k
  simp at h
  by_cases hdi : d[i]? = some k
  · have hlt : i < d.length + 1 := by
      obtain ⟨hidx, _⟩ := (List.getElem?_eq_some_iff.mp hdi)
      omega
    simp [hdi, hlt] at h
    simpa [hdi] using h
  · simp [hdi] at h
    simpa [hdi] using h

private theorem idx_count_eq (d : List Nat) (i k : Nat) :
    (List.map (fun e : Nat × Nat => if i == e.2 then 1 else 0)
      (List.filter (fun e : Nat × Nat => e.1 == k) d.zipIdx)).sum =
      if d[i]? == some k then 1 else 0 := by
  have h := idx_count_eq_prop d i k
  simpa [beq_iff_eq] using h

private theorem count_filterMap_cons (d ids : List Nat) (i k : Nat) :
    ((i :: ids).filterMap (fun j => d[j]?)).count k =
      (if d[i]? == some k then 1 else 0) + (ids.filterMap (fun j => d[j]?)).count k := by
  match h : d[i]? with
  | none => simp [h]
  | some x => simp [List.count_cons, h, beq_iff_eq, Nat.add_comm]

private theorem mass_cons_step (d ids : List Nat) (i k : Nat) :
    ((d.zipIdx.filter (fun e : Nat × Nat => e.1 == k)).map (fun e => (i :: ids).count e.2)).sum =
      ((d.zipIdx.filter (fun e : Nat × Nat => e.1 == k)).map (fun e => ids.count e.2)).sum +
        (if d[i]? == some k then 1 else 0) := by
  simp only [List.count_cons]
  rw [sum_map_add, idx_count_eq]

-- The multiplicity kernel counts exactly the occurrences produced by `expand`.
private theorem count_expand_eq_mass (p : List Nat × List Nat) (k : Nat) :
    (bag.expand p).count k = bag.mass p k := by
  rcases p with ⟨d, ids⟩
  change (ids.filterMap (fun i => d[i]?)).count k =
    ((d.zipIdx.filter (fun e : Nat × Nat => e.1 == k)).map (fun e => ids.count e.2)).sum
  induction ids with
  | nil => simp [sum_map_zero]
  | cons i ids ih =>
      rw [count_filterMap_cons, mass_cons_step, ih]
      omega

private theorem exists_pos_of_sum_pos {α : Type} (l : List α) (f : α → Nat)
    (h : 0 < (l.map f).sum) : ∃ x ∈ l, 0 < f x := by
  induction l with
  | nil => simp at h
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons] at h
      by_cases hx : 0 < f x
      · exact ⟨x, by simp, hx⟩
      · have hx0 : f x = 0 := by omega
        simp [hx0] at h
        obtain ⟨y, hy, hypos⟩ := ih h
        exact ⟨y, by simp [hy], hypos⟩

private theorem fst_mem_of_mem_zipIdx_aux (d : List Nat) (start : Nat) (e : Nat × Nat)
    (h : e ∈ d.zipIdx start) : e.1 ∈ d := by
  induction d generalizing start with
  | nil => simp [List.zipIdx_nil] at h
  | cons a ds ih =>
      rw [List.zipIdx_cons] at h
      simp at h
      rcases h with h | h
      · cases h
        simp
      · exact List.mem_cons_of_mem a (ih (start + 1) h)

private theorem pos_mass_mem_live (p : List Nat × List Nat) (k : Nat) (h : 0 < bag.mass p k) :
    k ∈ live bag p := by
  rcases p with ⟨d, ids⟩
  change k ∈ (d.eraseDups.filter (fun k => 0 < bag.mass (d, ids) k))
  rw [List.mem_filter]
  constructor
  · rw [List.mem_eraseDups]
    have hex : ∃ e ∈ d.zipIdx.filter (fun e : Nat × Nat => e.1 == k), 0 < ids.count e.2 :=
      exists_pos_of_sum_pos _ _ h
    rcases hex with ⟨⟨v, j⟩, hmem, hpos⟩
    have hvk : v == k := (List.mem_filter.mp hmem).2
    have hmemzip : (v, j) ∈ d.zipIdx := (List.mem_filter.mp hmem).1
    have hvin : v ∈ d := fst_mem_of_mem_zipIdx_aux d 0 (v, j) hmemzip
    have hvk' : v = k := by simpa [beq_iff_eq] using hvk
    simpa [hvk'] using hvin
  · simpa using h

theorem sound_IntersectAll : IntersectAll.Goal := by
  intro a c k
  rw [count_flatMap_replicate]
  have hnodup : (live bag a).Nodup := by
    rcases a with ⟨d, ids⟩
    change (d.eraseDups.filter (fun k => 0 < bag.mass (d, ids) k)).Nodup
    exact nodup_filter _ (nodup_eraseDups_self _) _
  rw [sum_eq_single_of_nodup (live bag a) hnodup]
  rw [count_expand_eq_mass a k, count_expand_eq_mass c k]
  by_cases hk : k ∈ live bag a
  · simp [hk]
  · have hm0 : bag.mass a k = 0 := by
      by_cases hpos : 0 < bag.mass a k
      · exact False.elim (hk (pos_mass_mem_live a k hpos))
      · omega
    simp [hk, hm0]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DictionaryAliases_IntersectAll
