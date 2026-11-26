import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `IntervalCover.IntersectAll` — Intersect two bags by the minimum multiplicity at each
  key.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_IntervalCover_IntersectAll

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.IntervalCover
open TinylambOpt.ResearchGoals.Compression.Wire

example : ((live bag [(1, 2), (2, 3)]).flatMap (fun x => List.replicate
    (min (bag.mass [(1, 2), (2, 3)] x) (bag.mass [(2, 1)] x)) x)).count 2 =
    min ((bag.expand [(1, 2), (2, 3)]).count 2) ((bag.expand [(2, 1)]).count 2) := by
  decide

example : ((live bag [(0, 4), (2, 2)]).flatMap (fun x => List.replicate
    (min (bag.mass [(0, 4), (2, 2)] x) (bag.mass [(1, 3)] x)) x)).count 3 =
    min ((bag.expand [(0, 4), (2, 2)]).count 3) ((bag.expand [(1, 3)]).count 3) := by
  decide

example : ((live bag [(3, 0), (1, 5)]).flatMap (fun x => List.replicate
    (min (bag.mass [(3, 0), (1, 5)] x) (bag.mass [(1, 1)] x)) x)).count 2 =
    min ((bag.expand [(3, 0), (1, 5)]).count 2) ((bag.expand [(1, 1)]).count 2) := by
  decide

example : ((live bag []).flatMap (fun x => List.replicate
    (min (bag.mass [] x) (bag.mass [(0, 2)] x)) x)).count 1 =
    min ((bag.expand []).count 1) ((bag.expand [(0, 2)]).count 1) := by
  decide

private theorem count_range_add (s n k : Nat) :
    ((List.range n).map (fun i => s + i)).count k = if s ≤ k && k < s + n then 1 else 0 := by
  induction n with
  | zero => simp
  | succ n ih =>
      by_cases hsn : s + n = k
      · have hsk : s ≤ k := by omega
        have hklt : k < s + (n + 1) := by omega
        simp [List.range_succ, ih, hsn, hsk, hklt]
      · have hsingleton : (List.map (fun i => s + i) [n]).count k = 0 := by
          simp [hsn]
        rw [List.range_succ, List.map_append, List.count_append, hsingleton, ih]
        by_cases hsk : s ≤ k
        · by_cases hklt : k < s + (n + 1)
          · have hklt' : k < s + n := by omega
            simp [hsk, hklt, hklt']
          · have hnlt : ¬k < s + n := by omega
            simp [hsk, hklt, hnlt]
        · simp [hsk]

private theorem mass_eq_countP (rs : List (Nat × Nat)) (k : Nat) :
    bag.mass rs k = rs.countP (fun r : Nat × Nat => r.1 ≤ k && k < r.1 + r.2) := by
  rfl

private theorem mass_cons (s n : Nat) (rs : List (Nat × Nat)) (k : Nat) :
    bag.mass ((s, n) :: rs) k =
      (if s ≤ k && k < s + n then 1 else 0) +
        rs.countP (fun r : Nat × Nat => r.1 ≤ k && k < r.1 + r.2) := by
  simp [bag, List.countP_cons, Nat.add_comm]

private theorem count_expand_eq_mass (rs : List (Nat × Nat)) (k : Nat) :
    (bag.expand rs).count k = bag.mass rs k := by
  induction rs with
  | nil => simp [bag]
  | cons r rs ih =>
      obtain ⟨s, n⟩ := r
      change (((List.range n).map (fun i => s + i)) ++ bag.expand rs).count k = bag.mass ((s, n) :: rs) k
      rw [List.count_append, ih, mass_cons, mass_eq_countP, count_range_add]

private theorem count_flatMap_replicate (xs : List Nat) (f : Nat → Nat) (k : Nat) :
    (xs.flatMap (fun x => List.replicate (f x) x)).count k =
      (xs.map (fun x => if x = k then f x else 0)).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp [ih, List.count_replicate, beq_iff_eq]

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
        by_cases hk : k ∈ xs
        · simp [hxk, hkx, hk, hi]
        · simp [hxk, hkx, hk, hi]

private theorem max?_some_of_ne_nil : ∀ {xs : List Nat}, xs ≠ [] → ∃ m, xs.max? = some m := by
  intro xs
  cases xs with
  | nil => simp
  | cons a as =>
      intro _
      rw [List.max?_cons]
      exact ⟨as.max?.elim a (max a), rfl⟩

private theorem le_peak {xs : List Nat} {x : Nat} (hx : x ∈ xs) : x ≤ peak xs := by
  have hxsne : xs ≠ [] := by
    intro hnil
    subst xs
    simp at hx
  obtain ⟨m, hm⟩ := max?_some_of_ne_nil (xs := xs) hxsne
  have hmprops := List.max?_eq_some_iff.mp hm
  have hxm : x ≤ m := hmprops.2 x hx
  have hp : peak xs = m := by simp [peak, hm]
  rw [hp]
  exact hxm

private theorem exists_cover_of_mass_pos (rs : List (Nat × Nat)) (k : Nat) :
    0 < bag.mass rs k ↔ ∃ r ∈ rs, r.1 ≤ k ∧ k < r.1 + r.2 := by
  induction rs with
  | nil => simp [bag]
  | cons r rs ih =>
      obtain ⟨s, n⟩ := r
      simp [bag]

private theorem pos_mass_mem_live (rs : List (Nat × Nat)) (k : Nat) (h : 0 < bag.mass rs k) :
    k ∈ live bag rs := by
  have hcov : ∃ r ∈ rs, r.1 ≤ k ∧ k < r.1 + r.2 := (exists_cover_of_mass_pos rs k).mp h
  rcases hcov with ⟨⟨s, n⟩, hrs, hsk, hkn⟩
  have hle : s + n ≤ peak (rs.map (fun r : Nat × Nat => r.1 + r.2)) := by
    apply le_peak
    exact List.mem_map.mpr ⟨(s, n), hrs, rfl⟩
  have hklt : k < peak (rs.map (fun r : Nat × Nat => r.1 + r.2)) := by omega
  change k ∈ (bag.keys rs).eraseDups.filter (fun x => 0 < bag.mass rs x)
  rw [List.mem_filter]
  constructor
  · rw [List.mem_eraseDups]
    change k ∈ List.range (peak (rs.map (fun r : Nat × Nat => r.1 + r.2)))
    simpa [List.mem_range] using hklt
  · simpa using h

theorem sound_IntersectAll : IntersectAll.Goal := by
  intro a c k
  rw [count_flatMap_replicate]
  have hnodup : (live bag a).Nodup := by
    change ((bag.keys a).eraseDups.filter (fun x => 0 < bag.mass a x)).Nodup
    exact nodup_filter _ (nodup_eraseDups_self _) _
  rw [sum_eq_single_of_nodup (live bag a) hnodup]
  rw [count_expand_eq_mass, count_expand_eq_mass]
  by_cases hk : k ∈ live bag a
  · simp [hk]
  · have hm0 : bag.mass a k = 0 := by
      by_cases hpos : 0 < bag.mass a k
      · exact False.elim (hk (pos_mass_mem_live a k hpos))
      · omega
    simp [hk, hm0]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_IntervalCover_IntersectAll
