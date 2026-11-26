import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `MultiplicityRuns.IntersectAll` — Intersect two bags by the minimum multiplicity at
  each key.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_MultiplicityRuns_IntersectAll

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.MultiplicityRuns

example : ((live bag [(1,2),(2,3)]).flatMap (fun x => List.replicate (min (bag.mass [(1,2),(2,3)] x) (bag.mass [(2,1)] x)) x)).count 2
    = min ((bag.expand [(1,2),(2,3)]).count 2) ((bag.expand [(2,1)]).count 2) := by decide

example : ((live bag [(1,2),(1,3)]).flatMap (fun x => List.replicate (min (bag.mass [(1,2),(1,3)] x) (bag.mass [(1,1)] x)) x)).count 1
    = min ((bag.expand [(1,2),(1,3)]).count 1) ((bag.expand [(1,1)]).count 1) := by decide

example : ((live bag [(1,0)]).flatMap (fun x => List.replicate (min (bag.mass [(1,0)] x) (bag.mass [(1,5)] x)) x)).count 1
    = min ((bag.expand [(1,0)]).count 1) ((bag.expand [(1,5)]).count 1) := by decide

private theorem count_expand_eq_mass (rs : List (Nat × Nat)) (k : Nat) :
    (bag.expand rs).count k = bag.mass rs k := by
  induction rs with
  | nil => simp [bag]
  | cons r rs ih =>
    rcases r with ⟨x, n⟩
    by_cases hxk : x = k
    · subst x
      change (List.replicate n k ++ bag.expand rs).count k = bag.mass ((k, n) :: rs) k
      rw [List.count_append, ih]
      simp [bag]
    · have hxbeq : (x == k) = false := by simp [hxk]
      change (List.replicate n x ++ bag.expand rs).count k = bag.mass ((x, n) :: rs) k
      rw [List.count_append, ih]
      simp [bag, hxbeq, List.count_replicate]

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
      · exact hn.1
      · exact ih hn.2
    · simp [hp]
      exact ih hn.2

private theorem nodup_eraseDups_self (l : List Nat) : l.eraseDups.Nodup := by
  suffices H : ∀ n (l : List Nat), l.length ≤ n → l.eraseDups.Nodup from H l.length l (Nat.le_refl _)
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
          have hf : (as.filter (fun b => !b == a)).length ≤ n :=
            Nat.le_trans (List.length_filter_le _ _) hlen
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

private theorem mass_filter_sum_eq_zero (rs : List (Nat × Nat)) (k : Nat)
    (h : k ∉ rs.map Prod.fst) :
    ((rs.filter (fun r => r.1 == k)).map Prod.snd).sum = 0 := by
  induction rs with
  | nil => simp
  | cons r rs ih =>
    rcases r with ⟨x, n⟩
    simp only [List.map_cons, List.mem_cons] at h
    have hx : x ≠ k := fun heq => h (Or.inl heq.symm)
    have hrs : k ∉ rs.map Prod.fst := fun hm => h (Or.inr hm)
    have hxbeq : (x == k) = false := by simp [hx]
    simp [hxbeq, ih hrs]

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
  · simp [hk]
    by_cases hkin : k ∈ bag.keys a
    · have hm0 : bag.mass a k = 0 := by
        have hnotpos : ¬ 0 < bag.mass a k := by
          intro hp
          apply hk
          change k ∈ (bag.keys a).eraseDups.filter (fun x => 0 < bag.mass a x)
          exact List.mem_filter.mpr ⟨List.mem_eraseDups.mpr hkin, by simp [hp]⟩
        omega
      simp [hm0]
    · have hm0 : bag.mass a k = 0 := by
        change ((a.filter (fun r => r.1 == k)).map Prod.snd).sum = 0
        exact mass_filter_sum_eq_zero a k (by simpa [bag] using hkin)
      simp [hm0]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_MultiplicityRuns_IntersectAll
