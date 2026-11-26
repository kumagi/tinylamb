import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_Sector_SparseOrDense

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

private theorem pairwise_head?_le {α : Type} {le : α → α → Bool}
    (hrefl : ∀ x, le x x = true)
    {l : List α} {a b : α} (hpw : l.Pairwise (fun x y => le x y = true))
    (hhead : l.head? = some a) (hmem : b ∈ l) : le a b = true := by
  cases l with
  | nil => simp at hmem
  | cons x xs =>
    simp at hhead
    subst hhead
    rw [List.pairwise_cons] at hpw
    rcases List.mem_cons.mp hmem with rfl | hmem
    · exact hrefl _
    · exact hpw.1 b hmem

-- The head of the cost-sorted list of covers is a minimum-cost cover.
private theorem choose_le (f : Price) (xs : List Nat) (ps : List Extent)
    (hmem : ps ∈ covers (points xs)) :
    cost f (choose f xs) ≤ cost f ps := by
  unfold choose
  let le := fun a b => decide (cost f a ≤ cost f b)
  have hpair : List.Pairwise (fun a b => le a b = true)
      ((covers (points xs)).mergeSort le) := by
    apply List.pairwise_mergeSort
    · intro a b c hab hbc
      simp only [le, decide_eq_true_eq] at hab hbc ⊢
      exact Nat.le_trans hab hbc
    · intro a b
      simp only [le, Bool.or_eq_true, decide_eq_true_eq]
      exact Nat.le_total (cost f a) (cost f b)
  have hmem' : ps ∈ (covers (points xs)).mergeSort le := List.mem_mergeSort.mpr hmem
  cases h : (covers (points xs)).mergeSort le with
  | nil =>
    simp [h] at hmem'
  | cons hd tl =>
    simp only [List.head?_cons, Option.getD_some]
    have hle := pairwise_head?_le (fun x => by simp [le]) hpair
      (show ((covers (points xs)).mergeSort le).head? = some hd by simp [h]) hmem'
    simp only [le, decide_eq_true_eq] at hle
    exact hle

-- Two legal covers: one point per request, and one bounding extent.
private theorem covers_singleton_mem (xs : List Nat) :
    xs.map (fun k => (k, k)) ∈ covers xs := by
  induction xs with
  | nil => simp [covers]
  | cons x xs ih =>
    simp only [covers, List.mem_flatMap]
    refine ⟨xs.map (fun k => (k, k)), ih, ?_⟩
    cases hxs : xs.map (fun k => (k, k)) with
    | nil => simp [hxs]
    | cons hd tl => simp [hxs]

private theorem covers_full_mem : (xs : List Nat) → xs ≠ [] →
    [(xs.head?.getD 0, xs.getLast?.getD 0)] ∈ covers xs
  | [], h => by cases h rfl
  | [x], _ => by simp [covers]
  | x :: y :: ys, _ => by
    have ih := covers_full_mem (y :: ys) (by simp)
    rw [covers]
    simp only [List.mem_flatMap]
    refine ⟨[((y :: ys).head?.getD 0, (y :: ys).getLast?.getD 0)], ⟨ih, ?_⟩⟩
    simp [List.head?, List.getLast?]

private theorem points_nonempty {xs : List Nat} (hne : xs ≠ []) : points xs ≠ [] := by
  intro h
  have hperm : (List.eraseDups xs).mergeSort.Perm (List.eraseDups xs) := List.mergeSort_perm _ _
  have hempty : List.eraseDups xs = [] := by
    have hlen := hperm.symm.length_eq
    have h' : (List.eraseDups xs).mergeSort = [] := by simpa [points] using h
    rw [h'] at hlen
    exact List.eq_nil_of_length_eq_zero (by simpa using hlen)
  exact hne (List.eq_nil_iff_forall_not_mem.mpr (fun a ha => by
    have := List.mem_eraseDups.mpr ha
    simp [hempty] at this))

theorem sound_SparseOrDense : Sector.SparseOrDense.Goal := by
  intro shift startup xs hne
  let ks := points xs
  have hks_ne : ks ≠ [] := by simpa [ks] using points_nonempty hne
  change cost (Sector.price shift startup) (choose (Sector.price shift startup) xs) ≤
    min ((ks.map (fun k => Sector.price shift startup k k)).sum)
      (Sector.price shift startup (ks.head?.getD 0) (ks.getLast?.getD 0))
  apply Nat.le_min.mpr
  constructor
  · have hle := choose_le (Sector.price shift startup) xs (ks.map (fun k => (k, k)))
      (covers_singleton_mem ks)
    simp only [cost, List.map_map] at hle ⊢
    exact hle
  · have hle := choose_le (Sector.price shift startup) xs
      [(ks.head?.getD 0, ks.getLast?.getD 0)] (covers_full_mem ks hks_ne)
    simpa [cost] using hle

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_Sector_SparseOrDense
