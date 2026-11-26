import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

/-!
Late materialization can gather payloads at selected row positions while preserving the
selection's order and multiplicities.

Proved

- `CodecRestarts.SparseOrDense` — `∀ (restart seekFee : Nat), sparseOrDense (price
  restart seekFee)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_CodecRestarts_SparseOrDense

open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.Compression.Wire

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

/-- The head of the cost-sorted plan list is no more expensive than any plan
    that `covers` enumerated. -/
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
    simp only [h, List.head?_cons, Option.getD_some]
    have hle := pairwise_head?_le (fun x => by simp [le]) hpair
      (show ((covers (points xs)).mergeSort le).head? = some hd by simp [h]) hmem'
    simp only [le, decide_eq_true_eq] at hle
    exact hle

/-- `covers` always enumerates at least one plan. -/
private theorem covers_nonempty : (xs : List Nat) → covers xs ≠ []
  | [] => by simp [covers]
  | x :: xs => by
    have ih := covers_nonempty xs
    simp only [covers, ne_eq]
    intro h
    cases hxs : covers xs with
    | nil => exact ih hxs
    | cons p ps =>
      have : (match p with | [] => [[(x,x)]] | (_,b)::rest => [(x,x)::p,(x,b)::rest]) = [] := by
        have hall : ∀ ps' ∈ p :: ps, (match ps' with | [] => [[(x,x)]] | (_,b)::rest => [(x,x)::ps',(x,b)::rest]) = [] := by
          simp only [List.flatMap_eq_nil_iff] at h
          rw [hxs] at h
          exact h
        exact hall p (by simp)
      cases p with
      | nil => simp at this
      | cons hd tl => simp at this

private theorem covers_singleton_mem (xs : List Nat) :
    xs.map (fun k => (k, k)) ∈ covers xs := by
  induction xs with
  | nil => simp [covers]
  | cons x xs ih =>
    simp only [covers, List.mem_flatMap]
    refine ⟨xs.map (fun k => (k, k)), ih, ?_⟩
    cases hxs : xs.map (fun k => (k, k)) with
    | nil =>
      simp [hxs]
    | cons hd tl =>
      simp [hxs]

private theorem covers_full_mem : (xs : List Nat) → xs ≠ [] →
    [(xs.head?.getD 0, xs.getLast?.getD 0)] ∈ covers xs
  | [], h => by cases h rfl
  | [x], _ => by simp [covers]
  | x :: y :: ys, _ => by
    have ih := covers_full_mem (y :: ys) (by simp)
    rw [covers]
    simp only [List.mem_flatMap]
    refine ⟨[((y :: ys).head?.getD 0, (y :: ys).getLast?.getD 0)], ⟨ih, ?_⟩⟩
    simp [List.head?, List.getLast?, List.getLast?_cons_cons]

private theorem points_ne_of_ne {xs : List Nat} (hne : xs ≠ []) : points xs ≠ [] := by
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

/-- The chosen gather plan is never worse than reading every point separately
    nor than reading one bounding extent. -/
theorem sound_SparseOrDense : CodecRestarts.SparseOrDense.Goal := by
  intro restart seekFee
  intro xs hne
  let f := CodecRestarts.price restart seekFee
  let ks := points xs
  have hks_ne : ks ≠ [] := points_ne_of_ne hne
  change cost f (choose f xs) ≤ min ((ks.map (fun k => f k k)).sum)
    (f (ks.head?.getD 0) (ks.getLast?.getD 0))
  apply Nat.le_min.mpr
  constructor
  · have hmem : ks.map (fun k => (k, k)) ∈ covers ks := covers_singleton_mem ks
    have hle := choose_le f xs (ks.map (fun k => (k, k))) hmem
    simpa [cost, Function.comp_def] using hle
  · have hmem : [(ks.head?.getD 0, ks.getLast?.getD 0)] ∈ covers ks := covers_full_mem ks hks_ne
    have hle := choose_le f xs [(ks.head?.getD 0, ks.getLast?.getD 0)] hmem
    simpa [cost] using hle

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_CodecRestarts_SparseOrDense
