import TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
A sorted take-`k` prefix of any list is characterized (among sorted lists of length at
most `k`) by how many of its elements are `≤ x` for each `x`: that count is
`min (count ≤ x) k`. Truncating each run to its local top-`k` first never removes an
element that could survive the final global top-`k`, because the per-run counts below any
threshold are only clipped at `k` and the outer clip removes exactly the same excess. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_AggregateReplay_BoundedRunCandidates
open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay
open TinylambOpt.ResearchGoals.QueryRewrites

/-- Sorting (with the default `Nat` order) yields a nondecreasing list. -/
private theorem ms_sorted : ∀ (l : List Nat),
    l.mergeSort.Pairwise (fun a b => decide (a ≤ b) = true) := by
  intro l
  refine List.pairwise_mergeSort (le := fun a b => decide (a ≤ b))
    (fun a b c h1 h2 =>
      decide_eq_true (Nat.le_trans (of_decide_eq_true h1) (of_decide_eq_true h2))) ?_ l
  intro a b
  rcases Nat.le_total a b with h | h
  · rw [decide_eq_true h]
    simp
  · rw [decide_eq_true h]
    simp

/-- On a nondecreasing list the `≤ x` elements of a prefix are the first `min` many. -/
private theorem initSeg : ∀ (k : Nat) (s : List Nat),
    s.Pairwise (fun a b => decide (a ≤ b) = true) →
    ∀ x : Nat,
      (s.take k).countP (fun a => a ≤ x) = Nat.min (s.countP (fun a => a ≤ x)) k := by
  intro k
  induction k with
  | zero => intro s _ x; simp
  | succ k ih =>
      intro s hp x
      cases s with
      | nil => simp
      | cons y t =>
          obtain ⟨hge, hpt⟩ := List.pairwise_cons.mp hp
          rw [show ((y :: t).take (k + 1)) = y :: t.take k from rfl,
            List.countP_cons, List.countP_cons, ih t hpt x]
          by_cases hy : decide (y ≤ x) = true
          · rw [if_pos hy]
            rw [← Nat.add_min_add_right (t.countP (fun a => decide (a ≤ x))) k 1]
          · have h1 : t.countP (fun a => a ≤ x) = 0 := by
              rw [List.countP_eq_zero]
              exact fun c hc => by
                intro hcx
                have hyc := of_decide_eq_true (hge c hc)
                have hcx' := of_decide_eq_true hcx
                exact absurd (Nat.le_trans hyc hcx') (by simpa using hy)
            rw [if_neg hy, h1]
            simp

/-- The `≤ x` census of `best k xs` is the clipped census of `xs`. -/
private theorem count_best : ∀ (k : Nat) (xs : List Nat) (x : Nat),
    (best k xs).countP (fun a => a ≤ x) = Nat.min (xs.countP (fun a => a ≤ x)) k := by
  intro k xs x
  unfold best
  have hp : (xs.mergeSort).Pairwise (fun a b => decide (a ≤ b) = true) := ms_sorted xs
  have hperm : (xs.mergeSort).countP (fun a => a ≤ x) = xs.countP (fun a => a ≤ x) :=
    (List.mergeSort_perm xs _).countP_eq (fun a => a ≤ x)
  rw [← hperm]
  exact initSeg k xs.mergeSort hp x

/-- The `p`-count of a concatenation of runs is the sum of the run counts. -/
private theorem flatten_countP : ∀ (R : List (List Nat)) (p : Nat → Bool),
    (R.flatten).countP p = (R.map fun r => r.countP p).sum := by
  intro R
  induction R with
  | nil => intro p; rfl
  | cons r rest ih =>
      intro p
      rw [List.flatten_cons, List.countP_append]
      simp only [List.map_cons, List.sum_cons, ih]

private theorem sum_min_le (cs : List Nat) (k : Nat) :
    (cs.map (fun c => c.min k)).sum ≤ cs.sum := by
  induction cs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      exact Nat.add_le_add (Nat.min_le_left x k) ih

private theorem min_sum : ∀ (cs : List Nat) (k : Nat),
    Nat.min ((cs.map fun c => Nat.min c k).sum) k = Nat.min cs.sum k := by
  intro cs
  induction cs with
  | nil => intro k; rfl
  | cons c cs ih =>
      intro k
      simp only [List.map_cons, List.sum_cons]
      let M := (List.map (fun c => c.min k) cs).sum
      change (c.min k + M).min k = (c + cs.sum).min k
      have hMle : M ≤ cs.sum := by
        dsimp [M]
        exact sum_min_le cs k
      have hMmin : M.min k = cs.sum.min k := by
        dsimp [M]
        exact ih k
      by_cases hct : c ≤ k
      · rw [show c.min k = c from Nat.min_eq_left hct]
        by_cases hst : cs.sum ≤ k
        · have hM : M = cs.sum := by
            have hmin := hMmin
            rw [show cs.sum.min k = cs.sum from Nat.min_eq_left hst] at hmin
            have hlemin : M.min k ≤ M := Nat.min_le_left M k
            omega
          rw [hM]
        · have hgt : k < cs.sum := Nat.lt_of_not_ge hst
          have hMk : k ≤ M := by
            have hmin := hMmin
            rw [show cs.sum.min k = k from Nat.min_eq_right (by omega)] at hmin
            have hlemin : M.min k ≤ M := Nat.min_le_left M k
            omega
          rw [show (c + M).min k = k from Nat.min_eq_right (by omega)]
          rw [show (c + cs.sum).min k = k from Nat.min_eq_right (by omega)]
      · have hcgt : k < c := Nat.lt_of_not_ge hct
        rw [show c.min k = k from Nat.min_eq_right (by omega)]
        rw [show (k + M).min k = k from Nat.min_eq_right (by omega)]
        rw [show (c + cs.sum).min k = k from Nat.min_eq_right (by omega)]

/-- Two nondecreasing lists with the same `≤ x` census for all `x` are equal. -/
private theorem list_eq_of_countP : ∀ (L R : List Nat),
    L.Pairwise (fun a b => decide (a ≤ b) = true) →
    R.Pairwise (fun a b => decide (a ≤ b) = true) →
    (∀ x, L.countP (fun a => a ≤ x) = R.countP (fun a => a ≤ x)) → L = R := by
  intro L
  induction L with
  | nil =>
      intro R _ hR h
      cases R with
      | nil => rfl
      | cons b t =>
          have h1 := h b
          rw [List.countP_cons, if_pos (decide_eq_true (Nat.le_refl b))] at h1
          have hz : (b::t).countP (fun a => a ≤ b) = 0 := by simpa using h1.symm
          have hpos : 0 < (b::t).countP (fun a => a ≤ b) := by simp
          omega
  | cons a L' ih =>
      intro R hL hR h
      cases R with
      | nil =>
          have h1 := h a
          rw [List.countP_cons, if_pos (decide_eq_true (Nat.le_refl a))] at h1
          have hz : (a::L').countP (fun z => z ≤ a) = 0 := by simpa using h1.symm
          have hpos : 0 < (a::L').countP (fun z => z ≤ a) := by simp
          omega
      | cons b R' =>
          obtain ⟨haL, hL't⟩ := List.pairwise_cons.mp hL
          obtain ⟨hbR, hR't⟩ := List.pairwise_cons.mp hR
          have hab : a = b := by
            rcases Nat.lt_trichotomy a b with h1 | h2 | h3
            · have hx := h a
              have hz : R'.countP (fun z => z ≤ a) = 0 := by
                rw [List.countP_eq_zero]
                exact fun c hc => by
                  intro hca
                  have hbc := of_decide_eq_true (hbR c hc)
                  have hca' := of_decide_eq_true hca
                  omega
              rw [List.countP_cons, if_pos (decide_eq_true (Nat.le_refl a)),
                List.countP_cons,
                if_neg (by
                  intro hh
                  have hba := of_decide_eq_true hh
                  omega), hz] at hx
              omega
            · exact h2
            · have hx := h b
              have hz : L'.countP (fun z => z ≤ b) = 0 := by
                rw [List.countP_eq_zero]
                exact fun c hc => by
                  intro hcb
                  have hac := of_decide_eq_true (haL c hc)
                  have hcb' := of_decide_eq_true hcb
                  omega
              rw [List.countP_cons,
                if_neg (by
                  intro hh
                  have hab' := of_decide_eq_true hh
                  omega), hz, List.countP_cons,
                if_pos (decide_eq_true (Nat.le_refl b))] at hx
              omega
          subst hab
          have htails : ∀ x, L'.countP (fun a => a ≤ x) = R'.countP (fun a => a ≤ x) := by
            intro x
            have h1 := h x
            rw [List.countP_cons, List.countP_cons] at h1
            omega
          rw [ih R' hL't hR't htails]

theorem sound : BoundedRunCandidates.Goal := by
  intro k runs
  show best k ((runs.map (best k)).flatten) = best k runs.flatten
  have hcount : ∀ x,
      (best k ((runs.map (best k)).flatten)).countP (fun a => a ≤ x) =
        (best k runs.flatten).countP (fun a => a ≤ x) := by
    intro x
    have hmap : ((runs.map (best k)).map fun r => r.countP (fun a => a ≤ x)) =
        (runs.map fun r => r.countP (fun a => a ≤ x)).map fun c => Nat.min c k := by
      rw [List.map_map, List.map_map]
      apply List.map_congr_left
      intro r hr
      exact count_best k r x
    rw [count_best k ((runs.map (best k)).flatten) x, count_best k runs.flatten x]
    rw [flatten_countP (R := runs.map (best k)) (p := fun a => a ≤ x),
        flatten_countP (R := runs) (p := fun a => a ≤ x)]
    rw [hmap, min_sum]
  refine list_eq_of_countP _ _ ?_ ?_ hcount
  · exact (ms_sorted _).take
  · exact (ms_sorted _).take

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_AggregateReplay_BoundedRunCandidates
