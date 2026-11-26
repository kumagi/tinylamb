import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-! Smallest-scratch-first completion minimizes the serial reserve: the
ascending-scratch merge-sort needs no more initial free bytes than any other
permutation of the same job set. The proof repeatedly moves a scratch-minimal
job to the front (adjacent swap repairs never increase `headroom`) and then
peels it off, so the sorted order dominates every permutation. -/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.GrantAdmission_LeastScratchCompletionOrder
open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

private abbrev leS (a b : Job) : Bool := decide (a.scratch ≤ b.scratch)

/-- Swapping an adjacent inverted pair `(a, b)` with `a.scratch ≤ b.scratch`
into the sorted arrangement never increases the required reserve: both sides
expand to explicit `max` towers over the shared tail and `omega` compares them. -/
private theorem swap_core : ∀ (a b : Job) (T : List Job), a.scratch ≤ b.scratch →
    headroom ([a, b] ++ T) ≤ headroom ([b, a] ++ T) := by
  intro a b T hab
  show max a.scratch (max b.scratch (headroom T - b.held) - a.held) ≤
    max b.scratch (max a.scratch (headroom T - a.held) - b.held)
  omega

/-- An inversion repair under any prefix: `headroom (j :: Z)` is monotone in
`headroom Z`, so the pairwise repair lifts through the prefix by induction. -/
private theorem swap_pre : ∀ (X : List Job) (a b : Job) (Y : List Job), a.scratch ≤ b.scratch →
    headroom (X ++ [a, b] ++ Y) ≤ headroom (X ++ [b, a] ++ Y) := by
  intro X
  induction X with
  | nil => intro a b Y hab; simpa using swap_core a b Y hab
  | cons j X ih =>
    intro a b Y hab
    have hi := ih a b Y hab
    show max j.scratch (headroom (X ++ [a, b] ++ Y) - j.held) ≤
      max j.scratch (headroom (X ++ [b, a] ++ Y) - j.held)
    omega

/-- Bubbling a minimal-scratch job to the front across any prefix never
increases the reserve (repeated adjacent inversion repairs). -/
private theorem bubble : ∀ (pre : List Job) (x : Job) (q : List Job),
    (∀ y ∈ pre, x.scratch ≤ y.scratch) → headroom (x :: pre ++ q) ≤ headroom (pre ++ [x] ++ q) := by
  intro pre
  induction pre with
  | nil => intro x q _; exact Nat.le_refl _
  | cons y pre ih =>
    intro x q hmem
    have hy : x.scratch ≤ y.scratch := hmem y List.mem_cons_self
    have h1 : headroom (x :: y :: pre ++ q) ≤ headroom (y :: x :: pre ++ q) :=
      swap_pre [] x y (pre ++ q) hy
    have h2 : headroom (x :: pre ++ q) ≤ headroom (pre ++ [x] ++ q) :=
      ih x q (fun z hz => hmem z (List.mem_cons_of_mem y hz))
    have h3 : headroom (y :: x :: pre ++ q) ≤ headroom (y :: (pre ++ [x] ++ q)) := by
      show max y.scratch (headroom (x :: pre ++ q) - y.held) ≤
        max y.scratch (headroom (pre ++ [x] ++ q) - y.held)
      omega
    exact Nat.le_trans h1 h3

/-- Splitting a list at the FIRST occurrence of a member leaves a prefix
without further occurrences. -/
private theorem first_split : ∀ (l : List Job) (a : Job), a ∈ l →
    ∃ s t, l = s ++ a :: t ∧ a ∉ s := by
  intro l
  induction l with
  | nil => intro a h; exact absurd h List.not_mem_nil
  | cons y l ih =>
    intro a h
    rcases List.mem_cons.mp h with hya | hta
    · exact ⟨[], l, by rw [← hya, List.nil_append], List.not_mem_nil⟩
    · by_cases hy2 : a = y
      · exact ⟨[], l, by rw [← hy2, List.nil_append], List.not_mem_nil⟩
      · obtain ⟨s, t, he, hns⟩ := ih a hta
        refine ⟨y :: s, t, congrArg (List.cons y) he, ?_⟩
        intro hm
        rcases List.mem_cons.mp hm with h1 | h2
        · exact absurd h1 hy2
        · exact hns h2

/-- Erasing an element removes it exactly when it did not occur earlier. -/
private theorem erase_split : ∀ (s : List Job) (a : Job) (t : List Job), a ∉ s →
    (s ++ a :: t).erase a = s ++ t := by
  intro s
  induction s with
  | nil =>
    intro a t _
    show (a :: t).erase a = t
    rw [List.erase_cons, if_pos (by simp [beq_iff_eq])]
  | cons y s ih =>
    intro a t hns
    have hbe : (y == a) = false := by
      cases h : (y == a) with
      | true =>
        have hya : y = a := by simpa [beq_iff_eq] using h
        subst hya
        exact absurd List.mem_cons_self hns
      | false => rfl
    have h1 : ((y :: s) ++ a :: t) = y :: (s ++ a :: t) := by rw [List.cons_append]
    have h2 : (y :: s) ++ t = y :: (s ++ t) := by rw [List.cons_append]
    rw [h1, List.erase_cons,
      if_neg (fun hh => absurd hh (by simpa using hbe)),
      ih a t (fun hm => hns (List.mem_cons_of_mem y hm)), h2]

/-- The sorted (ascending-scratch) order of a list needs no more reserve than
any of its permutations: peel the scratch-minimal job off both sides, moving
it to the front on the right, and recurse on the tails. -/
private theorem sorted_min : ∀ (L M : List Job),
    List.Pairwise (fun a b => decide (a.scratch ≤ b.scratch)) L → L.Perm M →
      headroom L ≤ headroom M := by
  intro L
  induction L with
  | nil =>
    intro M _ hp
    cases M with
    | nil => exact Nat.le_refl _
    | cons b l => exact absurd hp (List.not_perm_nil_cons b l)
  | cons x L ih =>
    intro M hpw hp
    obtain ⟨hmin, hpw'⟩ := List.pairwise_cons.mp hpw
    have hxm : x ∈ M := hp.subset List.mem_cons_self
    obtain ⟨M1, M2, hMeq, hM1⟩ := first_split M x hxm
    subst hMeq
    have hminM : ∀ y ∈ M1 ++ M2, x.scratch ≤ y.scratch := by
      intro y hy
      rcases List.mem_append.mp hy with h1 | h2
      · have hym : y ∈ x :: L := hp.symm.subset (List.mem_append.mpr (Or.inl h1))
        rcases List.mem_cons.mp hym with hyx | hyL
        · subst hyx; exact Nat.le_refl _
        · exact of_decide_eq_true (hmin y hyL)
      · have hym : y ∈ x :: L :=
          hp.symm.subset (List.mem_append.mpr (Or.inr (List.mem_cons_of_mem x h2)))
        rcases List.mem_cons.mp hym with hyx | hyL
        · subst hyx; exact Nat.le_refl _
        · exact of_decide_eq_true (hmin y hyL)
    have hperm : L.Perm (M1 ++ M2) := by
      have h1 := List.Perm.erase x hp
      have hL : (x :: L : List Job).erase x = L := by
        rw [List.erase_cons, if_pos (by simp [beq_iff_eq])]
      have hR : (M1 ++ x :: M2).erase x = M1 ++ M2 := erase_split M1 x M2 hM1
      rw [hL, hR] at h1
      exact h1
    have htail := ih (M1 ++ M2) hpw' hperm
    have hstep1 : headroom (x :: L) ≤ headroom (x :: M1 ++ M2) := by
      show max x.scratch (headroom L - x.held) ≤ max x.scratch (headroom (M1 ++ M2) - x.held)
      omega
    have hconv : (M1 ++ [x] ++ M2) = (M1 ++ x :: M2) := List.append_assoc M1 [x] M2
    rw [← hconv]
    exact Nat.le_trans hstep1 (bubble M1 x M2
      (fun y hy => hminM y (List.mem_append.mpr (Or.inl hy))))

/-- Smallest-scratch-first is an optimal serial completion order: among all
permutations of `jobs`, the ascending-scratch merge-sort minimizes the required
reserve `headroom`. `mergeSort` returns a permutation that is `Pairwise`-sorted
for a transitive total comparator, so the selection-sort argument applies. -/
theorem sound : LeastScratchCompletionOrder.Goal := by
  intro jobs order hp
  have htrans : ∀ a b c : Job, leS a b → leS b c → leS a c := by
    intro a b c hab hbc
    exact decide_eq_true (Nat.le_trans (of_decide_eq_true hab) (of_decide_eq_true hbc))
  have htotal : ∀ a b : Job, leS a b || leS b a := by
    intro a b
    rw [Bool.or_eq_true_iff]
    rcases Nat.le_total a.scratch b.scratch with h | h
    · exact Or.inl (decide_eq_true h)
    · exact Or.inr (decide_eq_true h)
  have hs := List.pairwise_mergeSort htrans htotal jobs
  have hperm : List.Perm (jobs.mergeSort leS) order :=
    (List.mergeSort_perm jobs leS).trans hp
  exact sorted_min (jobs.mergeSort leS) order hs hperm

end TinylambOpt.ResearchProofs.ResourcePolicies.GrantAdmission_LeastScratchCompletionOrder
