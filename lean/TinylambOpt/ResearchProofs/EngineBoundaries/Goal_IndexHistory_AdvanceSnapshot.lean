import TinylambOpt.ResearchGoals.EngineBoundaries.IndexHistory

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_IndexHistory_AdvanceSnapshot

open TinylambOpt.ResearchGoals.EngineBoundaries.IndexHistory

private def maxList : List Nat → Nat
  | [] => 0
  | x :: xs => max x (maxList xs)

private theorem maxList_eq (l : List Nat) : l.max?.getD 0 = maxList l := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      cases hm : xs.max? with
      | none =>
          have hnil : xs = [] := List.max?_eq_none_iff.mp hm
          subst xs
          simp [maxList]
      | some m =>
          rw [hm] at ih
          simp at ih
          rw [List.max?_cons, hm]
          simp [maxList, ih]

private theorem le_maxList_of_mem (l : List Nat) (x : Nat) (hx : x ∈ l) : x ≤ maxList l := by
  induction l with
  | nil => simp at hx
  | cons y ys ih =>
      simp only [maxList]
      rcases List.mem_cons.mp hx with hxy | hx
      · subst x
        exact Nat.le_max_left y (maxList ys)
      · exact Nat.le_trans (ih hx) (Nat.le_max_right y (maxList ys))

private theorem le_max_of_mem (l : List Nat) (x : Nat) (hx : x ∈ l) : x ≤ l.max?.getD 0 := by
  rw [maxList_eq]
  exact le_maxList_of_mem l x hx

private theorem maxList_append (a b : List Nat) : maxList (a ++ b) = max (maxList a) (maxList b) := by
  induction a with
  | nil => simp [maxList]
  | cons x xs ih =>
      simp [maxList, ih, Nat.max_left_comm, Nat.max_comm]

private theorem ts_le_latest (c : Change) (cs : List Change) (hc : c ∈ cs) : c.ts ≤ latest cs := by
  unfold latest
  apply le_max_of_mem (cs.map Change.ts) c.ts
  exact List.mem_map.mpr ⟨c, hc, rfl⟩

private theorem noTouch (q : Query) (d : Nat) (hsnap : d ≤ q.snap)
    (cs : List Change) (hbound : ∀ c ∈ cs, touches q c = true → c.ts ≤ d) :
    cs.any (touches q) = false := by
  induction cs with
  | nil => rfl
  | cons c cs ih =>
      have hc : touches q c = false := by
        by_cases ht : touches q c = true
        · have hts := hbound c (by simp) ht
          simp only [touches, Bool.and_eq_true, decide_eq_true_eq] at ht
          have hnot : ¬ q.snap < c.ts := by omega
          exact False.elim (hnot ht.2)
        · cases h : touches q c <;> simp_all
      have htail : ∀ z ∈ cs, touches q z = true → z.ts ≤ d := by
        intro z hz ht
        exact hbound z (by simp [hz]) ht
      simp [hc, ih htail]

private theorem impossibleTouch (q : Query) (c : Change) (hlo : q.lo ≥ q.hi) : touches q c = false := by
  unfold touches
  by_cases hr : c.root == q.root <;> simp [hr]
  by_cases hl : q.lo ≤ c.key <;> simp [hl]
  by_cases hh : c.key < q.hi <;> simp [hh]
  omega

theorem sound : AdvanceSnapshot.Goal := by
  intro q t cs hsnap hclean
  have hc : ∀ c, touches {q with snap := t} c = true → touches q c = true := by
    intro c ht
    simp only [touches, Bool.and_eq_true, decide_eq_true_eq] at ht ⊢
    exact ⟨⟨⟨ht.1.1.1, ht.1.1.2⟩, ht.1.2⟩, by omega⟩
  rw [exact] at hclean ⊢
  apply List.any_eq_false.mpr
  intro c hcmem htrue
  exact (List.any_eq_false.mp hclean c hcmem) (hc c htrue)

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_IndexHistory_AdvanceSnapshot
