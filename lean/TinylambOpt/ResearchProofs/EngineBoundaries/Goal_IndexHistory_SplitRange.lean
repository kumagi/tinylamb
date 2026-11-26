import TinylambOpt.ResearchGoals.EngineBoundaries.IndexHistory

/-!
Index reads across version changes must still observe the keys appropriate to the SQL
snapshot.

Proved

- `SplitRange` — Split a scan into half-open intervals with no blind spot at the pivot.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_IndexHistory_SplitRange

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

theorem sound : SplitRange.Goal := by
  intro q mid cs hlo hhi
  have hc : ∀ c, touches q c =
      (touches {q with hi := mid} c || touches {q with lo := mid} c) := by
    intro c
    apply Bool.eq_iff_iff.mpr
    constructor
    · intro ht
      simp only [touches, Bool.or_eq_true, Bool.and_eq_true, decide_eq_true_eq] at ht ⊢
      by_cases hm : c.key < mid
      · exact Or.inl ⟨⟨⟨ht.1.1.1, by omega⟩, hm⟩, ht.2⟩
      · exact Or.inr ⟨⟨⟨ht.1.1.1, by omega⟩, by omega⟩, ht.2⟩
    · intro ht
      simp only [touches, Bool.or_eq_true, Bool.and_eq_true, decide_eq_true_eq] at ht ⊢
      rcases ht with ht | ht
      · exact ⟨⟨⟨ht.1.1.1, by omega⟩, by omega⟩, ht.2⟩
      · exact ⟨⟨⟨ht.1.1.1, by omega⟩, by omega⟩, ht.2⟩
  induction cs with
  | nil => rfl
  | cons c cs ih =>
      change (touches q c || cs.any (touches q)) =
        (touches {q with hi := mid} c || cs.any (touches {q with hi := mid}) ||
          (touches {q with lo := mid} c || cs.any (touches {q with lo := mid})))
      change cs.any (touches q) = _ at ih
      simp only [exact, hc c, ih, List.any_cons]
      apply Bool.eq_iff_iff.mpr
      constructor
      · intro h
        simp only [Bool.or_eq_true] at h ⊢
        rcases h with (hA | hB) | (hC | hD)
        · exact Or.inl (Or.inl hA)
        · exact Or.inr (Or.inl hB)
        · exact Or.inl (Or.inr hC)
        · exact Or.inr (Or.inr hD)
      · intro h
        simp only [Bool.or_eq_true] at h ⊢
        rcases h with (hA | hC) | (hB | hD)
        · exact Or.inl (Or.inl hA)
        · exact Or.inr (Or.inl hC)
        · exact Or.inl (Or.inr hB)
        · exact Or.inr (Or.inr hD)

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_IndexHistory_SplitRange
