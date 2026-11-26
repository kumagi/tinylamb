import TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
When two spilled batches share no non-NULL value, no value of the second batch is ever
erased as a duplicate of the first, so the distinct set of the concatenation is the
concatenation of the distinct sets: summing local COUNT DISTINCT states is exact. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_AggregateReplay_DisjointDistinctSpillDomains
open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

private theorem beq_false {α : Type} [BEq α] [LawfulBEq α] {a b : α}
    (h : ¬ ((a == b) = true)) : (a == b) = false := by
  cases hab : a == b with
  | true => exact absurd hab (by simpa using h)
  | false => exact rfl

/-- Removing elements equal to any member of a disjoint list is the identity. -/
private theorem removeAll_of_disjoint : ∀ (as bs : List Int),
    (∀ x ∈ as, ∀ y ∈ bs, (x == y) = false) → bs.removeAll as = bs := by
  intro as
  induction as with
  | nil => intro bs _; simp
  | cons a as ih =>
      intro bs h
      have hb : ∀ y ∈ bs, (y == a) = false := by
        intro y hy
        refine beq_false (by
          intro hh
          have hEq : y = a := LawfulBEq.eq_of_beq hh
          have ha : (a == y) = false := h a (by simp) y hy
          rw [hEq] at ha
          simp_all)
      rw [List.removeAll_cons,
        List.filter_eq_self.mpr (fun y hy => by simpa using hb y hy),
        ih bs (fun x hx y hy => h x (List.mem_cons_of_mem _ hx) y hy)]

private theorem ed : ∀ (xs ys : List Int), (∀ x ∈ xs, ∀ y ∈ ys, (x == y) = false) →
    (xs ++ ys).eraseDups = xs.eraseDups ++ ys.eraseDups := by
  intro xs ys h
  rw [List.eraseDups_append, removeAll_of_disjoint xs ys h]

theorem sound : DisjointDistinctSpillDomains.Goal := by
  intro a b h
  simp only [distinctCount]
  have hne : ∀ x ∈ a.filterMap id, ∀ y ∈ b.filterMap id, (x == y) = false := by
    intro x hx y hy
    refine beq_false (by
      intro hxy
      have hEq : x = y := LawfulBEq.eq_of_beq hxy
      have hxa : some x ∈ a := by simpa using hx
      have hyb : some y ∈ b := by simpa using hy
      rw [← hEq] at hyb
      exact absurd hyb (h x hxa))
  rw [List.filterMap_append, ed _ _ hne, List.length_append]

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_AggregateReplay_DisjointDistinctSpillDomains
