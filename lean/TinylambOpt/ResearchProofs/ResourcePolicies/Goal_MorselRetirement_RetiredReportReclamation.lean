import TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement.RetiredReportReclamation.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Discarding reports below the retired frontier releases memory.

Claim (`RetiredReportReclamation.Goal`): dropping every report whose id is below
`start` does not change `emit rs start count`: the emission window only ever looks
up ids `≥ start`, and any report matching such a lookup survives the filter
(it has id `≥ start`), so `find?` returns the same entry; filtered-out reports
(id `< start`) can never match. This justifies the coordinator freeing
already-retired report storage without changing future output.
-/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MorselRetirement_RetiredReportReclamation
open TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement

private theorem lookup_filter (s : Nat) (rs : List Report) (i : Nat) (hi : s ≤ i) :
    lookup (List.filter (fun r => s ≤ r.1) rs) i = lookup rs i := by
  unfold lookup
  induction rs with
  | nil => rfl
  | cons x xs ih =>
    simp only [List.filter_cons]
    split <;> rename_i hsx
    · rw [List.find?_cons, List.find?_cons]
      cases x.1 == i
      · exact ih
      · rfl
    · rw [List.find?_cons]
      cases hxi : x.1 == i
      · exact ih
      · exfalso
        simp at hxi hsx
        omega

private theorem takeWhile_congr' {α} (p q : α → Bool) (l : List α)
    (h : ∀ x ∈ l, p x = q x) : l.takeWhile p = l.takeWhile q := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
    have hx := h x (by simp)
    simp only [List.takeWhile]
    rw [ih (fun y hy => h y (by simp [hy])), hx]

private theorem takeWhile_subset {α} (p : α → Bool) (l : List α) (x : α)
    (hx : x ∈ l.takeWhile p) : x ∈ l := by
  induction l with
  | nil => cases hx
  | cons y ys ih =>
    cases hpy : p y
    · simp only [List.takeWhile, hpy] at hx
      cases hx
    · simp only [List.takeWhile, hpy] at hx
      rcases List.mem_cons.mp hx with h | h
      · subst h
        exact by simp
      · exact List.mem_cons_of_mem y (ih h)

private theorem flatMap_congr' {α β} (f g : α → List β) (l : List α)
    (h : ∀ x ∈ l, f x = g x) : l.flatMap f = l.flatMap g := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
    rw [List.flatMap_cons, List.flatMap_cons, h x (by simp),
      ih (fun y hy => h y (by simp [hy]))]

theorem sound : RetiredReportReclamation.Goal := by
  intro rs start count
  have hlk : ∀ i, start ≤ i →
      lookup (List.filter (fun r => start ≤ r.1) rs) i = lookup rs i :=
    fun i hi => lookup_filter start rs i hi
  have hp : ∀ x ∈ List.map (fun y => start + y) (List.range count),
      (lookup (List.filter (fun r => start ≤ r.1) rs) x).isSome
        = (lookup rs x).isSome := by
    intro x hx
    obtain ⟨j, _, rfl⟩ := List.mem_map.mp hx
    rw [hlk]
    omega
  have hready : readyIds (List.filter (fun r => start ≤ r.1) rs) start count
      = readyIds rs start count :=
    takeWhile_congr' (fun i => (lookup (List.filter (fun r => start ≤ r.1) rs) i).isSome)
      (fun i => (lookup rs i).isSome) (List.map (fun y => start + y) (List.range count)) hp
  show List.flatMap (fun i => (lookup (List.filter (fun r => start ≤ r.1) rs) i).getD [])
        (readyIds (List.filter (fun r => start ≤ r.1) rs) start count)
      = List.flatMap (fun i => (lookup rs i).getD []) (readyIds rs start count)
  rw [hready]
  refine flatMap_congr' (fun i => (lookup (List.filter (fun r => start ≤ r.1) rs) i).getD [])
    (fun i => (lookup rs i).getD []) (readyIds rs start count) (fun i hi => ?_)
  obtain ⟨j, hj, rfl⟩ := List.mem_map.mp (takeWhile_subset _ _ i hi)
  rw [hlk]
  omega

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MorselRetirement_RetiredReportReclamation
