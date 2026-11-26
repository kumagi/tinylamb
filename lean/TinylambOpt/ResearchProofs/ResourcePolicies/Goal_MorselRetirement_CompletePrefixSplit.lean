import TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement.CompletePrefixSplit.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Two adjacent output windows drain independently once the first window is complete.

Claim (`CompletePrefixSplit.Goal`): if every id of the window `[start, start+a)` has
reported (`(readyIds rs start a).length = a`), then
`emit rs start (a+b) = emit rs start a ++ emit rs (start+a) b`. This lets SQL
execution decompose one retirement window into two adjacent windows without
changing the emitted rows.
-/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MorselRetirement_CompletePrefixSplit
open TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement

private theorem cands_split (s a b : Nat) :
    (List.range (a+b)).map (s + ·)
      = (List.range a).map (s + ·) ++ (List.range b).map (s + a + ·) := by
  rw [List.range_add, List.map_append, List.map_map]
  simp [Nat.add_assoc]

private theorem takeWhile_all {α} (p : α → Bool) (l : List α) (h : ∀ x ∈ l, p x = true) :
    l.takeWhile p = l := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
    simp [List.takeWhile, h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem takeWhile_append_all {α} (p : α → Bool) (l₁ l₂ : List α)
    (h : ∀ x ∈ l₁, p x = true) : (l₁ ++ l₂).takeWhile p = l₁ ++ l₂.takeWhile p := by
  induction l₁ with
  | nil => rfl
  | cons x xs ih =>
    simp [List.takeWhile, h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem take_length_takeWhile {α} (p : α → Bool) (l : List α) :
    l.take (l.takeWhile p).length = l.takeWhile p := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
    simp only [List.takeWhile]
    split <;> rename_i h2
    · show x :: xs.take (xs.takeWhile p).length = x :: xs.takeWhile p
      rw [ih]
    · simp

private theorem take_len_eq (X : List Nat) (a : Nat) (h : X.length = a) : X.take a = X := by
  rw [← h, List.take_length]

private theorem takeWhile_only {α} (p : α → Bool) (l : List α) (x : α)
    (hx : x ∈ l.takeWhile p) : p x = true := by
  induction l with
  | nil => cases hx
  | cons y ys ih =>
    cases hpy : p y
    · simp only [List.takeWhile, hpy] at hx
      cases hx
    · simp only [List.takeWhile, hpy] at hx
      rcases List.mem_cons.mp hx with h | h
      · subst h
        exact hpy
      · exact ih h

theorem sound : CompletePrefixSplit.Goal := by
  intro rs start a b h
  let p : Nat → Bool := fun j => (lookup rs j).isSome
  have hlen : ((List.range a).map (fun x => start + x)).length = a := by
    rw [List.length_map]
    simp
  have hA : ((List.range a).map (fun x => start + x)).takeWhile p
      = (List.range a).map (fun x => start + x) := by
    rw [← take_length_takeWhile]
    have hlen2 : (((List.range a).map (fun x => start + x)).takeWhile p).length = a := by
      show (readyIds rs start a).length = a
      exact h
    rw [hlen2]
    exact take_len_eq _ _ hlen
  have hall : ∀ i ∈ (List.range a).map (fun x => start + x), p i = true := by
    intro i hi
    exact takeWhile_only p _ i (by rw [hA]; exact hi)
  have hready : readyIds rs start (a+b) = readyIds rs start a ++ readyIds rs (start+a) b := by
    show ((List.range (a+b)).map (fun x => start + x)).takeWhile p = _
    rw [cands_split start a b]
    rw [takeWhile_append_all p _ _ hall]
    rw [← hA]
    rfl
  show (readyIds rs start (a+b)).flatMap (fun i => (lookup rs i).getD [])
      = (readyIds rs start a).flatMap (fun i => (lookup rs i).getD [])
        ++ (readyIds rs (start+a) b).flatMap (fun i => (lookup rs i).getD [])
  rw [hready, List.flatMap_append]

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MorselRetirement_CompletePrefixSplit
