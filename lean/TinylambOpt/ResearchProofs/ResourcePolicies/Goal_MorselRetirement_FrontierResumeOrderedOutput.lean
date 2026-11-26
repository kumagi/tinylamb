import TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement.FrontierResumeOrderedOutput.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Publishing a drained prefix now and resuming at the frontier composes correctly.

Claim (`FrontierResumeOrderedOutput.Goal`): with `consumed` the number of ids the
`old` reports can already drain from the window `[start, start+count)`, emitting
the prefix from `old` and then the remainder from `old++new` (resuming at
`start+consumed`) reproduces `emit (old++new) start count`. Reports are matched
with `find?`, so a hit in `old` is unaffected by appending `new`, and `find?` on a
miss in `old` scans exactly the appended tail; readiness can therefore only extend.
This lets the executor stream a completed prefix early and resume from the
retirement frontier as late results arrive.
-/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MorselRetirement_FrontierResumeOrderedOutput
open TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement

private theorem find?_append_none {α} (p : α → Bool) (l₁ l₂ : List α)
    (h : List.find? p l₁ = none) : List.find? p (l₁ ++ l₂) = List.find? p l₂ := by
  induction l₁ with
  | nil => rw [List.nil_append]
  | cons x xs ih =>
    rw [List.cons_append, List.find?_cons]
    cases hpx : p x
    · refine ih ?_
      rw [List.find?_cons, hpx] at h
      exact h
    · exact absurd h (by rw [List.find?_cons, hpx]; simp)

private theorem find?_append_some {α} (p : α → Bool) (l₁ l₂ : List α) {y}
    (h : List.find? p l₁ = some y) : List.find? p (l₁ ++ l₂) = some y := by
  induction l₁ with
  | nil => cases h
  | cons x xs ih =>
    rw [List.cons_append, List.find?_cons]
    cases hpx : p x
    · refine ih ?_
      rw [List.find?_cons, hpx] at h
      exact h
    · rw [List.find?_cons, hpx] at h
      exact h

private theorem isSome_elim {α} {o : Option α} (h : o.isSome = true) : ∃ v, o = some v := by
  cases o with
  | none => exact absurd h (by simp)
  | some v => exact ⟨v, rfl⟩

private theorem lookup_app_some (rs₁ rs₂ : List Report) (i : Nat) (h : ∃ v, lookup rs₁ i = some v) :
    lookup (rs₁ ++ rs₂) i = lookup rs₁ i := by
  unfold lookup
  cases hf : List.find? (fun r => r.1 == i) rs₁ with
  | none =>
    obtain ⟨v, hv⟩ := h
    unfold lookup at hv
    rw [hf] at hv
    simp only [Option.map_none] at hv
    cases hv
  | some r =>
    rw [find?_append_some (fun x => x.1 == i) rs₁ rs₂ hf]

private theorem lookup_app_none (rs₁ rs₂ : List Report) (i : Nat)
    (h : (lookup rs₁ i).isSome = false) : lookup (rs₁ ++ rs₂) i = lookup rs₂ i := by
  unfold lookup
  cases hf : List.find? (fun r => r.1 == i) rs₁ with
  | none => rw [find?_append_none (fun x => x.1 == i) rs₁ rs₂ hf]
  | some r =>
    cases r with
    | mk j v => exact absurd h (by rw [lookup, hf, Option.map_some]; simp)

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
        simp
      · exact List.mem_cons_of_mem y (ih h)

private theorem drop_map {α β} (n : Nat) (f : α → β) (l : List α) :
    List.drop n (List.map f l) = List.map f (List.drop n l) := by
  induction n generalizing l with
  | zero => simp
  | succ n ih =>
    cases l with
    | nil => simp
    | cons x xs =>
      show List.drop n (List.map f xs) = List.map f (List.drop n xs)
      rw [ih]

private theorem range_drop (c n : Nat) :
    (List.range c).drop n = (List.range (c - n)).map (n + ·) := by
  rcases Nat.lt_or_ge c n with h | h
  · have h1 : (List.range c).drop n = [] :=
      List.drop_eq_nil_of_le (by rw [List.length_range]; omega)
    have h2 : c - n = 0 := Nat.sub_eq_zero_of_le (Nat.le_of_lt h)
    rw [h1, h2, List.range_zero, List.map_nil]
  · obtain ⟨m, rfl⟩ : ∃ m, c = n + m := Nat.exists_eq_add_of_le h
    rw [Nat.add_sub_cancel_left, List.range_add, List.drop_append,
      List.drop_eq_nil_of_le (Nat.le_of_eq (show (List.range n).length = n from List.length_range))]
    simp

private theorem cands_drop (s c n : Nat) :
    List.drop n ((List.range c).map (s + ·)) = (List.range (c - n)).map (s + n + ·) := by
  rw [drop_map, range_drop, List.map_map]
  simp [Nat.add_assoc]

private theorem map_congr' {α β} (f g : α → β) (l : List α)
    (h : ∀ x ∈ l, f x = g x) : List.map f l = List.map g l := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
    rw [List.map_cons, List.map_cons, h x (by simp),
      ih (fun y hy => h y (by simp [hy]))]

private theorem tw_split {α} (p : α → Bool) (l : List α) (n : Nat)
    (h : ∀ x ∈ List.take n l, p x = true) :
    l.takeWhile p = List.take n l ++ (List.drop n l).takeWhile p := by
  induction n generalizing l with
  | zero => simp
  | succ n ih =>
    cases l with
    | nil => simp
    | cons x xs =>
      have ht : List.take n.succ (x :: xs) = x :: List.take n xs := rfl
      have hd : List.drop n.succ (x :: xs) = List.drop n xs := rfl
      cases hpx : p x
      · rw [List.takeWhile, hpx, ht, hd]
        exfalso
        have h1 := h x (by rw [ht]; simp)
        rw [hpx] at h1
        exact absurd h1 (by decide)
      · rw [List.takeWhile, hpx, ht, hd,
          ih xs (fun y hy => h y (by rw [ht]; simp [hy]))]
        rfl

private theorem flatMap_congr {α β} (f g : α → List β) (l : List α)
    (h : ∀ x ∈ l, f x = g x) : l.flatMap f = l.flatMap g := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
    rw [List.flatMap_cons, List.flatMap_cons, h x (by simp),
      ih (fun y hy => h y (by simp [hy]))]

theorem sound : FrontierResumeOrderedOutput.Goal := by
  intro old new start count hc
  let cands := List.map (fun y => start + y) (List.range count)
  let p : Nat → Bool := fun i => (lookup old i).isSome
  let p' : Nat → Bool := fun i => (lookup (old ++ new) i).isSome
  let n := (List.takeWhile p cands).length
  have hall : ∀ x ∈ List.takeWhile p cands, p' x = true := by
    intro x hx
    have hcands : x ∈ cands := takeWhile_subset p cands x hx
    obtain ⟨j, _, rfl⟩ := List.mem_map.mp hcands
    show (lookup (old ++ new) (start + j)).isSome = true
    rw [lookup_app_some old new (start + j)
      (isSome_elim (takeWhile_only p cands _ hx))]
    exact takeWhile_only p cands _ hx
  have ht : List.take n cands = List.takeWhile p cands := take_length_takeWhile p cands
  have hall' : ∀ x ∈ List.take n cands, p' x = true := by
    intro x hx
    rw [ht] at hx
    exact hall x hx
  have hready : List.takeWhile p' cands
      = List.takeWhile p cands ++ List.takeWhile p' (List.drop n cands) := by
    rw [tw_split p' cands n hall', ht]
  have hlk : ∀ x ∈ List.takeWhile p cands, lookup (old ++ new) x = lookup old x := by
    intro x hx
    exact lookup_app_some old new x (isSome_elim (takeWhile_only p cands x hx))
  have hready2 : List.takeWhile p' (List.drop n cands)
      = List.takeWhile p' (List.map (fun y => start + n + y) (List.range (count - n))) := by
    rw [cands_drop]
  show List.flatMap (fun i => (lookup old i).getD []) (List.takeWhile p cands)
        ++ List.flatMap (fun i => (lookup (old ++ new) i).getD [])
          (List.takeWhile p' (List.map (fun y => start + n + y) (List.range (count - n))))
      = List.flatMap (fun i => (lookup (old ++ new) i).getD []) (List.takeWhile p' cands)
  rw [← hready2, hready, List.flatMap_append]
  rw [flatMap_congr (fun i => (lookup old i).getD [])
    (fun i => (lookup (old ++ new) i).getD [])
    (List.takeWhile p cands) (fun i hi => congrArg (·.getD []) (hlk i hi).symm)]

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MorselRetirement_FrontierResumeOrderedOutput
