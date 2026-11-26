import TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement.LimitCancellationFrontier.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
A finished contiguous prefix already satisfying LIMIT makes later reports moot.

Claim (`LimitCancellationFrontier.Goal`): appending any `future` reports to `rs`
can only extend the drained output list — ids ready in `rs` stay ready with the
same rows because `find?` returns the first match and existing entries keep their
position — so once `k ≤ (emit rs start count).length` rows are available, the
first `k` output rows of `emit (rs++future) start count` equal those of
`emit rs start count`. This lets an ordered LIMIT cancel morsels as soon as a
contiguous completed prefix contains enough rows, regardless of late arrivals.
-/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MorselRetirement_LimitCancellationFrontier
open TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement

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

private theorem lookup_app_some (rs₁ rs₂ : List Report) (i : Nat)
    (h : ∃ v, lookup rs₁ i = some v) : lookup (rs₁ ++ rs₂) i = lookup rs₁ i := by
  unfold lookup
  cases hf : List.find? (fun r => r.1 == i) rs₁ with
  | none =>
    obtain ⟨v, hv⟩ := h
    unfold lookup at hv
    rw [hf] at hv
    simp only [Option.map_none] at hv
    cases hv
  | some r => rw [find?_append_some (fun x => x.1 == i) rs₁ rs₂ hf]

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

private theorem length_take_of_le (L : List Nat) (k : Nat) (h : k ≤ L.length) :
    (List.take k L).length = k := by
  induction k generalizing L with
  | zero => simp
  | succ k ih =>
    cases L with
    | nil => exact absurd h (by simp)
    | cons a as =>
      rw [show List.take k.succ (a :: as) = a :: List.take k as from rfl, List.length_cons,
        ih as (by simpa using h)]

private theorem take_len_app (A M : List Nat) : (A ++ M).take A.length = A := by
  induction A with
  | nil => rfl
  | cons a as ih =>
    exact congrArg (a :: ·) ih

private theorem take_app_le (L E : List Nat) (k : Nat) (h : k ≤ L.length) :
    (L ++ E).take k = L.take k := by
  obtain ⟨m, hm⟩ : ∃ m, L.length = k + m := Nat.exists_eq_add_of_le h
  have hk : (List.take k L).length = k := length_take_of_le L k h
  rw [show L ++ E = List.take k L ++ (List.drop k L ++ E)
      from by rw [← List.append_assoc, List.take_append_drop k L]]
  rw [show (List.take k L ++ (List.drop k L ++ E)).take k
        = (List.take k L ++ (List.drop k L ++ E)).take (List.take k L).length from by rw [hk]]
  rw [take_len_app]

theorem sound : LimitCancellationFrontier.Goal := by
  intro rs start count k future hc hle
  let cands := List.map (fun y => start + y) (List.range count)
  let p : Nat → Bool := fun i => (lookup rs i).isSome
  let p' : Nat → Bool := fun i => (lookup (rs ++ future) i).isSome
  let n := (List.takeWhile p cands).length
  have hlks : ∀ x ∈ List.takeWhile p cands, lookup (rs ++ future) x = lookup rs x := by
    intro x hx
    exact lookup_app_some rs future x (isSome_elim (takeWhile_only p cands x hx))
  have hall : ∀ x ∈ List.takeWhile p cands, p' x = true := by
    intro x hx
    show (lookup (rs ++ future) x).isSome = true
    rw [hlks x hx]
    exact takeWhile_only p cands x hx
  have ht : List.take n cands = List.takeWhile p cands := take_length_takeWhile p cands
  have hall' : ∀ x ∈ List.take n cands, p' x = true := by
    intro x hx
    rw [ht] at hx
    exact hall x hx
  have hsplit : List.takeWhile p' cands
      = List.takeWhile p cands ++ List.takeWhile p' (List.drop n cands) := by
    rw [tw_split p' cands n hall', ht]
  have hflat : List.flatMap (fun i => (lookup (rs ++ future) i).getD [])
        (List.takeWhile p cands)
      = List.flatMap (fun i => (lookup rs i).getD []) (List.takeWhile p cands) :=
    flatMap_congr (fun i => (lookup (rs ++ future) i).getD [])
      (fun i => (lookup rs i).getD []) (List.takeWhile p cands)
      (fun i hi => congrArg (·.getD []) (hlks i hi))
  show (List.flatMap (fun i => (lookup (rs ++ future) i).getD []) (List.takeWhile p' cands)).take k
      = (List.flatMap (fun i => (lookup rs i).getD []) (List.takeWhile p cands)).take k
  rw [hsplit, List.flatMap_append, hflat]
  exact take_app_le _ _ k hle

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MorselRetirement_LimitCancellationFrontier
