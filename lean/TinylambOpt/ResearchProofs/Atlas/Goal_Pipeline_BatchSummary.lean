import TinylambOpt.ResearchGoals.Atlas.ExecutorResources

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Pipeline_BatchSummary

open TinylambOpt.ResearchGoals.Atlas.ExecutorResources
open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Pipeline Pipeline

abbrev Job := Nat × Nat

private def sumP (xs : List Job) : Nat := (xs.map Prod.fst).sum
private def sumS (xs : List Job) : Nat := (xs.map Prod.snd).sum

/-! The first clock after a prefix is `c₁ + sumP xs`. -/

private theorem foldl_fst (xs : List Job) (c₁ c₂ : Nat) :
    (xs.foldl step (c₁, c₂)).1 = c₁ + sumP xs := by
  induction xs generalizing c₁ c₂ with
  | nil => simp [sumP]
  | cons x xs ih =>
      rcases x with ⟨p, s⟩
      have ih' := ih (step (c₁, c₂) (p, s)).1 (step (c₁, c₂) (p, s)).2
      rw [List.foldl_cons, ih']
      simp [step, sumP]
      omega

private theorem go_ge_sumP (xs : List Job) (c₁ c₂ : Nat) (h : c₁ ≤ c₂) :
    c₁ + sumP xs ≤ (xs.foldl step (c₁, c₂)).2 := by
  induction xs generalizing c₁ c₂ with
  | nil =>
      simpa [sumP] using h
  | cons x xs ih =>
      rcases x with ⟨p, s⟩
      have hc : (step (c₁, c₂) (p, s)).1 ≤ (step (c₁, c₂) (p, s)).2 := by
        simp [step]
        omega
      have ih' := ih (step (c₁, c₂) (p, s)).1 (step (c₁, c₂) (p, s)).2 hc
      rw [List.foldl_cons]
      simp only [sumP, List.map_cons, List.sum_cons] at ih' ⊢
      simp [step] at ih' ⊢
      omega

/-! Running `xs` from a state `(c₁, c₂)` with `c₁ ≤ c₂` either finishes on the
    second resource after `sumS xs` more work, or waits for the first resource
    and then behaves like a fresh run of `xs`. -/

private theorem go_from (xs : List Job) (c₁ c₂ : Nat) (h : c₁ ≤ c₂) :
    (xs.foldl step (c₁, c₂)).2 = max (c₂ + sumS xs) (c₁ + (xs.foldl step (0, 0)).2) := by
  induction xs generalizing c₁ c₂ with
  | nil =>
      simp [sumS, Nat.max_eq_left h]
  | cons x xs ih =>
      rcases x with ⟨p, s⟩
      let c := step (c₁, c₂) (p, s)
      have hc : c.1 ≤ c.2 := by
        simp [c, step]
        omega
      have ih₁ := ih c.1 c.2 hc
      change (xs.foldl step c).2 =
        max (c₂ + sumS ((p, s) :: xs)) (c₁ + (xs.foldl step (step (0, 0) (p, s))).2)
      rw [ih₁]
      let d := step (0, 0) (p, s)
      have hd : d.1 ≤ d.2 := by
        simp [d, step]
      have ih₀ := ih d.1 d.2 hd
      change max (c.2 + sumS xs) (c.1 + (xs.foldl step (0, 0)).2) =
        max (c₂ + sumS ((p, s) :: xs)) (c₁ + (xs.foldl step d).2)
      rw [ih₀]
      simp [c, d, step, sumS]
      omega

theorem sound_Pipeline_BatchSummary : BatchSummary.Goal := by
  intro a b
  have hfst : (a.foldl step (0, 0)).1 = sumP a := by
    simpa using foldl_fst a 0 0
  have hge : sumP a ≤ (a.foldl step (0, 0)).2 := by
    simpa using go_ge_sumP a 0 0 (by omega)
  have hstate : a.foldl step (0, 0) = (sumP a, (a.foldl step (0, 0)).2) := by
    ext <;> simp [hfst]
  simp only [finish, List.foldl_append]
  rw [hstate]
  simpa [sumP, sumS] using go_from b (sumP a) (a.foldl step (0, 0)).2 hge

end TinylambOpt.ResearchProofs.Atlas.Goal_Pipeline_BatchSummary
