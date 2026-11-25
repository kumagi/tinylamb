import TinylambOpt.ResearchGoals.Atlas.ExecutorResources

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Pipeline_SplitOverlap

open TinylambOpt.ResearchGoals.Atlas.ExecutorResources
open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Pipeline Pipeline

/-! Running a suffix from a state whose second clock is no larger never finishes later. -/
private theorem step_t_mono (s t₁ t₂ : Nat) (h : t₁ ≤ t₂) (xs : List Job) :
    (xs.foldl step (s, t₁)).2 ≤ (xs.foldl step (s, t₂)).2 := by
  induction xs generalizing s t₁ t₂ with
  | nil => simpa using h
  | cons j rest ih =>
      rcases j with ⟨a, b⟩
      rw [List.foldl_cons, List.foldl_cons]
      dsimp only [step]
      apply ih
      omega

/-! Splitting one job into two halves keeps the first clock and does not increase the second. -/
private theorem split_pair (s t a b c d : Nat) :
    let p := [(a, b), (c, d)].foldl step (s, t)
    let q := [(a + c, b + d)].foldl step (s, t)
    p.1 = q.1 ∧ p.2 ≤ q.2 := by
  dsimp
  simp [step]
  constructor <;> omega

theorem sound_Pipeline_SplitOverlap : SplitOverlap.Goal := by
  intro l r a b c d
  dsimp [finish]
  rw [show (l ++ [(a, b), (c, d)] ++ r).foldl step (0, 0) =
      r.foldl step ([(a, b), (c, d)].foldl step (l.foldl step (0, 0))) by
    rw [List.foldl_append, List.foldl_append]]
  rw [show (l ++ [(a + c, b + d)] ++ r).foldl step (0, 0) =
      r.foldl step ([(a + c, b + d)].foldl step (l.foldl step (0, 0))) by
    rw [List.foldl_append, List.foldl_append]]
  let s := (l.foldl step (0, 0)).1
  let t := (l.foldl step (0, 0)).2
  change (r.foldl step ([(a, b), (c, d)].foldl step (s, t))).2 ≤
    (r.foldl step ([(a + c, b + d)].foldl step (s, t))).2
  have hgen := split_pair s t a b c d
  dsimp at hgen
  have hmono := step_t_mono (([(a, b), (c, d)].foldl step (s, t)).1) _ _ hgen.2 r
  simpa [step, Nat.add_assoc] using hmono

end TinylambOpt.ResearchProofs.Atlas.Goal_Pipeline_SplitOverlap
