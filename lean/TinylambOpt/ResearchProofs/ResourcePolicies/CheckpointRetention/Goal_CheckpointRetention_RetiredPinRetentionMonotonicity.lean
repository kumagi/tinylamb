import TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CheckpointRetention_RetiredPinRetentionMonotonicity
open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention

private theorem foldl_min_le_init : ∀ (l : List Nat) (a : Nat), l.foldl min a ≤ a := by
  intro l
  induction l with
  | nil => intro a; simp
  | cons x xs ih =>
    intro a
    simp only [List.foldl_cons]
    exact Nat.le_trans (ih (min a x)) (Nat.min_le_left _ _)

private theorem foldl_min_le_of_mem : ∀ (l : List Nat) (a x : Nat), x ∈ l → l.foldl min a ≤ x := by
  intro l
  induction l with
  | nil => intro a x hx; cases hx
  | cons y ys ih =>
    intro a x hx
    simp only [List.foldl_cons]
    rcases List.mem_cons.mp hx with rfl | hx
    · exact Nat.le_trans (foldl_min_le_init ys (min a x)) (Nat.min_le_right _ _)
    · exact ih (min a y) x hx

private theorem foldl_min_eq_of_mem : ∀ (l : List Nat) (a p : Nat), p ∈ l →
    l.foldl min (min a p) = l.foldl min a := by
  intro l
  induction l with
  | nil => intro a p hp; cases hp
  | cons q qs ih =>
    intro a p hp
    simp only [List.foldl_cons]
    rcases List.mem_cons.mp hp with rfl | hp
    · -- p == q
      have h1 : min (min a p) p = min a p := by omega
      simp [h1]
    · -- p ∈ qs
      have h1 : min (min a p) q = min (min a q) p := by omega
      rw [h1]
      exact ih (min a q) p hp

private theorem foldl_min_le_of_subset : ∀ (fresh old : List Nat) (a : Nat),
    (∀ p ∈ fresh, p ∈ old) → old.foldl min a ≤ fresh.foldl min a := by
  intro fresh
  induction fresh with
  | nil =>
    intro old a hsub
    simp [foldl_min_le_init]
  | cons p ps ih =>
    intro old a hsub
    simp only [List.foldl_cons]
    have hp : p ∈ old := hsub p (by simp)
    have hps : ∀ q ∈ ps, q ∈ old := fun q hq => hsub q (List.mem_cons_of_mem _ hq)
    have h1 : old.foldl min (min a p) ≤ ps.foldl min (min a p) := ih old (min a p) hps
    have h2 : old.foldl min (min a p) = old.foldl min a := foldl_min_eq_of_mem old a p hp
    rw [h2] at h1
    exact h1

theorem sound : RetiredPinRetentionMonotonicity.Goal := by
  intro segs cp old fresh hsub seg hseg
  simp only [retained, List.mem_filter, decide_eq_true_eq] at hseg ⊢
  refine ⟨hseg.1, ?_⟩
  have hle : floorAt cp old ≤ floorAt cp fresh :=
    foldl_min_le_of_subset fresh old cp hsub
  exact Nat.lt_of_le_of_lt hle hseg.2
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CheckpointRetention_RetiredPinRetentionMonotonicity
