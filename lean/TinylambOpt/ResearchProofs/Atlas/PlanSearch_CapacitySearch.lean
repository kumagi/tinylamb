import TinylambOpt.ResearchGoals.Atlas.PlanSearch

/-!
The SQL optimizer can prune or reorganize its plan search only under the relevant
ordering and cost assumptions.

Proved

- `ParallelCuts.CapacitySearch` — `∀ a b xs, a ≤ b → (∀ w ∈ xs, w ≤ a) → required b xs
  ≤ required a xs`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.PlanSearch_CapacitySearch

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.ParallelCuts

/-! `pack` is next-fit: the state `(k, s)` records the number of already closed
blocks and the load of the block currently open.  Increasing the capacity can
never increase `k`.  The induction needs a second component: when both scans
have closed the same number of blocks, the larger capacity has at least as
much room left in the open block.  This also covers the delicate case where
the larger-capacity scan closes a block while the smaller-capacity scan does
not: then the two scans are at different `k` and the open loads are irrelevant. -/

private def csStep (cap : Nat) (s : Nat × Nat) (w : Nat) : Nat × Nat :=
  if s.2 + w ≤ cap then (s.1, s.2 + w) else (s.1 + 1, w)

private def csInv (sb sa : Nat × Nat) : Prop :=
  sb.1 ≤ sa.1 ∧ (sb.1 = sa.1 → sb.2 ≤ sa.2)

private theorem csStep_mono (a b : Nat) (hab : a ≤ b) (sb sa : Nat × Nat)
    (h : csInv sb sa) (w : Nat) : csInv (csStep b sb w) (csStep a sa w) := by
  unfold csInv
  constructor
  · have hk : sb.1 ≤ sa.1 := h.1
    have hs_eq : sb.1 = sa.1 → sb.2 ≤ sa.2 := h.2
    unfold csStep
    by_cases ha : sa.2 + w ≤ a
    · by_cases hb : sb.2 + w ≤ b
      · simp [ha, hb]
        exact hk
      · simp [ha, hb]
        by_cases hle : sa.1 ≤ sb.1
        · have heq : sb.1 = sa.1 := by omega
          have hs := hs_eq heq
          omega
        · omega
    · by_cases hb : sb.2 + w ≤ b
      · simp [ha, hb]
        omega
      · simp [ha, hb]
        omega
  · intro heq
    have hk : sb.1 ≤ sa.1 := h.1
    have hs_eq : sb.1 = sa.1 → sb.2 ≤ sa.2 := h.2
    unfold csStep at heq ⊢
    by_cases ha : sa.2 + w ≤ a
    · by_cases hb : sb.2 + w ≤ b
      · simp [ha, hb] at heq ⊢
        have hs := hs_eq heq
        omega
      · simp [ha, hb] at heq ⊢
    · by_cases hb : sb.2 + w ≤ b
      · simp [ha, hb] at heq ⊢
        omega
      · simp [ha, hb] at heq ⊢

private theorem csFold_mono (a b : Nat) (hab : a ≤ b) (sb sa : Nat × Nat)
    (h : csInv sb sa) (xs : List Nat) :
    csInv (xs.foldl (csStep b) sb) (xs.foldl (csStep a) sa) := by
  induction xs generalizing sb sa with
  | nil => exact h
  | cons w ws ih =>
      simpa using ih (csStep b sb w) (csStep a sa w)
        (csStep_mono a b hab sb sa h w)

theorem sound_CapacitySearch : ParallelCuts.CapacitySearch.Goal := by
  intro a b xs hab _
  have hmono : csInv (xs.foldl (csStep b) (0, 0)) (xs.foldl (csStep a) (0, 0)) :=
    csFold_mono a b hab (0, 0) (0, 0) (by simp [csInv]) xs
  unfold ParallelCuts.required
  by_cases hempty : xs.isEmpty
  · simp [hempty]
  · simp [hempty]
    have hk : (pack b xs).1 ≤ (pack a xs).1 := by
      change (xs.foldl (csStep b) (0, 0)).1 ≤ (xs.foldl (csStep a) (0, 0)).1
      exact hmono.1
    omega

end TinylambOpt.ResearchProofs.Atlas.PlanSearch_CapacitySearch
