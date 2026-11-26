import TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CheckpointRetention.Goal_RetiredPinRetentionMonotonicity

open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention


/-! Removing reader pins cannot lower the recycle floor. Every retained
segment at the larger (post-removal) floor also clears the smaller floor of
the superset of pins, so retirement never loses a needed segment. -/

open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention

private theorem floorAt_le_start (cp : Nat) (pins : List Nat) :
    floorAt cp pins ≤ cp := by
  induction pins generalizing cp with
  | nil => simp [floorAt]
  | cons p ps ih =>
      simp only [floorAt, List.foldl_cons]
      exact Nat.le_trans (ih (min cp p)) (Nat.min_le_left cp p)

private theorem floorAt_min_start (ps : List Nat) (a b : Nat) :
    floorAt (min a b) ps = min a (floorAt b ps) := by
  simpa [floorAt] using
    (List.foldl_assoc (op := min) (l := ps) (a₁ := a) (a₂ := b))

private theorem floorAt_cons (cp p : Nat) (ps : List Nat) :
    floorAt cp (p :: ps) = min p (floorAt cp ps) := by
  cases ps with
  | nil => simp [floorAt, Nat.min_comm]
  | cons q qs =>
      change floorAt (min (min cp p) q) qs = min p (floorAt (min cp q) qs)
      rw [show min (min cp p) q = min p (min cp q) by omega]
      exact floorAt_min_start qs p (min cp q)

private theorem floorAt_le_of_mem (cp p : Nat) (old : List Nat) (hp : p ∈ old) :
    floorAt cp old ≤ p := by
  induction old generalizing cp with
  | nil => simp at hp
  | cons q qs ih =>
      simp only [List.mem_cons] at hp
      rcases hp with rfl | hq
      · exact Nat.le_trans (floorAt_le_start (min cp p) qs) (Nat.min_le_right cp p)
      · simpa [floorAt] using ih (min cp q) hq

private theorem floorAt_subset_le (old pins : List Nat) (cp : Nat)
    (h : ∀ p ∈ pins, p ∈ old) : floorAt cp old ≤ floorAt cp pins := by
  induction pins with
  | nil => exact floorAt_le_start cp old
  | cons p ps ih =>
      have hps : floorAt cp old ≤ floorAt cp ps := by
        apply ih
        intro q hq
        exact h q (by simp [hq])
      have hpold : floorAt cp old ≤ p := floorAt_le_of_mem cp p old (h p (by simp))
      rw [floorAt_cons]
      exact Nat.le_min.mpr ⟨hpold, hps⟩

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention.RetiredPinRetentionMonotonicity.Goal := by
  intro segs cp old fresh hfresh seg hseg
  have hle : floorAt cp old ≤ floorAt cp fresh := floorAt_subset_le old fresh cp hfresh
  simp only [retained, List.mem_filter] at hseg ⊢
  exact ⟨hseg.1, by simp [Nat.lt_of_le_of_lt hle (of_decide_eq_true hseg.2)]⟩
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CheckpointRetention.Goal_RetiredPinRetentionMonotonicity
