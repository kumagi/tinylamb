import TinylambOpt.ResearchGoals.Atlas.SynopsisBounds

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PageMoments_AllInside

open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.PageMoments

/-- The centred squares of a page are all non-negative, so their sum is too. -/
theorem sum_sq_nonneg (c : Int) : ∀ xs : List Int, 0 ≤ (xs.map (fun y => (y-c)^2)).sum := by
  intro xs
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      have hx : 0 ≤ (x-c)^2 := Int.sq_nonneg _
      omega

/-- A single centred square is at most the page's total energy. -/
theorem mem_sq_le_energy (c : Int) : ∀ (s : Page) (a : Int), a ∈ s → (a-c)^2 ≤ energy s c := by
  intro s
  induction s with
  | nil => simp
  | cons x xs ih =>
      intro a ha
      simp only [List.mem_cons] at ha
      simp only [energy, List.map_cons, List.sum_cons]
      have hsum : 0 ≤ (xs.map (fun y => (y-c)^2)).sum := sum_sq_nonneg c xs
      rcases ha with rfl | ha
      · omega
      · have hle := ih a ha
        simp only [energy] at hle
        have hx : 0 ≤ (x-c)^2 := Int.sq_nonneg _
        omega

/-- If the energy is below (r+1)^2, no entry can lie outside [-r, r] around c.
    Otherwise that entry alone contributes at least (r+1)^2 to the energy. -/
theorem sound_PageMoments_AllInside : PageMoments.AllInside.Goal := by
  intro s c r h
  simp [trimmed]
  intro a ha
  by_cases hle : (a-c).natAbs ≤ r
  · exact hle
  · have hrle : r + 1 ≤ (a-c).natAbs := by omega
    have hsq1 : ((r+1:Nat):Int)^2 ≤ (((a-c).natAbs:Nat):Int)^2 := by
      have hnat : (r+1)^2 ≤ (a-c).natAbs^2 := by
        rw [Nat.pow_two, Nat.pow_two]
        exact Nat.mul_le_mul hrle hrle
      exact_mod_cast hnat
    have hsq : ((r+1:Nat):Int)^2 ≤ (a-c)^2 := by
      simp [Int.pow_succ] at hsq1 ⊢
      exact hsq1
    have hmemsq := mem_sq_le_energy c s a ha
    omega

end TinylambOpt.ResearchProofs.Atlas.Goal_PageMoments_AllInside
