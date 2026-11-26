import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_SaturatingLoss_SumCeiling

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.SaturatingLoss

-- Exhaustive check on a small grid: no hypothesis-satisfying counterexample.
private def smallBounds : List Bounds :=
  (List.range 6).flatMap fun lo =>
    ((List.range 6).filter (fun hi => decide (lo ≤ hi))).map fun hi => ⟨lo, hi⟩

private def counterexamples : List (Nat × Bounds × Bounds × List (Option Row)) :=
  (List.range 5).flatMap fun t =>
  smallBounds.flatMap fun a =>
  smallBounds.flatMap fun b =>
  ([[], [none], [some (a.lo, b.lo)], [some (a.hi, b.hi)],
    [none, some (a.lo, b.hi), some (a.hi, b.lo)]]).filterMap fun p =>
    let covered := p.all fun r =>
      match r with
      | none => true
      | some (x, y) => decide (a.lo ≤ x ∧ x ≤ a.hi ∧ b.lo ≤ y ∧ y ≤ b.hi)
    if decide (covered ∧
        ¬ (let vs := scores kernel t p
           vs.length * (kernel.bounds t a b).lo ≤ vs.sum ∧
           vs.sum ≤ vs.length * (kernel.bounds t a b).hi)) then
      some (t, a, b, p)
    else none

#eval counterexamples

/-- Truncated subtraction is monotone in the minuend and antitone in the
    subtrahend, so every non-NULL score of a covered page lies between the
    independently derived corner scores. -/
private theorem saturating_score_bounds {t : Nat} {p : List (Option Row)} {a b : Bounds}
    (hc : covers p a b) {v : Nat} (hv : v ∈ scores kernel t p) :
    a.lo - (b.hi + t) ≤ v ∧ v ≤ a.hi - (b.lo + t) := by
  rcases List.mem_filterMap.mp hv with ⟨r, hrp, hmap⟩
  rcases Option.map_eq_some_iff.mp hmap with ⟨xy, hrxy, hscore⟩
  rcases xy with ⟨x, y⟩
  have hcovxy := hc r hrp x y hrxy
  simp only [inside] at hcovxy
  rw [← hscore]
  dsimp [kernel]
  omega

/-- A list of n values, each between lo and hi, has its sum between n*lo
    and n*hi. -/
private theorem sum_bounds {lo hi : Nat} {vs : List Nat}
    (hv : ∀ v ∈ vs, lo ≤ v ∧ v ≤ hi) :
    vs.length * lo ≤ vs.sum ∧ vs.sum ≤ vs.length * hi := by
  induction vs with
  | nil => simp
  | cons v vs ih =>
    have hhead := hv v (by simp)
    have htail := ih (fun x hx => hv x (by simp [hx]))
    simp only [List.length_cons, List.sum_cons, Nat.add_mul]
    omega

theorem sound_SumCeiling : SaturatingLoss.SumCeiling.Goal := by
  intro t p a b hc
  change (scores kernel t p).length * (a.lo - (b.hi + t)) ≤ (scores kernel t p).sum ∧
    (scores kernel t p).sum ≤ (scores kernel t p).length * (a.hi - (b.lo + t))
  exact sum_bounds (fun v hv => saturating_score_bounds (t := t) hc hv)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_SaturatingLoss_SumCeiling
