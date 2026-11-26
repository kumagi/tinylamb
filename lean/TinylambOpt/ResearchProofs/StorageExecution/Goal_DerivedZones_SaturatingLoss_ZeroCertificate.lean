import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_SaturatingLoss_ZeroCertificate

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.SaturatingLoss

-- Exhaustive check on a small grid: no counterexample to
-- `(kernel.bounds t a b).hi = 0 ↔ a.hi ≤ b.lo + t`.
private def counterexamples : List (Nat × Bounds × Bounds) :=
  (List.range 10).flatMap fun t =>
  (List.range 10).flatMap fun alo =>
  (List.range 10).flatMap fun ahi =>
  (List.range 10).flatMap fun blo =>
  (List.range 10).filterMap fun bhi =>
    let b : Bounds := ⟨blo, bhi⟩
    if decide (((kernel.bounds t ⟨alo, ahi⟩ b).hi = 0) ≠ (ahi ≤ blo + t)) then
      some (t, ⟨alo, ahi⟩, b)
    else none

#eval counterexamples

private def checkSmall : Bool :=
  List.all
    ((List.range 10).flatMap fun t =>
      (List.range 10).flatMap fun alo =>
      (List.range 10).flatMap fun ahi =>
      (List.range 10).flatMap fun blo =>
      (List.range 10).map fun bhi =>
        decide ((kernel.bounds t ⟨alo, ahi⟩ ⟨blo, bhi⟩).hi = 0 ↔ ahi ≤ blo + t))
    id

#eval checkSmall

theorem sound_ZeroCertificate : SaturatingLoss.ZeroCertificate.Goal := by
  intro t a b
  rcases a with ⟨alo, ahi⟩
  rcases b with ⟨blo, bhi⟩
  simp [kernel]
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_SaturatingLoss_ZeroCertificate
