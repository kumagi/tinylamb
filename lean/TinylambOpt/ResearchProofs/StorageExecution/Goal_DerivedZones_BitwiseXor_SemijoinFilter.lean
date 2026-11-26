import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
import TinylambOpt.ResearchProofs.StorageExecution.SE_DerivedZones_BitwiseXor

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BitwiseXor_SemijoinFilter

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BitwiseXor

/-! Truth check on a small grid: for every hypothesis-satisfying input the
build-key set restriction to the page's derived XOR interval leaves the
semijoin filter unchanged. -/
private def sublists : List Nat → List (List Nat)
  | [] => [[]]
  | x :: xs => (sublists xs).flatMap (fun ys => [ys, x :: ys])

private def smallBounds : List Bounds :=
  (List.range 5).flatMap fun lo =>
    ((List.range 5).filter fun hi => decide (lo ≤ hi)).map fun hi => ⟨lo, hi⟩

private def counterexamples : List (Nat × Bounds × Bounds × List Nat × Nat × Nat) :=
  (List.range 2).flatMap fun t =>
  smallBounds.flatMap fun a =>
  smallBounds.flatMap fun b =>
  (sublists [0, 1, 2, 3, 4, 5, 6, 7]).flatMap fun ks =>
  (List.range 4).flatMap fun x =>
  (List.range 4).filterMap fun y =>
    if a.lo ≤ x ∧ x ≤ a.hi ∧ b.lo ≤ y ∧ y ≤ b.hi then
      let p : List (Option Row) := [some (x, y), none, some (x, y)]
      let r := kernel.bounds t a b
      if (scores kernel t p).filter ks.contains ≠
          (scores kernel t p).filter
            ((ks.filter (fun k => r.lo ≤ k && k ≤ r.hi)).contains) then
        some (t, a, b, ks, x, y)
      else none
    else none

#eval counterexamples.length

/-- Reuse the already proven XOR sound-interval certificate. -/
private theorem soundInterval_xor : soundInterval kernel := by
  exact TinylambOpt.ResearchProofs.StorageExecution.DerivedZones.BitwiseXor.sound_SoundInterval

/-- Every non-NULL score of a covered page lies in the certified XOR interval. -/
private theorem inside_scores {t : Nat} {p : List (Option Row)} {a b : Bounds}
    (hc : covers p a b) :
    ∀ v ∈ scores kernel t p, inside (kernel.bounds t a b) v := by
  intro v hv
  obtain ⟨r, hrp, hm⟩ : ∃ r ∈ p, r.map (kernel.score t) = some v := by
    simpa [scores] using hv
  obtain ⟨x, y, hr2, hscore⟩ : ∃ x y, some (x, y) ∈ p ∧ kernel.score t (x, y) = v := by
    cases r with
    | none => cases hm
    | some xy =>
        cases xy with
        | mk x y => exact ⟨x, y, hrp, by simpa using hm⟩
  obtain ⟨hax, hby⟩ := hc (some (x, y)) hr2 x y rfl
  rw [← hscore]
  exact soundInterval_xor t a b x y hax hby

/-- Restricting the build keys to the page's certified XOR-score interval is a
no-op on scores actually produced by the page, so the semijoin filter is
unchanged. -/
theorem sound_SemijoinFilter : SemijoinFilter.Goal := by
  intro t p a b ks hcov
  change (scores kernel t p).filter ks.contains =
    (scores kernel t p).filter
      ((ks.filter
        (fun k => (kernel.bounds t a b).lo ≤ k && k ≤ (kernel.bounds t a b).hi)).contains)
  apply List.filter_congr
  intro v hv
  have hvR := inside_scores hcov v hv
  simp only [List.contains]
  simp [hvR.1, hvR.2]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BitwiseXor_SemijoinFilter
