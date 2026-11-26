import Std
import TinylambOpt.ResearchGoals.JitKernels.FloatingReduction
import TinylambOpt.ResearchProofs.JitKernels.Common_FloatingReduction
/-! Certificate for `TinylambOpt.ResearchGoals.JitKernels.FloatingReduction.FirstInfinityFromFinalAccumulator.Goal`. -/

/-! With finite inputs an overflow to infinity is absorbing: the unpacked adder
returns infinity for `inf + finite`, and a sum of two finite values never produces
NaN. Therefore the first infinity in the scan is still present in the final
accumulator, and conversely the final accumulator is itself one of the scanned
prefixes. Skipping the prefix scan and keeping only the sticky infinity flag is
hence safe for a finite-input reduction. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace Nov26.Proof_FloatingReduction_FirstInfinityFromFinalAccumulator

open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction
open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction.FirstInfinityFromFinalAccumulator
open TinylambOpt.ResearchProofs.JitKernels.FloatingReductionCommon
open Float.Model
open Float.Model.UnpackedFloat

private theorem unpackFinite_of_mem {x : Float} (hx : finite x = true) :
    x.toModel.unpack.isFinite = true := by
  simpa [_root_.TinylambOpt.ResearchGoals.JitKernels.FloatingReduction.finite, Float.isFinite, Float.Model.isFinite] using hx

private theorem foldl_unpack_inf_of_inf (acc : Float) (xs : List Float) (s : Sign)
    (hs : acc.toModel.unpack = .infinity s) (hxs : ∀ x ∈ xs, finite x) :
    (xs.foldl (fun a x => a + x) acc).toModel.unpack = .infinity s := by
  induction xs generalizing acc with
  | nil => simpa
  | cons x xs ih =>
      rw [List.foldl_cons]
      have hs' : (acc + x).toModel.unpack = .infinity s :=
        add_unpack_inf_of_inf_of_finite acc x s hs (unpackFinite_of_mem (hxs x (by simp)))
      apply ih (acc + x) hs'
      intro y hy
      exact hxs y (by simp [hy])

private theorem foldl_inf_of_inf (acc : Float) (xs : List Float)
    (hacc : acc.isInf = true) (hxs : ∀ x ∈ xs, finite x) :
    (xs.foldl (fun a x => a + x) acc).isInf = true := by
  obtain ⟨s, hs⟩ : ∃ s, acc.toModel.unpack = .infinity s := by
    simp only [Float.isInf, Float.Model.isInf, UnpackedFloat.isInf] at hacc
    cases hu : acc.toModel.unpack with
    | infinity s => exact ⟨s, rfl⟩
    | _ => simp [hu] at hacc
  simp only [Float.isInf, Float.Model.isInf]
  rw [foldl_unpack_inf_of_inf acc xs s hs hxs]
  rfl

private theorem aux (acc : Float) (xs : List Float) (hnan : acc.isNaN = false)
    (hxs : ∀ x ∈ xs, finite x) :
    ((xs.scanl (fun a x => a + x) acc).findIdx? Float.isInf).isSome =
      (xs.foldl (fun a x => a + x) acc).isInf := by
  induction xs generalizing acc with
  | nil =>
      by_cases hp : Float.isInf acc = true <;> simp [hp]
  | cons x xs ih =>
      rw [List.scanl_cons, List.findIdx?_cons, List.foldl_cons]
      by_cases hinf : Float.isInf acc
      · obtain ⟨s, hs⟩ : ∃ s, acc.toModel.unpack = .infinity s := by
          simp only [Float.isInf, Float.Model.isInf, UnpackedFloat.isInf] at hinf
          cases hu : acc.toModel.unpack with
          | infinity s => exact ⟨s, rfl⟩
          | _ => simp [hu] at hinf
        have htail := foldl_inf_of_inf (acc + x) xs
          (add_inf_of_inf_of_finite acc x s hs (unpackFinite_of_mem (hxs x (by simp))))
          (by intro y hy; exact hxs y (by simp [hy]))
        simp [hinf, htail]
      · have hnn : (acc + x).isNaN = false :=
          add_not_nan_of_not_nan_of_finite acc x hnan (unpackFinite_of_mem (hxs x (by simp)))
        have ih' := ih (acc + x) hnn (by intro y hy; exact hxs y (by simp [hy]))
        simp [hinf, ih']

theorem sound : Goal := by
  intro xs s hs hxs
  unfold firstInf total
  exact aux s xs (finite_not_nan hs) hxs

end Nov26.Proof_FloatingReduction_FirstInfinityFromFinalAccumulator
