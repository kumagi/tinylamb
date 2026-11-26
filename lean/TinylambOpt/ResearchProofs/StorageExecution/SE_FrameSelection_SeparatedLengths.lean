import TinylambOpt.ResearchProofs.StorageExecution.SE_FrameSelection_Common
import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Proved (9 claims, including the following)

- `SeparatedLengths.BoundedRaw` — A forced frame cap still limits worst-case expansion
  to raw cells and per-frame overhead.
- `SeparatedLengths.ByteFrontier` — Use the actual codec and raw fallback in the
  size/seek optimum.
- `SeparatedLengths.ControlBytes` — `∀ xs, admissible xs → (write xs).length =
  byteCount (2*xs.length) + (sizes xs).sum`

Refuted by counterexample

- `SeparatedLengths.GreedyCut` — Aggressive hypothesis: the cheapest two adjacent
  groups never need reconsideration when a third arrives.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.FrameSelection.SeparatedLengths

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.SeparatedLengths

theorem sound_ByteFrontier : ByteFrontier.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.byteFrontier_gen write
theorem sound_StrictSavings : StrictSavings.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.strictSavings_holds write
theorem sound_HotPointReads : HotPointReads.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.hotPointReads_holds write
theorem sound_SeekCap : SeekCap.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.seekCap_holds write
theorem sound_ParallelEncode : ParallelEncode.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.parallelEncode_holds write
theorem sound_MandatoryCuts : MandatoryCuts.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.mandatoryCuts_holds write
theorem sound_BoundedRaw : BoundedRaw.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.boundedRaw_gen write

private theorem sizes_length (xs : List Nat) : (sizes xs).length = xs.length := by
  simp [sizes]

private theorem pack_length (w : Nat) (xs : List Nat) :
    (pack w xs).length = byteCount (xs.length * w) := by
  simp [pack, TinylambOpt.ResearchProofs.StorageExecution.SECommon.little_len]

private theorem snd_zip (xs ys : List Nat) (h : ys.length ≤ xs.length) :
    (xs.zip ys).map Prod.snd = ys := by
  induction xs generalizing ys with
  | nil =>
      cases ys with
      | nil => rfl
      | cons y r => simp at h
  | cons x xt ih =>
      cases ys with
      | nil => rfl
      | cons y r =>
          have hl : r.length ≤ xt.length := by
            simpa using h
          show y :: (xt.zip r).map Prod.snd = y :: r
          rw [ih r hl]

theorem sound_ControlBytes : ControlBytes.Goal := by
  intro xs _hadm
  simp only [SeparatedLengths.write, List.length_append, pack_length, List.length_map,
    List.length_flatMap, List.map_map, Function.comp]
  have h2 : ((xs.zip (sizes xs)).map (fun x => x.2)) = sizes xs :=
    snd_zip xs (sizes xs) (by rw [sizes_length]; omega)
  have hsl : (sizes xs).length = xs.length := sizes_length xs
  simp only [TinylambOpt.ResearchProofs.StorageExecution.SECommon.little_len, h2, hsl]
  rw [Nat.mul_comm]

theorem sound_FourControls : FourControls.Goal := by
  intro a b ha4 _hadm
  simp only [SeparatedLengths.write, List.length_append]
  have hsa : sizes (a ++ b) = sizes a ++ sizes b := by simp [sizes, List.map_append]
  have hlenA : (sizes a).length = a.length := sizes_length a
  have hlenB : (sizes b).length = b.length := sizes_length b
  have hdata : ((a ++ b).zip (sizes (a ++ b))).flatMap (fun (v,w) => little v w) =
      (a.zip (sizes a)).flatMap (fun (v,w) => little v w) ++
        (b.zip (sizes b)).flatMap (fun (v,w) => little v w) := by
    rw [hsa, List.zip_append (by rw [hlenA]), List.flatMap_append]
  rw [hdata, List.length_append, hsa, List.map_append]
  simp only [pack_length, List.length_append, List.length_map, sizes_length]
  rw [Nat.add_mul]
  have hbc2 : byteCount (a.length * 2 + b.length * 2) =
      byteCount (a.length * 2) + byteCount (b.length * 2) := by
    have h8 : a.length * 2 = 8 * (a.length / 4) := by omega
    rw [h8, Nat.add_comm (8 * (a.length / 4)) (b.length * 2)]
    unfold byteCount
    omega
  rw [hbc2]
  omega

/-- Mirror-verified counterexample: a=[2^49], b=[2^17], c=[0]. -/
theorem disproof_GreedyCut : ¬ GreedyCut.Goal := by
  intro h
  have hne1 : ([2^49] : List Nat) ≠ [] := by decide
  have hne2 : ([2^17] : List Nat) ≠ [] := by decide
  have hne3 : ([0] : List Nat) ≠ [] := by decide
  have hcond : chosen write ([2^49] ++ [2^17]) ≤
      chosen write [2^49] + chosen write [2^17] := by native_decide
  have := h [2^49] [2^17] [0] hne1 hne2 hne3 hcond
  revert this
  native_decide

end TinylambOpt.ResearchProofs.StorageExecution.FrameSelection.SeparatedLengths
