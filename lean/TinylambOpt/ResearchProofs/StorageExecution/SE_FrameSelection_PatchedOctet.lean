import TinylambOpt.ResearchProofs.StorageExecution.SE_FrameSelection_Common
import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Proved (9 claims, including the following)

- `PatchedOctet.BoundedRaw` — A forced frame cap still limits worst-case expansion to
  raw cells and per-frame overhead.
- `PatchedOctet.ByteFrontier` — Use the actual codec and raw fallback in the size/seek
  optimum.
- `PatchedOctet.EscapeCliff` — `∀ v, (write [254,v]).length+1 = (write [255,v]).length`

Refuted by counterexample

- `PatchedOctet.GreedyCut` — Aggressive hypothesis: the cheapest two adjacent groups
  never need reconsideration when a third arrives.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.FrameSelection.PatchedOctet

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.PatchedOctet

theorem sound_ByteFrontier : ByteFrontier.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.byteFrontier_gen write
theorem sound_StrictSavings : StrictSavings.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.strictSavings_holds write
theorem sound_HotPointReads : HotPointReads.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.hotPointReads_holds write
theorem sound_SeekCap : SeekCap.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.seekCap_holds write
theorem sound_ParallelEncode : ParallelEncode.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.parallelEncode_holds write
theorem sound_MandatoryCuts : MandatoryCuts.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.mandatoryCuts_holds write
theorem sound_BoundedRaw : BoundedRaw.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.boundedRaw_gen write

theorem sound_EscapeCost : EscapeCost.Goal := by
  intro xs
  induction xs with
  | nil => simp [PatchedOctet.write]
  | cons v xs ih =>
      have hkey : write (v :: xs) =
          (if v < 255 then [v] else 255 :: uleb (v - 255)) ++ write xs := by
        show List.flatMap (fun a => if a < 255 then [a] else 255 :: uleb (a - 255)) (v :: xs) = _
        rw [List.flatMap_cons]
        rfl
      rw [hkey, List.length_append, ih]
      by_cases hv : v < 255
      · have hge : ¬ (v ≥ 255) := by omega
        simp [hv, hge]
        omega
      · have hge : v ≥ 255 := by omega
        simp [hv, hge, TinylambOpt.ResearchProofs.StorageExecution.SECommon.uleb_len]
        omega

theorem sound_EscapeCliff : EscapeCliff.Goal := by
  intro v
  by_cases hv : v < 255
  · have hge : ¬ (v ≥ 255) := by omega
    simp [write, hv, hge, List.length_append, TinylambOpt.ResearchProofs.StorageExecution.SECommon.uleb_len, ubytes, width]
  · have hge : v ≥ 255 := by omega
    simp [write, hv, hge, List.length_append, TinylambOpt.ResearchProofs.StorageExecution.SECommon.uleb_len, ubytes, width]
    omega

/-- Mirror-verified counterexample: a=[0], b=[2^29+255], c=[2^29+255]. -/
theorem disproof_GreedyCut : ¬ GreedyCut.Goal := by
  intro h
  have hne1 : ([0] : List Nat) ≠ [] := by decide
  have hne2 : ([536871167] : List Nat) ≠ [] := by decide
  have hne3 : ([536871167] : List Nat) ≠ [] := by decide
  have hcond : chosen write ([0] ++ [536871167]) ≤
      chosen write [0] + chosen write [536871167] := by native_decide
  have := h [0] [536871167] [536871167] hne1 hne2 hne3 hcond
  revert this
  native_decide

end TinylambOpt.ResearchProofs.StorageExecution.FrameSelection.PatchedOctet
