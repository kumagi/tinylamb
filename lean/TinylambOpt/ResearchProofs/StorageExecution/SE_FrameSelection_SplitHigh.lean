import TinylambOpt.ResearchProofs.StorageExecution.SE_FrameSelection_Common
import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-!
Storage Execution / Frame Selection research batch — model `SplitHigh`.
Seven shared properties are proved once in `Common.lean` and instantiated
here; `HighRunMerge` and `CommonUpper` hold; `greedyCut` subadditivity
fails for a concrete witness.
-/

namespace TinylambOpt.ResearchProofs.StorageExecution.FrameSelection.SplitHigh

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.SplitHigh

theorem sound_ByteFrontier : ByteFrontier.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.byteFrontier_gen write
theorem sound_StrictSavings : StrictSavings.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.strictSavings_holds write
theorem sound_HotPointReads : HotPointReads.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.hotPointReads_holds write
theorem sound_SeekCap : SeekCap.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.seekCap_holds write
theorem sound_ParallelEncode : ParallelEncode.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.parallelEncode_holds write
theorem sound_MandatoryCuts : MandatoryCuts.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.mandatoryCuts_holds write
theorem sound_BoundedRaw : BoundedRaw.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.boundedRaw_gen write

private theorem replicate_append_same (x : Nat) : ∀ n m,
    List.replicate n x ++ List.replicate m x = List.replicate (n + m) x := by
  intro n
  induction n with
  | zero => intro m; simp
  | succ n ih =>
      intro m
      rw [List.replicate_succ, List.cons_append, ih m, ← List.replicate_succ]
      congr 1
      omega

private theorem runs_step (h : Nat) : ∀ l n, runs l = [(h, n)] → runs (h :: l) = [(h, n+1)] := by
  intro l n hl
  simp [runs, hl]

private theorem runs_replicate_pos : ∀ (v : Nat) (k : Nat), 0 < k →
    runs (List.replicate k v) = [(v, k)] := by
  intro v k hk
  induction k with
  | zero => omega
  | succ k ih =>
      cases k with
      | zero => simp [runs, List.replicate]
      | succ k =>
          rw [show List.replicate (k+2) v = v :: List.replicate (k+1) v from rfl]
          exact runs_step v (List.replicate (k+1) v) (k+1) (ih (by omega))

theorem sound_HighRunMerge : HighRunMerge.Goal := by
  intro x y n m hxy hn hm
  cases n with
  | zero => omega
  | succ n =>
      cases m with
      | zero => omega
      | succ m =>
          have hhigh : (List.replicate (n+1) x ++ List.replicate (m+1) y).map (· / 256) =
              List.replicate (n+1+m+1) (x / 256) := by
            rw [List.map_append, List.map_replicate, List.map_replicate, hxy]
            rw [replicate_append_same (y / 256)]
            exact rfl
          have hruns : runs (List.replicate (n+1+m+1) (x / 256)) = [(x / 256, n+1+m+1)] :=
            runs_replicate_pos (x / 256) (n+1+m+1) (by omega)
          rw [SplitHigh.write, hhigh, hruns]
          simp only [List.flatMap_cons, List.flatMap_nil, List.append_nil, List.length_append,
            List.length_map, List.length_replicate]
          have h1 := TinylambOpt.ResearchProofs.StorageExecution.SECommon.uleb_len (x / 256)
          have h2 := TinylambOpt.ResearchProofs.StorageExecution.SECommon.uleb_len (n+1+m+1)
          have hu : ubytes (n+1+m+1) = ubytes (n+1+(m+1)) :=
            congrArg ubytes (by omega)
          rw [hu] at h2
          omega

theorem sound_CommonUpper : CommonUpper.Goal := by
  intro xs h hne hall
  have hruns : runs (xs.map (· / 256)) = [(h, xs.length)] := by
    induction xs with
    | nil => exact absurd rfl hne
    | cons x xs ih =>
        have hx : x / 256 = h := hall x (by simp)
        cases xs with
        | nil =>
            have hm : List.map (fun v => v / 256) [x] = [h] := by simp [hx]
            rw [hm]
            simp [runs]
        | cons y ys =>
            have hy : y / 256 = h := hall y (by simp)
            have hall' : ∀ z ∈ y::ys, z / 256 = h := fun z hz => hall z (by simp [hz])
            have hne' : (y::ys : List Nat) ≠ [] := by simp
            have ih' := ih hne' hall'
            rw [List.map_cons, List.map_cons, hx, hy]
            have ht : runs (h :: List.map (fun x => x / 256) ys) = [(h, ys.length + 1)] := by
              rw [List.map_cons, hy] at ih'
              rw [List.length_cons] at ih'
              exact ih'
            rw [runs_step h (h :: List.map (fun x => x / 256) ys) (ys.length + 1) ht]
            exact rfl
  rw [SplitHigh.write, hruns]
  simp only [List.flatMap_cons, List.flatMap_nil, List.append_nil, List.length_append,
    List.length_map]
  have h1 := TinylambOpt.ResearchProofs.StorageExecution.SECommon.uleb_len h
  have h2 := TinylambOpt.ResearchProofs.StorageExecution.SECommon.uleb_len xs.length
  omega

/-- Mirror-verified counterexample: a=[2^44], b=[256], c=[256]. -/
theorem disproof_GreedyCut : ¬ GreedyCut.Goal := by
  intro hall
  exact absurd (hall [2^44] [256] [256]) (by native_decide)

end TinylambOpt.ResearchProofs.StorageExecution.FrameSelection.SplitHigh
