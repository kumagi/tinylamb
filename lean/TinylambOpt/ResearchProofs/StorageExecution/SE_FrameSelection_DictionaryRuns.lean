import TinylambOpt.ResearchProofs.StorageExecution.SE_FrameSelection_Common
import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Proved (9 claims, including the following)

- `DictionaryRuns.BoundedRaw` — A forced frame cap still limits worst-case expansion to
  raw cells and per-frame overhead.
- `DictionaryRuns.ByteFrontier` — Use the actual codec and raw fallback in the
  size/seek optimum.
- `DictionaryRuns.DictionaryBoundary` — `∀ a b, a≠b → ∀ n m, 0<n → 0<m → (write
  (List.replicate n a ++ List.replicate m b)).length=11+ubytes n+ubytes m`

Refuted by counterexample

- `DictionaryRuns.GreedyCut` — Aggressive hypothesis: the cheapest two adjacent groups
  never need reconsideration when a third arrives.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.FrameSelection.DictionaryRuns

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.DictionaryRuns

theorem sound_ByteFrontier : ByteFrontier.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.byteFrontier_gen write
theorem sound_StrictSavings : StrictSavings.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.strictSavings_holds write
theorem sound_HotPointReads : HotPointReads.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.hotPointReads_holds write
theorem sound_SeekCap : SeekCap.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.seekCap_holds write
theorem sound_ParallelEncode : ParallelEncode.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.parallelEncode_holds write
theorem sound_MandatoryCuts : MandatoryCuts.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.mandatoryCuts_holds write
theorem sound_BoundedRaw : BoundedRaw.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.boundedRaw_gen write

private theorem eraseDups_replicate (x : Nat) : ∀ (n : Nat),
    (List.replicate (n+1) x).eraseDups = [x] := by
  intro n
  rw [show List.replicate (n+1) x = x :: List.replicate n x from rfl, List.eraseDups_cons]
  rw [List.filter_replicate]
  simp

private theorem eraseDups_repApp (a b : Nat) (hab : a ≠ b) (n m : Nat) :
    (List.replicate (n+1) a ++ List.replicate (m+1) b).eraseDups = [a, b] := by
  rw [show List.replicate (n+1) a ++ List.replicate (m+1) b =
        a :: (List.replicate n a ++ b :: List.replicate m b) from rfl,
      List.eraseDups_cons]
  have hf : (List.replicate n a ++ b :: List.replicate m b).filter (fun bb => !bb == a)
      = b :: List.replicate m b := by
    have hba : ¬ b = a := fun h => hab h.symm
    simp [List.filter_append, List.filter_cons, List.filter_replicate, hba]
  rw [hf]
  rw [List.eraseDups_cons]
  rw [List.filter_replicate]
  simp

private theorem runs_replicate_one (x : Nat) : ∀ (k : Nat),
    runs (List.replicate (k+1) x) = [(x, k+1)] := by
  intro k
  induction k with
  | zero => simp [runs, List.replicate]
  | succ k ih =>
      rw [show List.replicate (k+2) x = x :: List.replicate (k+1) x from rfl]
      simp [runs, ih]

private theorem runs_two : ∀ (n m : Nat),
    runs (List.replicate (n+1) 0 ++ List.replicate (m+1) 1) = [(0, n+1), (1, m+1)] := by
  intro n
  induction n with
  | zero =>
      intro m
      rw [show List.replicate (0+1) 0 ++ List.replicate (m+1) 1 =
            (0 : Nat) :: List.replicate (m+1) 1 from rfl]
      simp [runs, runs_replicate_one 1 m]
  | succ k ih =>
      intro m
      rw [show List.replicate (k+2) 0 = (0 : Nat) :: List.replicate (k+1) 0 from rfl,
        List.cons_append]
      simp [runs, ih m]

theorem sound_RepeatCliff : RepeatCliff.Goal := by
  intro x n hn
  obtain ⟨k, rfl⟩ : ∃ k, n = k+1 := ⟨n-1, by omega⟩
  have hdict : (List.replicate (k+1) x).eraseDups = [x] := eraseDups_replicate x k
  have hidx : ((List.replicate (k+1) x).map ([x].idxOf)) = List.replicate (k+1) 0 := by
    have h : List.idxOf x [x] = 0 := by simp [List.idxOf]
    rw [List.map_replicate, h]
  have hruns : runs (List.replicate (k+1) 0) = [(0, k+1)] := runs_replicate_one 0 k
  simp only [DictionaryRuns.write, hdict, hidx, hruns, List.length_append, List.length_cons,
    List.length_nil, fixed, List.flatMap_cons, List.flatMap_nil, List.map,
    TinylambOpt.ResearchProofs.StorageExecution.SECommon.little_len, TinylambOpt.ResearchProofs.StorageExecution.SECommon.uleb_len]
  simp [ubytes, width]
  omega

theorem sound_DictionaryBoundary : DictionaryBoundary.Goal := by
  intro a b hab n m hn hm
  obtain ⟨k, rfl⟩ : ∃ k, n = k+1 := ⟨n-1, by omega⟩
  obtain ⟨l, rfl⟩ : ∃ l, m = l+1 := ⟨m-1, by omega⟩
  have hdict : (List.replicate (k+1) a ++ List.replicate (l+1) b).eraseDups = [a, b] :=
    eraseDups_repApp a b hab k l
  have hidx : ((List.replicate (k+1) a ++ List.replicate (l+1) b).map ([a,b].idxOf)) =
      List.replicate (k+1) 0 ++ List.replicate (l+1) 1 := by
    have ha : List.idxOf a [a,b] = 0 := by
      show List.findIdx (fun x => x == a) [a,b] = 0
      simp only [List.findIdx_cons]
      rw [if_pos (by simp)]
    have hb : List.idxOf b [a,b] = 1 := by
      show List.findIdx (fun x => x == b) [a,b] = 1
      simp only [List.findIdx_cons]
      rw [if_neg (by simp [hab]), if_pos (by simp)]
    rw [List.map_append, List.map_replicate, List.map_replicate, ha, hb]
  have hruns : runs (List.replicate (k+1) 0 ++ List.replicate (l+1) 1) =
      [(0, k+1), (1, l+1)] := runs_two k l
  simp only [DictionaryRuns.write, hdict, hidx, hruns, List.length_append, List.length_cons,
    List.length_nil, fixed, List.flatMap_cons, List.flatMap_nil, List.map,
    TinylambOpt.ResearchProofs.StorageExecution.SECommon.little_len, TinylambOpt.ResearchProofs.StorageExecution.SECommon.uleb_len]
  simp [ubytes, width]
  omega

/-- Mirror-verified counterexample: a=[0,1], b=[2], c=[2,2]. -/
theorem disproof_GreedyCut : ¬ GreedyCut.Goal := by
  intro h
  have hne1 : ([0, 1] : List Nat) ≠ [] := by decide
  have hne2 : ([2] : List Nat) ≠ [] := by decide
  have hne3 : ([2, 2] : List Nat) ≠ [] := by decide
  have hcond : chosen write ([0, 1] ++ [2]) ≤
      chosen write [0, 1] + chosen write [2] := by native_decide
  have := h [0, 1] [2] [2, 2] hne1 hne2 hne3 hcond
  revert this
  native_decide

end TinylambOpt.ResearchProofs.StorageExecution.FrameSelection.DictionaryRuns
