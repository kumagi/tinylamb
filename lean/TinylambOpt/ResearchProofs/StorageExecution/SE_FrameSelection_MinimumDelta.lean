import TinylambOpt.ResearchProofs.StorageExecution.SE_FrameSelection_Common
import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Proved (9 claims, including the following)

- `MinimumDelta.ArithmeticKeys` — `∀ d n, 0<n → (write ((List.range n).map (fun i =>
  (i+1)*d))).length = ubytes (2*d)+1`
- `MinimumDelta.BoundedRaw` — A forced frame cap still limits worst-case expansion to
  raw cells and per-frame overhead.
- `MinimumDelta.ByteFrontier` — Use the actual codec and raw fallback in the size/seek
  optimum.

Refuted by counterexample

- `MinimumDelta.GreedyCut` — Aggressive hypothesis: the cheapest two adjacent groups
  never need reconsideration when a third arrives.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-!
Storage Execution / Frame Selection research batch — model `MinimumDelta`.
Seven shared properties from `Common.lean`; `ConstantAmbiguity` and
`ArithmeticKeys` hold; `greedyCut` subadditivity fails for a concrete witness.
-/

namespace TinylambOpt.ResearchProofs.StorageExecution.FrameSelection.MinimumDelta

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.MinimumDelta

theorem sound_ByteFrontier : ByteFrontier.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.byteFrontier_gen write
theorem sound_StrictSavings : StrictSavings.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.strictSavings_holds write
theorem sound_HotPointReads : HotPointReads.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.hotPointReads_holds write
theorem sound_SeekCap : SeekCap.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.seekCap_holds write
theorem sound_ParallelEncode : ParallelEncode.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.parallelEncode_holds write
theorem sound_MandatoryCuts : MandatoryCuts.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.mandatoryCuts_holds write
theorem sound_BoundedRaw : BoundedRaw.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.boundedRaw_gen write

private theorem minrep (a : Nat) (k : Nat) (hk : 0 < k) :
    (List.replicate k a).min?.getD 0 = a := by
  rw [List.min?_replicate_of_pos hk]
  rfl

private theorem maxrep (a : Nat) (k : Nat) (hk : 0 < k) :
    (List.replicate k a).max?.getD 0 = a := by
  rw [List.max?_replicate_of_pos hk]
  rfl

theorem sound_ConstantAmbiguity : ConstantAmbiguity.Goal := by
  intro x n hn
  cases n with
  | zero => omega
  | succ n =>
      -- write (replicate (n+1) 0) has length 2
      have h0 : (MinimumDelta.write (List.replicate (n+1) 0)).length = 2 := by
        have hdelta : delta 0 (List.replicate (n+1) 0) = List.replicate (n+1) 0 := by
          induction n with
          | zero => simp [delta, List.replicate, zig]
          | succ n ih =>
              have h1 : List.replicate (n+1+1) 0 = 0 :: List.replicate (n+1) 0 := by
                simp [List.replicate]
              rw [h1]
              simp [delta, ih, zig]
        have hmin : (List.replicate (n+1) 0).min?.getD 0 = 0 :=
          minrep 0 (n+1) (by omega)
        have hoff : (List.replicate (n+1) 0).map (· - 0) = List.replicate (n+1) 0 := by simp
        have hpeak : peak (List.replicate (n+1) 0) = 0 :=
          maxrep 0 (n+1) (by omega)
        simp [MinimumDelta.write, hdelta, hmin, hoff, hpeak, width, pack, little, byteCount,
          TinylambOpt.ResearchProofs.StorageExecution.SECommon.uleb_len, ubytes]
      -- write (replicate (n+1) x) has length >= 2
      have h2 : 2 ≤ (MinimumDelta.write (List.replicate (n+1) x)).length := by
        simp [MinimumDelta.write, List.length_append, TinylambOpt.ResearchProofs.StorageExecution.SECommon.uleb_len]
        have : 1 ≤ ubytes ((delta 0 (List.replicate (n+1) x)).min?.getD 0) := by
          unfold ubytes; exact Nat.le_max_left _ _
        have : 1 ≤ ubytes (width (peak ((delta 0 (List.replicate (n+1) x)).map
            (· - (delta 0 (List.replicate (n+1) x)).min?.getD 0)))) := by
          unfold ubytes; exact Nat.le_max_left _ _
        omega
      omega

private theorem delta_last (p : Nat) (xs : List Nat) (y : Nat) :
    delta p (xs ++ [y]) = delta p xs ++ [zig ((y : Int) - (xs.getLastD p : Int))] := by
  induction xs generalizing p with
  | nil => simp [delta, List.getLastD_nil]
  | cons x xt ih =>
      rw [show (x :: xt ++ [y]) = x :: (xt ++ [y]) from rfl]
      simp only [delta, ih]
      rw [← List.cons_append, List.getLastD_cons]

private theorem lastD_range_map (d q j a : Nat) :
    ((List.range (j+1)).map (fun i => (q + i + 1) * d)).getLastD a = (q + j + 1) * d := by
  rw [List.range_succ, List.map_append, List.map_singleton, List.getLastD_concat]

private theorem last_zig (d u : Nat) :
    zig ((((u + 1) * d : Nat) : Int) - ((u * d : Nat) : Int)) = zig (d : Int) := by
  have t : (u + 1) * d = u * d + d := Nat.succ_mul u d
  rw [t, ← Int.ofNat_add_ofNat]
  have hsub : ((u * d : Nat) : Int) + (d : Int) - ((u * d : Nat) : Int) = (d : Int) := by omega
  rw [hsub]

private theorem delta_arith (d p k : Nat) :
    delta (p * d) ((List.range k).map (fun i => (p + i + 1) * d)) = List.replicate k (zig (d : Int)) := by
  induction k generalizing p with
  | zero => simp [delta]
  | succ k ih =>
      rw [List.range_succ, List.map_append, List.map_singleton]
      rw [delta_last (p * d) ((List.range k).map (fun i => (p + i + 1) * d)) ((p + k + 1) * d)]
      rw [ih]
      cases k with
      | zero =>
          have hnil : List.map (fun i => (p + i + 1) * d) (List.range 0) = [] := rfl
          rw [hnil, List.getLastD_nil]
          have hz0 : (((p + 0 + 1) * d : Nat) : Int) - (((p * d : Nat)) : Int) = (d : Int) := by
            rw [show p + 0 + 1 = p + 1 from rfl, Nat.succ_mul, ← Int.ofNat_add_ofNat]
            omega
          rw [congrArg zig hz0, ← List.replicate_succ']
      | succ j =>
          rw [lastD_range_map d p j (p * d)]
          have hz1 : (((p + (j + 1) + 1) * d : Nat) : Int) - (((p + j + 1) * d : Nat) : Int) =
              (d : Int) := by
            have hf : p + (j + 1) + 1 = (p + j + 1) + 1 := rfl
            rw [hf, Nat.succ_mul, ← Int.ofNat_add_ofNat]
            omega
          rw [congrArg zig hz1, ← List.replicate_succ']

theorem sound_ArithmeticKeys : ArithmeticKeys.Goal := by
  intro d n hn
  cases n with
  | zero => omega
  | succ n =>
      have hdelta : delta 0 ((List.range (n+1)).map (fun i => (i+1) * d)) =
          List.replicate (n+1) (zig (d : Int)) := by
        have := delta_arith d 0 (n+1)
        simpa [Nat.zero_add] using this
      have hmin : (delta 0 ((List.range (n+1)).map (fun i => (i+1) * d))).min?.getD 0 =
          zig (d : Int) := by
        rw [hdelta]
        exact minrep (zig (d : Int)) (n+1) (by omega)
      have hoff : (delta 0 ((List.range (n+1)).map (fun i => (i+1) * d))).map
          (· - (delta 0 ((List.range (n+1)).map (fun i => (i+1) * d))).min?.getD 0) =
          List.replicate (n+1) 0 := by
        rw [hdelta, minrep (zig (d : Int)) (n+1) (by omega)]
        simp [List.map_replicate]
      have hpeak : peak (List.replicate (n+1) 0) = 0 := maxrep 0 (n+1) (by omega)
      have hw : width 0 = 0 := by simp [width]
      simp [MinimumDelta.write, hdelta, hmin, hoff, hpeak, hw, pack, little, byteCount,
        TinylambOpt.ResearchProofs.StorageExecution.SECommon.uleb_len]
      have hz : zig (d : Int) = 2 * d := by simp [zig]
      simp [hz, ubytes, width]

/-- Mirror-verified counterexample: a=[0], b=[1000,0], c=[1000,0]. -/
theorem disproof_GreedyCut : ¬ GreedyCut.Goal := by
  intro hall
  exact absurd (hall [0] [1000, 0] [1000, 0]) (by native_decide)

end TinylambOpt.ResearchProofs.StorageExecution.FrameSelection.MinimumDelta
