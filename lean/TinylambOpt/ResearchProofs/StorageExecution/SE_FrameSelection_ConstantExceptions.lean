import TinylambOpt.ResearchProofs.StorageExecution.SE_FrameSelection_Common
import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Proved (9 claims, including the following)

- `ConstantExceptions.BoundedRaw` — A forced frame cap still limits worst-case
  expansion to raw cells and per-frame overhead.
- `ConstantExceptions.ByteFrontier` — Use the actual codec and raw fallback in the
  size/seek optimum.
- `ConstantExceptions.ConstantCharge` — `∀ x n, (write (List.replicate n x)).length =
  5`

Refuted by counterexample

- `ConstantExceptions.GreedyCut` — Aggressive hypothesis: the cheapest two adjacent
  groups never need reconsideration when a third arrives.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.FrameSelection.ConstantExceptions

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.ConstantExceptions

theorem sound_ByteFrontier : ByteFrontier.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.byteFrontier_gen write
theorem sound_StrictSavings : StrictSavings.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.strictSavings_holds write
theorem sound_HotPointReads : HotPointReads.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.hotPointReads_holds write
theorem sound_SeekCap : SeekCap.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.seekCap_holds write
theorem sound_ParallelEncode : ParallelEncode.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.parallelEncode_holds write
theorem sound_MandatoryCuts : MandatoryCuts.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.mandatoryCuts_holds write
theorem sound_BoundedRaw : BoundedRaw.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.boundedRaw_gen write

private theorem filter_zipIdx_replicate (x : Nat) :
    ∀ (n start : Nat),
      List.filter (fun (v,_) => (v : Nat) != x) ((List.replicate n x).zipIdx start) = [] := by
  intro n
  induction n with
  | zero => intro start; simp [List.replicate]
  | succ n ih =>
      intro start
      have h := ih (start + 1)
      simp [List.replicate, List.zipIdx_cons, List.filter_cons, h]

private theorem filter_zipIdx_replicate_ne (x y : Nat) (h : ¬ x = y) :
    ∀ (n start : Nat),
      List.filter (fun (v,_) => (v : Nat) != y) ((List.replicate n x).zipIdx start) =
        (List.replicate n x).zipIdx start := by
  intro n
  induction n with
  | zero => intro start; simp [List.replicate]
  | succ n ih =>
      intro start
      have h' := ih (start + 1)
      simp only [List.replicate, List.zipIdx_cons, List.filter_cons, h']
      rw [ite_eq_left (by simpa using h)]

theorem sound_ConstantCharge : ConstantCharge.Goal := by
  intro x n
  cases n with
  | zero =>
      rw [show (List.replicate 0 x) = [] from rfl]
      simp [write, TinylambOpt.ResearchProofs.StorageExecution.SECommon.little_len, TinylambOpt.ResearchProofs.StorageExecution.SECommon.uleb_len, ubytes, width]
  | succ m =>
      have heq : List.replicate (m+1) x = (x :: List.replicate m x) := rfl
      have hf : List.filter (fun (v,_) => (v : Nat) != x) ((x :: List.replicate m x).zipIdx) = [] :=
        filter_zipIdx_replicate x (m+1) 0
      rw [heq]
      simp only [write]
      rw [show ((x :: List.replicate m x).head?.getD 0) = x from rfl]
      rw [hf]
      simp [List.length_append, TinylambOpt.ResearchProofs.StorageExecution.SECommon.little_len, TinylambOpt.ResearchProofs.StorageExecution.SECommon.uleb_len, ubytes, width]

theorem sound_ModalBeatsFirst : ModalBeatsFirst.Goal := by
  intro a b n hab hn
  have hex1 : List.filter (fun (v,_) => (v : Nat) != b)
      ((b :: a :: List.replicate (n-1) b).zipIdx) = [(a,1)] := by
    have hrep := filter_zipIdx_replicate b (n-1) 2
    simp only [List.zipIdx_cons, List.filter_cons, hrep]
    rw [ite_eq_right (by simp), ite_eq_left (by simpa using hab)]
  have hb1 : ((b :: a :: List.replicate (n-1) b).head?.getD 0) = b := rfl
  have hw1v : ConstantExceptions.write (b :: a :: List.replicate (n-1) b) =
      little b 4 ++ uleb 1 ++ (uleb 1 ++ little a 4) := by
    simp only [write]
    rw [hb1]
    rw [hex1]
    simp
  have hz : List.filter (fun (v,_) => (v : Nat) != a) ((a :: List.replicate n b).zipIdx) =
      (List.replicate n b).zipIdx 1 := by
    have hrep := filter_zipIdx_replicate_ne b a (fun hh => hab hh.symm) n 1
    simp only [List.zipIdx_cons, List.filter_cons, hrep]
    rw [ite_eq_right (by simp)]
  have hb2 : ((a :: List.replicate n b).head?.getD 0) = a := rfl
  have hw2v : ConstantExceptions.write (a :: List.replicate n b) =
      little a 4 ++ uleb n ++ ((List.replicate n b).zipIdx 1).flatMap
        (fun x => uleb x.snd ++ little x.fst 4) := by
    simp only [write]
    rw [hb2]
    rw [hz, List.length_zipIdx, List.length_replicate]
  have hub1 : ubytes 1 = 1 := by simp [ubytes, width]
  rw [hw1v, hw2v]
  simp only [List.length_append, TinylambOpt.ResearchProofs.StorageExecution.SECommon.little_len, TinylambOpt.ResearchProofs.StorageExecution.SECommon.uleb_len]
  rw [hub1]
  -- goal: 4 + 1 + (1 + 4) < 4 + ubytes n + ((replicate n b).zipIdx 1).flatMap _ |>.length
  have hub : 1 ≤ ubytes n := by unfold ubytes; exact Nat.le_max_left _ _
  have key : ∀ (l : List (Nat × Nat)), 5 * l.length ≤
      (List.flatMap (fun x => uleb x.snd ++ little x.fst 4) l).length := by
    intro l
    induction l with
    | nil => simp
    | cons p l ih =>
        simp only [List.length_cons, List.flatMap_cons, List.length_append, Nat.mul_succ]
        have h5 : 5 ≤ (uleb (Prod.snd p)).length + (little (Prod.fst p) 4).length := by
          rw [TinylambOpt.ResearchProofs.StorageExecution.SECommon.uleb_len, TinylambOpt.ResearchProofs.StorageExecution.SECommon.little_len]
          have h1 : 1 ≤ ubytes (Prod.snd p) := by unfold ubytes; exact Nat.le_max_left _ _
          omega
        omega
  have hlen : ((List.replicate n b).zipIdx 1).length = n := by
    rw [List.length_zipIdx, List.length_replicate]
  have hflat : 5 * n ≤ (((List.replicate n b).zipIdx 1).flatMap
      (fun x => uleb x.snd ++ little x.fst 4)).length := by
    have h := key ((List.replicate n b).zipIdx 1)
    rw [hlen] at h
    exact h
  omega

/-- Mirror-verified counterexample: a=[0], b=[1], c=[1]. -/
theorem disproof_GreedyCut : ¬ GreedyCut.Goal := by
  intro h
  have hne1 : ([0] : List Nat) ≠ [] := by decide
  have hne2 : ([1] : List Nat) ≠ [] := by decide
  have hne3 : ([1] : List Nat) ≠ [] := by decide
  have hcond : chosen write ([0] ++ [1]) ≤
      chosen write [0] + chosen write [1] := by native_decide
  have := h [0] [1] [1] hne1 hne2 hne3 hcond
  revert this
  native_decide

end TinylambOpt.ResearchProofs.StorageExecution.FrameSelection.ConstantExceptions
