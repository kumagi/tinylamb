import TinylambOpt.ResearchProofs.StorageExecution.SE_FrameSelection_Common
import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-!
Storage Execution / Frame Selection research batch — model `XorPredecessor`.
Seven shared properties are proved once in `Common.lean` and instantiated
here; `JoinCorrection` and `EqualTail` hold; `greedyCut` subadditivity
fails for a concrete witness.
-/

namespace TinylambOpt.ResearchProofs.StorageExecution.FrameSelection.XorPredecessor

open SECommon
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.XorPredecessor

theorem sound_ByteFrontier : ByteFrontier.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SECommon.byteFrontier_gen write

theorem sound_StrictSavings : StrictSavings.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SECommon.strictSavings_holds write

theorem sound_HotPointReads : HotPointReads.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SECommon.hotPointReads_holds write

theorem sound_SeekCap : SeekCap.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SECommon.seekCap_holds write

theorem sound_ParallelEncode : ParallelEncode.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SECommon.parallelEncode_holds write

theorem sound_MandatoryCuts : MandatoryCuts.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SECommon.mandatoryCuts_holds write

theorem sound_BoundedRaw : BoundedRaw.Goal :=
  TinylambOpt.ResearchProofs.StorageExecution.SECommon.boundedRaw_gen write

/-- Cost model of `write` as a fold over the xor-predecessor chain: `wlen s xs` counts
the emitted bytes for `xs` when the chain starts with predecessor `s`. -/
private def wlen : Nat → List Nat → Nat
  | _, [] => 0
  | s, h :: t => ubytes (h ^^^ s) + wlen h t

private theorem wlen_zip : ∀ (s : Nat) (xs : List Nat),
    List.length (List.flatMap (fun v => uleb (v.1 ^^^ v.2)) (List.zip xs (s :: xs))) = wlen s xs := by
  intro s xs
  induction xs generalizing s with
  | nil => rfl
  | cons h t ih =>
      have step : List.zip (h :: t) (s :: (h :: t)) = (h, s) :: List.zip t (h :: t) := rfl
      rw [step, List.flatMap_cons, List.length_append, ih h]
      simp [wlen, TinylambOpt.ResearchProofs.StorageExecution.SECommon.uleb_len]

private theorem write_eq_wlen (xs : List Nat) : (write xs).length = wlen 0 xs := by
  show List.length (List.flatMap (fun v => uleb (v.1 ^^^ v.2)) (List.zip xs (0 :: xs))) = wlen 0 xs
  exact wlen_zip 0 xs

private theorem wlen_append : ∀ (s : Nat) (a b : List Nat),
    wlen s (a ++ b) = wlen s a + wlen ((s :: a).getLastD 0) b := by
  intro s a
  induction a generalizing s with
  | nil => intro b; simp [wlen]
  | cons h a ih =>
      intro b
      rw [show wlen s ((h :: a) ++ b) = ubytes (h ^^^ s) + wlen h (a ++ b) from rfl,
        show wlen s (h :: a) = ubytes (h ^^^ s) + wlen h a from rfl, ih h,
        show ((h :: a).getLastD 0) = (s :: h :: a).getLastD 0 from rfl]
      omega

theorem sound_EqualTail : EqualTail.Goal := by
  intro x n
  rw [write_eq_wlen]
  have h : ∀ (s : Nat) (m : Nat),
      wlen s (List.replicate (Nat.succ m) x) = ubytes (x ^^^ s) + m * ubytes 0 := by
    intro s m
    induction m generalizing s with
    | zero =>
        have key : (List.replicate (Nat.succ 0) x) = [x] := rfl
        rw [key]
        simp [wlen]
    | succ m ih =>
        show wlen s (x :: List.replicate (Nat.succ m) x) = ubytes (x ^^^ s) + Nat.succ m * ubytes 0
        show ubytes (x ^^^ s) + wlen x (List.replicate (Nat.succ m) x) = _
        rw [ih, Nat.xor_self, Nat.succ_mul]
        omega
  cases n with
  | zero =>
      have key : (x :: List.replicate 0 x) = List.replicate (Nat.succ 0) x := rfl
      rw [key, h 0 0, Nat.xor_zero]
      omega
  | succ m =>
      show wlen 0 (x :: List.replicate (m + 1) x) = ubytes x + (m + 1)
      have key : (x :: List.replicate (m + 1) x) = List.replicate (Nat.succ (m + 1)) x :=
        (List.replicate_succ (a := x) (n := m + 1)).symm
      rw [key, h 0 (m + 1), Nat.xor_zero]
      have h0 : ubytes 0 = 1 := by decide
      rw [h0, Nat.mul_one]

theorem sound_JoinCorrection : JoinCorrection.Goal := by
  intro a b
  cases b with
  | nil =>
      intro hne _ _
      exact absurd rfl hne
  | cons y u =>
      intro _ p x
      have hx : x = y := rfl
      have hp : p = a.getLastD 0 := by
        show a.getLast?.getD 0 = a.getLastD 0
        rw [← List.getLastD_eq_getLast?]
      rw [hx, hp]
      rw [show (write (a ++ y :: u)).length = wlen 0 (a ++ y :: u) from write_eq_wlen _,
        show (write a).length = wlen 0 a from write_eq_wlen _,
        show (write (y :: u)).length = wlen 0 (y :: u) from write_eq_wlen _,
        wlen_append, List.getLastD_cons,
        show wlen (a.getLastD 0) (y :: u) = ubytes (y ^^^ a.getLastD 0) + wlen y u from rfl,
        show wlen 0 (y :: u) = ubytes (y ^^^ 0) + wlen y u from rfl,
        Nat.xor_zero, Nat.xor_comm y (a.getLastD 0)]
      omega

theorem disproof_GreedyCut : ¬ GreedyCut.Goal := by
  intro hall
  exact absurd (hall [0] [2 ^ 28] [2 ^ 36]) (by native_decide)

end TinylambOpt.ResearchProofs.StorageExecution.FrameSelection.XorPredecessor
