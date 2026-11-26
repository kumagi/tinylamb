import TinylambOpt.ResearchProofs.StorageExecution.SE_FrameSelection_Common
import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Proved (9 claims, including the following)

- `OrdinalResidual.BoundedRaw` — A forced frame cap still limits worst-case expansion
  to raw cells and per-frame overhead.
- `OrdinalResidual.ByteFrontier` — Use the actual codec and raw fallback in the
  size/seek optimum.
- `OrdinalResidual.ConsecutiveKeys` — `∀ n, (write (List.range n)).length = 1`

Refuted by counterexample

- `OrdinalResidual.GreedyCut` — Aggressive hypothesis: the cheapest two adjacent groups
  never need reconsideration when a third arrives.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.FrameSelection.OrdinalResidual

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.OrdinalResidual

theorem sound_ByteFrontier : ByteFrontier.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.byteFrontier_gen write
theorem sound_StrictSavings : StrictSavings.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.strictSavings_holds write
theorem sound_HotPointReads : HotPointReads.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.hotPointReads_holds write
theorem sound_SeekCap : SeekCap.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.seekCap_holds write
theorem sound_ParallelEncode : ParallelEncode.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.parallelEncode_holds write
theorem sound_MandatoryCuts : MandatoryCuts.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.mandatoryCuts_holds write
theorem sound_BoundedRaw : BoundedRaw.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.boundedRaw_gen write

private theorem zipIdx_append_s : ∀ (a c : List Nat) (s : Nat),
    (a ++ c).zipIdx s = a.zipIdx s ++ c.zipIdx (s + a.length) := by
  intro a
  induction a with
  | nil => intro c s; simp
  | cons y yt ih =>
      intro c s
      rw [List.cons_append, List.zipIdx_cons, ih c (s+1), List.zipIdx_cons]
      simp [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

private theorem zipIdx_map_add : ∀ (b : List Nat) (L s : Nat),
    (b.map (fun x => x + L)).zipIdx s = (b.zipIdx s).map (fun (v,i) => (v + L, i)) := by
  intro b
  induction b with
  | nil => intro L s; rfl
  | cons y yt ih =>
      intro L s
      simp only [List.map_cons, List.zipIdx_cons, ih L (s+1), List.map_cons]

private theorem zipIdx_shift : ∀ (b : List Nat) (s : Nat),
    b.zipIdx s = (b.zipIdx).map (fun (v,i) => (v, i + s)) := by
  intro b
  induction b with
  | nil => intro s; rfl
  | cons y yt ih =>
      intro s
      rw [List.zipIdx_cons, List.zipIdx_cons, ih (s+1), ih 1]
      simp only [List.map_cons, List.map_map, Function.comp_apply]
      congr 1
      · simp
      · congr 1
        funext p
        cases p with
        | mk v i => simp [Function.comp_apply, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

private theorem max?_cons' (y : Nat) (yt : List Nat) :
    List.max? (y :: yt) = some (yt.foldl max y) := rfl

private theorem foldl_max_eq : ∀ (ys : List Nat) (a : Nat),
    ys.foldl max a = max a (peak ys) := by
  intro ys
  induction ys with
  | nil => intro a; simp [peak]
  | cons y yt ih =>
      intro a
      rw [List.foldl_cons]
      show yt.foldl max (max a y) = max a ((y :: yt).max?.getD 0)
      rw [max?_cons', Option.getD_some, ih (max a y), ih y]
      omega

private theorem peak_append : ∀ (xs ys : List Nat),
    peak (xs ++ ys) = max (peak xs) (peak ys) := by
  intro xs
  induction xs with
  | nil => intro ys; simp [peak]
  | cons x xt ih =>
      intro ys
      rw [List.cons_append]
      show ((xt ++ ys).foldl max x) = max (xt.foldl max x) (peak ys)
      rw [List.foldl_append, foldl_max_eq]

private theorem log2_mono_aux : ∀ (fuel m n : Nat), m ≤ n → m ≤ fuel → m.log2 ≤ n.log2 := by
  intro fuel
  induction fuel with
  | zero =>
      intro m n hmn hf
      have hm : m = 0 := Nat.eq_zero_of_le_zero (by omega)
      subst hm
      rw [Nat.log2_def]
      simp
  | succ f ih =>
      intro m n hmn hf
      rw [Nat.log2_def n, Nat.log2_def m]
      by_cases hm : 2 ≤ m
      · have hn : 2 ≤ n := by omega
        rw [if_pos hm, if_pos hn]
        have hdiv : m / 2 ≤ n / 2 := Nat.div_le_div hmn (Nat.le_refl 2) (by omega)
        have hlt : m / 2 < m := Nat.div_lt_self (by omega) (by omega)
        have hle : m / 2 ≤ f := by omega
        have := ih (m / 2) (n / 2) hdiv hle
        omega
      · rw [if_neg hm]
        split <;> omega

private theorem log2_mono' : ∀ {m n : Nat}, m ≤ n → m.log2 ≤ n.log2 := fun h =>
  log2_mono_aux _ _ _ h (Nat.le_refl _)

private theorem width_max : ∀ p q : Nat, width (max p q) = max (width p) (width q) := by
  intro p q
  by_cases hpq : p ≤ q
  · rw [Nat.max_eq_right hpq]
    by_cases hq : q = 0
    · subst hq
      have hp : p = 0 := Nat.eq_zero_of_le_zero hpq
      subst hp
      simp [width]
    · have hwidthq : width q = q.log2 + 1 := by simp [width, hq]
      rw [hwidthq]
      by_cases hp : p = 0
      · subst hp
        rw [show width 0 = 0 from by simp [width]]
        rw [Nat.max_eq_right (by omega : (0:Nat) ≤ q.log2 + 1)]
      · rw [show width p = p.log2 + 1 from by simp [width, hp]]
        rw [Nat.max_eq_right (by
          have := log2_mono' hpq
          omega)]
  · rw [Nat.max_eq_left (Nat.le_of_not_le hpq)]
    have hqp : q ≤ p := Nat.le_of_not_le hpq
    by_cases hp : p = 0
    · subst hp
      have hq : q = 0 := Nat.eq_zero_of_le_zero hqp
      subst hq
      simp [width]
    · have hwidthp : width p = p.log2 + 1 := by simp [width, hp]
      rw [hwidthp]
      by_cases hq : q = 0
      · subst hq
        rw [show width 0 = 0 from by simp [width]]
        rw [Nat.max_eq_left (by omega : (0:Nat) ≤ p.log2 + 1)]
      · rw [show width q = q.log2 + 1 from by simp [width, hq]]
        rw [Nat.max_eq_left (by
          have := log2_mono' hqp
          omega)]

private theorem foldl_max_zero : ∀ (k : Nat), (List.replicate k (0:Nat)).foldl max 0 = 0 := by
  intro k
  induction k with
  | zero => rfl
  | succ k ih =>
      show (0 :: List.replicate k 0).foldl max 0 = 0
      rw [List.foldl_cons]
      exact ih

private theorem peak_replicate_zero : ∀ (n : Nat), peak (List.replicate n 0) = 0 := by
  intro n
  cases n with
  | zero => simp [peak]
  | succ k =>
      show peak (0 :: List.replicate k 0) = 0
      rw [peak, max?_cons', Option.getD_some, foldl_max_zero]

theorem sound_ShiftedShard : ShiftedShard.Goal := by
  intro a b
  have hres : residuals (a ++ b.map (· + a.length)) = residuals a ++ residuals b := by
    unfold residuals
    rw [zipIdx_append_s, Nat.zero_add, List.map_append]
    congr 1
    rw [zipIdx_map_add, zipIdx_shift b a.length, List.map_map, List.map_map]
    congr 1
    funext p
    cases p with
    | mk v i =>
        simp only [Function.comp_apply]
        congr 1
        omega
  simp only [bitWidth, hres, peak_append, width_max]

theorem sound_ConsecutiveKeys : ConsecutiveKeys.Goal := by
  intro n
  have hzip : (List.range n).zipIdx = (List.range n).map (fun i => (i, i)) := by
    induction n with
    | zero => rfl
    | succ k ih =>
        rw [List.range_succ, zipIdx_append_s, ih, List.map_append, List.length_range]
        simp
  have hmap : (List.range n).map (fun _ => (0:Nat)) = List.replicate n 0 := by
    show (List.range n).map (Function.const Nat (0:Nat)) = List.replicate n 0
    rw [List.map_const, List.length_range]
  have hres : residuals (List.range n) = List.replicate n 0 := by
    unfold residuals
    rw [hzip, List.map_map, ← hmap]
    congr 1
    funext i
    simp only [Function.comp_apply]
    rw [Int.sub_self]
    rfl
  have hbw : bitWidth (List.range n) = 0 := by
    simp [bitWidth, hres, peak_replicate_zero, width]
  simp [OrdinalResidual.write, hbw, pack, List.length_append,
    TinylambOpt.ResearchProofs.StorageExecution.SECommon.uleb_len, TinylambOpt.ResearchProofs.StorageExecution.SECommon.little_len, ubytes, width, byteCount]

/-- Mirror-verified counterexample: a=[256], b=[0,0], c=[0,1]. -/
theorem disproof_GreedyCut : ¬ GreedyCut.Goal := by
  intro h
  have hne1 : ([256] : List Nat) ≠ [] := by decide
  have hne2 : ([0, 0] : List Nat) ≠ [] := by decide
  have hne3 : ([0, 1] : List Nat) ≠ [] := by decide
  have hcond : chosen write ([256] ++ [0, 0]) ≤
      chosen write [256] + chosen write [0, 0] := by native_decide
  have := h [256] [0, 0] [0, 1] hne1 hne2 hne3 hcond
  revert this
  native_decide

end TinylambOpt.ResearchProofs.StorageExecution.FrameSelection.OrdinalResidual
