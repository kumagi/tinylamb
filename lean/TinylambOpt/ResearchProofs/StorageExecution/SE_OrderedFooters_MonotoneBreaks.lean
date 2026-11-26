import TinylambOpt.ResearchProofs.StorageExecution.SE_OrderedFooters_Common
set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.StorageExecution.OrderedFooters.MonotoneBreaks
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open SE_ORF

theorem ref_eq (xs : List Nat) :
    MonotoneBreaks.agg.reference xs =
      xs.head?.map (fun a => (a, xs.getLast?.getD a, (edges xs).countP (fun (x, y) => y < x))) := rfl

theorem join_eq :
    MonotoneBreaks.agg.join =
      optionalJoin (fun (a, b, n) (c, d, m) => (a, d, n + m + (decide (c < b)).toNat)) := rfl

theorem stitch_agg : stitch MonotoneBreaks.agg := by
  intro a b
  cases a with
  | nil =>
    cases b with
    | nil => rfl
    | cons z zb => rfl
  | cons y ya =>
    cases b with
    | nil =>
      rw [List.append_nil]
      rfl
    | cons z zb =>
      rw [ref_eq, ref_eq, ref_eq, join_eq]
      rw [edges_append _ _ (by simp) (by simp)]
      show (some (y, (z :: zb).getLast?.getD z,
            (edges (y :: ya)).countP (fun (x, y) => y < x)
              + (edges (z :: zb)).countP (fun (x, y) => y < x)
              + (decide (z < (y :: ya).getLast?.getD y)).toNat)
          : Option (Nat × Nat × Nat))
        = some (y, (y :: (ya ++ z :: zb)).getLast?.getD y,
            List.countP (fun (x, y) => y < x)
              (edges (y :: ya) ++ (((y :: ya).getLast?.getD 0, z) :: edges (z :: zb))))
      simp only [Option.some.injEq, Prod.mk.injEq]
      refine ⟨trivial, (last_getD_append_cons y ya zb z).symm, ?_⟩
      rw [getD_last_cons, List.countP_append, List.countP_cons]
      by_cases h : z < (y :: ya).getLast?.getD y
      · simp [h]
        omega
      · simp [h]

theorem sound_Stitch : MonotoneBreaks.Stitch.Goal := stitch_agg
theorem sound_TreeReduce : MonotoneBreaks.TreeReduce.Goal := treeReduce_of_stitch stitch_agg
theorem sound_RepeatPower : MonotoneBreaks.RepeatPower.Goal := repeatPower_of_stitch stitch_agg
theorem sound_FrameSlice : MonotoneBreaks.FrameSlice.Goal := frameSlice_of_stitch stitch_agg
theorem sound_PrefixCheckpoint : MonotoneBreaks.PrefixCheckpoint.Goal :=
  prefixCheckpoint_of_stitch stitch_agg
theorem sound_AmendLeaf : MonotoneBreaks.AmendLeaf.Goal := amendLeaf_of_stitch stitch_agg
theorem sound_RunInput : MonotoneBreaks.RunInput.Goal :=
  runInput_of_stitch stitch_agg (repeatPower_of_stitch stitch_agg)
theorem sound_ReorderNeighbors : MonotoneBreaks.ReorderNeighbors.Goal :=
  reorderNeighbors_of_stitch stitch_agg

theorem breaks_zero_iff (xs : List Nat) :
    (edges xs).countP (fun (x, y) => y < x) = 0 ↔ xs.Pairwise (· ≤ ·) := by
  induction xs with
  | nil => exact ⟨fun _ => List.Pairwise.nil, fun _ => rfl⟩
  | cons x xs ih =>
    cases xs with
    | nil =>
      exact ⟨fun _ => List.Pairwise.cons (fun _ h => absurd h (by simp)) List.Pairwise.nil,
        fun _ => rfl⟩
    | cons z zs =>
      rw [edges_cons_three, List.countP_cons, List.pairwise_cons]
      by_cases h : z < x
      · rw [if_pos (by simpa using h)]
        constructor
        · intro hh
          exfalso
          omega
        · intro hh
          have hx : x ≤ z := hh.1 z (by simp)
          omega
      · rw [if_neg (by simpa using h), Nat.add_zero]
        constructor
        · intro hz
          refine ⟨?_, ih.mp hz⟩
          intro a ha
          rcases List.mem_cons.mp ha with rfl | ha2
          · exact Nat.le_of_not_lt h
          · exact Nat.le_trans (Nat.le_of_not_lt h) ((List.pairwise_cons.mp (ih.mp hz)).1 a ha2)
        · intro hall
          exact ih.mpr hall.2

theorem sound_SortingCertificate : MonotoneBreaks.SortingCertificate.Goal := by
  intro xs
  cases xs with
  | nil => exact ⟨fun _ => List.Pairwise.nil, fun _ => rfl⟩
  | cons y ys =>
    show ((edges (y :: ys)).countP (fun (x, y) => y < x) = 0) ↔ (y :: ys).Pairwise (· ≤ ·)
    exact breaks_zero_iff (y :: ys)

theorem getD_last_le_local (xs : List Nat) (h : xs ≠ []) (z : Nat)
    (hz : ∀ x ∈ xs, x ≤ z) : xs.getLast?.getD 0 ≤ z := by
  obtain ⟨as, a, rfl⟩ := exists_append_last xs h
  have hl : ((as ++ [a]).getLast? : Option Nat) = some a := by
    rcases as with _ | ⟨y, ys⟩
    · rfl
    · rw [List.getLast?_append]
      have h0 : (([a] : List Nat).getLast? : Option Nat) = some a := rfl
      rw [h0]
      have h2 : ((some a).or ((y :: ys).getLast?) : Option Nat) = some a := rfl
      rw [h2]
  rw [hl, Option.getD_some]
  exact hz a (by simp)

theorem sound_ParallelMergeFree : MonotoneBreaks.ParallelMergeFree.Goal := by
  intro a b hpa hpb hle
  have ha0 := breaks_zero_iff a |>.mpr hpa
  have hb0 := breaks_zero_iff b |>.mpr hpb
  have key : (edges (a ++ b)).countP (fun (x, y) => y < x) = 0 := by
    cases a with
    | nil =>
      cases b with
      | nil => rfl
      | cons z zb => exact hb0
    | cons y ya =>
      cases b with
      | nil =>
        rw [List.append_nil]
        exact ha0
      | cons z zb =>
        rw [edges_append _ _ (by simp) (by simp), getD_last_cons, List.countP_append,
          List.countP_cons]
        have key2 : (y :: ya).getLast?.getD y = (y :: ya).getLast?.getD 0 :=
          (getD_last_cons y ya).symm
        have hl := getD_last_le_local (y :: ya) (by simp) z fun x hx => hle x hx z (by simp)
        have hlt : ¬ ((fun (x, y) => y < x) ((y :: ya).getLast?.getD y, (z :: zb).head?.getD 0) =
            true) := by
          show ¬ (decide (z < (y :: ya).getLast?.getD y) = true)
          rw [key2]
          intro hc
          simp only [decide_eq_true_eq] at hc
          omega
        rw [if_neg hlt, ha0, hb0]
  cases a with
  | nil =>
    cases b with
    | nil => exact rfl
    | cons z zb =>
      show ((edges ([] ++ (z :: zb))).countP (fun (x, y) => y < x) == 0) = true
      rw [key]
      rfl
  | cons y ya =>
    cases b with
    | nil =>
      show ((edges ((y :: ya) ++ [])).countP (fun (x, y) => y < x) == 0) = true
      rw [key]
      rfl
    | cons z zb =>
      show ((edges ((y :: ya) ++ (z :: zb))).countP (fun (x, y) => y < x) == 0) = true
      rw [key]
      rfl

end TinylambOpt.ResearchProofs.StorageExecution.OrderedFooters.MonotoneBreaks
