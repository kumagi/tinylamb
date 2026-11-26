import TinylambOpt.ResearchProofs.StorageExecution.SE_OrderedFooters_Common

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved (10 claims, including the following)

- `FirstLastChange.AmendLeaf` — Update one cached block using the left and right
  partial states.
- `FirstLastChange.DuplicateBlock` — `∀ xs k, xs≠[] → 0<k → let n := ((agg.reference
  xs).map (fun r => r.2.2)).getD 0 ((agg.reference (List.replicate k xs).flatten).map
  (fun r => r.2.2)).getD 0 = k*n-(if xs.head?==xs.getLast? then k-1 else 0)`
- `FirstLastChange.FrameSlice` — Use only the two ragged boundaries and whole-page
  summaries for a window.
-/

set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.StorageExecution.OrderedFooters.FirstLastChange
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open SE_ORF

theorem ref_eq (xs : List Nat) :
    FirstLastChange.agg.reference xs =
      xs.head?.map (fun a =>
        (a, xs.getLast?.getD a, 1 + (edges xs).countP (fun (x, y) => x != y))) := rfl

theorem join_eq :
    FirstLastChange.agg.join =
      optionalJoin (fun (a, b, n) (c, d, m) => (a, d, n + m - (decide (b = c)).toNat)) := rfl

theorem stitch_agg : stitch FirstLastChange.agg := by
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
      show (some (y, ((z :: zb).getLast? : Option Nat).getD z,
            (1 + (edges (y :: ya)).countP (fun (x, y) => x != y))
              + (1 + (edges (z :: zb)).countP (fun (x, y) => x != y))
              - (decide ((y :: ya).getLast?.getD y = (z :: zb).head?.getD z)).toNat)
          : Option (Nat × Nat × Nat))
          = some (y, (y :: (ya ++ z :: zb)).getLast?.getD y,
              1 + List.countP (fun (x, y) => x != y)
                (edges (y :: ya) ++ (((y :: ya).getLast?.getD 0, z) :: edges (z :: zb))))
      simp only [Option.some.injEq, Prod.mk.injEq]
      refine ⟨trivial, (last_getD_append_cons y ya zb z).symm, ?_⟩
      rw [getD_last_cons, List.countP_append, List.countP_cons]
      by_cases h : (y :: ya).getLast?.getD y = z
      · simp [h]
        omega
      · simp [h]
        omega

theorem sound_Stitch : FirstLastChange.Stitch.Goal := stitch_agg
theorem sound_TreeReduce : FirstLastChange.TreeReduce.Goal := treeReduce_of_stitch stitch_agg
theorem sound_RepeatPower : FirstLastChange.RepeatPower.Goal :=
  repeatPower_of_stitch stitch_agg
theorem sound_FrameSlice : FirstLastChange.FrameSlice.Goal := frameSlice_of_stitch stitch_agg
theorem sound_PrefixCheckpoint : FirstLastChange.PrefixCheckpoint.Goal :=
  prefixCheckpoint_of_stitch stitch_agg
theorem sound_AmendLeaf : FirstLastChange.AmendLeaf.Goal := amendLeaf_of_stitch stitch_agg
theorem sound_RunInput : FirstLastChange.RunInput.Goal :=
  runInput_of_stitch stitch_agg (repeatPower_of_stitch stitch_agg)
theorem sound_ReorderNeighbors : FirstLastChange.ReorderNeighbors.Goal :=
  reorderNeighbors_of_stitch stitch_agg

/-- Run count of a non-empty list. -/
private def N (l : List Nat) : Nat :=
  ((FirstLastChange.agg.reference l).map (fun r => r.2.2)).getD 0

/-- Append of non-empty regions: run counts add, minus the merge indicator. -/
private theorem N_append (a b : List Nat) (ha : a ≠ []) (hb : b ≠ []) :
    N (a ++ b) = N a + N b
      - (decide (a.getLast?.getD (a.head?.getD 0) = b.head?.getD (b.head?.getD 0))).toNat := by
  cases a with
  | nil => exact absurd ha (by simp)
  | cons y ya =>
      cases b with
      | nil => exact absurd hb (by simp)
      | cons z zb =>
          have ha1 : N (y :: ya) = 1 + (edges (y :: ya)).countP (fun (x, y) => x != y) := rfl
          have hb1 : N (z :: zb) = 1 + (edges (z :: zb)).countP (fun (x, y) => x != y) := rfl
          have hab : N ((y :: ya) ++ (z :: zb))
              = 1 + (edges ((y :: ya) ++ (z :: zb))).countP (fun (x, y) => x != y) := rfl
          have hhd : ((z :: zb).head?.getD 0 : Nat) = z := rfl
          have hhd2 : ((z :: zb).head?.getD ((z :: zb).head?.getD 0) : Nat) = z := rfl
          have hhl : ((y :: ya).getLast?.getD ((y :: ya).head?.getD 0) : Nat)
              = (y :: ya).getLast?.getD y := rfl
          rw [ha1, hb1, hab, hhd2, hhl]
          rw [edges_append _ _ (by simp) (by simp)]
          rw [getD_last_cons, List.countP_append, List.countP_cons]
          by_cases hq : (y :: ya).getLast?.getD y = z
          · simp [hq]
            omega
          · simp [hq]
            omega

private theorem head_flatten_rep (y : Nat) (ya : List Nat) : ∀ m, m ≠ 0 →
    ((List.replicate m (y :: ya)).flatten.head? : Option Nat) = some y := by
  intro m hm
  cases m with
  | zero => exact absurd hm (by simp)
  | succ m =>
      rw [List.replicate_succ, List.flatten_cons]
      rfl

private theorem getD_last_snoc (l : List Nat) (y : Nat) :
    ((l ++ [y]).getLast? : Option Nat).getD 0 = y := by
  have h1 : ((l ++ [y]).getLast? : Option Nat) = (some y).or (l.getLast? : Option Nat) := by
    rw [List.getLast?_append]
    have h0 : (([y] : List Nat).getLast? : Option Nat) = some y := rfl
    rw [h0]
  have h2 : ((some y).or (l.getLast? : Option Nat) : Option Nat) = some y := rfl
  have h3 : ((some y : Option Nat).getD 0 : Nat) = y := rfl
  rw [h1, h2, h3]

theorem sound_DuplicateBlock : FirstLastChange.DuplicateBlock.Goal := by
  intro xs k hx hk
  cases xs with
  | nil => exact absurd hx (by simp)
  | cons y ya =>
      let d : Nat := ((y :: ya).getLast? : Option Nat).getD y
      have hslc : ((y :: ya).getLast? : Option Nat) = some d := by
        obtain ⟨as, w, hw⟩ := exists_append_last (y :: ya) (by simp)
        rw [hw]
        have hl : ((as ++ [w]).getLast? : Option Nat) = some w := by
          rcases as with _ | ⟨a', as'⟩
          · rfl
          · rw [List.getLast?_append]
            have h0 : (([w] : List Nat).getLast? : Option Nat) = some w := rfl
            rw [h0]
            have h2 : ((some w).or ((a' :: as').getLast? : Option Nat) : Option Nat) = some w := rfl
            rw [h2]
        rw [hl]
        have hd : d = w := by
          show (((y :: ya).getLast? : Option Nat).getD y : Nat) = w
          rw [hw, hl, Option.getD_some]
        rw [hd]
      have hne : ∀ m, (List.replicate (m + 1) (y :: ya)).flatten ≠ [] := by
        intro m hm
        have h1 : (List.replicate (m + 1) (y :: ya)).flatten
            = (y :: ya) ++ (List.replicate m (y :: ya)).flatten := by
          rw [List.replicate_succ, List.flatten_cons]
        rw [h1] at hm
        exact absurd (List.eq_nil_of_append_eq_nil hm).1 (by simp)
      have hhd : ∀ m, ((List.replicate (m + 1) (y :: ya)).flatten.head? : Option Nat) = some y := by
        intro m
        rw [List.replicate_succ, List.flatten_cons]
        rfl
      -- glue of the circular boundary, in both branches of `y = d`
      have hgl_aux : ∀ m, (decide (((y :: ya).getLast?.getD ((y :: ya).head?.getD 0) : Nat)
          = ((List.replicate (m + 1) (y :: ya)).flatten.head?.getD
          ((List.replicate (m + 1) (y :: ya)).flatten.head?.getD 0) : Nat))).toNat
          = (decide (d = y)).toNat := by
        intro m
        have e1 : ((y :: ya).getLast?.getD ((y :: ya).head?.getD 0) : Nat) = d := by
          rw [hslc, Option.getD_some]
        have e2 : ((List.replicate (m + 1) (y :: ya)).flatten.head?.getD
            ((List.replicate (m + 1) (y :: ya)).flatten.head?.getD 0) : Nat) = y := by
          rw [hhd m]
          have : ((some y : Option Nat).getD 0 : Nat) = y := rfl
          rw [this, Option.getD_some]
        rw [e1, e2]
      have hrep_succ : ∀ m, (List.replicate (m + 1 + 1) (y :: ya)).flatten
          = (y :: ya) ++ (List.replicate (m + 1) (y :: ya)).flatten := by
        intro m
        rw [List.replicate_succ, List.flatten_cons]
      have hlin_aux : ∀ (A m : Nat), (m + 1 + 1) * A = (m + 1) * A + A := by
        intro A m
        rw [Nat.add_mul, Nat.one_mul]
      have hA : 1 ≤ N (y :: ya) := by
        show (1 + (edges (y :: ya)).countP (fun (x, y) => x != y)) ≥ 1
        exact Nat.le_add_right 1 _
      by_cases hz : y = d
      · -- cyclic boundary: each join saves one run
        have h2 : (decide (d = y)).toNat = 1 := by
          rw [hz.symm]
          simp
        have hkey : ∀ m : Nat,
            N (List.replicate (m + 1) (y :: ya)).flatten + m = (m + 1) * N (y :: ya) := by
          intro m
          induction m with
          | zero =>
              have h1 : (List.replicate 1 (y :: ya)).flatten = (y :: ya) := by simp
              rw [h1]
              simp
          | succ m ih =>
              have hstep := N_append (y :: ya) (List.replicate (m + 1) (y :: ya)).flatten
                (by simp) (hne m)
              have hlin : (m + 1 + 1) * N (y :: ya) = (m + 1) * N (y :: ya) + N (y :: ya) :=
                hlin_aux _ _
              rw [hrep_succ, hstep, hgl_aux, h2, hlin]
              omega
        have hbeq : (((y :: ya).head? : Option Nat) == (y :: ya).getLast?) = decide (y = d) := by
          have h1 : ((y :: ya).head? : Option Nat) = some y := rfl
          rw [h1, hslc]
          rfl
        show N (List.replicate k (y :: ya)).flatten
            = k * N (y :: ya) - if (y :: ya).head? == (y :: ya).getLast? then k - 1 else 0
        rw [hbeq]
        have h1 : decide (y = d) = true := by simp [hz]
        rw [h1]
        simp
        cases k with
        | zero => exact absurd hk (by omega)
        | succ m =>
            have h := hkey m
            simp at h ⊢
            omega
      · -- acyclic boundary: joins keep runs separate
        have hn : ¬d = y := fun hh => hz hh.symm
        have h2 : (decide (d = y)).toNat = 0 := by simp [hn]
        have hkey : ∀ m : Nat,
            N (List.replicate (m + 1) (y :: ya)).flatten = (m + 1) * N (y :: ya) := by
          intro m
          induction m with
          | zero =>
              have h1 : (List.replicate 1 (y :: ya)).flatten = (y :: ya) := by simp
              rw [h1]
              simp
          | succ m ih =>
              have hstep := N_append (y :: ya) (List.replicate (m + 1) (y :: ya)).flatten
                (by simp) (hne m)
              have hlin : (m + 1 + 1) * N (y :: ya) = (m + 1) * N (y :: ya) + N (y :: ya) :=
                hlin_aux _ _
              rw [hrep_succ, hstep, hgl_aux, h2, hlin, Nat.sub_zero, ih]
              omega
        have hbeq : (((y :: ya).head? : Option Nat) == (y :: ya).getLast?) = decide (y = d) := by
          have h1 : ((y :: ya).head? : Option Nat) = some y := rfl
          rw [h1, hslc]
          rfl
        show N (List.replicate k (y :: ya)).flatten
            = k * N (y :: ya) - if (y :: ya).head? == (y :: ya).getLast? then k - 1 else 0
        rw [hbeq]
        have h1 : decide (y = d) = false := by simp [hz]
        rw [h1]
        simp
        cases k with
        | zero => exact absurd hk (by omega)
        | succ m => exact hkey m

private theorem hswap (l : List (Nat × Nat)) :
    (l.map Prod.swap).countP (fun (x, y) => x != y) = l.countP (fun (x, y) => x != y) := by
  induction l with
  | nil => rfl
  | cons t l ih =>
      rw [List.map_cons, List.countP_cons, List.countP_cons, ih]
      rcases t with ⟨a, b⟩
      by_cases hq : a = b <;> simp [hq] <;> omega

private theorem hrevc (l : List (Nat × Nat)) :
    l.countP (fun (x, y) => x != y) = l.reverse.countP (fun (x, y) => x != y) := by
  induction l with
  | nil => rfl
  | cons t l ih =>
      rw [List.countP_cons, List.reverse_cons, List.countP_append, ih, List.countP_cons,
        List.countP_nil, Nat.zero_add]

private theorem edges_reverse (xs : List Nat) :
    edges xs.reverse = ((edges xs).map Prod.swap).reverse := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      cases xs with
      | nil => rfl
      | cons y ys =>
          have hrev : ((x :: y :: ys : List Nat).reverse : List Nat)
              = (y :: ys : List Nat).reverse ++ [x] := List.reverse_cons
          have hb : ((y :: ys : List Nat).reverse : List Nat) ≠ [] := by simp
          have he : edges ((y :: ys : List Nat).reverse ++ [x])
              = edges ((y :: ys : List Nat).reverse)
                ++ (((y :: ys : List Nat).reverse.getLast?.getD 0, x) :: edges [x]) :=
            edges_append _ _ hb (by simp)
          have hl : ((y :: ys : List Nat).reverse : List Nat).getLast?.getD 0 = y := by
            rw [List.reverse_cons, getD_last_snoc]
          have hcons : edges (x :: y :: ys) = (x, y) :: edges (y :: ys) := rfl
          have hsw : ([(x, y)] : List (Nat × Nat)).map Prod.swap = [(y, x)] := rfl
          rw [hrev, he, ih, hl, hcons]
          rw [List.map_cons, List.reverse_cons]
          rfl

theorem sound_ReverseCount : FirstLastChange.ReverseCount.Goal := by
  intro xs
  rw [ref_eq (xs.reverse), ref_eq xs]
  cases xs with
  | nil => rfl
  | cons y ya =>
      rw [List.head?_reverse, List.getLast?_cons]
      show (some (1 + (edges ((y :: ya).reverse : List Nat)).countP (fun (x, y) => x != y))
          : Option Nat)
          = some (1 + (edges (y :: ya)).countP (fun (x, y) => x != y))
      rw [edges_reverse (y :: ya)]
      rw [(hrevc ((edges (y :: ya)).map Prod.swap)).symm]
      rw [hswap (edges (y :: ya))]

end TinylambOpt.ResearchProofs.StorageExecution.OrderedFooters.FirstLastChange
