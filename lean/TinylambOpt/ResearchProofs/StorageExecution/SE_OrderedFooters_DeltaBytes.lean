import TinylambOpt.ResearchProofs.StorageExecution.SE_OrderedFooters_Common
set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.StorageExecution.OrderedFooters.DeltaBytes
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.Compression.Wire
open SE_ORF

theorem ref_eq (xs : List Nat) :
    DeltaBytes.agg.reference xs =
      xs.head?.map (fun a =>
        (a, xs.getLast?.getD a, ((edges xs).map fun (x, y) => sbytes ((y : Int) - (x : Int))).sum)) := rfl

theorem join_eq :
    DeltaBytes.agg.join =
      optionalJoin (fun (a, b, n) (c, d, m) => (a, d, n + m + sbytes ((c : Int) - (b : Int)))) := rfl

theorem stitch_agg : stitch DeltaBytes.agg := by
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
            ((edges (y :: ya)).map fun (x, y) => sbytes ((y : Int) - (x : Int))).sum
              + ((edges (z :: zb)).map fun (x, y) => sbytes ((y : Int) - (x : Int))).sum
              + sbytes ((z : Int) - (y :: ya).getLast?.getD y))
          : Option (Nat × Nat × Nat))
          = some (y, (y :: (ya ++ z :: zb)).getLast?.getD y,
              ((edges (y :: ya)
                  ++ ((y :: ya).getLast?.getD 0, z) :: edges (z :: zb)).map
                fun (x, y) => sbytes ((y : Int) - (x : Int))).sum)
      simp only [Option.some.injEq, Prod.mk.injEq, List.map_append, List.sum_append,
        List.map_cons, List.map_nil, List.sum_cons, Nat.add_zero]
      refine ⟨trivial, (last_getD_append_cons y ya zb z).symm, ?_⟩
      rw [getD_last_cons]
      omega

theorem sound_Stitch : DeltaBytes.Stitch.Goal := stitch_agg
theorem sound_TreeReduce : DeltaBytes.TreeReduce.Goal := treeReduce_of_stitch stitch_agg
theorem sound_RepeatPower : DeltaBytes.RepeatPower.Goal := repeatPower_of_stitch stitch_agg
theorem sound_FrameSlice : DeltaBytes.FrameSlice.Goal := frameSlice_of_stitch stitch_agg
theorem sound_PrefixCheckpoint : DeltaBytes.PrefixCheckpoint.Goal :=
  prefixCheckpoint_of_stitch stitch_agg
theorem sound_AmendLeaf : DeltaBytes.AmendLeaf.Goal := amendLeaf_of_stitch stitch_agg
theorem sound_RunInput : DeltaBytes.RunInput.Goal :=
  runInput_of_stitch stitch_agg (repeatPower_of_stitch stitch_agg)
theorem sound_ReorderNeighbors : DeltaBytes.ReorderNeighbors.Goal :=
  reorderNeighbors_of_stitch stitch_agg

/-- Byte cost never grows by more than one byte per edge under reversal. -/
private theorem log2_mono {a b : Nat} (h : a ≤ b) : a.log2 ≤ b.log2 := by
  rcases Nat.eq_zero_or_pos a with ha | ha
  · rw [ha]
    simp
  · have hb : b ≠ 0 := by omega
    have h2 := (Nat.le_log2 (by omega : a ≠ 0)).mp (Nat.le_refl (a.log2))
    exact (Nat.le_log2 hb).mpr (Nat.le_trans h2 h)

private theorem width_mono {a b : Nat} (h : a ≤ b) : width a ≤ width b := by
  rcases Nat.eq_zero_or_pos a with ha | ha
  · subst ha
    cases b with
    | zero => simp [width]
    | succ b => simp [width]
  · have hb : b ≠ 0 := by omega
    have ha' : ¬(a == 0) := by simpa using (by omega : a ≠ 0)
    have hb' : ¬(b == 0) := by simpa using (by omega : b ≠ 0)
    simp [width, ha', hb']
    exact log2_mono h

private theorem width_succ (n : Nat) : width (n + 1) ≤ width n + 1 := by
  cases n with
  | zero => simp [width]
  | succ m =>
      have h0 : ¬(m + 1 + 1 == 0) := by simp
      have h1 : ¬(m + 1 == 0) := by simp
      have h2 : m + 1 < 2 ^ ((m + 1).log2 + 1) :=
        (Nat.log2_lt (show m + 1 ≠ 0 by simp) (k := (m + 1).log2 + 1)).mp (by omega)
      have h3 : m + 1 + 1 ≤ 2 ^ ((m + 1).log2 + 1) := by omega
      have hp : 1 ≤ 2 ^ ((m + 1).log2 + 1) := Nat.one_le_pow _ _ (by omega)
      have h4 : m + 1 + 1 < 2 ^ ((m + 1).log2 + 2) := by
        rw [show (m + 1).log2 + 2 = (m + 1).log2 + 1 + 1 by omega, Nat.pow_succ]
        omega
      have h5 := (Nat.log2_lt (show m + 1 + 1 ≠ 0 by simp) (k := (m + 1).log2 + 2)).mpr h4
      simp only [width, if_neg h0, if_neg h1]
      omega

private theorem ubytes_mono {a b : Nat} (h : a ≤ b) : ubytes a ≤ ubytes b := by
  show max 1 ((width a + 6) / 7) ≤ max 1 ((width b + 6) / 7)
  have h1 : 1 ≤ max 1 ((width b + 6) / 7) := Nat.le_max_left _ _
  have h2 : (width a + 6) / 7 ≤ (width b + 6) / 7 :=
    Nat.div_le_div_right (Nat.add_le_add_right (width_mono h) 6)
  have h3 : (width b + 6) / 7 ≤ max 1 ((width b + 6) / 7) := Nat.le_max_right _ _
  exact Nat.max_le.mpr ⟨h1, Nat.le_trans h2 h3⟩

private theorem ubytes_succ (n : Nat) : ubytes (n + 1) ≤ ubytes n + 1 := by
  show max 1 ((width (n + 1) + 6) / 7) ≤ max 1 ((width n + 6) / 7) + 1
  have hw : width (n + 1) ≤ width n + 1 := width_succ n
  have hd : (width (n + 1) + 6) / 7 ≤ (width n + 6) / 7 + 1 := by
    have hx : width (n + 1) + 6 ≤ 7 * ((width n + 6) / 7 + 1) := by
      have hmod := Nat.div_add_mod (width n + 6) 7
      omega
    exact Nat.div_le_of_le_mul hx
  have hm : (width n + 6) / 7 ≤ max 1 ((width n + 6) / 7) := Nat.le_max_right _ _
  exact Nat.max_le.mpr ⟨by omega, by omega⟩

private theorem zig_neg_le (t : Int) : zig (-t) ≤ zig t + 1 := by
  cases t with
  | ofNat n =>
      cases n with
      | zero => simp [zig]
      | succ m =>
          show zig (-((m + 1 : Nat) : Int)) ≤ zig ((m + 1 : Nat) : Int) + 1
          have h1 : zig (-((m + 1 : Nat) : Int)) = 2 * m + 1 := rfl
          have h2 : zig ((m + 1 : Nat) : Int) = 2 * (m + 1) := rfl
          rw [h1, h2]
          omega
  | negSucc n =>
      have h1 : zig (Int.negSucc n) = 2 * n + 1 := rfl
      have h2 : zig (-Int.negSucc n) = 2 * (n + 1) := rfl
      rw [h1, h2]
      omega

private theorem sbytes_neg_le (t : Int) : sbytes (-t) ≤ sbytes t + 1 := by
  show ubytes (zig (-t)) ≤ ubytes (zig t) + 1
  have h1 : zig (-t) ≤ zig t + 1 := zig_neg_le t
  have h2 : ubytes (zig t + 1) ≤ ubytes (zig t) + 1 := ubytes_succ _
  exact Nat.le_trans (ubytes_mono h1) h2

private theorem swap_sum_le (l : List (Nat × Nat)) :
    ((l.map Prod.swap).map fun (x, y) => sbytes ((y : Int) - (x : Int))).sum
      ≤ (l.map fun (x, y) => sbytes ((y : Int) - (x : Int))).sum + l.length := by
  induction l with
  | nil => simp
  | cons p t ih =>
      rcases p with ⟨a, b⟩
      have hle : sbytes ((a : Int) - (b : Int))
          ≤ sbytes ((b : Int) - (a : Int)) + 1 := by
        rw [show ((a : Int) - (b : Int)) = -((b : Int) - (a : Int)) from (Int.neg_sub _ _).symm]
        exact sbytes_neg_le _
      rw [List.map_cons, List.map_cons, List.map_cons, List.sum_cons, List.sum_cons,
        List.length_cons]
      simp only [Prod.swap] at ih ⊢
      omega

private theorem getD_last_snoc (l : List Nat) (y : Nat) :
    ((l ++ [y]).getLast? : Option Nat).getD 0 = y := by
  have h1 : ((l ++ [y]).getLast? : Option Nat) = (some y).or (l.getLast? : Option Nat) := by
    rw [List.getLast?_append]
    have h0 : (([y] : List Nat).getLast? : Option Nat) = some y := rfl
    rw [h0]
  have h2 : ((some y).or (l.getLast? : Option Nat) : Option Nat) = some y := rfl
  have h3 : ((some y : Option Nat).getD 0 : Nat) = y := rfl
  rw [h1, h2, h3]

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
          rw [hrev, he, ih, hl, hcons]
          rw [List.map_cons, List.reverse_cons]
          rfl

private theorem hsz (l : List Nat) :
    ((DeltaBytes.agg.reference l).map (fun v => v.2.2)).getD 0
      = ((edges l).map fun (x, y) => sbytes ((y : Int) - (x : Int))).sum := by
  cases l with
  | nil => rfl
  | cons a r => rfl

private theorem elen (xs : List Nat) : (edges xs).length = xs.length - 1 := by
  cases xs with
  | nil => rfl
  | cons x xs =>
      rw [show edges (x :: xs) = (x :: xs).zip ((x :: xs).drop 1) from rfl]
      have h1 : ((x :: xs : List Nat).drop 1) = xs := rfl
      rw [h1, List.length_zip]
      simp

theorem sound_ReverseResidualCliff : DeltaBytes.ReverseResidualCliff.Goal := by
  intro xs
  show ((DeltaBytes.agg.reference xs.reverse).map (fun v => v.2.2)).getD 0
      ≤ ((DeltaBytes.agg.reference xs).map (fun v => v.2.2)).getD 0 + (xs.length - 1)
  rw [hsz, hsz]
  rw [edges_reverse, List.map_reverse, List.sum_reverse]
  have h2 := swap_sum_le (edges xs)
  rw [elen] at h2
  omega

theorem sound_RepeatedConstant : DeltaBytes.RepeatedConstant.Goal := by
  intro x n
  have h1 : sbytes ((x : Int) - (x : Int)) = 1 := by
    rw [Int.sub_self]
    rfl
  induction n with
  | zero => rfl
  | succ n ih =>
      have hrec : x :: List.replicate (n + 1) x = x :: x :: List.replicate n x := by
        rw [List.replicate_succ]
      have hget : ∀ k : Nat, ((List.replicate k x).getLast? : Option Nat).getD x = x := by
        intro k
        induction k with
        | zero => rfl
        | succ k ihk =>
            rw [List.replicate_succ, List.getLast?_cons]
            rw [ihk]
            have : ((some x : Option Nat).getD x : Nat) = x := rfl
            rw [this]
      have h2 : ((x :: x :: List.replicate n x : List Nat).getLast? : Option Nat).getD x = x := by
        have e1 : (((x :: x :: List.replicate n x : List Nat).getLast? : Option Nat).getD x : Nat)
            = ((x :: List.replicate n x).getLast? : Option Nat).getD x := by
          rw [List.getLast?_cons]
          rfl
        have e2 : (((x :: List.replicate n x).getLast? : Option Nat).getD x : Nat) = x := by
          rw [List.getLast?_cons]
          have := hget n
          rw [this]
          rfl
        rw [e1, e2]
      have he : edges (x :: x :: List.replicate n x)
          = (x, x) :: edges (x :: List.replicate n x) := by
        have hd : ((x :: x :: List.replicate n x : List Nat).drop 1) = x :: List.replicate n x := rfl
        show (x :: x :: List.replicate n x).zip (x :: List.replicate n x) = _
        rw [show (x :: List.replicate n x) = (x :: List.replicate n x) from rfl]
        rfl
      show some (x, (x :: List.replicate (n + 1) x).getLast?.getD x,
            ((edges (x :: List.replicate (n + 1) x)).map
              fun (x_1, y) => sbytes ((y : Int) - (x_1 : Int))).sum)
          = some (x, x, n + 1)
      have hm : (match (x, x) with | (x_1, y) => sbytes ((y : Int) - (x_1 : Int))) = 1 := by
        show sbytes ((x : Int) - (x : Int)) = 1
        rw [Int.sub_self]
        rfl
      rw [hrec, he, List.map_cons, List.sum_cons, hm, h2]
      have hs : ((edges (x :: List.replicate n x)).map
            fun (x_1, y) => sbytes ((y : Int) - (x_1 : Int))).sum = n := by
        have h3 : DeltaBytes.agg.reference (x :: List.replicate n x)
            = some (x, (x :: List.replicate n x).getLast?.getD x,
                ((edges (x :: List.replicate n x)).map
                  fun (x_1, y) => sbytes ((y : Int) - (x_1 : Int))).sum) := rfl
        rw [h3] at ih
        simp only [Option.some.injEq, Prod.mk.injEq] at ih
        exact ih.2.2
      rw [hs]
      simp
      omega

