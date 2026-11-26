import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `BalanceFloor.RunInput` — Consume the lengths on a run-encoded column directly.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BalanceFloor_RunInput

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BalanceFloor

/-! ## Small-input truth check -/

private def genRuns : Nat → List (List (Nat × Nat))
  | 0 => [[]]
  | n + 1 =>
      let p := genRuns n
      p ++ p.flatMap (fun rs =>
        [(0, 0) :: rs, (0, 1) :: rs, (1, 2) :: rs, (2, 1) :: rs,
         (3, 3) :: rs, (7, 4) :: rs, (5, 5) :: rs])

private def probe (n : Nat) : Bool :=
  (genRuns n).all (fun rs =>
    (rs.map (fun (v, k) => repeatState agg (agg.reference [v]) k)).foldl agg.join
        (agg.reference []) ==
      agg.reference (rs.flatMap (fun (v, k) => List.replicate k v)))

#eval probe 1
#eval probe 2
#eval probe 3


/-! ## `stitch` for BalanceFloor: an explicit minimum-prefix accumulator. -/

private def step (x : Nat) : Int :=
  if x % 2 == 0 then ((x / 2 : Nat) : Int) else -(((x / 2 + 1 : Nat) : Int))

private def steps (xs : List Nat) : List Int := xs.map step

private def minFrom (start : Int) : List Int → Int
  | [] => start
  | x :: xs => min start (minFrom (start + x) xs)

private theorem minFrom_le_start (start : Int) (l : List Int) : minFrom start l ≤ start := by
  cases l with
  | nil => simp [minFrom]
  | cons x xs => simp [minFrom]; omega

private theorem min_add (a b t : Int) : min (a + t) (b + t) = min a b + t := by
  omega

private theorem minFrom_shift (start t : Int) (l : List Int) :
    minFrom (start + t) l = minFrom start l + t := by
  induction l generalizing start with
  | nil => simp [minFrom]
  | cons x xs ih =>
      simp only [minFrom]
      rw [show (start + t) + x = (start + x) + t by omega]
      rw [ih (start + x)]
      exact min_add _ _ _

private theorem minFrom_append (start : Int) (a b : List Int) :
    minFrom start (a ++ b) = min (minFrom start a) (minFrom (start + a.sum) b) := by
  induction a generalizing start with
  | nil =>
      simp [minFrom]
      have hle := minFrom_le_start start b
      omega
  | cons x xs ih =>
      simp only [List.cons_append, List.sum_cons, minFrom]
      rw [show start + (x + xs.sum) = (start + x) + xs.sum by omega]
      rw [ih (start + x)]
      omega

private theorem scanl_minFrom (start : Int) (l : List Int) :
    (l.scanl (· + ·) start).min? = some (minFrom start l) := by
  induction l generalizing start with
  | nil => simp [minFrom]
  | cons x xs ih =>
      simp only [List.scanl_cons]
      simp [List.min?_cons, ih, minFrom]

private theorem minFrom_eq (start : Int) (l : List Int) :
    minFrom start l = (l.scanl (· + ·) start).min?.getD start := by
  rw [scanl_minFrom]
  simp

private theorem ref_eq (xs : List Nat) :
    agg.reference xs = ((steps xs).sum, minFrom 0 (steps xs)) := by
  simp only [agg]
  rw [← minFrom_eq]
  rfl

private theorem stitch_agg : stitch agg := by
  intro a b
  rw [ref_eq a, ref_eq b, ref_eq (a ++ b)]
  simp only [agg, Prod.mk.injEq]
  constructor
  · simp [steps, List.map_append, List.sum_append]
  · simp only [steps, List.map_append]
    rw [minFrom_append]
    rw [minFrom_shift]
    omega

/-! ## Binary powering of a stitching fold is correct. -/

private theorem flatten_replicate_add (n m : Nat) (xs : List Nat) :
    (List.replicate (n + m) xs).flatten =
      (List.replicate n xs).flatten ++ (List.replicate m xs).flatten := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n + 1 + m = (n + m) + 1 by omega, List.replicate_succ,
        List.replicate_succ, List.flatten_cons, ih, List.flatten_cons]
      exact (List.append_assoc _ _ _).symm

private theorem flatten_replicate_double (n : Nat) (xs : List Nat) :
    (List.replicate (n * 2) xs).flatten =
      (List.replicate n xs).flatten ++ (List.replicate n xs).flatten := by
  rw [← flatten_replicate_add]
  apply congrArg List.flatten
  apply congrArg (fun n => List.replicate n xs)
  omega

private theorem flatten_replicate_rotate (n : Nat) (xs : List Nat) :
    (List.replicate n xs).flatten ++ xs = xs ++ (List.replicate n xs).flatten := by
  induction n with
  | zero => simp
  | succ n ih =>
      simp only [List.replicate_succ, List.flatten_cons]
      calc
        (xs ++ (List.replicate n xs).flatten) ++ xs =
            xs ++ ((List.replicate n xs).flatten ++ xs) := List.append_assoc _ _ _
        _ = xs ++ (xs ++ (List.replicate n xs).flatten) := by rw [ih]

private theorem flatten_replicate_double_succ (n : Nat) (xs : List Nat) :
    ((List.replicate n xs).flatten ++ (List.replicate n xs).flatten) ++ xs =
      (List.replicate (n * 2 + 1) xs).flatten := by
  rw [show n * 2 + 1 = (n + n) + 1 by omega, List.replicate_succ,
    List.flatten_cons, flatten_replicate_add]
  let a := (List.replicate n xs).flatten
  calc
    (a ++ a) ++ xs = a ++ (a ++ xs) := List.append_assoc _ _ _
    _ = a ++ (xs ++ a) := by rw [flatten_replicate_rotate]
    _ = xs ++ (a ++ a) := by
      rw [← List.append_assoc, flatten_replicate_rotate, List.append_assoc]

private theorem repeatPower_of_stitch {σ : Type} (f : Fold σ) (h : stitch f) : repeatPower f := by
  intro xs n
  induction n using Nat.strongRecOn with
  | _ k ih =>
      by_cases hk : k = 0
      · subst k
        simp [repeatState]
      · rw [repeatState.eq_1, dite_eq_right hk]
        have hdiv : k / 2 < k := Nat.div_lt_self (by omega) (by decide)
        have ih2 := ih (k / 2) hdiv
        have hsq : f.join (repeatState f (f.reference xs) (k / 2))
              (repeatState f (f.reference xs) (k / 2)) =
            f.reference ((List.replicate (k / 2) xs).flatten ++
              (List.replicate (k / 2) xs).flatten) := by
          simp only [ih2]
          exact h _ _
        have hdecomp : k / 2 * 2 + k % 2 = k := by
          rw [Nat.mul_comm]
          exact Nat.div_add_mod k 2
        by_cases he : k % 2 = 0
        · simp only [he, beq_self_eq_true, ite_true]
          rw [hsq]
          apply congrArg f.reference
          calc
            (List.replicate (k / 2) xs).flatten ++
                (List.replicate (k / 2) xs).flatten =
                (List.replicate (k / 2 * 2) xs).flatten :=
                  (flatten_replicate_double (k / 2) xs).symm
            _ = (List.replicate k xs).flatten :=
                  congrArg (fun n => (List.replicate n xs).flatten)
                    (by simpa [he] using hdecomp)
        · have ho : k % 2 = 1 := by omega
          simp only [ho, show (1 == 0) = false by decide, Bool.false_eq_true, ite_false]
          rw [hsq, h]
          apply congrArg f.reference
          calc
            ((List.replicate (k / 2) xs).flatten ++
                (List.replicate (k / 2) xs).flatten) ++ xs =
                (List.replicate (k / 2 * 2 + 1) xs).flatten :=
                  flatten_replicate_double_succ (k / 2) xs
            _ = (List.replicate k xs).flatten :=
                  congrArg (fun n => (List.replicate n xs).flatten)
                    (by simpa [ho] using hdecomp)

private theorem runInput_of_stitch {σ : Type} (f : Fold σ) (h : stitch f) : runInput f := by
  intro rs
  have gen : ∀ (l : List (Nat × Nat)) (acc : List Nat),
      List.foldl f.join (f.reference acc)
        (l.map (fun (v, n) => repeatState f (f.reference [v]) n)) =
      f.reference (acc ++ l.flatMap (fun (v, n) => List.replicate n v)) := by
    intro l
    induction l with
    | nil => intro acc; simp
    | cons r rs ih =>
        obtain ⟨v, n⟩ := r
        intro acc
        simp only [List.map_cons, List.foldl, List.flatMap_cons]
        rw [repeatPower_of_stitch f h]
        have hflat : (List.replicate n [v]).flatten = List.replicate n v := by
          induction n with
          | zero => simp
          | succ n ihn => simp [List.replicate_succ, ihn]
        rw [hflat, h, ih]
        simp only [List.append_assoc]
  simpa using gen rs []

/-! ## The goal -/

theorem sound_RunInput : BalanceFloor.RunInput.Goal := by
  exact runInput_of_stitch agg stitch_agg

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BalanceFloor_RunInput
