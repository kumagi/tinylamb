import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `BalanceFloor.RepeatPower` — Evaluate grammar repetition by binary powers of the
  summary.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BalanceFloor_RepeatPower

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BalanceFloor

/-! Generic consequences of a stitching fold (binary powering of a summary). -/

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

/-! The signed-balance summary is a homomorphism for list concatenation. -/

private abbrev step (x : Nat) : Int :=
  if x % 2 == 0 then ((x / 2 : Nat) : Int) else -(((x / 2 + 1 : Nat) : Int))
private def steps (xs : List Nat) : List Int := xs.map step

private theorem scanl_append (a b : List Int) (init : Int) :
    (a ++ b).scanl (· + ·) init = a.scanl (· + ·) init ++ (b.scanl (· + ·) (init + a.sum)).tail := by
  induction a generalizing init with
  | nil => cases b <;> simp [List.scanl_nil]
  | cons x xs ih =>
      rw [List.cons_append, List.scanl_cons, List.scanl_cons, List.sum_cons]
      rw [ih (init + x)]
      have hinit : init + x + xs.sum = init + (x + xs.sum) := by omega
      rw [hinit]
      rfl

private theorem scanl_shift (b : List Int) (s : Int) :
    b.scanl (· + ·) s = (b.scanl (· + ·) 0).map (· + s) := by
  induction b generalizing s with
  | nil => simp [List.scanl_nil]
  | cons x xs ih =>
      simp only [List.scanl_cons, List.map_cons]
      have h0x : (0 : Int) + x = x := by omega
      have h0s : (0 : Int) + s = s := by omega
      rw [h0x, h0s]
      congr 1
      rw [ih (s + x), ih x, List.map_map]
      congr 1
      ext y
      simp only [Function.comp_apply]
      omega

private theorem foldl_min_map_add (l : List Int) (s init : Int) :
    (l.map (· + s)).foldl min (init + s) = l.foldl min init + s := by
  induction l generalizing init with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.foldl_cons]
      have h : min (init + s) (x + s) = min init x + s := by omega
      rw [h, ih (min init x)]

private theorem scanl_ne_nil (f : Int → Int → Int) (init : Int) (l : List Int) :
    l.scanl f init ≠ [] := by
  cases l <;> simp [List.scanl_nil, List.scanl_cons]

private theorem scanl_min_getD (l : List Int) :
    (l.scanl (· + ·) 0).min?.getD 0 = (l.scanl (· + ·) 0).foldl min 0 := by
  cases l with
  | nil => simp [List.scanl_nil, List.min?]
  | cons x xs =>
      rw [List.scanl_cons]
      simp [List.min?]

private theorem foldl_min_le_init (l : List Int) (a : Int) : l.foldl min a ≤ a := by
  induction l generalizing a with
  | nil => simp
  | cons y ys ih =>
      simp only [List.foldl_cons]
      have h := ih (min a y)
      omega

private theorem foldl_min_le_mem (l : List Int) (a x : Int) (hx : x ∈ l) :
    l.foldl min a ≤ x := by
  induction l generalizing a x with
  | nil => cases hx
  | cons y ys ih =>
      simp only [List.foldl_cons, List.mem_cons] at hx ⊢
      rcases hx with hxy | hx
      · subst x
        have h1 := foldl_min_le_init ys (min a y)
        omega
      · have h1 := ih (min a y) x hx
        omega

private theorem sum_mem_scanl (l : List Int) (init : Int) :
    init + l.sum ∈ l.scanl (· + ·) init := by
  induction l generalizing init with
  | nil => simp [List.scanl_nil]
  | cons x xs ih =>
      rw [List.scanl_cons, List.sum_cons]
      simp only [List.mem_cons]
      right
      have h := ih (init + x)
      have heq : init + (x + xs.sum) = init + x + xs.sum := by omega
      rw [heq]
      exact h

private theorem scanl_foldl_min_le_sum (l : List Int) :
    (l.scanl (· + ·) 0).foldl min 0 ≤ l.sum := by
  apply foldl_min_le_mem
  simpa using sum_mem_scanl l 0

private theorem scanl_append_min (l : List Int) (t : List Int) :
    ((l.scanl (· + ·) 0) ++ t).min?.getD 0 =
      t.foldl min ((l.scanl (· + ·) 0).foldl min 0) := by
  cases l with
  | nil => simp [List.scanl_nil, List.min?]
  | cons x xs =>
      rw [List.scanl_cons]
      simp only [List.cons_append]
      simp [List.min?, List.foldl_append]

private theorem stitch_agg : stitch agg := by
  intro a b
  simp only [agg]
  apply Prod.ext
  · simp [List.map_append, List.sum_append]
  · simp only [List.map_append]
    rw [scanl_append]
    simp only [Int.zero_add]
    rw [scanl_shift (b.map step) ((a.map step).sum)]
    rw [scanl_append_min]
    cases hb : (List.scanl (· + ·) 0 (b.map step)) with
    | nil => exact (scanl_ne_nil (· + ·) 0 (b.map step) hb).elim
    | cons b0 bs =>
        have hb0 : b0 = 0 := by
          cases hmap : b.map step with
          | nil =>
              rw [hmap] at hb
              simp [List.scanl_nil] at hb
              omega
          | cons y ys =>
              rw [hmap, List.scanl_cons] at hb
              exact (List.cons.inj hb).1.symm
        subst hb0
        rw [show (0 :: bs).min?.getD 0 = bs.foldl min 0 by simp [List.min?]]
        rw [scanl_min_getD (a.map step)]
        simp only [List.map_cons, List.tail_cons]
        have hbs :
            (bs.map (· + (a.map step).sum)).foldl min
                  (min (((a.map step).scanl (· + ·) 0).foldl min 0) ((a.map step).sum)) =
              min (((a.map step).scanl (· + ·) 0).foldl min 0)
                  ((bs.map (· + (a.map step).sum)).foldl min ((a.map step).sum)) := by
          have gen : ∀ (l : List Int) (x y : Int), l.foldl min (min x y) = min x (l.foldl min y) := by
            intro l
            induction l with
            | nil => intro x y; simp
            | cons z zs ih =>
                intro x y
                simp only [List.foldl_cons]
                rw [show min (min x y) z = min x (min y z) by omega]
                exact ih x (min y z)
          exact gen _ _ _
        have hleA : (((a.map step).scanl (· + ·) 0).foldl min 0) ≤ (a.map step).sum := by
          simpa using scanl_foldl_min_le_sum (a.map step)
        have hminA : min (((a.map step).scanl (· + ·) 0).foldl min 0) ((a.map step).sum) =
            ((a.map step).scanl (· + ·) 0).foldl min 0 := by omega
        rw [hminA] at hbs
        rw [hbs]
        congr 1
        have hf : (bs.map (· + (a.map step).sum)).foldl min ((a.map step).sum) =
            bs.foldl min 0 + (a.map step).sum := by
          simpa using foldl_min_map_add bs ((a.map step).sum) 0
        rw [hf]
        change (a.map step).sum + bs.foldl min 0 = bs.foldl min 0 + (a.map step).sum
        omega

theorem sound_RepeatPower : BalanceFloor.RepeatPower.Goal :=
  repeatPower_of_stitch agg stitch_agg

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BalanceFloor_RepeatPower
