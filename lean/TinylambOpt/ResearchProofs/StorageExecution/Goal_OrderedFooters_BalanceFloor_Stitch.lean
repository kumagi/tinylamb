import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BalanceFloor_Stitch

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BalanceFloor

private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := gen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs, 2 :: xs, 3 :: xs])

private def probe (n : Nat) : Bool :=
  let ls := gen n
  ls.all (fun a => ls.all (fun b =>
    agg.join (agg.reference a) (agg.reference b) == agg.reference (a ++ b)))

#eval probe 4

private theorem scanl_min?_some (start : Int) (l : List Int) :
    ∃ m, (l.scanl (· + ·) start).min? = some m := by
  cases l with
  | nil => exact ⟨start, by simp⟩
  | cons x xs => simp [List.scanl_cons]

private def minFrom (start : Int) : List Int → Int
  | [] => start
  | x :: xs => min start (minFrom (start + x) xs)

private theorem minFrom_eq (start : Int) (l : List Int) :
    minFrom start l = (l.scanl (· + ·) start).min?.getD start := by
  induction l generalizing start with
  | nil => simp [minFrom]
  | cons x xs ih =>
      obtain ⟨m, hm⟩ := scanl_min?_some (start + x) xs
      have him : minFrom (start + x) xs = m := by
        rw [ih (start + x), hm]
        simp
      simp [minFrom, List.scanl_cons, hm, him]

private theorem minFrom_le (start : Int) (l : List Int) : minFrom start l ≤ start := by
  cases l with
  | nil => simp [minFrom]
  | cons x xs =>
      simp only [minFrom]
      omega

private theorem minFrom_append (start : Int) (l1 l2 : List Int) :
    minFrom start (l1 ++ l2) =
      min (minFrom start l1) (minFrom (start + l1.sum) l2) := by
  induction l1 generalizing start with
  | nil =>
      simp only [List.nil_append, List.sum_nil]
      rw [show start + 0 = start by omega]
      change minFrom start l2 = min start (minFrom start l2)
      have hle := minFrom_le start l2
      omega
  | cons x xs ih =>
      simp only [List.cons_append, List.sum_cons, minFrom]
      rw [ih (start + x)]
      have hsum : start + x + xs.sum = start + (x + xs.sum) := by omega
      rw [hsum, Int.min_assoc]

private theorem minFrom_shift (start t : Int) (l : List Int) :
    minFrom (start + t) l = start + minFrom t l := by
  induction l generalizing start t with
  | nil => simp [minFrom]
  | cons x xs ih =>
      simp only [minFrom]
      rw [ih (start + t) x, ih t x]
      omega

private theorem minFrom_eq_add (s : Int) (l : List Int) :
    minFrom s l = s + minFrom 0 l := by
  simpa using minFrom_shift s 0 l

private def steps (xs : List Nat) : List Int :=
  xs.map (fun x : Nat => if x % 2 == 0 then ((x / 2 : Nat) : Int) else -((x / 2 + 1 : Nat) : Int))

private theorem agg_reference_eq (xs : List Nat) :
    agg.reference xs = ((steps xs).sum, minFrom 0 (steps xs)) := by
  simp [agg, steps, ← minFrom_eq]

private theorem stitch_agg : stitch agg := by
  intro a b
  rw [agg_reference_eq a, agg_reference_eq b, agg_reference_eq (a ++ b)]
  change ((steps a).sum + (steps b).sum,
      min (minFrom 0 (steps a)) ((steps a).sum + minFrom 0 (steps b))) =
    ((steps (a ++ b)).sum, minFrom 0 (steps (a ++ b)))
  rw [show steps (a ++ b) = steps a ++ steps b by simp [steps]]
  rw [List.sum_append, minFrom_append]
  simp
  rw [minFrom_eq_add ((steps a).sum) (steps b)]

theorem sound_Stitch : Stitch.Goal := stitch_agg

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BalanceFloor_Stitch
