import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_TotalVariation_DropDuplicates

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

local notation "tv" => TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.TotalVariation.measure

/-! Brute-force sanity check: all series of length ≤ 4 over [-3, 3] with
    replication counts 1..4. `false` means no counterexample was found. -/
#eval do
  let vals : List Int := [-3, -2, -1, 0, 1, 2, 3]
  let rec lists : Nat → List (List Int)
    | 0 => [[]]
    | n + 1 => lists n ++ vals.flatMap (fun a => (lists n).map (fun xs => a :: xs))
  let ls : List (List Int) := lists 4
  let ns : List Nat := [1, 2, 3, 4]
  pure ((ls.flatMap (fun (x : List Int) => ns.map (fun n =>
    decide (tv (x.flatMap (repeated n)) = tv x)))).contains false)

private theorem tv_cons₂ (a b : Int) (xs : Series) :
    tv (a :: b :: xs) = distance a b + tv (b :: xs) := by
  simp [TotalVariation.measure, delta, adj, distance]

/-! Repeating the head value before a suffix contributes only zero jumps. -/
private theorem tv_replicate_head (a : Int) (n : Nat) (ys : Series) :
    tv (a :: (List.replicate n a ++ ys)) = tv (a :: ys) := by
  induction n with
  | zero => simp
  | succ n ih =>
      change tv (a :: a :: (List.replicate n a ++ ys)) = tv (a :: ys)
      rw [tv_cons₂ a a (List.replicate n a ++ ys)]
      simp [distance]
      exact ih

/-! Replicating every element preserves the first jump as well. -/
private theorem tv_cons_flatMap (n : Nat) (hn : 0 < n) (x : Series) (a : Int) :
    tv (a :: x.flatMap (repeated n)) = tv (a :: x) := by
  obtain ⟨m, rfl⟩ := Nat.exists_eq_succ_of_ne_zero (Nat.ne_of_gt hn)
  induction x generalizing a with
  | nil => simp
  | cons b ys ih =>
      simp only [List.flatMap_cons]
      change tv (a :: b :: (List.replicate m b ++ ys.flatMap (repeated (m + 1)))) =
        tv (a :: b :: ys)
      rw [tv_cons₂ a b (List.replicate m b ++ ys.flatMap (repeated (m + 1)))]
      rw [tv_replicate_head b m (ys.flatMap (repeated (m + 1)))]
      rw [ih b]
      rw [tv_cons₂ a b ys]

theorem sound_TotalVariation_DropDuplicates : TotalVariation.DropDuplicates.Goal := by
  intro x n hn
  obtain ⟨m, rfl⟩ := Nat.exists_eq_succ_of_ne_zero (Nat.ne_of_gt hn)
  cases x with
  | nil => simp [TotalVariation.measure, delta, adj]
  | cons a xs =>
      simp only [List.flatMap_cons]
      change tv (a :: (List.replicate m a ++ xs.flatMap (repeated (m + 1)))) = tv (a :: xs)
      rw [tv_replicate_head a m (xs.flatMap (repeated (m + 1)))]
      exact tv_cons_flatMap (m + 1) (Nat.succ_pos m) xs a

end TinylambOpt.ResearchProofs.Atlas.Goal_TotalVariation_DropDuplicates
