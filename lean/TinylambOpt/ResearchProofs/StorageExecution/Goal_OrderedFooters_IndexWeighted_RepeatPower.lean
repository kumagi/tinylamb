import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_IndexWeighted_RepeatPower

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.IndexWeighted

/-! ## Small-input truth checks -/

private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := gen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs, 2 :: xs])

private def probe (n k : Nat) : Bool :=
  (gen n).all (fun xs =>
    repeatState agg (agg.reference xs) k == agg.reference (List.replicate k xs).flatten)

#eval probe 3 5
#eval probe 4 7
#eval probe 4 11

/-! ## `agg.reference` in a structurally recursive form -/

/-- Weighted index sum, with an optional starting ordinal. -/
private def mom (xs : List Nat) (k : Nat) : Nat :=
  ((xs.zipIdx k).map (fun p => p.2 * p.1)).sum

private theorem mom_shift (xs : List Nat) (k : Nat) : mom xs k = mom xs 0 + k * xs.sum := by
  induction xs generalizing k with
  | nil => simp [mom]
  | cons x xs ih =>
      change k * x + mom xs (k + 1) = (0 * x + mom xs 1) + k * (x + xs.sum)
      rw [ih (k + 1), ih 1]
      rw [Nat.succ_mul, Nat.one_mul, Nat.mul_add]
      omega

private theorem mom_append (a b : List Nat) :
    mom (a ++ b) 0 = mom a 0 + mom b 0 + a.length * b.sum := by
  change (((a ++ b).zipIdx 0).map (fun p => p.2 * p.1)).sum =
    ((a.zipIdx 0).map (fun p => p.2 * p.1)).sum +
      ((b.zipIdx 0).map (fun p => p.2 * p.1)).sum + a.length * b.sum
  rw [List.zipIdx_append]
  simp only [List.map_append, List.sum_append]
  change mom a 0 + mom b (0 + a.length) = mom a 0 + mom b 0 + a.length * b.sum
  rw [show 0 + a.length = a.length by omega]
  rw [mom_shift b a.length]
  omega

/-! ## `agg` stitches arbitrary lists -/

private theorem stitch_agg : stitch agg := by
  intro a b
  change (a.length + b.length, a.sum + b.sum, mom a 0 + mom b 0 + a.length * b.sum) =
    ((a ++ b).length, (a ++ b).sum, mom (a ++ b) 0)
  simp [List.length_append, List.sum_append, mom_append]

/-! ## Generic binary-power derivation from a stitching fold -/

private theorem repeatPower_of_stitch {σ : Type} (f : Fold σ) (h : stitch f) : repeatPower f := by
  intro xs n
  induction n using Nat.strongRecOn with
  | _ k ih =>
    unfold repeatState
    by_cases hk : k = 0
    · simp [hk]
    · simp only [hk, dite_false]
      have hdiv : k / 2 < k := Nat.div_lt_self (by omega) (by decide)
      have ih2 := ih (k / 2) hdiv
      let a := repeatState f (f.reference xs) (k / 2)
      have ha' : a = f.reference (List.replicate (k / 2) xs).flatten := by simpa [a] using ih2
      have hjoin : f.join a a = f.reference ((List.replicate (k / 2) xs).flatten ++ (List.replicate (k / 2) xs).flatten) := by
        rw [ha']; exact h _ _
      have hrep2 : (List.replicate (k / 2) xs).flatten ++ (List.replicate (k / 2) xs).flatten =
          (List.replicate (k / 2 * 2) xs).flatten := by
        rw [← List.flatten_append]
        congr 1
        rw [show k / 2 * 2 = k / 2 + k / 2 by omega, ← List.replicate_append_replicate]
      by_cases he : k % 2 = 0
      · have hk2 : k / 2 * 2 = k := by omega
        have hcond : (k % 2 == 0) = true := by simp [he]
        simp only [hcond, ite_true]
        change f.join a a = f.reference (List.replicate k xs).flatten
        rw [hjoin, hrep2, hk2]
      · have hodd : k % 2 = 1 := by omega
        have hk1 : k = k / 2 * 2 + 1 := by omega
        have hcond : (k % 2 == 0) = false := by simp [hodd]
        simp only [hcond]
        change f.join (f.join a a) (f.reference xs) = f.reference (List.replicate k xs).flatten
        rw [hjoin, hrep2, h]
        congr 1
        rw [hk1, show k / 2 * 2 + 1 = (k / 2 + k / 2) + 1 by omega, ← List.replicate_append_replicate,
            List.flatten_append, List.replicate_one, List.flatten_singleton]
        have heq : (k / 2 + k / 2 + 1) / 2 * 2 = k / 2 + k / 2 := by omega
        simp [heq]

/-! ## Goal -/

theorem sound_RepeatPower : IndexWeighted.RepeatPower.Goal :=
  repeatPower_of_stitch agg stitch_agg

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_IndexWeighted_RepeatPower
