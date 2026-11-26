import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_MonotoneBreaks_RepeatPower

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.MonotoneBreaks

/-! ## Small-input truth checks -/

private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := gen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs, 2 :: xs])

private def probe (n k : Nat) : Bool :=
  (gen n).all (fun xs =>
    repeatState agg (agg.reference xs) k == agg.reference (List.replicate k xs).flatten)

#eval probe 3 5
#eval probe 4 7

/-! ## `agg` stitches arbitrary lists -/

private def cdesc (xs : List Nat) : Nat := (edges xs).countP (fun (x, y) => y < x)

private theorem cdesc_cons (x : Nat) : ∀ xs : List Nat,
    cdesc (x :: xs) = cdesc xs + (match xs with | [] => 0 | y :: _ => (decide (y < x)).toNat)
  | [] => by simp [cdesc, edges]
  | y :: ys => by
      simp [cdesc, edges, List.countP_cons, List.zip_cons_cons]
      by_cases h : y < x <;> simp [h]

private theorem cdesc_append_cons (x : Nat) : ∀ (xs : List Nat) (y : Nat) (ys : List Nat),
    cdesc ((x :: xs) ++ (y :: ys)) = cdesc (x :: xs) + cdesc (y :: ys) +
      (decide (y < (x :: xs).getLast?.getD x)).toNat
  | [], y, ys => by
      change cdesc (x :: y :: ys) = cdesc [x] + cdesc (y :: ys) + (decide (y < x)).toNat
      rw [cdesc_cons x (y :: ys)]
      simp [cdesc, edges]
  | z :: zs, y, ys => by
      have hlast : (z :: zs).getLast?.getD z = (x :: z :: zs).getLast?.getD x := by
        simp [List.getLast?_cons]
      rw [show (x :: z :: zs) ++ (y :: ys) = x :: ((z :: zs) ++ (y :: ys)) by rfl]
      rw [cdesc_cons x ((z :: zs) ++ (y :: ys))]
      rw [cdesc_cons x (z :: zs)]
      rw [cdesc_append_cons z zs y ys, hlast]
      simp
      omega

private theorem stitch_agg : stitch agg := by
  intro a b
  cases a with
  | nil => simp [agg, optionalJoin]
  | cons x xs =>
      cases b with
      | nil => simp [agg, optionalJoin]
      | cons y ys =>
          have hdesc := cdesc_append_cons x xs y ys
          have hlast : (x :: (xs ++ y :: ys)).getLast?.getD x = (y :: ys).getLast?.getD y := by
            simp [List.getLast?_cons, List.getLast?_append]
          simp [agg, optionalJoin]
          constructor
          · simpa using hlast.symm
          · simpa [cdesc] using hdesc.symm

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

theorem sound_RepeatPower : MonotoneBreaks.RepeatPower.Goal :=
  repeatPower_of_stitch agg stitch_agg

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_MonotoneBreaks_RepeatPower
