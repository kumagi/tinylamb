import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `FirstLastChange.RepeatPower` — Evaluate grammar repetition by binary powers of the
  summary.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_FirstLastChange_RepeatPower

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.FirstLastChange

/-! ## Small-input truth checks -/

private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := gen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs, 2 :: xs])

private def probe (n k : Nat) : Bool :=
  (gen n).all (fun xs =>
    repeatState agg (agg.reference xs) k == agg.reference (List.replicate k xs).flatten)

#eval probe 3 5
#eval probe 4 7
#eval probe 5 9

/-! ## `agg` stitches arbitrary lists -/

private theorem getLast_cons_eq_getD (x : Nat) (xs : List Nat) :
    (x :: xs).getLast (by simp) = xs.getLast?.getD x := by
  have h := List.getLast?_eq_some_getLast (l := x :: xs) (by simp)
  rw [List.getLast?_cons] at h
  simpa using h.symm

private theorem getLast?_append_right (a b : List Nat) (hb : b ≠ []) :
    (a ++ b).getLast? = b.getLast? := by
  rw [List.getLast?_append]
  cases b with
  | nil => contradiction
  | cons y ys => simp [List.getLast?_cons]

private theorem edges_append (a b : List Nat) (ha : a ≠ []) (hb : b ≠ []) :
    edges (a ++ b) = edges a ++ ((a.getLast ha, b.head hb) :: edges b) := by
  induction a with
  | nil => contradiction
  | cons x xs ih =>
      cases xs with
      | nil =>
          cases b with
          | nil => contradiction
          | cons y ys => simp [edges]
      | cons y ys =>
          have ha' : y :: ys ≠ [] := by simp
          have hih := ih ha'
          cases b with
          | nil => contradiction
          | cons z zs =>
              simp [edges] at hih ⊢
              simpa [List.getLast_cons] using hih

private theorem stitch_agg : stitch agg := by
  intro a b
  cases a with
  | nil => simp [agg, optionalJoin]
  | cons x xs =>
      cases b with
      | nil => simp [agg, optionalJoin]
      | cons y ys =>
          simp only [agg, List.head?_cons, Option.map_some, optionalJoin]
          have hlast : (x :: xs ++ y :: ys).getLast? = (y :: ys).getLast? :=
            getLast?_append_right (x :: xs) (y :: ys) (by simp)
          have hlastB : (y :: ys).getLast? = some ((y :: ys).getLast (by simp)) :=
            List.getLast?_eq_some_getLast (by simp)
          have hedges := edges_append (x :: xs) (y :: ys) (by simp) (by simp)
          simp only [List.head_cons, getLast_cons_eq_getD] at hedges
          simp only [hlast, hlastB, List.getLast?_cons, Option.getD_some]
          simp only [hedges, List.countP_append, List.countP_cons]
          by_cases h : xs.getLast?.getD x = y
          · simp [h]
            omega
          · simp [h]
            omega

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

theorem sound_RepeatPower : FirstLastChange.RepeatPower.Goal :=
  repeatPower_of_stitch agg stitch_agg

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_FirstLastChange_RepeatPower
