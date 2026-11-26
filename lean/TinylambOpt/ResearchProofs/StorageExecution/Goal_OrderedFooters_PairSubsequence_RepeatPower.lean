import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `PairSubsequence.RepeatPower` — Evaluate grammar repetition by binary powers of the
  summary.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_PairSubsequence_RepeatPower

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.PairSubsequence

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

private def pairs (xs : List Nat) : Nat :=
  (xs.zipIdx.map (fun (v, i) => if v == 1 then (xs.drop (i + 1)).count 2 else 0)).sum

private theorem zipIdx_shift (xs : List Nat) : ∀ k, xs.zipIdx k = (xs.zipIdx).map (fun p => (p.1, p.2 + k)) := by
  induction xs with
  | nil => intro k; simp
  | cons x xs ih =>
      intro k
      simp only [List.zipIdx_cons]
      rw [ih (k + 1), ih 1]
      simp [Nat.add_comm, Nat.add_left_comm]

private theorem pairs_cons (x : Nat) (xs : List Nat) :
    pairs (x :: xs) = (if x == 1 then xs.count 2 else 0) + pairs xs := by
  unfold pairs
  rw [List.zipIdx_cons]
  rw [zipIdx_shift xs 1]
  simp [List.drop_succ_cons, Function.comp_def]

private theorem pairs_append : ∀ (a b : List Nat),
    pairs (a ++ b) = pairs a + pairs b + a.count 1 * b.count 2 := by
  intro a
  induction a with
  | nil => intro b; simp [pairs]
  | cons x xs ih =>
      intro b
      rw [List.cons_append, pairs_cons, ih, pairs_cons]
      by_cases hx : x == 1
      · simp [hx, List.count_cons, Nat.succ_mul]
        omega
      · simp [hx, List.count_cons]

/-! ## `agg` stitches arbitrary lists -/

private theorem stitch_agg : stitch agg := by
  intro a b
  change (a.count 1 + b.count 1, a.count 2 + b.count 2, pairs a + pairs b + a.count 1 * b.count 2) =
    ((a ++ b).count 1, (a ++ b).count 2, pairs (a ++ b))
  rw [pairs_append]
  simp [List.count_append]

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

theorem sound_RepeatPower : PairSubsequence.RepeatPower.Goal :=
  repeatPower_of_stitch agg stitch_agg

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_PairSubsequence_RepeatPower
