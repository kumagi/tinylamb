import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `PairSubsequence.RunInput` — Consume the lengths on a run-encoded column directly.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_PairSubsequence_RunInput

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.PairSubsequence

/-! ## Small-input truth checks -/

private def lhs (rs : List (Nat × Nat)) :=
  (rs.map (fun (v, n) => repeatState agg (agg.reference [v]) n)).foldl agg.join (agg.reference [])
private def rhs (rs : List (Nat × Nat)) :=
  agg.reference (rs.flatMap (fun (v, n) => List.replicate n v))

#eval lhs [(1, 2), (2, 3), (1, 1), (0, 4), (2, 2)] ==
  rhs [(1, 2), (2, 3), (1, 1), (0, 4), (2, 2)]
#eval lhs [(2, 1), (1, 3), (2, 2)] == rhs [(2, 1), (1, 3), (2, 2)]
#eval lhs [(1, 0), (2, 0)] == rhs [(1, 0), (2, 0)]

/-! ## The pair-count fold stitches -/

private def cross (xs : List Nat) : Nat :=
  (xs.zipIdx.map (fun (v, i) => if v == 1 then (xs.drop (i + 1)).count 2 else 0)).sum

private theorem zipIdx_shift (xs : List Nat) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, i) => (v, i + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons x xs ih =>
      simp only [List.zipIdx_cons, List.map_cons]
      congr 1
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (n + 1)

private theorem cross_nil : cross [] = 0 := by simp [cross]

private theorem cross_cons (x : Nat) (xs : List Nat) :
    cross (x :: xs) = (if x == 1 then xs.count 2 else 0) + cross xs := by
  unfold cross
  rw [List.zipIdx_cons]
  simp only [List.map_cons, List.sum_cons, Nat.zero_add]
  rw [show xs.zipIdx 1 = (xs.zipIdx).map (fun (v, i) => (v, i + 1)) by
    simpa using zipIdx_shift xs 0 1]
  simp [List.map_map, Function.comp_def]

private theorem cross_append (a b : List Nat) :
    cross (a ++ b) = cross a + cross b + a.count 1 * b.count 2 := by
  induction a with
  | nil => simp [cross_nil]
  | cons x xs ih =>
      simp only [List.cons_append, cross_cons, List.count_append, ih, List.count_cons]
      by_cases hx : x == 1
      · simp [hx, Nat.add_mul, Nat.add_assoc, Nat.add_left_comm, Nat.add_comm]
      · simp [hx, Nat.add_comm]

private theorem stitch_agg : stitch agg := by
  intro a b
  change (a.count 1 + b.count 1, a.count 2 + b.count 2,
    cross a + cross b + a.count 1 * b.count 2) =
    ((a ++ b).count 1, (a ++ b).count 2, cross (a ++ b))
  rw [List.count_append, List.count_append, cross_append]

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

/-! ## General run-input derivation -/

private theorem replicate_singleton_flatten (v n : Nat) :
    (List.replicate n [v]).flatten = List.replicate n v := by
  induction n with
  | zero => simp
  | succ n => simp

private theorem foldl_run_states {σ : Type} (f : Fold σ) (h : stitch f)
    (l : List Nat) (rs : List (Nat × Nat)) :
    (rs.map (fun (v, n) => f.reference (List.replicate n v))).foldl f.join (f.reference l) =
      f.reference (l ++ rs.flatMap (fun (v, n) => List.replicate n v)) := by
  induction rs generalizing l with
  | nil => simp
  | cons p rs ih =>
      rcases p with ⟨v, n⟩
      simp only [List.map_cons, List.foldl_cons, List.flatMap_cons]
      rw [h l (List.replicate n v)]
      rw [ih (l ++ List.replicate n v)]
      simp [List.append_assoc]

private theorem runInput_of_stitch_repeatPower {σ : Type} (f : Fold σ)
    (hj : stitch f) (hp : repeatPower f) : runInput f := by
  intro rs
  have hmap : rs.map (fun (v, n) => repeatState f (f.reference [v]) n) =
      rs.map (fun (v, n) => f.reference (List.replicate n v)) := by
    induction rs with
    | nil => simp
    | cons p rs ih =>
        rcases p with ⟨v, n⟩
        simp only [List.map_cons]
        rw [hp [v] n, replicate_singleton_flatten]
        rw [ih]
  rw [hmap]
  simpa using foldl_run_states f hj [] rs

theorem sound_RunInput : PairSubsequence.RunInput.Goal := by
  exact runInput_of_stitch_repeatPower agg stitch_agg (repeatPower_of_stitch agg stitch_agg)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_PairSubsequence_RunInput
