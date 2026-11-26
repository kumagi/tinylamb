import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `FirstLastChange.RunInput` — Consume the lengths on a run-encoded column directly.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_FirstLastChange_RunInput

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.FirstLastChange

/-! ## Small-input truth checks -/

private def genRuns : Nat → List (List (Nat × Nat))
  | 0 => [[]]
  | n + 1 =>
      let p := genRuns n
      p ++ p.flatMap (fun rs => [(0, 0) :: rs, (1, 0) :: rs, (2, 1) :: rs, (2, 2) :: rs, (3, 5) :: rs])

private def probe (n : Nat) : Bool :=
  (genRuns n).all (fun rs =>
    (rs.map (fun (v, k) => repeatState agg (agg.reference [v]) k)).foldl agg.join (agg.reference []) ==
      agg.reference (rs.flatMap (fun (v, k) => List.replicate k v)))

#eval probe 3
#eval probe 4

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

/-! ## Generic run-length input consumption -/

private theorem runInput_of_stitch_repeatPower {σ : Type} (f : Fold σ)
    (hs : stitch f) (hr : repeatPower f) : runInput f := by
  intro rs
  have hgen : ∀ (rs : List (Nat × Nat)) (acc : List Nat),
      (rs.map (fun (v, n) => repeatState f (f.reference [v]) n)).foldl f.join (f.reference acc) =
        f.reference (acc ++ rs.flatMap (fun (v, n) => List.replicate n v)) := by
    intro rs
    induction rs with
    | nil =>
        intro acc
        simp
    | cons p rs ih =>
        intro acc
        rcases p with ⟨v, n⟩
        simp only [List.map_cons, List.flatMap_cons, List.foldl_cons]
        have hrep : repeatState f (f.reference [v]) n = f.reference (List.replicate n v) := by
          have h := hr [v] n
          simpa using h
        rw [hrep]
        rw [hs]
        rw [ih]
        simp [List.append_assoc]
  simpa using hgen rs []

/-! ## Goal -/

theorem sound_RunInput : FirstLastChange.RunInput.Goal :=
  runInput_of_stitch_repeatPower agg stitch_agg (repeatPower_of_stitch agg stitch_agg)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_FirstLastChange_RunInput
