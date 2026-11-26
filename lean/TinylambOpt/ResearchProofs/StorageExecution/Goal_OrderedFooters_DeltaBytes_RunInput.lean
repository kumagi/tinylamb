import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `DeltaBytes.RunInput` — Consume the lengths on a run-encoded column directly.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_DeltaBytes_RunInput

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.DeltaBytes

/-! ## Small-input truth checks -/

private def genRuns : Nat → List (List (Nat × Nat))
  | 0 => [[]]
  | n + 1 =>
      let p := genRuns n
      p ++ p.flatMap (fun rs =>
        [(0, 0) :: rs, (0, 1) :: rs, (1, 2) :: rs, (2, 1) :: rs, (7, 3) :: rs, (3, 4) :: rs])

private def probe (n : Nat) : Bool :=
  (genRuns n).all (fun rs =>
    (rs.map (fun (v, k) => repeatState agg (agg.reference [v]) k)).foldl agg.join
        (agg.reference []) ==
      agg.reference (rs.flatMap (fun (v, k) => List.replicate k v)))

#eval probe 2
#eval probe 3

/-! ## Binary powering of a fold is correct whenever `stitch` holds. -/

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
      have hjoin : f.join a a =
          f.reference ((List.replicate (k / 2) xs).flatten ++
                       (List.replicate (k / 2) xs).flatten) := by
        rw [ha']
        exact h _ _
      have hrep2 : (List.replicate (k / 2) xs).flatten ++
          (List.replicate (k / 2) xs).flatten =
          (List.replicate (k / 2 * 2) xs).flatten := by
        rw [← List.flatten_append]
        congr 1
        rw [show k / 2 * 2 = k / 2 + k / 2 by omega, ← List.replicate_append_replicate]
      by_cases he : k % 2 = 0
      · have hk2 : k / 2 * 2 = k := by omega
        have hcond : (k % 2 == 0) = true := by simp [he]
        simp only [hcond]
        change f.join a a = f.reference (List.replicate k xs).flatten
        rw [hjoin, hrep2, hk2]
      · have hodd : k % 2 = 1 := by omega
        have hk1 : k = k / 2 * 2 + 1 := by omega
        have hcond : (k % 2 == 0) = false := by simp [hodd]
        simp only [hcond]
        change f.join (f.join a a) (f.reference xs) =
          f.reference (List.replicate k xs).flatten
        rw [hjoin, hrep2, h]
        congr 1
        rw [hk1, show k / 2 * 2 + 1 = (k / 2 + k / 2) + 1 by omega,
            ← List.replicate_append_replicate, List.flatten_append,
            List.replicate_one, List.flatten_singleton]
        have heq : (k / 2 + k / 2 + 1) / 2 * 2 = k / 2 + k / 2 := by omega
        simp [heq]

/-! ## Supporting list facts for the predecessor-coded byte summary. -/

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
          simp only [hedges, List.map_append, List.sum_append]
          simp
          omega

/-! ## `stitch` implies that a run-encoded column can be consumed directly. -/

private theorem runInput_of_stitch {σ : Type} (f : Fold σ) (h : stitch f) :
    runInput f := by
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
      rw [h acc (List.replicate n [v]).flatten]
      have hflat : (List.replicate n [v]).flatten = List.replicate n v := by
        induction n with
        | zero => simp
        | succ n ih2 => simp [List.replicate_succ, ih2]
      rw [hflat, ih (acc ++ List.replicate n v)]
      simp [List.append_assoc]
  simpa using gen rs []

/-! ## The goal -/

theorem sound_RunInput : DeltaBytes.RunInput.Goal := by
  exact runInput_of_stitch agg stitch_agg

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_DeltaBytes_RunInput
