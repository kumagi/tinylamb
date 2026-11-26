import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `IndexWeighted.RunInput` — Consume the lengths on a run-encoded column directly.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_IndexWeighted_RunInput

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.IndexWeighted

/-! ## Generic consequences of the stitch law -/

private theorem repeatPower_of_stitch {σ : Type} (f : Fold σ) (h : stitch f) :
    repeatPower f := by
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

/-! ## The stitch law for `IndexWeighted.agg` -/

/-- The index-weighted moment of a list: sum of value times physical ordinal. -/
private def wsum (xs : List Nat) : Nat :=
  (xs.zipIdx.map (fun (v, i) => i * v)).sum

/-- The same moment, but starting physical ordinals at `k`. -/
private def wsumFrom (k : Nat) (xs : List Nat) : Nat :=
  ((xs.zipIdx k).map (fun (v, i) => i * v)).sum

private theorem wsumFrom_append (k : Nat) (a b : List Nat) :
    wsumFrom k (a ++ b) = wsumFrom k a + wsumFrom (k + a.length) b := by
  simp [wsumFrom, List.zipIdx_append]

/-- Increasing the starting ordinal by `n` adds `n` times the total sum. -/
private theorem wsumFrom_add (k n : Nat) (b : List Nat) :
    wsumFrom (k + n) b = wsumFrom k b + n * b.sum := by
  induction b generalizing k with
  | nil => simp [wsumFrom]
  | cons x xs ih =>
      simp only [wsumFrom, List.zipIdx_cons, List.map_cons, List.sum_cons]
      have hih := ih (k + 1)
      have harg : k + 1 + n = k + n + 1 := by omega
      rw [harg] at hih
      unfold wsumFrom at hih
      rw [hih, Nat.add_mul, Nat.mul_add]
      ac_rfl

/-- Appending shifts every index of the right list by the left list's length. -/
private theorem wsum_append (a b : List Nat) :
    wsum (a ++ b) = wsum a + wsum b + a.length * b.sum := by
  unfold wsum
  change wsumFrom 0 (a ++ b) = wsumFrom 0 a + wsumFrom 0 b + a.length * b.sum
  rw [wsumFrom_append]
  rw [Nat.zero_add]
  have hadd := wsumFrom_add 0 a.length b
  rw [Nat.zero_add] at hadd
  rw [hadd]
  omega

private theorem stitch_agg : stitch agg := by
  intro a b
  simp only [agg, List.length_append, List.sum_append]
  refine Prod.ext rfl (Prod.ext rfl ?_)
  exact (wsum_append a b).symm

/-! ## The goal -/

theorem sound_RunInput : IndexWeighted.RunInput.Goal := by
  exact runInput_of_stitch agg stitch_agg

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_IndexWeighted_RunInput
