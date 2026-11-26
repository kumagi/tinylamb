import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_MonotoneBreaks_RunInput

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.MonotoneBreaks

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

/-! ## `agg` stitches arbitrary lists -/

private theorem getLast?_getD_cons_cons (x z : Nat) (zs : List Nat) :
    (x :: z :: zs).getLast?.getD x = (z :: zs).getLast?.getD z := by
  cases zs with
  | nil => simp
  | cons w ws => simp [List.getLast?]

private theorem getLast?_getD_append_right (a b : List Nat) (hb : b ≠ [])
    (d₁ d₂ : Nat) : (a ++ b).getLast?.getD d₁ = b.getLast?.getD d₂ := by
  rw [List.getLast?_append]
  cases b with
  | nil => contradiction
  | cons y ys => simp [List.getLast?]

private theorem edges_append_cons (x : Nat) (xs : List Nat) (y : Nat) (ys : List Nat) :
    edges ((x :: xs) ++ (y :: ys)) =
      edges (x :: xs) ++ (((x :: xs).getLast?.getD x, y) :: edges (y :: ys)) := by
  induction xs generalizing x with
  | nil => simp [edges]
  | cons z zs ih =>
      simp only [List.cons_append] at ih ⊢
      rw [show edges (x :: z :: (zs ++ y :: ys)) =
          (x, z) :: edges (z :: (zs ++ y :: ys)) by simp [edges]]
      rw [show edges (x :: z :: zs) = (x, z) :: edges (z :: zs) by simp [edges]]
      rw [ih z]
      rw [getLast?_getD_cons_cons]
      rfl

private theorem stitch_agg : stitch agg := by
  intro a b
  cases a with
  | nil => simp [agg, optionalJoin]
  | cons x xs =>
      cases b with
      | nil => simp [agg, optionalJoin]
      | cons y ys =>
          simp only [agg, List.head?_cons, Option.map_some, optionalJoin]
          have hhead : (x :: xs ++ y :: ys).head? = some x := by simp
          have hlast : ((x :: xs) ++ (y :: ys)).getLast?.getD x =
              (y :: ys).getLast?.getD y :=
            getLast?_getD_append_right (x :: xs) (y :: ys) (by simp) x y
          have hedges := edges_append_cons x xs y ys
          rw [hhead]
          simp only [Option.map_some]
          rw [hlast, hedges]
          by_cases hlt : y < (x :: xs).getLast?.getD x <;> simp [hlt] <;> omega

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
        simp only [hcond, ite_true]
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

/-! ## The goal -/

theorem sound_RunInput : MonotoneBreaks.RunInput.Goal := by
  exact runInput_of_stitch agg stitch_agg

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_MonotoneBreaks_RunInput
