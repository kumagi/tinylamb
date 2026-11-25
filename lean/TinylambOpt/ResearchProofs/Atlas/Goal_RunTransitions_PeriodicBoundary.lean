import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RunTransitions_PeriodicBoundary

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.RunTransitions

/-! Brute-force sanity check on all lists of length ≤ 3 over a small range and n ≤ 4. -/
#eval do
  let vals : List Int := [-2, -1, 0, 1, 2]
  let rec gen : Nat → List (List Int)
    | 0 => [[]]
    | k + 1 => gen k ++ vals.flatMap (fun v => (gen k).map (fun xs => v :: xs))
  let bad := (gen 3).flatMap (fun x =>
    (List.range 5).filterMap (fun n =>
      let lhs := measure ((List.replicate n x).flatten)
      let rhs := (n : Int) * measure x +
        ((n - 1 : Nat) : Int) * seam x x (fun a b => if a == b then 0 else 1)
      if lhs != rhs then some (x, n, lhs, rhs) else none))
  pure bad

private def edgeBridge (x y : Series) : List (Int × Int) :=
  (x.getLast?.bind (fun a => y.head?.map (fun b => (a, b)))).toList

private theorem adj_cons₂ (a b : Int) (xs : Series) :
    adj (a :: b :: xs) = (a, b) :: adj (b :: xs) := by
  simp [adj]

private theorem adj_append (x y : Series) :
    adj x ++ edgeBridge x y ++ adj y = adj (x ++ y) := by
  induction x with
  | nil => simp [adj, edgeBridge]
  | cons a xs ih =>
      cases xs with
      | nil =>
          cases y with
          | nil => simp [adj, edgeBridge]
          | cons b ys =>
              simp only [List.cons_append, List.nil_append, adj_cons₂]
              simp [adj, edgeBridge, List.getLast?_cons, List.head?_cons]
      | cons b bs =>
          simp only [List.cons_append]
          have hbridge : edgeBridge (a :: b :: bs) y = edgeBridge (b :: bs) y := by
            simp [edgeBridge, List.getLast?_cons]
          have hi : adj (b :: bs) ++ edgeBridge (b :: bs) y ++ adj y =
              adj (b :: (bs ++ y)) := by
            simpa only [List.cons_append] using ih
          rw [adj_cons₂, hbridge, adj_cons₂]
          simpa only [List.append_assoc, List.cons_append] using
            congrArg (fun l => (a, b) :: l) hi

private theorem bridge_countP_eq_seam (x y : Series) :
    ((edgeBridge x y).countP (fun (a, b) => a != b) : Int) =
      seam x y (fun a b => if a == b then 0 else 1) := by
  simp only [edgeBridge, seam]
  cases hxa : x.getLast? with
  | none => simp
  | some a =>
      cases hyb : y.head? with
      | none => simp
      | some b =>
          by_cases hab : a = b <;> simp [hab]

private theorem measure_append (x y : Series) :
    RunTransitions.measure (x ++ y) = RunTransitions.measure x + RunTransitions.measure y +
      seam x y (fun a b => if a == b then 0 else 1) := by
  unfold RunTransitions.measure
  rw [← adj_append x y]
  simp only [List.countP_append]
  push_cast
  rw [bridge_countP_eq_seam]
  omega

private theorem seam_append_head (x y : Series) (hx : x ≠ []) :
    seam x (x ++ y) (fun a b => if a == b then 0 else 1) =
      seam x x (fun a b => if a == b then 0 else 1) := by
  cases x with
  | nil => contradiction
  | cons a xs => simp [seam]

theorem sound_PeriodicBoundary : RunTransitions.PeriodicBoundary.Goal := by
  intro x n
  induction n with
  | zero =>
      simp [RunTransitions.measure, adj]
  | succ n ih =>
      cases n with
      | zero =>
          simp
      | succ m =>
          rw [show List.replicate (Nat.succ (Nat.succ m)) x = x :: List.replicate (Nat.succ m) x by rfl]
          simp only [List.flatten_cons]
          rw [measure_append, ih]
          have hseam :
              seam x ((List.replicate (Nat.succ m) x).flatten) (fun a b => if a == b then 0 else 1) =
                seam x x (fun a b => if a == b then 0 else 1) := by
            by_cases hx : x = []
            · subst x
              simp [seam]
            · have hrep : (List.replicate (Nat.succ m) x).flatten =
                  x ++ (List.replicate m x).flatten := by
                rw [List.replicate_succ]
                simp
              rw [hrep]
              exact seam_append_head x ((List.replicate m x).flatten) hx
          rw [hseam]
          simp [Int.add_mul, Int.one_mul]
          omega

end TinylambOpt.ResearchProofs.Atlas.Goal_RunTransitions_PeriodicBoundary
