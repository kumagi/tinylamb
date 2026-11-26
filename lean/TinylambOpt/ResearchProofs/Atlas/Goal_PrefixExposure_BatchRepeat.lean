import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `PrefixExposure.BatchRepeat` — `∀ x n, measure ((List.replicate n x).flatten) =
  (n:Int)*measure x + (choose2 n:Int)*size x*x.sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PrefixExposure_BatchRepeat

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PrefixExposure
open TinylambOpt.ResearchGoals.PairAggregates (choose2)

/-! Exhaustive sanity check on all lists of length <= 3 over a small range,
    with n = 0..4. -/
private def rowsOf : Nat → List Int → List (List Int)
  | 0, _ => [[]]
  | n + 1, pool => pool.flatMap (fun r => (rowsOf n pool).map (r :: ·))

#eval do
  let pool : List Int := [-2, -1, 0, 1, 2]
  let lists := (List.range 4).flatMap (fun k => rowsOf k pool)
  let bad := lists.filter (fun x : List Int =>
    (List.range 5).any (fun n =>
      PrefixExposure.measure ((List.replicate n x).flatten) !=
        (n : Int) * PrefixExposure.measure x + (choose2 n : Int) * size x * x.sum))
  pure bad.length

/-! ### Prefix sums -/

private theorem prefs_nil : prefs ([] : Series) = [] := rfl

private theorem prefs_cons (a : Int) (xs : Series) :
    prefs (a :: xs) = a :: (prefs xs).map (fun v => a + v) := by
  simp [prefs, List.range_succ_eq_map, List.take_succ_cons, List.map_map, Function.comp_def]

private theorem prefs_append (x y : Series) :
    prefs (x ++ y) = prefs x ++ (prefs y).map (fun v => x.sum + v) := by
  induction x with
  | nil => simp [prefs_nil]
  | cons a xs ih =>
      rw [List.cons_append, prefs_cons, ih, prefs_cons]
      simp only [List.sum_cons, List.map_append, List.map_map, Function.comp_def,
        List.cons_append]
      apply congrArg (fun l : List Int => a :: l)
      apply congrArg (fun l : List Int => (prefs xs).map (fun v => a + v) ++ l)
      apply List.map_congr_left
      intro v hv
      grind

private theorem sum_map_add_const (xs : List Int) (f : Int → Int) (c : Int) :
    (xs.map (fun x => f x + c)).sum = (xs.map f).sum + xs.length * c := by
  induction xs with
  | nil => simp
  | cons a xs ih => simp only [List.map_cons, List.sum_cons, List.length_cons, ih]; grind

/-! The balance inherited by the right partition from the left one. -/
private theorem inherited (x y : Series) :
    PrefixExposure.measure (x ++ y) =
      PrefixExposure.measure x + PrefixExposure.measure y + x.sum * size y := by
  unfold PrefixExposure.measure size
  rw [prefs_append, List.sum_append]
  rw [show ((prefs y).map (fun v => x.sum + v)).sum =
      (prefs y).sum + (prefs y).length * x.sum by
    simpa [Int.add_comm] using sum_map_add_const (prefs y) (fun v => v) x.sum]
  have hlen : (prefs y).length = y.length := by simp [prefs]
  rw [hlen]
  grind

/-! ### Binomial and replication algebra -/

private theorem choose2_succ (n : Nat) : choose2 (n + 1) = choose2 n + n := by
  unfold choose2
  have h1 : (n + 1) * ((n + 1) - 1) = n * (n - 1) + 2 * n := by
    cases n with
    | zero => decide
    | succ m =>
        have e1 : m + 1 + 1 - 1 = m + 1 := by omega
        have e2 : m + 1 - 1 = m := by omega
        rw [e1, e2]
        grind
  rw [h1]
  exact Nat.add_mul_div_left (n * (n - 1)) n (by decide : 0 < 2)

private theorem flatten_replicate_succ (x : Series) (n : Nat) :
    (List.replicate (n + 1) x).flatten = x ++ (List.replicate n x).flatten := by
  rw [show n + 1 = Nat.succ n by omega, List.replicate_succ, List.flatten_cons]

private theorem size_flatten_replicate (x : Series) (n : Nat) :
    size ((List.replicate n x).flatten) = n * size x := by
  unfold size
  simp

theorem sound_PrefixExposure_BatchRepeat : PrefixExposure.BatchRepeat.Goal := by
  intro x n
  induction n with
  | zero => simp [PrefixExposure.measure, prefs_nil, choose2, size]
  | succ n ih =>
      rw [flatten_replicate_succ, inherited, ih, choose2_succ, size_flatten_replicate]
      grind

end TinylambOpt.ResearchProofs.Atlas.Goal_PrefixExposure_BatchRepeat
