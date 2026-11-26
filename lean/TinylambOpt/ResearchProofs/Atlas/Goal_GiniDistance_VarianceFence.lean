import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `GiniDistance.VarianceFence` — `∀ x, measure x^2 ≤ (choose2
  x.length:Int)*CenteredScatter.measure x`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_GiniDistance_VarianceFence

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.PairAggregates (choose2)

/-! ### Exhaustive sanity check on small inputs -/

private def rowsOf : Nat → List Int → List (List Int)
  | 0, _ => [[]]
  | n + 1, pool => pool.flatMap (fun r => (rowsOf n pool).map (r :: ·))

private def smallPool : List Int := [-2, -1, 0, 1, 2]

#eval (rowsOf 4 smallPool).all (fun x =>
  decide (GiniDistance.measure x ^ 2 ≤
    (choose2 x.length : Int) * CenteredScatter.measure x))

/-! ### `pairs` length -/

private theorem flatMap_map {α β γ : Type} (f : β → List γ) (g : α → β)
    (xs : List α) : (xs.map g).flatMap f = xs.flatMap (fun x => f (g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem zipIdx_add {α : Type} (xs : List α) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, i) => (v, i + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons a xs ih =>
      simp only [List.zipIdx_cons, List.map_cons]
      congr 1
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (n + 1)

private theorem pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  unfold pairs
  rw [List.zipIdx_cons]
  simp only [List.flatMap_cons, List.drop]
  rw [show (1 : Nat) = 0 + 1 by omega, zipIdx_add]
  rw [flatMap_map]

private theorem even_pred_mul (n : Nat) : ∃ k : Nat, n * (n - 1) = 2 * k := by
  rcases Nat.mod_two_eq_zero_or_one n with h | h
  · have hn : n = 2 * (n / 2) := by
      have hd := Nat.mod_add_div n 2
      omega
    refine ⟨(n / 2) * (n - 1), ?_⟩
    calc
      n * (n - 1) = (2 * (n / 2)) * (n - 1) :=
        congrArg (fun k => k * (n - 1)) hn
      _ = 2 * ((n / 2) * (n - 1)) := by grind
  · have hn : n = 2 * ((n - 1) / 2) + 1 := by
      have hd := Nat.mod_add_div n 2
      omega
    have hpred : n - 1 = 2 * ((n - 1) / 2) := by omega
    refine ⟨n * ((n - 1) / 2), ?_⟩
    calc
      n * (n - 1) = n * (2 * ((n - 1) / 2)) :=
        congrArg (fun k => n * k) hpred
      _ = 2 * (n * ((n - 1) / 2)) := by grind

private theorem choose2_double (n : Nat) : 2 * choose2 n = n * (n - 1) := by
  rcases even_pred_mul n with ⟨k, hk⟩
  unfold choose2
  rw [hk]
  simp

private theorem pairs_length (xs : Series) : (pairs xs).length = choose2 xs.length := by
  induction xs with
  | nil => rfl
  | cons a xs ih =>
      rw [pairs_cons, List.length_append, List.length_map, ih]
      apply Nat.eq_of_mul_eq_mul_left (by decide : 0 < 2)
      have hm := choose2_double xs.length
      have hs := choose2_double (xs.length + 1)
      simp only [Nat.add_sub_cancel] at hs
      have hsub : xs.length * (xs.length - 1) + xs.length = xs.length * xs.length := by
        rw [Nat.mul_sub_left_distrib, Nat.mul_one]
        apply Nat.sub_add_cancel
        by_cases hz : xs.length = 0
        · simp [hz]
        · exact Nat.le_mul_of_pos_left xs.length (Nat.pos_of_ne_zero hz)
      simp only [List.length_cons]
      have hdistr : 2 * (xs.length + choose2 xs.length) =
          2 * xs.length + 2 * choose2 xs.length := by grind
      rw [hdistr, hm, hs]
      simp only [Nat.add_mul, Nat.one_mul]
      omega

/-! ### Cauchy--Schwarz over `Int`, instantiated on physical pairs -/

private theorem sq_nonneg (a : Int) : 0 ≤ a ^ 2 := by
  have h : a ^ 2 = a * a := by grind
  rw [h, ← Int.natAbs_mul_self (a := a)]
  exact Int.natCast_nonneg _

private theorem sum_sq_cauchy (xs : List Int) :
    xs.sum ^ 2 ≤ (xs.length : Int) * (xs.map fun x => x ^ 2).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.sum_cons, List.length_cons, List.map_cons]
      push_cast
      by_cases hk : xs.length = 0
      · simp [hk]
        grind
      · have hkpos : (0 : Int) < xs.length := by exact_mod_cast Nat.pos_of_ne_zero hk
        let k : Int := xs.length
        let Q : Int := (xs.map fun x => x ^ 2).sum
        let S : Int := xs.sum
        have hih : S ^ 2 ≤ k * Q := by simpa [k, Q, S] using ih
        have hv : 0 ≤ k * Q - S ^ 2 := by omega
        have hsq : 0 ≤ (k * x - S) ^ 2 := sq_nonneg _
        have hident : (k * x - S) ^ 2 + (k * Q - S ^ 2) =
            k * (k * x ^ 2 - 2 * x * S + Q) := by grind
        have hprod : 0 ≤ k * (k * x ^ 2 - 2 * x * S + Q) := by
          rw [← hident]
          omega
        have hmain : 0 ≤ k * x ^ 2 - 2 * x * S + Q := by
          by_cases hn : 0 ≤ k * x ^ 2 - 2 * x * S + Q
          · exact hn
          · have hneg : k * x ^ 2 - 2 * x * S + Q < 0 := by omega
            have hlt : k * (k * x ^ 2 - 2 * x * S + Q) < 0 :=
              Int.mul_neg_of_pos_of_neg hkpos hneg
            omega
        have hlhs : (x + S) ^ 2 = x ^ 2 + 2 * x * S + S ^ 2 := by grind
        have hrhs : (k + 1) * (x ^ 2 + Q) = x ^ 2 + Q + k * x ^ 2 + k * Q := by grind
        change (x + S) ^ 2 ≤ (k + 1) * (x ^ 2 + Q)
        rw [hlhs, hrhs]
        omega

private theorem distance_sq (a b : Int) : distance a b ^ 2 = (a - b) ^ 2 := by
  unfold distance
  have h1 : ((b - a).natAbs : Int) ^ 2 =
      ((b - a).natAbs : Int) * ((b - a).natAbs : Int) := by grind
  have h2 : (a - b) ^ 2 = (a - b) * (a - b) := by grind
  rw [h1, Int.natAbs_mul_self', h2]
  grind

/-! ### Main theorem -/

theorem sound_GiniDistance_VarianceFence : GiniDistance.VarianceFence.Goal := by
  intro x
  have hc := sum_sq_cauchy ((pairs x).map fun t => distance t.1 t.2)
  have hsq : (((pairs x).map fun t => distance t.1 t.2).map fun v => v ^ 2).sum =
      CenteredScatter.measure x := by
    simp only [List.map_map, Function.comp_def]
    apply congrArg List.sum
    apply List.map_congr_left
    intro t _
    exact distance_sq t.1 t.2
  rw [hsq, List.length_map, pairs_length] at hc
  simpa only [GiniDistance.measure] using hc

end TinylambOpt.ResearchProofs.Atlas.Goal_GiniDistance_VarianceFence
