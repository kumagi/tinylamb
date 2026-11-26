import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `FrameTotals.UniformShift` — `∀ x b, measure (shifted b x) = measure x + b*choose3
  (x.length+2)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_FrameTotals_UniformShift

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.PairAggregates (choose2)

private theorem sum_map_add {α : Type} (xs : List α) (f g : α → Int) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp only [List.map_cons, List.sum_cons, ih]; grind

private theorem sum_map_const {α : Type} (xs : List α) (c : Int) :
    (xs.map (fun _ => c)).sum = (xs.length : Int) * c := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp only [List.map_cons, List.sum_cons, List.length_cons, ih]; grind

private theorem prefs_cons (a : Int) (xs : Series) :
    prefs (a :: xs) = a :: (prefs xs).map (a + ·) := by
  unfold prefs
  rw [List.length_cons, List.range_succ_eq_map]
  simp only [List.flatMap_cons, List.map_cons, List.flatMap_map, List.map_map,
    Function.comp_def, List.take_succ_cons, List.sum_cons, List.take_zero,
    Nat.add_zero, Nat.succ_eq_add_one]
  simp only [List.sum_nil, Int.add_zero]

private theorem prefixExposure_cons (a : Int) (xs : Series) :
    PrefixExposure.measure (a :: xs) =
      PrefixExposure.measure xs + ((xs.length + 1 : Nat) : Int) * a := by
  unfold PrefixExposure.measure
  rw [prefs_cons]
  simp only [List.map_cons, List.sum_cons]
  rw [sum_map_add, sum_map_const, List.map_id']
  change a + ((prefs xs).length * a + (prefs xs).sum) =
    (prefs xs).sum + ((xs.length + 1 : Nat) : Int) * a
  have hlen : (prefs xs).length = xs.length := by simp [prefs]
  rw [hlen]
  simp only [Int.natCast_add, Int.natCast_one, Int.add_mul, Int.mul_add,
    Int.mul_one, Int.one_mul]
  ac_rfl

private theorem choose2_step (n : Nat) :
    choose2 (n + 2) = choose2 (n + 1) + (n + 1) := by
  unfold choose2
  simp only [show n + 2 - 1 = n + 1 by omega, show n + 1 - 1 = n by omega]
  rw [show (n + 2) * (n + 1) = (n + 1) * n + 2 * (n + 1) by grind,
    Nat.mul_comm 2 (n + 1), Nat.add_mul_div_right _ _ (by decide)]

private theorem prefixExposure_deposit (b : Int) (x : Series) :
    PrefixExposure.measure (shifted b x) =
      PrefixExposure.measure x + b * (choose2 (x.length + 1) : Int) := by
  induction x with
  | nil => simp [PrefixExposure.measure, shifted, prefs, choose2]
  | cons a xs ih =>
      change PrefixExposure.measure ((a + b) :: shifted b xs) =
        PrefixExposure.measure (a :: xs) + b * (choose2 ((a :: xs).length + 1) : Int)
      rw [prefixExposure_cons, prefixExposure_cons, ih]
      simp only [List.length_cons]
      have hs := choose2_step xs.length
      have hc : (choose2 (xs.length + 2) : Int) =
          (choose2 (xs.length + 1) : Int) + (xs.length + 1 : Nat) := by
        exact_mod_cast hs
      rw [hc]
      have hlen : (shifted b xs).length = xs.length := by simp [shifted]
      rw [hlen]
      simp only [Int.natCast_add, Int.natCast_one, Int.add_mul, Int.mul_add,
        Int.mul_one, Int.one_mul]
      ac_rfl

private theorem choose3_step (n : Nat) :
    choose3 (n + 3) = choose3 (n + 2) + choose2 (n + 2) := by
  unfold choose3 choose2
  simp only [show n + 3 - 1 = n + 2 by omega,
    show n + 3 - 2 = n + 1 by omega, show n + 2 - 1 = n + 1 by omega,
    show n + 2 - 2 = n by omega]
  have hb : 2 ∣ (n + 2) * (n + 1) := by
    rcases Nat.mod_two_eq_zero_or_one n with h | h
    · have hd : 2 ∣ n + 2 := by omega
      exact Nat.dvd_mul_right_of_dvd hd (n + 1)
    · have hd : 2 ∣ n + 1 := by omega
      exact Nat.dvd_mul_left_of_dvd hd (n + 2)
  have h2 : 2 ∣ (n + 2) * (n + 1) * n := Nat.dvd_mul_right_of_dvd hb n
  have h3 : 3 ∣ (n + 2) * (n + 1) * n := by
    have hm := Nat.mod_lt n (by decide : 0 < 3)
    rcases (show n % 3 = 0 ∨ n % 3 = 1 ∨ n % 3 = 2 by omega) with h | h | h
    · have hd : 3 ∣ n := Nat.dvd_of_mod_eq_zero h
      simpa [Nat.mul_comm, Nat.mul_left_comm, Nat.mul_assoc] using
        Nat.dvd_mul_right_of_dvd hd ((n + 2) * (n + 1))
    · have hd : 3 ∣ n + 2 := by
        apply Nat.dvd_iff_mod_eq_zero.mpr
        omega
      simpa [Nat.mul_comm, Nat.mul_left_comm, Nat.mul_assoc] using
        Nat.dvd_mul_right_of_dvd hd ((n + 1) * n)
    · have hd : 3 ∣ n + 1 := by
        apply Nat.dvd_iff_mod_eq_zero.mpr
        omega
      simpa [Nat.mul_comm, Nat.mul_left_comm, Nat.mul_assoc] using
        Nat.dvd_mul_left_of_dvd hd ((n + 2) * n)
  have hc : 6 ∣ (n + 2) * (n + 1) * n :=
    (by decide : Nat.Coprime 2 3).mul_dvd_of_dvd_of_dvd h2 h3
  have h3B : 6 ∣ 3 * ((n + 2) * (n + 1)) := by
    rcases hb with ⟨k, hk⟩
    refine ⟨k, ?_⟩
    rw [hk]
    simp [Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm]
  have hAC : (n + 3) * (n + 2) * (n + 1) =
      (n + 2) * (n + 1) * n + 3 * ((n + 2) * (n + 1)) := by grind
  rw [hAC, Nat.add_div (by decide),
    Nat.mod_eq_zero_of_dvd hc, Nat.mod_eq_zero_of_dvd h3B]
  simp only [Nat.add_zero]
  rcases hb with ⟨k, hk⟩
  rw [hk]
  simp [Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm]

private theorem windows_cons_prefix (a : Int) (xs : Series) :
    windows (a :: xs) =
      [[a]] ++ ((List.range xs.length).map (fun i => a :: xs.take (i + 1))) ++ windows xs := by
  unfold windows
  rw [List.length_cons, List.range_succ_eq_map]
  simp only [List.flatMap_cons, List.map_cons, List.flatMap_map, List.map_map,
    Function.comp_def, List.flatMap_append, List.take_succ_cons, List.take_zero,
    Nat.add_zero, Nat.succ_eq_add_one]
  congr 2
  · simp [List.range_succ_eq_map, List.drop_zero, List.take_succ_cons,
      List.take_zero, Nat.add_assoc]
  · funext i
    simp [List.drop_drop, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]

private theorem frameTotals_cons (a : Int) (xs : Series) :
    FrameTotals.measure (a :: xs) =
      FrameTotals.measure xs + PrefixExposure.measure xs +
        ((xs.length + 1 : Nat) : Int) * a := by
  unfold FrameTotals.measure
  rw [windows_cons_prefix]
  simp only [List.map_append, List.sum_append, List.map_cons, List.sum_cons,
    List.map_map, Function.comp_def]
  rw [sum_map_add, sum_map_const]
  simp only [List.length_map, List.length_range, List.sum_nil, Int.add_zero]
  unfold PrefixExposure.measure prefs
  simp only [Int.natCast_add, Int.natCast_one, Int.add_mul, Int.mul_add,
    Int.mul_one, Int.one_mul]
  ac_rfl

theorem sound_FrameTotals_UniformShift : FrameTotals.UniformShift.Goal := by
  intro x b
  induction x with
  | nil => simp [FrameTotals.measure, shifted, windows, choose3]
  | cons a xs ih =>
      change FrameTotals.measure ((a + b) :: shifted b xs) =
        FrameTotals.measure (a :: xs) + b * (choose3 ((a :: xs).length + 2) : Int)
      rw [frameTotals_cons, frameTotals_cons, prefixExposure_deposit, ih]
      simp only [List.length_cons]
      have hs := choose3_step xs.length
      have hc : (choose3 (xs.length + 3) : Int) =
          (choose3 (xs.length + 2) : Int) + (choose2 (xs.length + 2) : Int) := by
        exact_mod_cast hs
      have hs2 := choose2_step xs.length
      have hc2 : (choose2 (xs.length + 2) : Int) =
          (choose2 (xs.length + 1) : Int) + (xs.length + 1 : Nat) := by
        exact_mod_cast hs2
      rw [hc, hc2]
      have hlen : (shifted b xs).length = xs.length := by simp [shifted]
      rw [hlen]
      simp only [List.length_cons, Int.natCast_add, Int.natCast_one,
        Int.add_mul, Int.mul_add, Int.mul_one, Int.one_mul]
      ac_rfl

end TinylambOpt.ResearchProofs.Atlas.Goal_FrameTotals_UniformShift
