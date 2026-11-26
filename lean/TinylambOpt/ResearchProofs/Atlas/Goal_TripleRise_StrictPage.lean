import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `TripleRise.StrictPage` — `∀ x, x.Pairwise (· < ·) → measure x = (choose3
  x.length:Int)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_TripleRise_StrictPage

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.PairAggregates (choose2)

/-! ### Generic list helpers -/

private theorem countP_map_comp {α β : Type} (P : β → Bool) (g : α → β) (xs : List α) :
    (xs.map g).countP P = xs.countP (fun x => P (g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih => by_cases h : P (g x) = true <;> simp [h, ih]

private theorem zipIdx_add {α : Type} (xs : List α) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, i) => (v, i + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      simp only [List.zipIdx_cons, List.map_cons]
      congr 1
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (n + 1)

/-! ### Pair / triple enumeration helpers -/

private theorem pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  unfold pairs
  rw [List.zipIdx_cons]
  simp only [List.flatMap_cons, List.drop]
  rw [show (1 : Nat) = 0 + 1 by rfl, zipIdx_add]
  rw [List.flatMap_map]

private theorem triples_cons (a : Int) (xs : Series) :
    triples (a :: xs) = (pairs xs).map (fun (b, c) => (a, b, c)) ++ triples xs := by
  unfold triples
  rw [List.zipIdx_cons]
  simp only [List.flatMap_cons, List.drop]
  rw [show (1 : Nat) = 0 + 1 by rfl, zipIdx_add]
  rw [List.flatMap_map]

private theorem pairs_fst_mem {xs : Series} {p : Int × Int} (h : p ∈ pairs xs) : p.1 ∈ xs := by
  induction xs with
  | nil => simp [pairs] at h
  | cons a ys ih =>
      rw [pairs_cons] at h
      rcases List.mem_append.mp h with h | h
      · rcases List.mem_map.mp h with ⟨b, hb, hp⟩
        rw [← hp]
        simp
      · exact List.mem_cons_of_mem a (ih h)

private theorem pairs_mem_rising (xs : Series) (h : xs.Pairwise (· < ·)) :
    ∀ p ∈ pairs xs, rising p = true := by
  induction xs with
  | nil => simp [pairs]
  | cons a ys ih =>
      intro p hp
      rw [List.pairwise_cons] at h
      rw [pairs_cons] at hp
      rcases List.mem_append.mp hp with hp | hp
      · rcases List.mem_map.mp hp with ⟨b, hb, rfl⟩
        exact decide_eq_true (h.1 b hb)
      · exact ih h.2 p hp

/-! ### Binomial coefficient recurrences -/

private theorem choose2_step (n : Nat) :
    choose2 (n + 2) = choose2 (n + 1) + (n + 1) := by
  unfold choose2
  simp only [show n + 2 - 1 = n + 1 by omega, show n + 1 - 1 = n by omega]
  rw [show (n + 2) * (n + 1) = (n + 1) * n + 2 * (n + 1) by grind,
    Nat.mul_comm 2 (n + 1), Nat.add_mul_div_right _ _ (by decide)]

private theorem choose2_succ (n : Nat) : choose2 (n + 1) = choose2 n + n := by
  cases n with
  | zero => rfl
  | succ n => exact choose2_step n

private theorem pairs_length (x : Series) : (pairs x).length = choose2 x.length := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      rw [pairs_cons, List.length_append, List.length_map, ih]
      simp only [List.length_cons]
      rw [choose2_succ]
      omega

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
      simpa [Nat.mul_assoc, Nat.mul_comm] using
        Nat.dvd_mul_right_of_dvd hd ((n + 2) * (n + 1))
    · have hd : 3 ∣ n + 2 := by
        apply Nat.dvd_iff_mod_eq_zero.mpr
        omega
      simpa [Nat.mul_assoc, Nat.mul_comm] using
        Nat.dvd_mul_right_of_dvd hd ((n + 1) * n)
    · have hd : 3 ∣ n + 1 := by
        apply Nat.dvd_iff_mod_eq_zero.mpr
        omega
      simpa [Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm] using
        Nat.dvd_mul_left_of_dvd hd ((n + 2) * n)
  have hc : 6 ∣ (n + 2) * (n + 1) * n :=
    (by decide : Nat.Coprime 2 3).mul_dvd_of_dvd_of_dvd h2 h3
  have h3B : 6 ∣ 3 * ((n + 2) * (n + 1)) := by
    rcases hb with ⟨k, hk⟩
    refine ⟨k, ?_⟩
    rw [hk]
    simp [Nat.mul_assoc, Nat.mul_comm]
  have hAC : (n + 3) * (n + 2) * (n + 1) =
      (n + 2) * (n + 1) * n + 3 * ((n + 2) * (n + 1)) := by grind
  rw [hAC, Nat.add_div (by decide),
    Nat.mod_eq_zero_of_dvd hc, Nat.mod_eq_zero_of_dvd h3B]
  simp only [Nat.add_zero]
  rcases hb with ⟨k, hk⟩
  rw [hk]
  simp [Nat.mul_assoc, Nat.mul_comm]

private theorem choose3_succ (n : Nat) : choose3 (n + 1) = choose3 n + choose2 n := by
  cases n with
  | zero => rfl
  | succ n =>
      cases n with
      | zero => rfl
      | succ n => exact choose3_step n

/-! ### StrictPage -/

private def measureNat (x : Series) : Nat := (triples x).countP strict3

private theorem measureNat_cons (a : Int) (xs : Series) :
    measureNat (a :: xs) =
      (pairs xs).countP (fun (b, c) => a < b && b < c) + measureNat xs := by
  unfold measureNat
  rw [triples_cons, List.countP_append, countP_map_comp]
  rfl

private theorem pairs_countP_rising_pairwise (xs : Series) (h : xs.Pairwise (· < ·)) :
    (pairs xs).countP rising = choose2 xs.length := by
  rw [← pairs_length xs]
  rw [List.countP_eq_length_filter]
  apply congrArg List.length
  apply List.filter_eq_self.mpr
  intro p hp
  exact pairs_mem_rising xs h p hp

theorem sound_TripleRise_StrictPage : TripleRise.StrictPage.Goal := by
  intro x hx
  induction x with
  | nil => simp [TripleRise.measure, triples, choose3]
  | cons a xs ih =>
      rw [List.pairwise_cons] at hx
      have hxs : xs.Pairwise (· < ·) := hx.2
      have ha : ∀ b ∈ xs, a < b := hx.1
      have hfirst : (pairs xs).countP (fun p : Int × Int => a < p.1 && p.1 < p.2) =
          (pairs xs).countP rising := by
        rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
        apply congrArg List.length
        apply List.filter_congr
        intro p hp
        rcases p with ⟨b, c⟩
        have hbmem : b ∈ xs := pairs_fst_mem hp
        have hab : a < b := ha b hbmem
        simp [rising, hab]
      have hm : measureNat (a :: xs) = (pairs xs).countP rising + measureNat xs := by
        rw [measureNat_cons, hfirst]
      have ih' : (measureNat xs : Int) = (choose3 xs.length : Int) := by
        simpa [TripleRise.measure, measureNat] using ih hxs
      change (measureNat (a :: xs) : Int) = (choose3 (xs.length + 1) : Int)
      rw [hm, Int.natCast_add, ih', pairs_countP_rising_pairwise xs hxs]
      have hc : (choose3 (xs.length + 1) : Int) =
          (choose3 xs.length : Int) + (choose2 xs.length : Int) := by
        exact_mod_cast (choose3_succ xs.length)
      rw [hc]
      grind

end TinylambOpt.ResearchProofs.Atlas.Goal_TripleRise_StrictPage
