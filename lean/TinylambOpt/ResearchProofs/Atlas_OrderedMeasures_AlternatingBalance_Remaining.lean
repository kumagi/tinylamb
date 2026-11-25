import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.AlternatingBalance.Remaining

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.AlternatingBalance

/-! #### Alternating (parity-weighted) sum helpers -/

private def alt (k : Nat) (x : Series) : Int :=
  ((x.zipIdx k).map (fun (v, i) => (-1 : Int) ^ i * v)).sum

private theorem measure_alt (x : Series) : AlternatingBalance.measure x = alt 0 x := by
  rfl

private theorem alt_cons (k : Nat) (a : Int) (xs : Series) :
    alt k (a :: xs) = (-1 : Int) ^ k * a + alt (k + 1) xs := by
  simp [alt, List.zipIdx_cons]

private theorem neg_one_pow_succ (n : Nat) : (-1 : Int) ^ (n + 1) = -((-1 : Int) ^ n) := by
  rw [Int.pow_succ]
  omega

private theorem alt_shift (k : Nat) (x : Series) : alt k x = (-1 : Int) ^ k * alt 0 x := by
  induction x generalizing k with
  | nil => simp [alt]
  | cons a xs ih =>
      rw [alt_cons, ih (k + 1)]
      rw [alt_cons, ih 1]
      rw [neg_one_pow_succ]
      simp only [Int.pow_zero]
      grind

private theorem alt_append (k : Nat) (x y : Series) :
    alt k (x ++ y) = alt k x + alt (k + x.length) y := by
  induction x generalizing k with
  | nil => simp [alt]
  | cons a xs ih =>
      rw [List.cons_append, alt_cons, alt_cons, ih (k + 1)]
      rw [show k + 1 + xs.length = k + (xs.length + 1) by omega]
      simp only [List.length_cons]
      omega

private theorem measure_cons (a : Int) (xs : Series) :
    AlternatingBalance.measure (a :: xs) = a - AlternatingBalance.measure xs := by
  rw [measure_alt, measure_alt, alt_cons, alt_shift 1]
  rw [neg_one_pow_succ]
  simp only [Int.pow_zero]
  grind

private theorem neg_one_pow_even (k : Nat) : (-1 : Int) ^ (2 * k) = 1 := by
  induction k with
  | zero => decide
  | succ k ih =>
      rw [show 2 * (k + 1) = 2 * k + 2 by omega]
      rw [Int.pow_add, ih]
      rw [show (-1 : Int) ^ 2 = 1 by decide]
      omega

private theorem neg_one_pow_odd (k : Nat) : (-1 : Int) ^ (2 * k + 1) = -1 := by
  rw [Int.pow_succ, neg_one_pow_even]
  omega

private theorem neg_one_pow_mod (n : Nat) :
    (-1 : Int) ^ n = if n % 2 == 0 then 1 else -1 := by
  by_cases h : n % 2 == 0
  · have h0 : n % 2 = 0 := by simpa using h
    have hm := Nat.mod_add_div n 2
    have hn : n = 2 * (n / 2) := by omega
    rw [ite_eq_left h, hn]
    exact neg_one_pow_even (n / 2)
  · have hlt : n % 2 < 2 := Nat.mod_lt _ (by decide)
    have hn0 : ¬ n % 2 = 0 := by simpa using h
    have h0 : n % 2 = 1 := by omega
    have hm := Nat.mod_add_div n 2
    have hn : n = 2 * (n / 2) + 1 := by omega
    rw [ite_eq_right h, hn]
    exact neg_one_pow_odd (n / 2)

private theorem measure_singleton (a : Int) : AlternatingBalance.measure [a] = a := by
  simp [AlternatingBalance.measure]

private theorem measure_repeated (n : Nat) (a : Int) :
    AlternatingBalance.measure (repeated n a) = ((n % 2 : Nat) : Int) * a := by
  induction n with
  | zero => simp [repeated, AlternatingBalance.measure]
  | succ n ih =>
      rw [show repeated (n + 1) a = a :: repeated n a by
        simp [repeated, List.replicate_succ], measure_cons, ih]
      have hlt : n % 2 < 2 := Nat.mod_lt _ (by decide)
      by_cases h : n % 2 == 0
      · have h0 : n % 2 = 0 := by simpa using h
        have h1 : (n + 1) % 2 = 1 := by omega
        rw [h0, h1]
        grind
      · have hn0 : ¬ n % 2 = 0 := by simpa using h
        have h0 : n % 2 = 1 := by omega
        have h1 : (n + 1) % 2 = 0 := by omega
        rw [h0, h1]
        grind

/-! #### Goals -/

theorem sound_ParityCarry : ParityCarry.Goal := by
  intro x y
  rw [measure_alt, measure_alt, measure_alt]
  rw [alt_append]
  simp only [Nat.zero_add]
  rw [alt_shift (x.length) y]

theorem sound_BackwardPhase : BackwardPhase.Goal := by
  intro x
  induction x with
  | nil => simp [AlternatingBalance.measure]
  | cons a xs ih =>
      rw [List.reverse_cons]
      rw [sound_ParityCarry]
      rw [measure_singleton]
      rw [List.length_reverse]
      rw [ih]
      rw [measure_cons]
      cases xs with
      | nil => simp [AlternatingBalance.measure]
      | cons b bs =>
          simp only [List.length_cons, Nat.add_sub_cancel_right]
          rw [measure_cons]
          rw [neg_one_pow_succ]
          grind

theorem sound_PeriodicParity : PeriodicParity.Goal := by
  intro x n
  induction n with
  | zero => simp [AlternatingBalance.measure]
  | succ n ih =>
      rw [List.replicate_succ, List.flatten_cons]
      rw [sound_ParityCarry, ih]
      by_cases hl : x.length % 2 == 0
      · have hpow : (-1 : Int) ^ x.length = 1 := by
          rw [neg_one_pow_mod, ite_eq_left hl]
        rw [ite_eq_left hl] at ih ⊢
        rw [hpow]
        grind
      · have hpow : (-1 : Int) ^ x.length = -1 := by
          rw [neg_one_pow_mod, ite_eq_right hl]
        rw [ite_eq_right hl] at ih ⊢
        rw [hpow]
        by_cases hn : n % 2 == 0
        · have hlt : n % 2 < 2 := Nat.mod_lt _ (by decide)
          have h0 : n % 2 = 0 := by simpa using hn
          have h1 : (n + 1) % 2 = 1 := by omega
          rw [h0, h1]
          grind
        · have hlt : n % 2 < 2 := Nat.mod_lt _ (by decide)
          have hn0 : ¬ n % 2 = 0 := by simpa using hn
          have h0 : n % 2 = 1 := by omega
          have h1 : (n + 1) % 2 = 0 := by omega
          rw [h0, h1]
          grind

theorem sound_DuplicateCancellation : DuplicateCancellation.Goal := by
  intro x n
  induction x with
  | nil => simp [AlternatingBalance.measure]
  | cons a xs ih =>
      rw [List.flatMap_cons]
      rw [sound_ParityCarry]
      rw [measure_repeated]
      rw [ih]
      rw [measure_cons]
      rw [show (repeated n a).length = n by simp [repeated]]
      by_cases h : n % 2 == 0
      · have hpow : (-1 : Int) ^ n = 1 := by rw [neg_one_pow_mod, ite_eq_left h]
        rw [ite_eq_left h, hpow]
        have h0 : n % 2 = 0 := by simpa using h
        rw [h0]
        grind
      · have hpow : (-1 : Int) ^ n = -1 := by rw [neg_one_pow_mod, ite_eq_right h]
        rw [ite_eq_right h, hpow]
        have hlt : n % 2 < 2 := Nat.mod_lt _ (by decide)
        have hn0 : ¬ n % 2 = 0 := by simpa using h
        have h0 : n % 2 = 1 := by omega
        rw [h0]
        grind

theorem sound_ShiftResidue : ShiftResidue.Goal := by
  intro b x
  induction x with
  | nil => simp [AlternatingBalance.measure, shifted]
  | cons a xs ih =>
      change AlternatingBalance.measure ((a + b) :: shifted b xs) =
        AlternatingBalance.measure (a :: xs) + (((xs.length + 1) % 2 : Nat) : Int) * b
      rw [measure_cons, measure_cons, ih]
      by_cases h : xs.length % 2 == 0
      · have hlt : xs.length % 2 < 2 := Nat.mod_lt _ (by decide)
        have h0 : xs.length % 2 = 0 := by simpa using h
        have h1 : (xs.length + 1) % 2 = 1 := by omega
        rw [h0, h1]
        grind
      · have hlt : xs.length % 2 < 2 := Nat.mod_lt _ (by decide)
        have hn0 : ¬ xs.length % 2 = 0 := by simpa using h
        have h0 : xs.length % 2 = 1 := by omega
        have h1 : (xs.length + 1) % 2 = 0 := by omega
        rw [h0, h1]
        grind

end TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.AlternatingBalance.Remaining
