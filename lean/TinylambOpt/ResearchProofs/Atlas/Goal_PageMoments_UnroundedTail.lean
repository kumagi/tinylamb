import TinylambOpt.ResearchGoals.Atlas.SynopsisBounds

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PageMoments_UnroundedTail

open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.PageMoments

/-! Exhaustive small-input sanity check (page length <= 4, values in [-3,3],
    r <= 4): the unrounded-tail bound holds on every case. -/

private def pages (n : Nat) : List (List Int) :=
  (List.range (7 ^ n)).map (fun c =>
    (List.range n).map (fun i => ((c / 7 ^ i) % 7 : Nat) - 3))

private def checkCase (s : List Int) (r : Nat) : Bool :=
  decide
    ((s.countP (fun x => r < ((s.length : Int) * x - s.sum).natAbs) : Int) *
        (r + 1 : Nat) ^ 2 ≤
      s.length * ((s.length : Int) * (s.map fun x => x ^ 2).sum - s.sum ^ 2))

example : (List.range 5).all (fun n => (pages n).all (fun s =>
    (List.range 5).all (fun r => checkCase s r))) := by
  native_decide

private theorem sq_nonneg_int (a : Int) : 0 ≤ a ^ 2 := by
  have hsq : a ^ 2 = a * a := by grind
  rw [hsq]
  by_cases h : 0 ≤ a
  · exact Int.mul_nonneg h h
  · have hn : 0 ≤ -a := by omega
    have hmul := Int.mul_nonneg hn hn
    grind

private theorem sq_le_sq_of_nonneg {a b : Int} (ha : 0 ≤ a) (hab : a ≤ b) :
    a ^ 2 ≤ b ^ 2 := by
  have hb : 0 ≤ b := by omega
  have ha2 : a ^ 2 = a * a := by grind
  have hb2 : b ^ 2 = b * b := by grind
  rw [ha2, hb2]
  have h1 : a * a ≤ a * b := Int.mul_le_mul_of_nonneg_left hab ha
  have h2 : a * b ≤ b * b := Int.mul_le_mul_of_nonneg_right hab hb
  omega

private theorem natAbs_sq_cast (z : Int) : (((z.natAbs : Nat) : Int) ^ 2) = z ^ 2 := by
  have h1 : (((z.natAbs : Nat) : Int) ^ 2) =
      ((z.natAbs : Nat) : Int) * ((z.natAbs : Nat) : Int) := by grind
  have h2 : z ^ 2 = z * z := by grind
  rw [h1, h2]
  exact Int.natAbs_mul_self

/-- The scaled second moment equals the sum of squared centered values. -/
private theorem scaled_sum_sq_aux (s : List Int) (n S : Int) :
    (s.map fun x => (n * x - S) ^ 2).sum =
      n ^ 2 * (s.map fun x => x ^ 2).sum - 2 * n * S * s.sum +
        (s.length : Int) * S ^ 2 := by
  induction s with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [ih]
      grind

private theorem mul_succ_cast (c : Nat) (R : Int) : ((c : Int) + 1) * R = (c : Int) * R + R := by
  grind

/-- Each far row contributes at least (r+1)^2 to the scaled second moment. -/
private theorem far_count_aux (s : List Int) (n S : Int) (r : Nat) :
    (s.countP (fun x => r < (n * x - S).natAbs) : Int) * ((r : Int) + 1) ^ 2 ≤
      (s.map fun x => (n * x - S) ^ 2).sum := by
  induction s with
  | nil => simp
  | cons x xs ih =>
      simp only [List.countP_cons, List.map_cons, List.sum_cons]
      by_cases h : r < (n * x - S).natAbs
      · simp [h]
        have hsucc : r + 1 ≤ (n * x - S).natAbs := Nat.succ_le_of_lt h
        have hle : ((r + 1 : Nat) : Int) ≤ (((n * x - S).natAbs : Nat) : Int) := by
          exact_mod_cast hsucc
        have hrnon : 0 ≤ ((r + 1 : Nat) : Int) := by exact_mod_cast Nat.zero_le _
        have hsq' : ((r + 1 : Nat) : Int) ^ 2 ≤ (n * x - S) ^ 2 := by
          rw [← natAbs_sq_cast (n * x - S)]
          exact sq_le_sq_of_nonneg hrnon hle
        have hcast : ((r + 1 : Nat) : Int) = (r : Int) + 1 := by simp
        rw [hcast] at hsq'
        rw [mul_succ_cast ((xs.countP fun x => r < (n * x - S).natAbs)) (((r : Int) + 1) ^ 2)]
        omega
      · simp [h]
        have hnon : 0 ≤ (n * x - S) ^ 2 := sq_nonneg_int (n * x - S)
        omega

theorem sound_PageMoments_UnroundedTail : UnroundedTail.Goal := by
  intro s r
  have hcnt := far_count_aux s (s.length : Int) s.sum r
  have hscl := scaled_sum_sq_aux s (s.length : Int) s.sum
  have hlen : (s.length : Int) ^ 2 * (s.map fun x => x ^ 2).sum -
        2 * (s.length : Int) * s.sum * s.sum + (s.length : Int) * s.sum ^ 2 =
      (s.length : Int) * ((s.length : Int) * (s.map fun x => x ^ 2).sum - s.sum ^ 2) := by
    grind
  have hcast : ((r + 1 : Nat) : Int) = (r : Int) + 1 := by simp
  rw [hcast]
  rw [hscl] at hcnt
  rw [hlen] at hcnt
  exact hcnt

end TinylambOpt.ResearchProofs.Atlas.Goal_PageMoments_UnroundedTail
