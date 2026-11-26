import TinylambOpt.ResearchGoals.Atlas.TextExecution

/-!
SQL string predicates can use encoded or summarized text when the modeled match,
boundaries, and collation are preserved.

Proved

- `Wavelet.SameBranch` — `∀ a b bit, pairs a b = pairs (lower bit a) (lower bit b) +
  pairs (upper bit a) (upper bit b)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Wavelet_SameBranch

open TinylambOpt.ResearchGoals.Atlas.TextExecution
open TinylambOpt.ResearchGoals.Atlas.TextExecution.Wavelet

/-- Brute-force sanity check: all words of length <= 3 over alphabet {0,1,2,3},
    all bit levels 0..3 (returned `true`). -/
private def genWord : Nat → List Word
  | 0 => [[]]
  | n + 1 => List.flatMap (fun w => (List.range 4).map (fun x => w ++ [x])) (genWord n)

private def bruteSameBranch : Bool :=
  let ws := List.flatMap genWord (List.range 4)
  ws.all (fun a => ws.all (fun b =>
    (List.range 4).all (fun bit =>
      pairs a b == pairs (lower bit a) (lower bit b) + pairs (upper bit a) (upper bit b))))

#guard bruteSameBranch

/-- Pointwise split of a sum of two functions over a list. -/
private theorem sum_map_add_nat (l : Word) (f g : Nat → Nat) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Nat.add_assoc, Nat.add_left_comm]

/-- `lower` and `upper` partition a list by the bit, so their counts add back
    to the count in the original list. -/
private theorem count_lower_add_count_upper (bit : Nat) (b : Word) (x : Nat) :
    (lower bit b).count x + (upper bit b).count x = b.count x := by
  induction b with
  | nil => simp [lower, upper]
  | cons y ys ih =>
      simp [lower, upper] at ih
      by_cases hbit : y.testBit bit
      · by_cases hyx : y = x
        · subst x
          simp [lower, upper, hbit] at ih ⊢
          omega
        · simpa [lower, upper, hbit, hyx] using ih
      · by_cases hyx : y = x
        · subst x
          simp [lower, upper, hbit] at ih ⊢
          omega
        · simpa [lower, upper, hbit, hyx] using ih

/-- An element whose bit is set never appears in the `lower` branch. -/
private theorem count_lower_eq_zero_of_bit (bit : Nat) (b : Word) (x : Nat)
    (h : x.testBit bit = true) : (lower bit b).count x = 0 := by
  induction b with
  | nil => simp [lower]
  | cons y ys ih =>
      simp [lower] at ih
      by_cases hyx : y = x
      · subst y
        simpa [lower, h] using ih
      · by_cases hbit : y.testBit bit <;> simpa [lower, hbit, hyx] using ih

/-- An element whose bit is unset never appears in the `upper` branch. -/
private theorem count_upper_eq_zero_of_not_bit (bit : Nat) (b : Word) (x : Nat)
    (h : x.testBit bit = false) : (upper bit b).count x = 0 := by
  induction b with
  | nil => simp [upper]
  | cons y ys ih =>
      simp [upper] at ih
      by_cases hyx : y = x
      · subst y
        simpa [upper, h] using ih
      · by_cases hbit : y.testBit bit <;> simpa [upper, hbit, hyx] using ih

/-- Filtering out zero-contributing terms does not change the sum. -/
private theorem sum_filter_eq_sum_of_zero (p : Nat → Bool) (f : Nat → Nat) (l : Word)
    (hzero : ∀ x ∈ l, !p x → f x = 0) :
    ((l.filter p).map f).sum = ((l.map f).sum) := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases hx : p x
      · simp [hx]
        apply ih
        intro y hy hnot
        exact hzero y (by simp [hy]) hnot
      · have hnot : (!p x) = true := by
          cases h : p x <;> simp [h] at hx ⊢
        have hfx : f x = 0 := hzero x (by simp) hnot
        simp [hx, hfx]
        apply ih
        intro y hy hnot
        exact hzero y (by simp [hy]) hnot

theorem sound_SameBranch : SameBranch.Goal := by
  intro a b bit
  unfold pairs
  have hmap : a.map b.count =
      a.map (fun x => (lower bit b).count x + (upper bit b).count x) := by
    apply List.map_congr_left
    intro x hx
    exact (count_lower_add_count_upper bit b x).symm
  rw [hmap]
  rw [sum_map_add_nat]
  have hlo : ((lower bit a).map (lower bit b).count).sum =
      (a.map (lower bit b).count).sum := by
    apply sum_filter_eq_sum_of_zero
    intro x hx hnot
    have hbit : x.testBit bit = true := by simpa using hnot
    exact count_lower_eq_zero_of_bit bit b x hbit
  have hup : ((upper bit a).map (upper bit b).count).sum =
      (a.map (upper bit b).count).sum := by
    apply sum_filter_eq_sum_of_zero
    intro x hx hnot
    have hbit : x.testBit bit = false := by
      cases h : x.testBit bit <;> simp [h] at hnot ⊢
    exact count_upper_eq_zero_of_not_bit bit b x hbit
  rw [hlo, hup]

end TinylambOpt.ResearchProofs.Atlas.Goal_Wavelet_SameBranch
