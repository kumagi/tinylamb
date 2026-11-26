import TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `RaggedStripe.NullCount` — `∀ c, (rows layout c).length = c.n - c.n/width c`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RaggedStripe_NullCount

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.RaggedStripe

/-! Brute-force sanity check: no counterexample for small configurations. -/
#eval do
  let mut bad : Array (Nat × Nat × Int × Int) := #[]
  for n in List.range 20 do
    for p in List.range 8 do
      for b in [-2, 0, 3] do
        for s in [-3, 0, 2] do
          let c : Config := ⟨n, 0, p, b, s, 0, []⟩
          let lhs := (rows layout c).length
          let rhs := c.n - c.n / width c
          if lhs != rhs then bad := bad.push (n, p, b, s)
  pure bad.toList

/-! The quotient increases exactly when the current remainder is the last
    slot of a stripe. -/
private theorem succ_div_step (n w : Nat) (hw : 0 < w) :
    (n + 1) / w = n / w + if n % w = w - 1 then 1 else 0 := by
  let q := n / w
  let r := n % w
  have hdecomp : n = r + w * q := by
    dsimp [r, q]
    exact (Nat.mod_add_div n w).symm
  have hrlt : r < w := by
    dsimp [r]
    exact Nat.mod_lt n hw
  change (n + 1) / w = q + if r = w - 1 then 1 else 0
  by_cases hr : r = w - 1
  · simp [hr]
    rw [hdecomp, hr]
    have hw1 : w - 1 + 1 = w := by omega
    have heq : w - 1 + w * q + 1 = w * (q + 1) := by
      rw [show w - 1 + w * q + 1 = (w - 1 + 1) + w * q by omega]
      rw [hw1]
      calc
        w + w * q = w * 1 + w * q := by rw [Nat.mul_one]
        _ = w * (1 + q) := by rw [Nat.mul_add]
        _ = w * (q + 1) := by rw [Nat.add_comm 1 q]
    rw [heq]
    exact Nat.mul_div_right (q + 1) hw
  · have hrlt' : r < w - 1 := by omega
    simp [hr]
    rw [hdecomp]
    have heq : r + w * q + 1 = (r + 1) + q * w := by
      rw [Nat.mul_comm w q]
      omega
    rw [heq]
    rw [Nat.add_mul_div_right (r + 1) q hw]
    rw [Nat.div_eq_of_lt (by omega)]
    simp

private theorem count_nulls (n w : Nat) (hw : 0 < w) :
    (List.range n).countP (fun i => i % w == w - 1) = n / w := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.countP_append]
      simp only [List.countP_cons, List.countP_nil]
      rw [ih, succ_div_step n w hw]
      by_cases h : n % w = w - 1
      · simp [h, beq_iff_eq]
      · simp [h, beq_iff_eq]

private theorem length_filterMap_eq_count_isSome {α : Type} (f : Nat → Option α) (n : Nat) :
    ((List.range n).filterMap f).length = (List.range n).countP (fun i => (f i).isSome) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.filterMap_append, List.countP_append]
      cases hfn : f n <;> simp [hfn, ih]

private theorem countP_not (p : Nat → Bool) (n : Nat) :
    (List.range n).countP (fun i => !p i) = n - (List.range n).countP p := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.countP_append, List.countP_append]
      simp only [List.countP_cons, List.countP_nil]
      rw [ih]
      have hc : (List.range n).countP p ≤ n := by
        simpa using (List.countP_le_length (p := p) (l := List.range n))
      by_cases h : p n <;> simp [h] <;> omega

theorem sound_RaggedStripe_NullCount : RaggedStripe.NullCount.Goal := by
  intro c
  have hw : 0 < width c := by simp [width]
  have hrows : (rows layout c).length =
      (List.range c.n).countP (fun i => !(i % width c == width c - 1)) := by
    unfold rows
    rw [length_filterMap_eq_count_isSome]
    apply congrArg (fun f => (List.range c.n).countP f)
    funext i
    by_cases h : i % width c = width c - 1 <;> simp [RaggedStripe.layout, h, beq_iff_eq]
  rw [hrows, countP_not, count_nulls c.n (width c) hw]

end TinylambOpt.ResearchProofs.Atlas.Goal_RaggedStripe_NullCount
