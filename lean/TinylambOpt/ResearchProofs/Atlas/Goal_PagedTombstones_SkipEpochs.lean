import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PagedTombstones_SkipEpochs

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.PagedTombstones

/-! Brute-force sanity check: no counterexample for small configurations. -/
#eval do
  let mut bad : Array (Nat × Nat × Nat) := #[]
  for n in List.range 80 do
    for p in List.range 12 do
      for b in [-2, 0, 3] do
        for s in [-3, 0, 2] do
          let c : Config := ⟨n, 0, p, b, s, 0, []⟩
          let lhs := (rows layout c).length
          let rhs := (c.n / (2 * width c)) * width c + min (width c) (c.n % (2 * width c))
          if lhs != rhs then bad := bad.push (n, p, lhs)
  pure bad.toList

private theorem length_filterMap_eq_count_isSome {α : Type} (f : Nat → Option α) (n : Nat) :
    ((List.range n).filterMap f).length = (List.range n).countP (fun i => (f i).isSome) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.filterMap_append, List.countP_append]
      cases hfn : f n <;> simp [hfn, ih]

/-! Inside the first two tiles an ordinal survives iff it lies in the first tile. -/
private theorem survivor_bool (w n : Nat) (hw : 0 < w) (hlt : n < 2 * w) :
    ((n / w) % 2 == 0) = decide (n < w) := by
  by_cases hnw : n < w
  · simp [Nat.div_eq_of_lt hnw, hnw]
  · have hle : w ≤ n := Nat.le_of_not_gt hnw
    obtain ⟨m, hm⟩ := Nat.exists_eq_add_of_le hle
    have hm_lt : m < w := by omega
    have hdiv : (w + m) / w = 1 := by
      rw [Nat.add_comm]
      rw [show (m + w) / w = (m + w * 1) / w by rw [Nat.mul_one]]
      rw [Nat.add_mul_div_left m 1 hw, Nat.div_eq_of_lt hm_lt]
    rw [hm, hdiv]
    have hfalse : ¬ w + m < w := by omega
    simp [hfalse]

private theorem count_small (w n : Nat) (hw : 0 < w) (hn : n ≤ 2 * w) :
    (List.range n).countP (fun i => (i / w) % 2 == 0) = min w n := by
  induction n with
  | zero => simp
  | succ n ih =>
      have hnle : n ≤ 2 * w := by omega
      have hnlt : n < 2 * w := by omega
      rw [List.range_succ, List.countP_append]
      simp only [List.countP_cons, List.countP_nil]
      rw [ih hnle, survivor_bool w n hw hnlt]
      by_cases h : n < w
      · have hmin : min w n = n := Nat.min_eq_right (Nat.le_of_lt h)
        have hn1 : n + 1 ≤ w := by omega
        have hmin1 : min w (n + 1) = n + 1 := Nat.min_eq_right hn1
        simp [h, hmin, hmin1]
      · have hmin : min w n = w := Nat.min_eq_left (by omega)
        have hmin1 : min w (n + 1) = w := Nat.min_eq_left (by omega)
        simp [h, hmin, hmin1]

/-! Shifting by a full pair of tiles preserves the survivor predicate. -/
private theorem count_add_period (w m : Nat) (hw : 0 < w) :
    (List.range (2 * w + m)).countP (fun i => (i / w) % 2 == 0) =
      w + (List.range m).countP (fun i => (i / w) % 2 == 0) := by
  induction m with
  | zero =>
      rw [Nat.add_zero, count_small w (2 * w) hw (Nat.le_refl _)]
      rw [Nat.min_eq_left (by omega)]
      simp
  | succ m ih =>
      have hdiv : (2 * w + m) / w = 2 + m / w := by
        rw [Nat.add_comm]
        rw [show (m + 2 * w) / w = (m + w * 2) / w by rw [Nat.mul_comm]]
        rw [Nat.add_mul_div_left m 2 hw]
        omega
      have hsurv : (((2 * w + m) / w) % 2 == 0) = ((m / w) % 2 == 0) := by
        rw [hdiv, Nat.add_comm]
        simpa using (Nat.add_mul_mod_self_left (m / w) 2 1)
      rw [Nat.add_succ, List.range_succ, List.countP_append]
      simp only [List.countP_cons, List.countP_nil]
      rw [ih]
      rw [List.range_succ, List.countP_append]
      simp only [List.countP_cons, List.countP_nil]
      rw [hsurv]
      omega

private theorem count_skip (w n : Nat) (hw : 0 < w) :
    (List.range n).countP (fun i => (i / w) % 2 == 0) =
      (n / (2 * w)) * w + min w (n % (2 * w)) := by
  induction n using Nat.strongRecOn with
  | ind n ih =>
      by_cases hnlt : n < 2 * w
      · rw [count_small w n hw (Nat.le_of_lt hnlt)]
        rw [Nat.div_eq_of_lt hnlt, Nat.mod_eq_of_lt hnlt]
        simp
      · have hle : 2 * w ≤ n := by omega
        obtain ⟨m, hm⟩ := Nat.exists_eq_add_of_le hle
        have hmlt : m < n := by omega
        have hP : 0 < 2 * w := by omega
        have hdiv : n / (2 * w) = m / (2 * w) + 1 := by
          rw [hm, Nat.add_comm]
          simpa [Nat.mul_one] using (Nat.add_mul_div_left m 1 hP)
        have hmod : n % (2 * w) = m % (2 * w) := by
          rw [hm, Nat.add_comm]
          simpa using (Nat.add_mul_mod_self_left m (2 * w) 1)
        rw [hdiv, hmod, hm, count_add_period w m hw, ih m hmlt]
        rw [Nat.add_mul, Nat.one_mul]
        omega

theorem sound_PagedTombstones_SkipEpochs : PagedTombstones.SkipEpochs.Goal := by
  intro c
  have hw : 0 < width c := by simp [width]
  have hrows : (rows layout c).length =
      (List.range c.n).countP (fun i => (i / width c) % 2 == 0) := by
    unfold rows
    rw [length_filterMap_eq_count_isSome]
    apply congrArg (fun f => (List.range c.n).countP f)
    funext i
    by_cases h : (i / width c) % 2 = 0 <;> simp [PagedTombstones.layout, h, beq_iff_eq]
  rw [hrows]
  exact count_skip (width c) c.n hw

end TinylambOpt.ResearchProofs.Atlas.Goal_PagedTombstones_SkipEpochs
