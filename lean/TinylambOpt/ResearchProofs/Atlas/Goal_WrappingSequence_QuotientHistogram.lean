import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.Atlas.Goal_WrappingSequence_QuotientHistogram

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.WrappingSequence

/-! Brute-force sanity check on small configurations: no counterexample among
n < 12, parameter < 6, step != 0, and every residue r < width. -/
#eval do
  let mut bad : Array (Nat × Nat × Int × Int × Nat × Nat × Nat) := #[]
  for n in List.range 12 do
    for p in List.range 6 do
      for b in [-2, 0, 3] do
        for s in [-3, -1, 1, 2] do
          let c : Config := ⟨n, 0, p, b, s, 0, []⟩
          for r in List.range (width c) do
            let r : Nat := r
            let lhs := occurrence layout c (c.base + c.step * (r : Int))
            let rhs := c.n / width c + if r < c.n % width c then 1 else 0
            if lhs != rhs then bad := bad.push (n, p, b, s, r, lhs, rhs)
  pure bad.toList

private theorem int_eq_iff (s b : Int) (hs : s ≠ 0) (a r : Nat) :
    (b + s * (a : Int) = b + s * (r : Int)) ↔ a = r := by
  constructor
  · intro h
    have hm : (a : Int) = (r : Int) := Int.eq_of_mul_eq_mul_left hs (by omega)
    exact Int.natCast_inj.mp hm
  · intro h
    subst h
    rfl

private theorem beq_eq_iff (s b : Int) (hs : s ≠ 0) (a r : Nat) :
    (b + s * (a : Int) == b + s * (r : Int)) = (a == r) := by
  rw [Bool.eq_iff_iff]
  constructor <;> intro ht
  · rw [beq_iff_eq] at ht ⊢
    exact (int_eq_iff s b hs a r).mp ht
  · rw [beq_iff_eq] at ht ⊢
    exact (int_eq_iff s b hs a r).mpr ht

/-! For a period shorter than a single full cycle the only matching ordinal is
r itself, provided r < n. -/
private theorem count_small (w r n : Nat) (_hr : r < w) (hn : n ≤ w) :
    (List.range n).countP (fun i => i % w == r) = if r < n then 1 else 0 := by
  induction n with
  | zero => simp
  | succ n ih =>
      have hnle : n ≤ w := by omega
      have imod : n % w = n := Nat.mod_eq_of_lt (by omega)
      rw [List.range_succ, List.countP_append]
      simp only [List.countP_cons, List.countP_nil]
      rw [ih hnle, imod]
      by_cases h : r < n
      · have hne : n ≠ r := by omega
        simp [h, hne] <;> omega
      · have hlt_or_eq : n < r ∨ n = r := by omega
        rcases hlt_or_eq with hlt | heq
        · have hne : n ≠ r := by omega
          simp [h, hne] <;> omega
        · subst n
          simp

/-! Each additional full period contributes exactly one further occurrence. -/
private theorem count_add_period (w r m : Nat) (_hw : 0 < w) (hr : r < w) :
    (List.range (w + m)).countP (fun i => i % w == r) =
      (List.range m).countP (fun i => i % w == r) + 1 := by
  induction m with
  | zero =>
      rw [Nat.add_zero]
      rw [count_small w r w hr (Nat.le_refl w)]
      simp [hr]
  | succ m ih =>
      have himod : (w + m) % w = m % w := by
        rw [Nat.add_comm]
        simpa using (Nat.add_mul_mod_self_left m w 1)
      rw [Nat.add_succ, List.range_succ, List.countP_append]
      simp only [List.countP_cons, List.countP_nil, himod]
      rw [ih]
      rw [List.range_succ, List.countP_append]
      simp only [List.countP_cons, List.countP_nil]
      omega

private theorem count_mod_range (w r n : Nat) (hw : 0 < w) (hr : r < w) :
    (List.range n).countP (fun i => i % w == r) = n / w + if r < n % w then 1 else 0 := by
  induction n using Nat.strongRecOn with
  | ind n ih =>
      by_cases hnw : n < w
      · rw [count_small w r n hr (Nat.le_of_lt hnw)]
        rw [Nat.div_eq_of_lt hnw, Nat.mod_eq_of_lt hnw]
        simp
      · have hle : w ≤ n := by omega
        obtain ⟨m, hm⟩ := Nat.exists_eq_add_of_le hle
        rw [hm] at ih ⊢
        have hmlt : m < w + m := by omega
        have ihm := ih m hmlt
        have himod : (w + m) % w = m % w := by
          rw [Nat.add_comm]
          simpa using (Nat.add_mul_mod_self_left m w 1)
        have hdiv : (w + m) / w = m / w + 1 := by
          rw [Nat.add_comm]
          simpa using (Nat.add_mul_div_left m (z := 1) hw)
        rw [count_add_period w r m hw hr, hdiv, himod, ihm]
        omega

theorem sound_WrappingSequence_QuotientHistogram : WrappingSequence.QuotientHistogram.Goal := by
  intro c r hstep hr
  have hw : 0 < width c := by simp [width]
  have hrows : rows WrappingSequence.layout c =
      (List.range c.n).map (fun i : Nat => (i, c.base + c.step * ((i % width c : Nat) : Int))) := by
    unfold rows
    simp [WrappingSequence.layout]
  have hcnt : occurrence WrappingSequence.layout c (c.base + c.step * r) =
      (List.range c.n).countP (fun i => i % width c == r) := by
    unfold occurrence
    rw [hrows, List.countP_map]
    apply congrArg (fun f => List.countP f (List.range c.n))
    funext i
    simp only [Function.comp_apply, Prod.snd]
    rw [beq_eq_iff c.step c.base hstep (i % width c) r]
  rw [hcnt]
  exact count_mod_range (width c) r c.n hw hr

end TinylambOpt.ResearchProofs.Atlas.Goal_WrappingSequence_QuotientHistogram
