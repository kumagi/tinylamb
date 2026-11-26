import TinylambOpt.ResearchGoals.Atlas.TextExecution

/-!
SQL string predicates can use encoded or summarized text when the modeled match,
boundaries, and collation are preserved.

Proved

- `Wavelet.DictionaryGrowth` — `∀ d old more k, (∀ x ∈ old, x < 2^d) → (∀ y ∈ more, 2^d
  ≤ y ∧ y < 2^(d+1)) → k < old.length+more.length → kth (d+1) (old++more) k = if k <
  old.length then kth d old k else 2^d+kth d more (k-old.length)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Wavelet_DictionaryGrowth

open TinylambOpt.ResearchGoals.Atlas.TextExecution
open TinylambOpt.ResearchGoals.Atlas.TextExecution.Wavelet

private theorem filter_eq_self_of_all {α} (p : α → Bool) (l : List α)
    (h : ∀ x ∈ l, p x = true) : l.filter p = l := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hx : p x = true := h x (by simp)
      have ht : ∀ y ∈ xs, p y = true := by intro y hy; exact h y (by simp [hy])
      simp [hx, ih ht]

private theorem filter_eq_nil_of_all {α} (p : α → Bool) (l : List α)
    (h : ∀ x ∈ l, p x = false) : l.filter p = [] := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hx : p x = false := h x (by simp)
      have ht : ∀ y ∈ xs, p y = false := by intro y hy; exact h y (by simp [hy])
      simp [hx, ih ht]

private theorem two_pow_pos (n : Nat) : 0 < 2^n := Nat.pow_pos (by decide)

private theorem lt_two_pow_of_testBit_false {d x : Nat} (h : x < 2^(d+1))
    (hb : x.testBit d = false) : x < 2^d := by
  have hdiv2 : x / 2^d < 2 := by
    rw [Nat.div_lt_iff_lt_mul (two_pow_pos d)]
    have hp : 2^(d+1) = 2*2^d := by rw [Nat.pow_succ, Nat.mul_comm]
    rw [hp] at h
    exact h
  have hb' : (x / 2^d).testBit 0 = false := by
    rw [← Nat.testBit_add]
    simpa using hb
  have hqle : x / 2^d ≤ 1 := Nat.le_of_lt_succ hdiv2
  have hq0 : x / 2^d = 0 := by
    rcases Nat.le_one_iff_eq_zero_or_eq_one.mp hqle with hq0 | hq1
    · exact hq0
    · have htb : (x / 2^d).testBit 0 = true := by simp [Nat.testBit_zero, hq1]
      rw [hb'] at htb
      cases htb
  have hlt1 : x / 2^d < 1 := by rw [hq0]; decide
  simpa using (Nat.div_lt_iff_lt_mul (two_pow_pos d)).mp hlt1

private theorem testBit_true_of_upper {d y : Nat} (hlo : 2^d ≤ y) (hhi : y < 2^(d+1)) :
    y.testBit d = true := by
  by_cases ht : y.testBit d = true
  · exact ht
  · have hf : y.testBit d = false := Bool.eq_false_of_ne_true ht
    have hylt : y < 2^d := lt_two_pow_of_testBit_false (by simpa [Nat.pow_succ] using hhi) hf
    exact False.elim (by omega)

theorem sound_Wavelet_DictionaryGrowth : Wavelet.DictionaryGrowth.Goal := by
  intro d old more k hold hmore hk
  have hlower : lower d (old ++ more) = old := by
    simp [lower, List.filter_append,
      filter_eq_self_of_all (fun x => !x.testBit d) old (by
        intro x hx
        have hb := Nat.testBit_lt_two_pow (hold x hx)
        simp [hb]),
      filter_eq_nil_of_all (fun x => !x.testBit d) more (by
        intro y hy
        have hyb := testBit_true_of_upper (hmore y hy).1 (hmore y hy).2
        simp [hyb])]
  have hupper : upper d (old ++ more) = more := by
    simp [upper, List.filter_append,
      filter_eq_nil_of_all (fun x => x.testBit d) old (by
        intro x hx
        exact Nat.testBit_lt_two_pow (hold x hx)),
      filter_eq_self_of_all (fun x => x.testBit d) more (by
        intro y hy
        exact testBit_true_of_upper (hmore y hy).1 (hmore y hy).2)]
  simp [kth, hlower, hupper]

end TinylambOpt.ResearchProofs.Atlas.Goal_Wavelet_DictionaryGrowth
