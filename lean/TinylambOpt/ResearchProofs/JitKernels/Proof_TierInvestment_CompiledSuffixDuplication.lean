import TinylambOpt.ResearchGoals.JitKernels.TierInvestment

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Soundness of "for a stable repeating trace with positive net savings, some repetition
count amortizes any finite compilation cost": if one period `xs` saves `S - C > 0` work
units in aggregate (`0 < Σ net`), then repeating it `compile + 1` times makes the total
compiled-path advantage `(compile+1)·(S - C)` strictly exceed the single compile charge,
so tier-up on the repeated trace strictly beats pure scalar execution. Formally: with
`ys = replicate (compile+1) xs |>.flatten`, `bill t ys 0 = compile + n·C` and
`bill t ys ys.length = n·S`, and `n·S - n·C = n·(S-C) ≥ n > compile`. This licenses a JIT
warm-up loop to keep a compiled kernel across recurring SQL batch patterns until it
amortizes its compilation cost.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.TierInvestment_CompiledSuffixDuplication
open TinylambOpt.ResearchGoals.JitKernels.TierInvestment

private theorem sum_net (t : Tariff) : ∀ (l : List Batch),
    (l.map (net t)).sum = ((l.map (scalar t)).sum : Int) - ((l.map (compiled t)).sum : Int) := by
  intro l
  induction l with
  | nil => rfl
  | cons b bs ih =>
      rw [List.map_cons, List.map_cons, List.map_cons,
        List.sum_cons, List.sum_cons, List.sum_cons, ih]
      simp only [net]
      omega

private theorem sum_flatten_replicate (n : Nat) (xs : List Batch) (g : Batch → Nat) :
    ((List.replicate n xs).flatten.map g).sum = n * (xs.map g).sum := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.replicate_succ, List.flatten_cons, List.map_append,
        List.sum_append, ih, Nat.succ_mul]
      exact Nat.add_comm _ _

theorem sound : CompiledSuffixDuplication.Goal := by
  intro t xs hsum
  rcases xs with _ | ⟨b, bs⟩
  · exact absurd hsum (by simp)
  · refine ⟨t.compile + 1, ?_⟩
    show bill t ((List.replicate (t.compile + 1) (b :: bs)).flatten) 0 <
      bill t ((List.replicate (t.compile + 1) (b :: bs)).flatten)
        ((List.replicate (t.compile + 1) (b :: bs)).flatten).length
    have hb : 0 < ((List.replicate (t.compile + 1) (b :: bs)).flatten).length := by
      rw [List.replicate_succ, List.flatten_cons]
      simp
    rw [sum_net t (b :: bs)] at hsum
    have hCS : ((b :: bs).map (compiled t)).sum < ((b :: bs).map (scalar t)).sum := by
      rcases Nat.lt_trichotomy _ _ with h | h | h
      · exact h
      · rw [h] at hsum
        omega
      · omega
    obtain ⟨e, he⟩ := Nat.exists_eq_add_of_lt hCS
    unfold bill
    rw [if_pos hb, if_neg (Nat.lt_irrefl _), List.take_zero, List.drop_zero,
      List.take_length, List.drop_length]
    simp only [List.map_nil, List.sum_nil, Nat.add_zero, Nat.zero_add]
    rw [sum_flatten_replicate (t.compile + 1) (b :: bs) (scalar t),
        sum_flatten_replicate (t.compile + 1) (b :: bs) (compiled t)]
    rw [he, Nat.mul_add, Nat.mul_add, Nat.mul_one]
    omega

end TinylambOpt.ResearchProofs.JitKernels.TierInvestment_CompiledSuffixDuplication
