import TinylambOpt.ResearchGoals.JitKernels.TierInvestment

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Soundness of "moving compilation by one batch depends only on that batch": when both cut
positions still compile (`i + 1 < xs.length`), the two schedules `bill t xs i` and
`bill t xs (i + 1)` differ only by whether batch `xs[i]` is scalar-executed or
compiled-executed, so their difference is exactly `net t b` for that batch — a
locality/incremental-replanning identity. A planner can shift a compile point by one
batch using only the marginal value of the batch moved, without re-evaluating the trace.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.TierInvestment_AdjacentTierCutDifference
open TinylambOpt.ResearchGoals.JitKernels.TierInvestment

private theorem sum_scalar_take_drop (t : Tariff) : ∀ (xs : List Batch) (k : Nat),
    ((xs.take k).map (scalar t)).sum + ((xs.drop k).map (scalar t)).sum = (xs.map (scalar t)).sum := by
  intro xs
  induction xs with
  | nil => intro k; cases k <;> rfl
  | cons a as ih =>
      intro k
      cases k with
      | zero => simp
      | succ k =>
          have h := ih k
          rw [List.take_succ_cons, List.drop_succ_cons]
          simp only [List.map_cons, List.sum_cons]
          omega

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

private theorem bill_eq (t : Tariff) : ∀ (xs : List Batch) (k : Nat), k < xs.length →
    (bill t xs k : Int) = ((xs.map (scalar t)).sum : Int) + (t.compile : Int)
      - ((xs.drop k).map (net t)).sum := by
  intro xs k hk
  have h1 := sum_scalar_take_drop t xs k
  have h2 := sum_net t (xs.drop k)
  unfold bill
  rw [if_pos hk, h2]
  omega

private theorem drop_cons_eq : ∀ (xs : List Batch) (i : Nat) (b : Batch),
    xs[i]? = some b → xs.drop i = b :: xs.drop (i + 1) := by
  intro xs
  induction xs with
  | nil => intro i b h; cases i <;> cases h
  | cons a as ih =>
      intro i b h
      cases i with
      | zero =>
          rw [List.getElem?_cons_zero] at h
          obtain rfl : a = b := Option.some.inj h
          simp
      | succ i =>
          rw [List.getElem?_cons_succ] at h
          have hi := ih i b h
          rw [List.drop_succ_cons, List.drop_succ_cons]
          exact hi

theorem sound : AdjacentTierCutDifference.Goal := by
  intro t xs i b hlt hb
  have h1 := bill_eq t xs i (by omega : i < xs.length)
  have h2 := bill_eq t xs (i + 1) hlt
  have hd : xs.drop i = b :: xs.drop (i + 1) := drop_cons_eq xs i b hb
  have hg : ((xs.drop i).map (net t)).sum = (net t b) + ((xs.drop (i + 1)).map (net t)).sum := by
    rw [hd, List.map_cons, List.sum_cons]
  rw [h1, h2, hg]
  omega

end TinylambOpt.ResearchProofs.JitKernels.TierInvestment_AdjacentTierCutDifference
