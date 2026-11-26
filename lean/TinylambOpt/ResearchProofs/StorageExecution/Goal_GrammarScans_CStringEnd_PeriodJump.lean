import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
import TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_CStringEnd_RepeatedState

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `CStringEnd.PeriodJump` — A cycle in a run of identical phrases permits skipping
  whole periods.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_CStringEnd_PeriodJump

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.CStringEnd

/-! Small-input truth check; triples not satisfying the cycle hypothesis are
ignored (they are vacuous). -/

private def samples : List Rope :=
  [ .lit [], .lit [0], .lit [65], .lit [0, 0], .lit [65, 0, 66],
    .cat (.lit [65]) (.lit [66, 0]), .cat (.lit [0]) (.lit [65]),
    .rep 0 (.lit [0]), .rep 2 (.lit [65]), .rep 3 (.lit []),
    .rep 2 (.lit [65, 0]), .cat (.rep 2 (.lit [65, 66])) (.lit [0]) ]

private def checkOne (x : Rope) (q period n : Nat) : Bool :=
  if _hq : q < reader.states then
    if _hper : 0 < period then
      if applyT (power period (compile reader x)) q = q then
        decide (matched reader (.rep n x) q = matched reader (.rep (n % period) x) q)
      else true
    else true
  else true

private def check : Bool :=
  samples.all fun x =>
    (List.range reader.states).all fun q =>
      [1, 2, 3, 4].all fun period =>
        (List.range 8).all fun n => checkOne x q period n

#eval check

example : check := by native_decide

/-! Splitting a repeated byte string into a prefix of `a` copies and a suffix
of `b` copies, then folding, composes the two folds in that order. -/

private theorem foldl_flatten_replicate_add {α : Type} (f : α → Nat → α)
    (a b : Nat) (s : List Nat) (q : α) :
    ((List.replicate (a + b) s).flatten).foldl f q =
      ((List.replicate b s).flatten).foldl f (((List.replicate a s).flatten).foldl f q) := by
  induction a generalizing q with
  | zero => simp
  | succ a ih => simp [Nat.succ_add, List.replicate_succ, ih]

theorem sound_PeriodJump : CStringEnd.PeriodJump.Goal := by
  intro x q period n hq hperiod hfix
  -- `sound_RepeatedState` identifies `power k (compile reader x)` with folding
  -- `k` copies of the phrase; write that fold as `F k q`.
  let F : Nat → Nat → Nat := fun k s => ((List.replicate k (expand x)).flatten).foldl reader.step s
  have hpow : ∀ k : Nat, applyT (power k (compile reader x)) q = F k q := by
    intro k
    exact TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_CStringEnd_RepeatedState.sound_RepeatedState x k q hq
  have hfixF : F period q = q := by
    rw [← hpow period]
    exact hfix
  have hcycle : ∀ k : Nat, F (k * period) q = q := by
    intro k
    induction k with
    | zero => simp [F]
    | succ k ih =>
        rw [Nat.succ_mul]
        change ((List.replicate (k * period + period) (expand x)).flatten).foldl reader.step q = q
        rw [foldl_flatten_replicate_add reader.step]
        change F period (F (k * period) q) = q
        rw [ih, hfixF]
  let k := n / period
  let rem := n % period
  have hn : k * period + rem = n := by
    dsimp [k, rem]
    rw [Nat.mul_comm (n / period) period]
    exact Nat.div_add_mod n period
  have hFn : F n q = F rem q := by
    rw [← hn]
    change ((List.replicate (k * period + rem) (expand x)).flatten).foldl reader.step q =
      ((List.replicate rem (expand x)).flatten).foldl reader.step q
    rw [foldl_flatten_replicate_add reader.step]
    change F rem (F (k * period) q) = F rem q
    rw [hcycle]
  unfold matched
  change reader.accept (applyT (power n (compile reader x)) q) =
    reader.accept (applyT (power (n % period) (compile reader x)) q)
  rw [hpow n, hpow (n % period)]
  simpa [rem] using congrArg reader.accept hFn

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_CStringEnd_PeriodJump
