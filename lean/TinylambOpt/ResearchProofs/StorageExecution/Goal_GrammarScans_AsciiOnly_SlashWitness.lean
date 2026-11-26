import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Refuted by counterexample

- `ContainsSlash.SlashWitness` — `∀ x, matched reader x 0 = (expand x).contains 47`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_AsciiOnly_SlashWitness

open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.AsciiOnly
open TinylambOpt.ResearchGoals.Compression.Wire

/-!
`AsciiOnly.SlashWitness.Goal` is **not** declared in
`TinylambOpt.ResearchGoals.StorageExecution.GrammarScans`: the only `SlashWitness`
goal in that module lives in `ContainsSlash`. The statement below is the natural
AsciiOnly analogue, obtained by instantiating the same `matched ... = contains 47`
shape with `AsciiOnly.reader`.

This analogue is false. `AsciiOnly.reader` starts in the accepting state 0 and
only leaves it after a byte >= 128; byte 47 is ASCII and therefore leaves the
state unchanged. Hence every all-ASCII rope ending in state 0 is accepted,
regardless of whether it contains a slash. The empty rope is the smallest
counterexample: `matched reader (.lit []) 0 = true` but `[].contains 47 = false`.
-/
private def SlashWitness.Goal : Prop :=
  ∀ x, matched reader x 0 = (expand x).contains 47

/-! ## Small-input truth checks -/

#eval matched reader (.lit ([] : Bytes)) 0
#eval (expand (.lit ([] : Bytes))).contains 47
#eval matched reader (.lit [47]) 0
#eval (expand (.lit [47])).contains 47
#eval matched reader (.lit [128]) 0
#eval (expand (.lit [128])).contains 47
#eval matched reader (.rep 3 (.lit [65])) 0
#eval (expand (.rep 3 (.lit [65]))).contains 47

-- The claimed equality already fails on the empty literal.
example : matched reader (.lit ([] : Bytes)) 0 ≠ (expand (.lit ([] : Bytes))).contains 47 := by
  native_decide

theorem disproof_SlashWitness : ¬ SlashWitness.Goal := by
  intro h
  exact (by
    native_decide :
    matched reader (.lit ([] : Bytes)) 0 ≠ (expand (.lit ([] : Bytes))).contains 47) (h (.lit []))

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_AsciiOnly_SlashWitness
