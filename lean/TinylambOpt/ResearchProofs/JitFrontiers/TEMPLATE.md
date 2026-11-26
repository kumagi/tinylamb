# JitFrontiers proof-writing conventions (read this first)

Workspace: `/home/kumagi/develop/tinylamb/lean`. Lean 4.36 at
`~/.elan/bin` — every command must start with `export PATH="$HOME/.elan/bin:$PATH"`.

## Goal

Each of the 10 domain modules under `TinylambOpt/ResearchGoals/JitFrontiers/`
defines 100 unproven `def …Goal : Prop` (10 models × 10 propositions). Your job
is to prove or refute as many as possible, in Lean, with no `sorry`/`admit`/
new axioms. For each Goal you must register exactly one theorem:

- proof: `theorem <name> : <fully.qualified.Goal> := by …`
- refutation: `theorem <name> : ¬ <fully.qualified.Goal> := by …`

## File layout (STRICT — other agents own the other directories)

Work ONLY in `TinylambOpt/ResearchProofs/JitFrontiers/<YourDomain>/`.
Create one file per Goal named `Goal_<Model>_<Prop>.lean`, e.g.
`TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedAdd_GuardExact.lean`.

Each file should look like:

```lean
import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_SignedAdd_GuardExact

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises
open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises.SignedAdd

theorem sound : SignedAdd.GuardExact.Goal := by
  ...

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_SignedAdd_GuardExact
```

For a refutation use `theorem refute : ¬ SignedAdd.Unguarded.Goal := by …` and
give an explicit counterexample input.

Add every created file as an `import` line to
`TinylambOpt/ResearchProofs/JitFrontiers/<YourDomain>_All.lean` (that file is
yours alone; do NOT edit `AllGoalProofs.lean`, `TinylambOpt.lean`, or any
other domain's files).

## Verification (concurrency-safe)

Verify each proof file **individually** with

```
cd /home/kumagi/develop/tinylamb/lean
export PATH="$HOME/.elan/bin:$PATH"
lake env lean TinylambOpt/ResearchProofs/JitFrontiers/<YourDomain>/Goal_<...>.lean
```

Do NOT run `lake build` (parallel agents share the build dir). No output means
success. Iterate until the file elaborates. Use `set_option maxHeartbeats 0`.

## Tactics / facts that work here

- `bv_decide` proves pure BitVec identities (including 64-bit ones).
- `native_decide` proves decidable propositions over small finite domains.
- `omega`, `simp`, `aesop`, `grind` for Nat/Int/list algebra.
- `by_contra` + explicit counterexample for false strong hypotheses.
- Existing useful lemmas: `BitVec.saddOverflow`, `BitVec.saddOverflow_eq`,
  `BitVec.uaddOverflow`, `BitVec.ssubOverflow`, `BitVec.getLsbD_and/or/xor/not`,
  `BitVec.getLsbD_shiftLeft`, `Bool.or_comm`. See
  `TinylambOpt/ResearchProofs/CheckedLaneAdd.lean` for a worked example relating
  an msb overflow formula to `BitVec.saddOverflow` and `toInt` bounds.
- Several generic contracts follow from `guardExact` (or the domain analogue):
  `dropFlag`, `speculateSelect`, `firstPoison`, `hoistAcrossPhi`,
  `freezeInvariant`, `deadInputs`. Prove the generic implication once in a
  `Common_<YourDomain>.lean` file and instantiate it per model instead of
  reproving it ten times.
- The deliberately strong hypotheses (e.g. `*Unguarded*`, `Substitute`,
  `EarlyAdvance`, `ChunkLocal`) are usually FALSE — refute them with the
  counterexample described in `issues/research/jit-frontiers.md`.

## Report

When done, report: number of goals proven, number refuted, list of remaining
uncovered goals, and any new reusable lemma files. Keep going until time runs
out; partial results are valuable.
