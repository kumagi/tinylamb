# New-goal batch: shared rules for proof agents

Working directory: `/home/kumagi/develop/tinylamb/lean`
Lean toolchain: `~/.elan/bin/lean`, `~/.elan/bin/lake` (Lean v4.36.0).
Goal modules live under `TinylambOpt/ResearchGoals/{Frontiers,Compression}/`.

## Your assignment
Each agent owns exactly one output file under `TinylambOpt/ResearchProofs/NewGoalsBatch/`.
**Create/edit only that file.** Never modify goal modules, `TinylambOpt.lean`, docs, or
any other agent's file. Do not touch `.lake/`.

## Output file template

```lean
import TinylambOpt.ResearchGoals.Frontiers.Arithmetic   -- your assigned goal module

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.NewGoalsBatch.A01   -- your namespace

open TinylambOpt.ResearchGoals.Frontiers.Arithmetic

/-! Per-goal notes: why true (proof idea) or why false (concrete witness). -/

theorem SignedCRT_sound :
    TinylambOpt.ResearchGoals.Frontiers.Arithmetic.SignedCRT.Goal := by
  ...

theorem ZeroJoin_disproof :
    ¬ TinylambOpt.ResearchGoals.Frontiers.Arithmetic.ZeroJoin.Goal := by
  intro h
  -- instantiate h with explicit concrete witnesses; `decide` / `native_decide` the contradiction
  ...

end TinylambOpt.ResearchProofs.NewGoalsBatch.A01
```

Naming rule: for a goal `Name.Goal` in module `M`, emit
`theorem Name_sound : M.Name.Goal := ...` when you prove it, or
`theorem Name_disproof : ¬ M.Name.Goal := ...` when you refute it. The theorem
name must be at top level in your namespace (no `private`).

## Hard rules
- **Never** use `sorry`, `admit`, `axiom`, `unsafe`, `by_contra!` on a false goal,
  or weaken/strengthen the statement. No `set_option autoImplicit` changes.
- For a refutation you must give an explicit concrete witness (list/nat/int/matrix)
  and a fully checked contradiction, e.g. `exact h ... (by decide)` or `nomatch`.
  `#guard` checks of the witness are encouraged.
- If a goal resists within your effort budget, **omit it entirely** and report it as
  `UNRESOLVED` with the obstruction. Do not leave holes or fake proofs.
- Check your file with:
  `cd /home/kumagi/develop/tinylamb/lean && ~/.elan/bin/lake env lean TinylambOpt/ResearchProofs/NewGoalsBatch/A01.lean`
  (replace with your filename). Do **not** run `lake build` — parallel builds clash.
  Iterate until Lean reports no errors.
- Keep proofs reasonably short and deterministic; avoid `omega` on huge goals if
  it takes minutes, prefer structural induction / `simp` / `decide` on concrete data.

## Final report format
Return a compact list, one line per assigned goal:
`<GOAL> PROVED | REFUTED(witness) | UNRESOLVED(reason)`.
Also state the exact output path and whether `lake env lean <file>` succeeded.
