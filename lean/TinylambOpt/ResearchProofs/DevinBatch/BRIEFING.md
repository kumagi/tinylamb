# DevinBatch: shared rules for proof agents (wave: Atlas/Compression/Frontiers/old-singles)

Working directory: `/home/kumagi/develop/tinylamb/lean`
Lean toolchain: `~/.elan/bin/lean`, `~/.elan/bin/lake` (Lean v4.36.0).
Goal modules live under `TinylambOpt/ResearchGoals/...`.

## Your assignment
Each agent owns exactly ONE output file under `TinylambOpt/ResearchProofs/DevinBatch/`.
**Create/edit only that file.** Never modify goal modules, `TinylambOpt.lean`, docs,
`issues/`, or any other agent's file. Do not touch `.lake/`.

## Output file template

```lean
import TinylambOpt.ResearchGoals.<Area>.<Module>   -- your assigned goal module(s)

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DevinBatch.<YourFileName>

open TinylambOpt.ResearchGoals.<Area>.<Module>

/-! Per-goal notes: why true (proof idea) or why false (concrete witness). -/

theorem <Leaf>_sound :
    TinylambOpt.ResearchGoals.<Area>.<Module>.<...>.<Leaf>.Goal := by
  ...

theorem <Leaf>_disproof :
    ¬ TinylambOpt.ResearchGoals.<Area>.<Module>.<...>.<Leaf>.Goal := by
  intro h
  -- instantiate h with explicit concrete witnesses; `decide`/`native_decide` the contradiction
  ...

end TinylambOpt.ResearchProofs.DevinBatch.<YourFileName>
```

Naming rule: for goal `<path>.<Leaf>.Goal`, emit `theorem <Leaf>_sound : <full goal name>`
or `theorem <Leaf>_disproof : ¬ <full goal name>` at top level of your namespace
(no `private`). If two goals in your assignment share a leaf name, disambiguate the
theorem name (e.g. `NsName_Leaf_sound`) — the STATEMENT must still be the exact goal.

## Hard rules
- **Never** use `sorry`, `admit`, `axiom`, `unsafe`, or weaken/strengthen statements.
- For a refutation give an explicit concrete witness and a fully checked contradiction
  (`decide`, `native_decide`, `omega`, or explicit term). `#guard`/`#eval` checks of the
  witness are encouraged.
- If a goal resists within your effort budget, **omit it** and report `UNRESOLVED(reason)`.
  Do not leave holes or fake proofs.
- **Check for prior art first**: `grep -rn "<Leaf>.Goal\|<Leaf>_sound\|<Leaf>_disproof"
  TinylambOpt/ResearchProofs/` — if a covering theorem already exists in ANY proof file,
  report `ALREADY_PROVED` and skip it.
- **Known-false goals**: `issues/research/atlas.md`, `issues/research/frontiers.md`,
  `issues/research/compression.md` document concrete counterexamples for some goals
  (e.g. atlas.md: StrictOverlap.PageVerdict, PeakCount.DuplicateLoss,
  PredicateOrder.MarginalTie, RiskEnvelope.MeanReserve, Versions.DropTombstones,
  BatchWatermarks.FinalSummary, Grammar.InteriorCache, Exchanges.GlobalPacking,
  Couplings.AlignedMarginals, PageMoments.MomentHistogram; frontiers.md: QuotientCycles,
  FanoutOnly, ThreeFromTwo, MeanHoleBudget, TwoWinners, ComplementaryViews, Unseparated,
  PairStats, LocalRounding, NegatedConsumer, PackingExact, AdjacentRuns). If your goal is
  one of these, it needs `disproof`, not `sound`.
- Verify with: `cd /home/kumagi/develop/tinylamb/lean && ~/.elan/bin/lake env lean
  -DrelaxedAutoImplicit=false TinylambOpt/ResearchProofs/DevinBatch/<YourFile>.lean`
  Iterate until exit 0. **Do not** run `lake build` — parallel builds clash.
- Keep proofs deterministic; prefer `decide`/`native_decide` on finite goals, structural
  induction, `simp`, `omega`. Avoid minutes-long `decide` on large instances.

## Final report format
One line per assigned goal:
`<GOAL-FULL-NAME> PROVED | REFUTED(witness) | ALREADY_PROVED | UNRESOLVED(reason)`.
State the output path and whether `lake env lean` succeeded.
